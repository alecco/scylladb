/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "server.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace raft {

server::server(server_id uuid, std::unique_ptr<rpc> rpc, std::unique_ptr<state_machine> state_machine,
        std::unique_ptr<storage> storage) :
                    _rpc(std::move(rpc)), _state_machine(std::move(state_machine)), _storage(std::move(storage)),
                    _id(uuid) {
    _rpc->set_server(*this);
}

future<> server::start() {
    _fsm = fsm(_id, co_await _storage->load_term(), co_await _storage->load_vote(), co_await _storage->load_log());
    _fsm.set_configuration(_config);
    assert(_fsm._current_term != term_t(0));

    // start fiber to persist entries added to in-memory log
    _log_status = log_fiber();
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    _ticker.arm_periodic(100ms);
    _ticker.set_callback([this] {
        _fsm.tick();
    });

    co_return;
}

future<> server::add_entry(command command) {

    logger.trace("An entry is submitted on a leader");

    // lock access to the raft log while it is been updated

    // @todo: ensure the reference to the entry is stable between
    // yields, before removing _log_lock.
    const log_entry& e = _fsm.add_entry(std::move(command));

    // This will track the commit status of the entry
    auto [it, inserted] = _awaited_commits.emplace(e.idx, commit_status{e.term, promise<>()});
    assert(inserted);
    return it->second.committed.get_future();
}

void server::append_entries(server_id from, append_request_recv append_request) {
    _fsm.step(from, std::move(append_request));
}

void server::append_entries_reply(server_id from, append_reply reply) {
    _fsm.step(from, std::move(reply));
}

void server::request_vote(server_id from, vote_request vote_request) {
    _fsm.step(from, std::move(vote_request));
}

void server::request_vote_reply(server_id from, vote_reply vote_reply) {
    _fsm.step(from, std::move(vote_reply));
}

void server::commit_entries(index_t commit_idx) {
    while (_awaited_commits.size() != 0) {
        auto it = _awaited_commits.begin();
        if (it->first > commit_idx) {
            break;
        }
        auto [entry_idx, status] = std::move(*it);

        _awaited_commits.erase(it);
        if (status.term == _fsm._log[entry_idx].term) {
            status.committed.set_value();
        } else {
            // term does not match which means that between the entry was submitted
            // and committed there was a leadership change and the entry was replaced.
            status.committed.set_exception(dropped_entry());
        }
    }
}

future<> server::log_fiber() {
    logger.trace("log_fiber start");
    try {
        index_t last_stable = _fsm._log.stable_idx();
        while (true) {
            auto batch = co_await _fsm.log_entries();

            if (batch.commit_idx) {
                commit_entries(*batch.commit_idx);
            }

            if (batch.term) {
                // this resets voted_for in persistent storage as well
                co_await _storage->store_term(*batch.term);
            }

            if (batch.vote) {
                co_await _storage->store_vote(*batch.vote);
            }

            if (batch.log_entries.size()) {
                auto& entries = batch.log_entries;

                if (last_stable > entries[0].idx) {
                    // We should never truncate committed entries.
                    assert(_fsm._commit_idx < entries[0].idx);
                    co_await _storage->truncate_log(entries[0].idx);
                }

                // Combine saving and truncating into one call?
                // will require storage to keep track of last idx
                co_await _storage->store_log_entries(entries);

                _fsm.stable_to(entries.crbegin()->term, entries.crbegin()->idx);

                last_stable = entries.crbegin()->idx;
            }
            if (batch.messages.size()) {
                // after entries are persisted we can send messages
                co_await seastar::parallel_for_each(std::move(batch.messages), [this] (std::pair<server_id, rpc_message>& message) {
                    return std::visit([this, id = message.first] (auto&& m) {
                        using T = std::decay_t<decltype(m)>;
                        if constexpr (std::is_same_v<T, append_reply>) {
                            return _rpc->send_append_entries_reply(id, m);
                        } else if constexpr (std::is_same_v<T, keep_alive>) {
                            _rpc->send_keepalive(id, m);
                            return make_ready_future<>();
                        } else if constexpr (std::is_same_v<T, append_request_send>) {
                            return _rpc->send_append_entries(id, m);
                        } else if constexpr (std::is_same_v<T, vote_request>) {
                            return _rpc->send_vote_request(id, m);
                        } else if constexpr (std::is_same_v<T, vote_reply>) {
                            return _rpc->send_vote_reply(id, m);
                        }

                        logger.error("log fiber {} tried to send unknown message type", _fsm._my_id);
                        return make_ready_future<>();
                    }, std::move(message.second));

                });
            }
        }
    } catch (seastar::broken_condition_variable&) {
        // log fiber is stopped explicitly.
    } catch (...) {
        logger.error("log fiber {} stopped because of the error: {}", _fsm._my_id, std::current_exception());
    }
    co_return;
}

future<> server::applier_fiber() {
    logger.trace("applier_fiber start");
    try {
        while (true) {
            auto batch = co_await _fsm.apply_entries();
            co_await _state_machine->apply(std::move(batch));
        }
    } catch (seastar::broken_condition_variable&) {
        // applier fiber is stopped explicitly.
    } catch (...) {
        logger.error("applier fiber {} stopped because of the error: {}", _fsm._my_id, std::current_exception());
    }
    co_return;
}

future<> server::stop() {
    logger.trace("stop() called");
    _fsm.stop();
    for (auto& ac: _awaited_commits) {
        ac.second.committed.set_exception(stopped_error());
    }
    _awaited_commits.clear();
    _ticker.cancel();

    return seastar::when_all_succeed(std::move(_log_status), std::move(_applier_status),
            _rpc->stop(), _state_machine->stop(), _storage->stop()).discard_result();
}

void server::make_me_leader() {
    _fsm.become_leader();
}

} // end of namespace raft

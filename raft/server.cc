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

server::server(
    fsm fsm, std::unique_ptr<rpc> rpc, std::unique_ptr<state_machine> state_machine,
    std::unique_ptr<storage> storage) :
            _rpc(std::move(rpc)), _state_machine(std::move(state_machine)), _storage(std::move(storage)),
            _fsm(std::move(fsm)) {
    _rpc->set_server(*this);
}

future<> server::start() {
    assert(_fsm._current_term != term_t(0));

    // start fiber to persist entries added to in-memory log
    _log_status = log_fiber();
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    _ticker.arm_periodic(100ms);
    _ticker.set_callback([this] {
        _fsm.tick();
    });

    return make_ready_future<>();
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

void server::append_entries_reply(server_id from, append_reply&& reply) {
    if (_fsm.append_entries_reply(from , reply)) {
        commit_entries();
    }
}

void server::commit_entries() {
    logger.trace("commit_entries {}: signal apply thread: committed: {} applied: {}", _fsm._my_id,
        _fsm._commit_idx, _fsm._last_applied);
    _apply_entries.signal();
    while (_awaited_commits.size() != 0) {
        auto it = _awaited_commits.begin();
        if (it->first > _fsm._commit_idx) {
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

void server::append_entries(server_id from, append_request_recv append_request) {
    if (_fsm.append_entries(from, append_request)) {
        commit_entries();
    }
}

future<> server::log_fiber() {
    logger.trace("log_fiber start");
    try {
        index_t last_stable = _fsm._log.stable_idx();
        while (true) {
            auto batch = _fsm.log_entries();

            if (!batch) {
                co_await _fsm._sm_events.wait();
                continue;
            }

            if (batch->term) {
                // this resets voted_for in persistent storage as well
                co_await _storage->store_term(*batch->term);
            }

            if (batch->vote) {
                co_await _storage->store_vote(*batch->vote);
            }

            if (batch->log_entries.size()) {
                if (last_stable > batch->log_entries[0].idx) {
                    // We should never truncate committed entries.
                    assert(_fsm._commit_idx < batch->log_entries[0].idx);
                    co_await _storage->truncate_log(batch->log_entries[0].idx);
                }

                // Combine saving and truncating into one call?
                // will require storage to keep track of last idx
                co_await _storage->store_log_entries(batch->log_entries);

                _fsm.stable_to(batch->log_entries.crbegin()->term, batch->log_entries.crbegin()->idx);
                // make apply fiber to re-check if anything should be applied
                _apply_entries.signal();

                if (_fsm._current_config.servers.size() == 1) { // special case for one node cluster
                    if (_fsm.check_committed()) {
                        commit_entries();
                    }
                }

                last_stable = batch->log_entries.crbegin()->idx;
            }
            if (batch->messages.size()) {
                // after entries are persisted we can send messages
                co_await seastar::parallel_for_each(std::move(batch->messages), [this] (std::pair<server_id, rpc_message>& message) {
                    return std::visit([this, id = message.first] (auto&& m) {
                        using T = std::decay_t<decltype(m)>;
                        if constexpr (std::is_same_v<T, append_reply>) {
                            return _rpc->send_append_entries_reply(id, std::move(m));
                        } else if constexpr (std::is_same_v<T, keep_alive>) {
                            _rpc->send_keepalive(id, std::move(m));
                            return make_ready_future<>();
                        } else if constexpr (std::is_same_v<T, append_request_send>) {
                            return _rpc->send_append_entries(id, std::move(m));
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
            std::optional<apply_batch> batch = _fsm.apply_entries();

            if (!batch) {
                co_await _apply_entries.wait();
                continue;
            }

            logger.trace("applier_fiber {} applying up to {}", _fsm._my_id, batch->idx);
            co_await _state_machine->apply(std::move(batch->commands));
            _fsm.applied_to(batch->idx);
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
    if (_fsm.is_leader()) {
        _fsm.become_follower(server_id{});
    }
    _fsm._sm_events.broken();
    _apply_entries.broken();
    for (auto& ac: _awaited_commits) {
        ac.second.committed.set_exception(stopped_error());
    }
    _awaited_commits.clear();
    _ticker.cancel();

    return seastar::when_all_succeed(std::move(_leadership_transition),
            std::move(_log_status), std::move(_applier_status),
            _rpc->stop(), _state_machine->stop(), _storage->stop()).discard_result();
}

void server::make_me_leader() {
    _fsm.become_leader();
}

} // end of namespace raft

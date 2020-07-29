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
    // start fiber to persist entries added to in-memory log
    _log_status = log_fiber();
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    co_return;
}

future<> server::add_entry(command command) {

    logger.trace("An entry is submitted on a leader");

    // lock access to the raft log while it is been updated

    // @todo: ensure the reference to the entry is stable between
    // yields, before removing _log_lock.
    const log_entry& e = _fsm.add_entry(std::move(command));

    _log_entries.broadcast();

    // This will track the commit status of the entry
    auto [it, inserted] = _awaited_commits.emplace(e.idx, commit_status{e.term, promise<>()});
    assert(inserted);
    return it->second.committed.get_future();
}

future<> server::replication_fiber(server_id server, follower_progress& state) {
    while (_fsm.is_leader()) {
        while (_fsm._log.empty() || state.next_idx > _fsm._log.stable_idx()) {
            // everything is replicated already, wait for the next entry to be added
            try {
                co_await _leader_state->_log_entry_added.wait();
            } catch (...) {
                co_return;
            }
        }
        assert(!_fsm._log.empty() && state.next_idx <= _fsm._log.stable_idx());
        const log_entry& entry = _fsm._log[state.next_idx];
        index_t prev_idx = index_t(0);
        term_t prev_term = _fsm._current_term;
        if (state.next_idx != 1) {
            prev_idx = index_t(state.next_idx - 1);
            prev_term = _fsm._log[state.next_idx - 1].term;
        }

        append_request_send req = {{
                .current_term = _fsm._current_term,
                .leader_id = _fsm._my_id,
                .prev_log_idx = prev_idx,
                .prev_log_term = prev_term,
                .leader_commit_idx = _fsm._commit_idx
            },
            // TODO: send only one entry for now, but we should batch in the future
            std::vector<log_entry_cref>(1, std::cref(entry))
        };

        logger.trace("replication_fiber[{}->{}]: send entry idx={}, term={}",
            _fsm._my_id, server, entry.idx, entry.term);

        // optimistically update next send index. In case a message is lost
        // there will be negative reply that will resend idx
        state.next_idx++;
        try {
            co_await _rpc->send_append_entries(server, req);
        } catch (...) {
            continue; // if there was an error sending try again
        }
    }
    co_return;
}

void server::append_entries_reply(server_id from, append_reply&& reply) {
    if (!_fsm.is_leader() || reply.current_term < _fsm._current_term) {
        // drop stray reply if we are no longer a leader or the term is too old
        return;
    }

    if (reply.current_term > _fsm._current_term) {
        // receiver knows something about newer leader, so this server has to convert to a follower
        _fsm.become_follower(server_id{});
        _leadership_transition = stop_leadership();
        return;
    }

    follower_progress& progress = (*_fsm._progress)[from];

    if (std::holds_alternative<append_reply::accepted>(reply.result)) {
        // accepted
        // a follower may have longer log, so cap it with our own log length
        index_t last_idx = std::min(std::get<append_reply::accepted>(reply.result).last_log_index, _fsm._log.last_idx());

        logger.trace("append_entries_reply[{}->{}]: accepted match={} last index={}", _fsm._my_id, from, progress.match_idx, last_idx);

        progress.match_idx = std::max(progress.match_idx, last_idx);

        // check if any new entry can be committed
        if (_fsm.check_committed()) {
            commit_entries();
        }
    } else {
        // rejected
        append_reply::rejected rejected = std::get<append_reply::rejected>(reply.result);

        logger.trace("append_entries_reply[{}->{}]: rejected match={} index={} non matching term={}, idx for term={}", _fsm._my_id, from, progress.match_idx, rejected.index, rejected.non_matching_term, rejected.first_idx_for_non_matching_term);

        if (rejected.index <= progress.match_idx) {
            // if rejected index is smaller that matched it means this is a stray reply
            return;
        }

        index_t n = progress.next_idx;

        // skip all the entries from next_idx to first_idx_for_non_matching_term that do not have non_matching_term
        for (; n >= std::max(_fsm._log.start_idx(), rejected.first_idx_for_non_matching_term); n--) {
            if (_fsm._log[n].term == rejected.non_matching_term) {
                break;
            }
        }
        logger.trace("append_entries_reply[{}->{}]: n={}", _fsm._my_id, from, n);
        n++; // we found a matching entry, now move to the next one
        progress.next_idx = n;
        logger.trace("replication_fiber[{}->{}]: next_idx={}, match_idx={}", _fsm._my_id, from, progress.next_idx, progress.match_idx);
        assert(progress.next_idx != progress.match_idx); // we should not fail to apply an entry next after a matched one
        _leader_state->_log_entry_added.broadcast(); // signal next_idx change
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

future<> server::start_leadership() {
    assert(!_leader_state);

    _leadership_transition = make_ready_future<>(); // prepare to next transition

    _leader_state.emplace(); // recreate leader's state
    // start sending keepalives to maintain leadership
    _leader_state->keepalive_status = keepalive_fiber();

    for (auto& p : *(_fsm._progress)) {
        if (p.first != _fsm._my_id) {
            _leader_state->_replicatoin_fibers.emplace_back(replication_fiber(p.first, p.second));
        }
    }

    co_return;
}

future<> server::stop_leadership() {
    assert(_leadership_transition.available());
    _leader_state->_log_entry_added.broken();

    // FIXME: waiting for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95895 to be fixed
    // to wrote like that:
    // co_await seastar::when_all_succeed();
    // co_await std::move(_leader_state->keepalive_status);
    // _leader_state = std::nullopt;
    // co_return;
    co_return seastar::when_all_succeed(_leader_state->_replicatoin_fibers.begin(), _leader_state->_replicatoin_fibers.end())
          .finally([this] {
               return std::move(_leader_state->keepalive_status);
          }).finally([this] {
            _leader_state = std::nullopt;
          });
}

future<> server::append_entries(server_id from, append_request_recv append_request) {
    logger.trace("append_entries[{}] received ct={}, prev idx={} prev term={} commit idx={}, idx={}", _fsm._my_id,
            append_request.current_term, append_request.prev_log_idx, append_request.prev_log_term, append_request.leader_commit_idx,
            append_request.entries.size() ? append_request.entries[0].idx : index_t(0));
    if (append_request.current_term < _fsm._current_term) {
        send_append_reply(from, append_reply{_fsm._current_term, append_reply::rejected{append_request.prev_log_idx, term_t(0), index_t(0)}});
        _log_entries.broadcast(); // signal to log_fiber to send the reply
        co_return;
    }

    // Can it happen that a leader gets append request with the same term?
    // What should we do about it?
    assert(!_fsm.is_leader() || _fsm._current_term > append_request.current_term);

    if (!_fsm.is_follower()) {
        bool was_leader = _fsm.is_leader();
        _fsm.become_follower(server_id{});
        if (was_leader) {
            _leadership_transition = stop_leadership();
        }
    }

    if (_fsm._current_term < append_request.current_term) {
        _fsm.update_current_term(append_request.current_term);
        // this resets voted_for in persistent storage as well
        co_await _storage->store_term(append_request.current_term);
    }

    // TODO: need to handle keep alive management here

    // Ensure log matching property, even if we append no entries.
    // 3.5
    // Until the leader has discovered where it and the
    // follower’s logs match, the leader can send
    // AppendEntries with no entries (like heartbeats) to save
    // bandwidth.
    if (! _fsm._log.match_term(append_request.prev_log_idx, append_request.prev_log_term)) {
        auto [i, t] = _fsm._log.find_first_idx_of_term(append_request.prev_log_idx);
        logger.trace("append_entries[{}]: no matching term at position {}: expected {}, found {}, reject hint {}",
            _fsm._my_id, append_request.prev_log_idx, append_request.prev_log_term, t, i);
        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        send_append_reply(from, append_reply{_fsm._current_term, append_reply::rejected{append_request.prev_log_idx, t, i}});
        _log_entries.broadcast(); // signal to log_fiber to send the reply
        co_return;
    }

    if (_fsm._log.maybe_append(append_request.entries)) {
        send_append_reply(from, append_reply{_fsm._current_term, append_reply::accepted{_fsm._log.last_idx()}});
        _log_entries.broadcast(); // signal to log_fiber
    }

    if (_fsm.commit_to(append_request.leader_commit_idx)) {
        commit_entries();
    }

    co_return;
}

future<> server::log_fiber() {
    logger.trace("log_fiber start");
    try {
        index_t last_stable = _fsm._log.stable_idx();
        while (true) {
            auto batch = _fsm.log_entries();

            if (!batch) {
                co_await _log_entries.wait();
                continue;
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
                } else if (_fsm.is_leader()) {
                    _leader_state->_log_entry_added.broadcast();
                }
                last_stable = batch->log_entries.crbegin()->idx;
            }
            if (batch->append_replies.size()) {
                // after entries are persisted we can send replies
                co_await seastar::parallel_for_each(std::move(batch->append_replies), [this] (std::pair<server_id, append_reply>& reply) {
                    return _rpc->send_append_entries_reply(reply.first, std::move(reply.second));
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

future<> server::keepalive_fiber() {
    logger.trace("keepalive_fiber starts");
    while (_fsm.is_leader()) {
        co_await sleep(100ms);

        if (!_fsm.is_leader()) { // may have lost leadership while sleeping
            break;
        }

        keep_alive ka {
            .current_term = _fsm._current_term,
            .leader_id = _fsm._current_leader,
            .leader_commit_idx = _fsm._commit_idx,
        };

        for (auto server : _fsm._current_config.servers) {
            if (server.id != _fsm._my_id) {
                // cap committed index by math_idx otherwise a follower may commit unmatched entries
                ka.leader_commit_idx = std::min(_fsm._commit_idx, (*_fsm._progress)[server.id].match_idx);
                _rpc->send_keepalive(server.id, ka);
            }
        }
    }
    logger.trace("keepalive_fiber stops");
    co_return;
}

future<> server::stop() {
    logger.trace("stop() called");
    if (_fsm.is_leader()) {
        _fsm.become_follower(server_id{});
        _leadership_transition = stop_leadership();
    }
    _log_entries.broken();
    _apply_entries.broken();
    for (auto& ac: _awaited_commits) {
        ac.second.committed.set_exception(stopped_error());
    }
    _awaited_commits.clear();
    return seastar::when_all_succeed(std::move(_leadership_transition),
            std::move(_log_status), std::move(_applier_status),
            _rpc->stop(), _state_machine->stop(), _storage->stop()).discard_result();
}

future<> server::make_me_leader() {
    // wait for previous transition to complete, it is done async
    co_await std::move(_leadership_transition);

    _fsm.become_leader();

    co_return start_leadership();
}

} // end of namespace raft

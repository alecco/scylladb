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
#include <seastar/util/log.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace raft {

static seastar::logger logger("raft");

server::server(
    server_id id, std::unique_ptr<rpc> rpc, std::unique_ptr<state_machine> state_machine,
    std::unique_ptr<storage> storage) :
            _rpc(std::move(rpc)), _state_machine(std::move(state_machine)), _storage(std::move(storage)), _my_id(id) {
    _rpc->set_server(*this);
}

future<> server::start() {
    // start fiber to apply committed entries
    _applier_status = applier_fiber();

    _current_term = co_await _storage->load_term();
    _voted_for = co_await _storage->load_vote();
    _log = co_await _storage->load_log();

    logger.trace("{}: starting log length {}", _my_id, _log.last_idx());
    co_return;
}

future<> server::add_entry(command command) {
    if (!is_leader()) {
        throw not_leader(_current_leader);
    }

    logger.trace("An entry is submitted on a leader");

    // lock access to the raft log while it is been updated
    seastar::semaphore_units<> units = co_await _log.lock();

    if (!is_leader()) { // re-check in case leader changed while we were waiting for the lock
        throw not_leader(_current_leader);
    }

    logger.trace("Log lock acquired");

    _log.ensure_capacity(1); // ensure we have enough memory to insert an entry
    log_entry e{_current_term, _leader_state->_progress[_my_id].next_idx, std::move(command)};
    co_await _storage->store_log_entry(e);

    logger.trace("Log entry is persisted locally");

    // put into the log after persisting, so that if persisting fails the entry will not end up in a log
    _log.emplace_back(std::move(e));
    // update this server's state
    _leader_state->_progress[_my_id].match_idx = _leader_state->_progress[_my_id].next_idx++;

    // this will track the commit status of the entry
    auto [it, inserted] = _awaited_commits.emplace(e.index, commit_status{_current_term, promise<>()});
    assert(inserted);
    // take future here since check_committed() may delete the _awaited_commits entry
    future<> f = it->second.committed.get_future();
    if (_current_config.servers.size() == 1) { // special case for one node cluster
        check_committed();
    } else {
        _leader_state->_log_entry_added.broadcast();
    }
    co_return std::move(f);
}

future<> server::replication_fiber(server_id server, follower_progress& state) {
    while (is_leader()) {
        if (_log.empty() || state.next_idx > _log.last_idx()) {
            // everything is replicated already, wait for the next entry to be added
            try {
                co_await _leader_state->_log_entry_added.wait();
            } catch(...) {
                continue; // if waiting for cv failed continue
            }
        }
        assert(!_log.empty());
        const log_entry& entry = _log[state.next_idx];
        index_t prev_index = index_t(0);
        term_t prev_term = _current_term;
        if (state.next_idx != 1) {
            prev_index = index_t(state.next_idx - 1);
            prev_term = _log[state.next_idx - 1].term;
        }

        append_request_send req = {{
                .current_term = _current_term,
                .leader_id = _my_id,
                .prev_log_index = prev_index,
                .prev_log_term = prev_term,
                .leader_commit = _commit_index
            },
            // TODO: send only one entry for now, but we should batch in the future
            std::vector<log_entry_cref>(1, std::cref(entry))
        };

        append_reply reply;

        try {
            reply = co_await _rpc->send_append_entries(server, req);
        } catch(...) {
            continue; // if there was an error sending try again
        }

        if (!is_leader()) { // check that leader did not change while we were sending
            break;
        }

        if (reply.current_term > _current_term) {
            // receiver knows something about newer leader, so this server has to convert to a follower
            become_follower();
            break;
        }

        // we cannot have stale responses and if a follower had smaller term it should have updated itself
        assert(reply.current_term == _current_term);

        if (!reply.appended) {
            index_t n = state.next_idx;
            // skip all the entries from next_idx to first_idx_for_non_matching_term that do not have non_matching_term
            for (; n >= std::max(_log.start_index(), reply.first_idx_for_non_matching_term); n--) {
                if (_log[n].term == reply.non_matching_term) {
                    break;
                }
            }
            logger.trace("replication_fiber[{}->{}]: n={}", _my_id, server, n);
            n++; // we found a matching entry, now move to the next one
            state.next_idx = n;
            logger.trace("replication_fiber[{}->{}]: next_idx={}, match_idx={}", _my_id, server, state.next_idx, state.match_idx);
            assert(state.next_idx != state.match_idx); // we should not fail to apply an entry next after a matched one
        } else {
            // update follower's state
            state.match_idx = state.next_idx++;

            // check if any new entry can be committed
            check_committed();
        }
    }
    co_return;
}

void server::check_committed() {
    index_t commit_index = _commit_index;
    while (true) {
        size_t count = 0;
        for (const auto& p : _leader_state->_progress) {
            logger.trace("check committed {}: {} {}", p.first, p.second.match_idx, _commit_index);
            if (p.second.match_idx > _commit_index) {
                count++;
            }
        }
        logger.trace("check committed count {} quorum {}", count, quorum());
        if (count < quorum()) {
            break;
        }
        commit_index++;
        if (_log[commit_index].term != _current_term) {
            // Only entries from current term can be committed
            // based on vote counting, so if current log entry has
            // different term lets move to the next one in hope it
            // is committed already and has current term
            logger.trace("check committed: cannot commit because of term {} != {}", _log[commit_index].term, _current_term);
            continue;
        }

        logger.trace("check committed commit {}", commit_index);
        // we have quorum of servers with match_idx greater than current commit
        // it means we can commit next entry
        commit_entries(commit_index);
    }
}

void server::commit_entries(index_t new_commit_idx) {
    assert(_commit_index <= new_commit_idx);
    if (new_commit_idx == _commit_index) {
        return;
    }
    _commit_index = new_commit_idx;
    logger.trace("commit_entries {}: signal apply thread: committed: {} applied: {}", _my_id, _commit_index, _last_applied);
    _apply_entries.signal();
    while (_awaited_commits.size() != 0) {
        auto it = _awaited_commits.begin();
        if (it->first > _commit_index) {
            break;
        }
        auto [entry_idx, status] = std::move(*it);

        _awaited_commits.erase(it);
        if (status.term == _log[entry_idx].term) {
            status.committed.set_value();
        } else {
            // term does not match which means that between the entry was submitted
            // and committed there was a leadership change and the entry was replaced.
            status.committed.set_exception(dropped_entry());
        }
    }
}

future<> server::become_leader() {
    // wait for previous transition to complete, it is done async
    co_await std::move(_leadership_transition);

    assert(_state != server_state::LEADER);
    assert(!_leader_state);

    _state = server_state::LEADER;
    _current_leader = _my_id;
    _leadership_transition = make_ready_future<>(); // prepare to next transition

    _leader_state.emplace(); // recreate leader's state
    // start sending keepalives to maintain leadership
    _leader_state->keepalive_status = keepalive_fiber();

    for (auto s : _current_config.servers) {
        auto e = _leader_state->_progress.emplace(s.id, follower_progress{_log.next_idx(), index_t(0)});
        if (s.id != _my_id) {
            _leader_state->_replicatoin_fibers.emplace_back(replication_fiber(s.id, e.first->second));
        }
    }

    co_return;
}

future<> server::drop_leadership(server_state new_state) {
    assert(_state == server_state::LEADER);
    assert(new_state != server_state::LEADER);

    _state = new_state;
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

void server::become_follower() {
    if (_state == server_state::LEADER) {
        assert(_leadership_transition.available());
        _leadership_transition = drop_leadership(server_state::FOLLOWER);
    } else {
        _state = server_state::FOLLOWER;
    }
}

future<append_reply> server::append_entries(server_id from, append_request_recv&& append_request) {
    if (append_request.current_term < _current_term) {
        co_return append_reply{_current_term, false, term_t(0), index_t(0)};
    }

    // Can it happen that a leader gets append request with the same term?
    // What should we do about it?
    assert(_state != server_state::LEADER || _current_term > append_request.current_term);

    if (_state != server_state::FOLLOWER) {
        become_follower();
    }

    if (_current_term < append_request.current_term) {
        co_await set_current_term(append_request.current_term);
    }

    _current_leader = from;

    // TODO: need to handle keep alive management here

    if (append_request.entries.size()) { // empty request is just a heartbeat, only leader_commit is interesting
        logger.trace("append_entries[{}]: my log length {}, received prev_log_index {}\n", _my_id, _log.last_idx(), append_request.prev_log_index);
        if (append_request.prev_log_index != 0) {
            if (_log.last_idx() >= append_request.prev_log_index) {
                // the follower has prev_log_index, so we need to check that it matches
                const log_entry& entry = _log[append_request.prev_log_index];
                // we should really get rid of keeping the index in the entry, but for now check that it is correct
                assert(entry.index == append_request.prev_log_index);
                if (entry.term != append_request.prev_log_term) {
                    logger.trace("append_entries[{}]: no match", _my_id);
                    // search for a first entry in the log with non matching term
                    term_t t = _log[append_request.prev_log_index].term;
                    index_t i = append_request.prev_log_index;
                    while (i >= _log.start_index() && _log[i].term == _log[append_request.prev_log_index].term) {
                        i--;
                    }
                    i++; // point to first entry that contains term t
                    logger.trace("append_entries[{}]: reply with term {} index {}", _my_id, t, i);
                    // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                    co_return append_reply{_current_term, false, t, i};

                } else {
                    logger.trace("append_entries[{}]: match", _my_id);
                }
            } else {
                // leader's log is longer, the follower does not have an entry to check
                co_return append_reply{_current_term, false, term_t(0), _log.last_idx()};
            }
        }

        bool append = _log.last_idx() < append_request.entries[0].index;
        std::vector<log_entry> to_add;
        to_add.reserve(append_request.entries.size());

        for (auto& e : append_request.entries) {
            if (!append) {
                if (_log[e.index].term == e.term) {
                    logger.trace("append_entries[{}]: entries with index {} has matching terms {}", _my_id, e.index, e.term);
                    // already have this one, skip to the next;
                    continue;
                }
                logger.trace("append_entries[{}]: entries with index {} has non matching terms {} != {}", _my_id, e.index, e.term, _log[e.index].term);
                // If an existing entry conflicts with a new one (same index but different terms), delete the existing
                // entry and all that follow it (§5.3)
                co_await _storage->truncate_log(e.index);
                _log.truncate_head(e.index);
                append = true; // append after we truncated
            }
            to_add.emplace_back(std::move(e));
        }

        _log.ensure_capacity(to_add.size()); // ensure that we have enough memory before trying IO
        co_await _storage->store_log_entries(to_add);

        for (auto&& e : to_add) {
            // put into the log after persisting, so that if persisting fails the entry will not end up in a log
            _log.emplace_back(std::move(e));
        }
    }

    logger.trace("append_entries[{}]: leader_commit={}", _my_id, append_request.leader_commit);
    commit_entries(append_request.leader_commit);

    co_return append_reply{_current_term, true};
}

future<> server::set_current_term(term_t term) {
    if (_current_term < term) {
        co_await _storage->store_term(term); // this resets voted_for in persistent storage as well
        _current_term = term;
        _voted_for = std::nullopt;
    }
    co_return;
}

future<> server::applier_fiber() {
    logger.trace("applier_fiber start");
    try {
        while(true) {
            co_await _apply_entries.wait([this] { return _commit_index > _last_applied && _log.last_idx() > _last_applied; });
            logger.trace("applier_fiber {} commit index: {} last applied: {}", _my_id, _commit_index, _last_applied);
            std::vector<command_cref> commands;
            commands.reserve(_commit_index - _last_applied);
            auto last_applied = _last_applied;
            while(last_applied < _commit_index && _log.last_idx() > last_applied) {
                const auto& entry = _log[++last_applied];
                if (std::holds_alternative<command>(entry.data)) {
                    commands.push_back(std::cref(std::get<command>(entry.data)));
                }
            }
            co_await _state_machine->apply(std::move(commands));
            _last_applied = last_applied; // has to be updated after apply succeeds, to not be snapshoted to early
        }
    } catch(seastar::broken_condition_variable&) {
        // replication fiber is stopped explicitly.
    } catch(...) {
        logger.error("replication fiber {} stopped because of the error: {}", _my_id, std::current_exception());
    }
    co_return;
}

future<> server::keepalive_fiber() {
    logger.trace("keepalive_fiber starts");
    while(is_leader()) {
        co_await sleep(100ms);

        if (!is_leader()) { // may have lost leadership while sleeping
            break;
        }

        keep_alive ka {
            .current_term = _current_term,
            .leader_id = _current_leader,
            .leader_commit = _commit_index
        };

        for (auto server : _current_config.servers) {
            if (server.id != _my_id) {
                _rpc->send_keepalive(server.id, ka);
            }
        }
    }
    logger.trace("keepalive_fiber stops");
    co_return;
}

future<> server::stop() {
    logger.trace("stop() called");
    become_follower();
    _apply_entries.broken();
    for (auto& ac: _awaited_commits) {
        ac.second.committed.set_exception(stopped_error());
    }
    _awaited_commits.clear();
    return seastar::when_all_succeed(std::move(_leadership_transition), std::move(_applier_status),
            _rpc->stop(), _state_machine->stop(), _storage->stop()).discard_result();
}

// dbg APIs
void server::set_config(configuration config) {
    _current_config = _commited_config = config;
}

future<> server::make_me_leader() {
    return become_leader();
}

void server::set_committed(index_t idx) {
    _commit_index = idx;
}

} // end of namespace raft

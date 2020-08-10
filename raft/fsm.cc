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
#include "fsm.hh"
#include <seastar/core/coroutine.hh>

namespace raft {

bool follower_progress::is_stray_reject(const append_reply::rejected& rejected) {
    switch (state) {
    case follower_progress::state::PIPELINE:
        if (rejected.non_matching_idx <= match_idx) {
            // If rejected index is smaller that matched it means this is a stray reply
            return true;
        }
        break;
    case follower_progress::state::PROBE:
        // In the probe state the reply is only valid if it matches next_idx - 1, since only
        // one append request is outstanding.
        if (rejected.non_matching_idx != index_t(next_idx - 1)) {
            return true;
        }
        break;
    default:
        assert(false);
    }
    return false;
}

void follower_progress::become_probe() {
    state = state::PROBE;
    probe_sent = false;
}

void follower_progress::become_pipeline() {
    if (state != state::PIPELINE) {
        // If a previous request was accepted, move to "pipeline" state
        // since we now know the follower's log state.
        state = state::PIPELINE;
        in_flight = 0;
    }
}

bool follower_progress::can_send_to() {
    if (state == state::PROBE && probe_sent) {
        return false;
    }

    // allow 10 outstanding indexes
    // FIXME: make it smarter
    return in_flight < follower_progress::max_in_flight;
}


fsm::fsm(server_id id, term_t current_term, server_id voted_for, log log) :
        _my_id(id), _current_term(current_term), _voted_for(voted_for),
        _log(std::move(log)) {

    _observed.advance(*this);
    // Make sure the state machine is consistent
    _current_config.servers.push_back(server_address{_my_id});
    logger.trace("{}: starting log length {}", _my_id, _log.last_idx());

    assert(_current_leader.is_nil());
}

const log_entry& fsm::add_entry(command command) {
    // It's only possible to add entries on a leader
    check_is_leader();

    _log.emplace_back(log_entry{_current_term, _log.next_idx(), std::move(command)});
    _sm_events.signal();

    return _log[_log.last_idx()];
}


void fsm::commit_to(index_t leader_commit_idx) {

    auto new_commit_idx = std::min(leader_commit_idx, _log.stable_idx());

    logger.trace("commit_to[{}]: leader_commit_idx={}, new_commit_idx={}",
        _my_id, leader_commit_idx, new_commit_idx);

    if (new_commit_idx > _commit_idx) {
        _commit_idx = new_commit_idx;
        _sm_events.signal();
        assert(_commit_idx > _last_applied);
        logger.trace("commit_to[{}]: signal apply_entries: committed: {} applied: {}",
            _my_id, _commit_idx, _last_applied);
    }
}


void fsm::become_leader() {
    assert(!std::holds_alternative<leader>(_state));
    assert(!_progress);
    _state = leader{};
    _current_leader = _my_id;
    _votes = std::nullopt;
    _progress.emplace();
    for (auto s : _current_config.servers) {
        _progress->emplace(s.id, follower_progress{_log.next_idx(), index_t(0), follower_progress::state::PROBE});
    }
    replicate();
}

void fsm::become_follower(server_id leader) {
    _current_leader = leader;
    _state = follower{};
    _progress = std::nullopt;
    _votes = std::nullopt;
}

void fsm::become_candidate() {
    _state = candidate{};
    _votes.emplace();
    _voted_for = _my_id;
    update_current_term(term_t{_current_term + 1});

    if (_votes->tally_votes(_current_config.servers.size()) == vote_result::WON) {
        // A single node cluster.
        become_leader();
        return;
    }

    for (const auto& server : _current_config.servers) {
        if (server.id == _my_id) {
            continue;
        }
        logger.trace("{} [term: {}, index: {}, last log term: {}] sent vote request to {}",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), server.id);

        send_to(server.id, vote_request{_current_term, _log.last_idx(), _log.last_term()});
    }
}

future<log_batch> fsm::log_entries() {
    logger.trace("fsm::log_entries() {} stable index: {} last index: {}",
        _my_id, _log.stable_idx(), _log.last_idx());

    index_t log_diff, apply_diff;

    while (true) {
        log_diff = _log.last_idx() - _log.stable_idx();
        apply_diff = std::min(_commit_idx, _log.stable_idx()) - _last_applied;

        if (log_diff > 0 || apply_diff > 0 || !_messages.empty() || !_observed.is_equal(*this)) {
            break;
        }
        co_await _sm_events.wait();
    }

    log_batch batch;

    // get a snapshot of all unsent replies
    std::swap(batch.messages, _messages);
    batch.log_entries.reserve(log_diff);

    for (auto i = _log.stable_idx() + 1; i <= _log.last_idx(); i++) {
        // Copy before saving to storage to prevent races with log updates,
        // e.g. truncation of the log.
        // TODO: avoid copies by making sure log truncate is
        // copy-on-write.
        batch.log_entries.emplace_back(_log[i]);
    }

    if (_observed._current_term != _current_term) {
        batch.term = _current_term;
    }

    if (_observed._voted_for != _voted_for) {
        batch.vote = _voted_for;
    }

    if (_observed._commit_idx != _commit_idx) {
        batch.commit_idx = _commit_idx;
    }
    _observed.advance(*this);

    // Return entries to apply.
    batch.apply.reserve(apply_diff);

    auto new_last_applied = _last_applied + apply_diff;

    for (auto idx = _last_applied + 1; idx <= new_last_applied; ++idx) {
        const auto& entry = _log[idx];
        if (std::holds_alternative<command>(entry.data)) {
            batch.apply.push_back(std::cref(std::get<command>(entry.data)));
        }
    }
    _last_applied = new_last_applied;

    co_return batch;
}

void fsm::stable_to(term_t term, index_t idx) {
    if (_log.last_idx() < idx) {
        // The log was truncated while being persisted
        return;
    }

    if (_log[idx].term == term) {
        // If the terms do not match it means the log was truncated.
        _log.stable_to(idx);
        if (is_leader()) {
            auto& progress = progress_for(_my_id);
            progress.match_idx = idx;
            progress.next_idx = index_t{idx + 1};
            replicate();
            check_committed();
        }
    }
}

void fsm::check_committed() {

    std::vector<index_t> match;
    size_t count = 0;

    for (const auto& p : *_progress) {
        logger.trace("check committed {}: {} {}", p.first, p.second.match_idx, _commit_idx);
        if (p.second.match_idx > _commit_idx) {
            count++;
        }
        match.push_back(p.second.match_idx);
    }
    logger.trace("check committed count {} quorum {}", count, quorum());
    if (count < quorum()) {
        return;
    }
    // The index of the pivot node is selected so that all nodes
    // with a larger match index plus the pivot form a majority,
    // for example:
    // cluster size  pivot node     majority
    // 1             0              1
    // 2             0              2
    // 3             1              2
    // 4             1              3
    // 5             2              3
    //
    auto pivot = (match.size() - 1) / 2;
    std::nth_element(match.begin(), match.begin() + pivot, match.end());
    index_t new_commit_idx = match[pivot];

    assert(new_commit_idx > _commit_idx);

    if (_log[new_commit_idx].term != _current_term) {
        // Only entries from the current term can be committed
        // based on vote counting, so if current log entry has
        // different term lets move to the next one in hope it
        // is committed already and has current term
        logger.trace("check committed: cannot commit because of term {} != {}",
            _log[new_commit_idx].term, _current_term);
        return;
    }
    logger.trace("check committed commit {}", new_commit_idx);
    _commit_idx = new_commit_idx;
    // We have quorum of servers with match_idx greater than current commit.
    // It means we can commit && apply the next entry. Use two
    // different events to be able to notify waiters on commit
    // events and apply events to the local state machine in
    // parallel.
    _sm_events.signal();
}

void fsm::tick() {
    _election_elapsed++;

    if (is_leader()) {
        for (auto server : _current_config.servers) {
            if (server.id != _my_id) {
                auto& progress = progress_for(server.id);
                if (progress.state == follower_progress::state::PROBE) {
                    // allow one probe to be resent per follower per time tick
                    progress.probe_sent = false;
                } else {
                    if (progress.in_flight == follower_progress::max_in_flight) {
                        progress.in_flight--; // allow one more packet to be sent
                    }
                }
                if (progress.match_idx < _log.stable_idx()) {
                    logger.trace("tick[{}]: replicate to {} because match={} < stable={}",
                        _my_id, server.id, progress.match_idx, _log.stable_idx());
                    replicate_to(server.id, true);
                }
                if (!progress.activity) {
                    keep_alive ka {
                        .current_term = _current_term,
                        .leader_id = _current_leader,
                        // cap committed index by math_idx otherwise a follower
                        .leader_commit_idx = std::min(_commit_idx, progress.match_idx),
                    };
                    logger.trace("tick[{}]: send keep aplive to {}", _my_id, server.id);
                    send_to(server.id, std::move(ka));
                }
                progress.activity = false;
            }
        }
    } else if (is_past_election_timeout()) {
        become_candidate();
    }
}

void fsm::append_entries(server_id from, append_request_recv&& request) {
    logger.trace("append_entries[{}] received ct={}, prev idx={} prev term={} commit idx={}, idx={}",
            _my_id, request.current_term, request.prev_log_idx, request.prev_log_term,
            request.leader_commit_idx, request.entries.size() ? request.entries[0].idx : index_t(0));

    // Can it happen that a leader gets append request with the same term?
    // What should we do about it?
    assert(is_follower());

    // TODO: need to handle keep alive management here

    if (request.prev_log_term) {
        // Keep alive messages do not have prev_log_term/prev_log_idx and do not need a reply
        // so skip log matching for them.
        // FIXME: introduce fsm::keep_alive()?

        // Ensure log matching property, even if we append no entries.
        // 3.5
        // Until the leader has discovered where it and the
        // follower’s logs match, the leader can send
        // AppendEntries with no entries (like heartbeats) to save
        // bandwidth.
        std::optional<term_t> mismatch = _log.match_term(request.prev_log_idx, request.prev_log_term);
        if (mismatch) {
            logger.trace("append_entries[{}]: no matching term at position {}: expected {}, found {}",
                    _my_id, request.prev_log_idx, request.prev_log_term, *mismatch);
            // Reply false if log doesn't contain an entry at prevLogIndex whose term matches
            // prevLogTerm (§5.3).
            send_to(from, append_reply{_current_term, append_reply::rejected{request.prev_log_idx, _log.last_idx()}});
            return;
        }

        // If there are no entries it means that the leader wants to ensure forward progress.
        // Reply with the last index that matches.
        index_t last_new_idx = request.prev_log_idx;

        if (!request.entries.empty()) {
            last_new_idx = _log.maybe_append(std::move(request.entries));
        }

        send_to(from, append_reply{_current_term, append_reply::accepted{last_new_idx}});
    }

    commit_to(request.leader_commit_idx);
}

void fsm::append_entries_reply(server_id from, append_reply&& reply) {
    assert(is_leader());

    follower_progress& progress = progress_for(from);

    if (progress.state == follower_progress::state::PIPELINE) {
        if (progress.in_flight) {
            // in_flight is not precise, so do not let it underflow
            progress.in_flight--;
        }
    }

    if (std::holds_alternative<append_reply::accepted>(reply.result)) {
        // accepted
        index_t last_idx = std::get<append_reply::accepted>(reply.result).last_new_idx;

        logger.trace("append_entries_reply[{}->{}]: accepted match={} last index={}",
            _my_id, from, progress.match_idx, last_idx);

        progress.match_idx = std::max(progress.match_idx, last_idx);
        // out next_idx may be large because of optimistic increase in pipeline mode
        progress.next_idx = std::max(progress.next_idx, index_t(last_idx + 1));

        progress.become_pipeline();

        // check if any new entry can be committed
        check_committed();
    } else {
        // rejected
        append_reply::rejected rejected = std::get<append_reply::rejected>(reply.result);

        logger.trace("append_entries_reply[{}->{}]: rejected match={} index={}",
            _my_id, from, progress.match_idx, rejected.non_matching_idx);

        // check reply validity
        if (progress.is_stray_reject(rejected)) {
            logger.trace("append_entries_reply[{}->{}]: drop stray append reject", _my_id, from);
        }

        // we should always be able to successfully commit start index
        assert(rejected.non_matching_idx != _log.start_idx() - 1);

        // Start re-sending from non matching, but of from last index in the follower's log.
        // FIXME: make it more efficient
        progress.next_idx = std::min(rejected.non_matching_idx, index_t(rejected.last_idx + 1));

        progress.become_probe();

        // We should not fail to apply an entry next after a matched one.
        assert(progress.next_idx != progress.match_idx);
    }

    logger.trace("append_entries_reply[{}->{}]: next_idx={}, match_idx={}",
        _my_id, from, progress.next_idx, progress.match_idx);

    replicate_to(from, false);
}

void fsm::request_vote(server_id from, vote_request&& request) {

    // We can cast a vote in any state. If the candidate's term is
    // lower than ours, we ignore the request. Otherwise we first
    // update our current term and convert to a follower.
    assert(_current_term == request.current_term);

    bool can_vote =
	    // We can vote if this is a repeat of a vote we've already cast...
        _voted_for == from ||
        // ...we haven't voted and we don't think there's a leader yet in this term...
        (_voted_for == server_id{} && _current_leader == server_id{});

    // ...and we believe the candidate is up to date.
    if (can_vote && _log.is_up_to_date(request.last_log_idx, request.last_log_term)) {

        logger.trace("{} [term: {}, index: {}, log_term: {}, voted_for: {}] "
            "voted for {} [log_term: {}, log_index: {}]",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), _voted_for,
            from, request.last_log_term, request.last_log_idx);

        _voted_for = from;

        send_to(from, vote_reply{_current_term, true});
    } else {
        logger.trace("{} [term: {}, index: {}, log_term: {}, voted_for: {}] "
            "rejected vote for {} [log_term: {}, log_index: {}]",
            _my_id, _current_term, _log.last_idx(), _log.last_term(), _voted_for,
            from, request.last_log_term, request.last_log_idx);

        send_to(from, vote_reply{_current_term, false});
    }

}

void fsm::request_vote_reply(server_id from, vote_reply&& reply) {
    assert(std::holds_alternative<candidate>(_state));

    logger.trace("{} received a {} vote from {}", _my_id, reply.vote_granted ? "yes" : "no", from);

    _votes->register_vote(reply.vote_granted);

    switch (_votes->tally_votes(_current_config.servers.size())) {
    case vote_result::UNKNOWN:
        break;
    case vote_result::WON:
        become_leader();
        break;
    case vote_result::LOST:
        become_follower(server_id{});
        break;
    }
}

void fsm::replicate_to(server_id dst, bool allow_empty) {
    auto& progress = progress_for(dst);

    logger.trace("replicate_to[{}->{}]: called next={} match={}",
        _my_id, dst, progress.next_idx, progress.match_idx);

    while (progress.can_send_to()) {
        index_t next_idx = progress.next_idx;
        if (progress.next_idx > _log.stable_idx()) {
            next_idx = index_t(0);
            logger.trace("replicate_to[{}->{}]: next past stable next={} stable={}, empty={}",
                    _my_id, dst, progress.next_idx, _log.stable_idx(), allow_empty);
            if (!allow_empty) {
                // send out only persisted entries
                return;
            }
        }

        allow_empty = false; // allow only one empty message

        index_t prev_idx = index_t(0);
        term_t prev_term = _current_term;
        if (progress.next_idx != 1) {
            prev_idx = index_t(progress.next_idx - 1);
            prev_term = _log[prev_idx].term;
        }

        append_request_send req = {{
                .current_term = _current_term,
                .leader_id = _my_id,
                .prev_log_idx = prev_idx,
                .prev_log_term = prev_term,
                .leader_commit_idx = _commit_idx
            },
            std::vector<log_entry_cref>()
        };

        if (next_idx) {
            const log_entry& entry = _log[next_idx];
            // TODO: send only one entry for now, but we should batch in the future
            req.entries.push_back(std::cref(entry));
            logger.trace("replicate_to[{}->{}]: send entry idx={}, term={}",
                _my_id, dst, entry.idx, entry.term);
        } else {
            logger.trace("replicate_to[{}->{}]: send empty", _my_id, dst);
        }

        send_to(dst, std::move(req));

        if (progress.state == follower_progress::state::PROBE) {
            progress.probe_sent = true;
        } else {
            progress.in_flight++;
            // Optimistically update next send index. In case a message is lost
            // there will be negative reply that will re-send idx.
            progress.next_idx++;
        }
        progress.activity = true;
    }
}

void fsm::replicate() {
    assert(is_leader());
    for (const auto& server : _current_config.servers) {
        if (server.id != _my_id) {
            replicate_to(server.id, false);
        }
    }
}

void fsm::stop() {
    _sm_events.broken();
}

} // end of namespace raft

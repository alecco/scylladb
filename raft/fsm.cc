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

namespace raft {

log_entry& log::operator[](size_t i) {
    assert(index_t(i) >= _start_idx);
    return _log[i - _start_idx];
}

void log::emplace_back(log_entry&& e) {
    _log.emplace_back(std::move(e));
}

bool log::empty() const {
    return _log.empty();
}

index_t log::last_idx() const {
    return index_t(_log.size()) + _start_idx - index_t(1);
}

index_t log::next_idx() const {
    return last_idx() + index_t(1);
}

void log::truncate_head(index_t idx) {
    assert(idx >= _start_idx);
    auto it = _log.begin() + (idx - _start_idx);
    _log.erase(it, _log.end());
    stable_to(std::min(_stable_idx, last_idx()));
}

index_t log::start_idx() const {
    return _start_idx;
}

void log::stable_to(index_t idx) {
    assert(idx <= last_idx());
    _stable_idx = idx;
}

bool log::match_term(index_t idx, term_t term) const {
    if (idx == 0) {
        // Special case of empty log on leader,
        // TLA+ line 324.
        return true;
    }
    // @todo idx can be legally < _start_idx if all entries
    // are committed and all logs are snapshotted away.
    // Revise when we implement log snapshots.
    assert(idx >= _start_idx);

    auto i = idx - _start_idx;

    if (i >= _log.size()) {
        // We have a gap between the follower and the leader.
        return false;
    }
    return _log[i].term == term;
}

std::pair<index_t, term_t> log::find_first_idx_of_term(index_t hint) const {

    assert(hint >= _start_idx);

    // @todo if _log.size() == 0 use snapshot index and term
    if (hint == 0 || _log.size() == 0) {
        // A special case of an empty log.
        return {last_idx(), term_t{0}};
    }

    auto i = hint - _start_idx;

    if (i >= _log.size()) {
        // We have a log gap between the follower and the leader.
        // Disable optimization
        return {index_t(0), term_t(0)};
    }

    term_t term = _log[i].term;

    while (i > 0) {
        if (_log[i-1].term != term)
            break;
        i--;
    }
    return {_start_idx + i, term};
}

index_t log::maybe_append(const std::vector<log_entry>& entries) {
    if (entries.size() == 0) {
        return last_idx();
    }

    index_t last_new_idx = entries.back().idx;

    // We must scan through all entries if the log already
    // contains them to ensure the terms match.
    for (auto& e : entries) {
        if (e.idx <= last_idx()) {
            if (e.idx < _start_idx) {
                logger.trace("append_entries: skipping entry with idx {} less than log start {}", e.idx, _start_idx);
                continue;
            }
            if (e.term == _log[e.idx - _start_idx].term) {
                logger.trace("append_entries: entries with index {} has matching terms {}", e.idx, e.term);
                continue;
            }
            logger.trace("append_entries: entries with index {} has non matching terms e.term={}, _log[i].term = {}",
                e.idx, e.term, _log[e.idx - _start_idx].term);
            // If an existing entry conflicts with a new one (same
            // index but different terms), delete the existing
            // entry and all that follow it (§5.3).
            truncate_head(e.idx);
        }
        // Assert log monotonicity
        assert(e.idx == next_idx());
        _log.emplace_back(std::move(e));
    }

    return last_new_idx;
}


fsm::fsm(server_id id, term_t current_term, server_id voted_for, log log) :
        _my_id(id), _current_term(current_term), _voted_for(voted_for),
        _log(std::move(log)) {

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


bool fsm::commit_to(index_t leader_commit_idx) {

    auto new_commit_idx = std::min(leader_commit_idx, _log.stable_idx());

    logger.trace("commit_to[{}]: leader_commit_idx={}, new_commit_idx={}",
        _my_id, leader_commit_idx, new_commit_idx);

    if (new_commit_idx > _commit_idx) {
        _commit_idx = new_commit_idx;
        return true;
    }
    return false;
}


void fsm::become_leader() {
    assert(_state != server_state::LEADER);
    assert(!_progress);
    _state = server_state::LEADER;
    _current_leader = _my_id;
    _votes = std::nullopt;
    _progress.emplace();
    for (auto s : _current_config.servers) {
        _progress->emplace(s.id, follower_progress{_log.next_idx(), index_t(0)});
    }
    replicate();
}

void fsm::become_follower(server_id leader) {
    assert(_state != server_state::FOLLOWER);
    _current_leader = leader;
    _state = server_state::FOLLOWER;
    _progress = std::nullopt;
    _votes = std::nullopt;
}

void fsm::become_candidate() {
    update_current_term(term_t{_current_term + 1});
    _state = server_state::CANDIDATE;
    _votes.emplace();
    _voted_for = _my_id;
    _voted_for_is_dirty = true;
}


std::optional<log_batch> fsm::log_entries() {
    logger.trace("fsm::log_entries() {} stable index: {} last index: {}",
        _my_id, _log.stable_idx(), _log.last_idx());

    auto diff = _log.last_idx() - _log.stable_idx();

    if (diff == 0 && _messages.empty() &&
        _current_term_is_dirty == false && _voted_for_is_dirty == false) {

        return {};
    }

    log_batch batch;

    // get a snapshot of all unsent replies
    std::swap(batch.messages, _messages);
    batch.log_entries.reserve(diff);

    for (auto i = _log.stable_idx() + 1; i <= _log.last_idx(); i++) {
        // Copy before saving to storage to prevent races with log updates,
        // e.g. truncation of the log.
        // TODO: avoid copies by making sure log truncate is
        // copy-on-write.
        batch.log_entries.emplace_back(_log[i]);
    }

    if (_current_term_is_dirty) {
        batch.term = _current_term;
    }

    if (_voted_for_is_dirty) {
        batch.vote = _voted_for;
    }

    _current_term_is_dirty = _voted_for_is_dirty = false;

    return batch;
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
            (*_progress)[_my_id].match_idx = idx;
            (*_progress)[_my_id].next_idx = index_t{idx + 1};
            replicate();
        }
    }
}

bool fsm::check_committed() {

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
        return false;
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
        return false;
    }
    logger.trace("check committed commit {}", new_commit_idx);
    _commit_idx = new_commit_idx;
    // We have quorum of servers with match_idx greater than current commit.
    // It means we can commit the next entry.
    return true;
}

std::optional<apply_batch> fsm::apply_entries() {

    std::optional<apply_batch> batch;
    auto diff = std::min(_commit_idx, _log.stable_idx()) - _last_applied;

    if (diff > 0) {
        batch.emplace();
        batch->idx = _last_applied + diff;
        batch->commands.reserve(diff);

        for (auto idx = _last_applied + 1; idx <= batch->idx; ++idx) {
            const auto& entry = _log[idx];
            if (std::holds_alternative<command>(entry.data)) {
                batch->commands.push_back(std::cref(std::get<command>(entry.data)));
            }
        }
    }
    return batch;
}

void fsm::tick() {
    _election_elapsed++;

    if (is_leader()) {
        keep_alive ka {
            .current_term = _current_term,
            .leader_id = _current_leader,
            .leader_commit_idx = _commit_idx,
        };

        for (auto server : _current_config.servers) {
            if (server.id != _my_id) {
                // cap committed index by math_idx otherwise a follower may commit unmatched entries
                ka.leader_commit_idx = std::min(_commit_idx, (*_progress)[server.id].match_idx);
                send_keepalive(server.id, ka);
            }
        }
    }
    if (is_past_election_timeout()) {
        if (is_follower()) {
            become_candidate();
        } else {
            // restart_election();
        }
    }
}

void fsm::step() {
    _election_elapsed = 0;
}

bool fsm::append_entries(server_id from, append_request_recv& append_request) {
    logger.trace("append_entries[{}] received ct={}, prev idx={} prev term={} commit idx={}, idx={}", _my_id,
            append_request.current_term, append_request.prev_log_idx, append_request.prev_log_term, append_request.leader_commit_idx,
            append_request.entries.size() ? append_request.entries[0].idx : index_t(0));

    step();
    if (append_request.current_term < _current_term) {
        send_append_reply(from, append_reply{_current_term, append_reply::rejected{append_request.prev_log_idx, term_t(0), index_t(0)}});
        return false;
    }

    // Can it happen that a leader gets append request with the same term?
    // What should we do about it?
    assert(!is_leader() || _current_term > append_request.current_term);

    if (!is_follower()) {
        become_follower(server_id{});
    }

    if (_current_term < append_request.current_term) {
        update_current_term(append_request.current_term);
    }

    // TODO: need to handle keep alive management here

    // Ensure log matching property, even if we append no entries.
    // 3.5
    // Until the leader has discovered where it and the
    // follower’s logs match, the leader can send
    // AppendEntries with no entries (like heartbeats) to save
    // bandwidth.
    if (!_log.match_term(append_request.prev_log_idx, append_request.prev_log_term)) {
        auto [i, t] = _log.find_first_idx_of_term(append_request.prev_log_idx);
        logger.trace("append_entries[{}]: no matching term at position {}: expected {}, found {}, reject hint {}",
            _my_id, append_request.prev_log_idx, append_request.prev_log_term, t, i);
        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        send_append_reply(from, append_reply{_current_term, append_reply::rejected{append_request.prev_log_idx, t, i}});
        return false;
    }

    send_append_reply(from, append_reply{_current_term, append_reply::accepted{_log.maybe_append(append_request.entries)}});

    return commit_to(append_request.leader_commit_idx);
}

bool fsm::append_entries_reply(server_id from, append_reply& reply) {
    step();
    if (!is_leader() || reply.current_term < _current_term) {
        // drop stray reply if we are no longer a leader or the term is too old
        return false;
    }

    if (reply.current_term > _current_term) {
        // receiver knows something about newer leader, so this server has to convert to a follower
        become_follower(server_id{});
        return false;
    }

    follower_progress& progress = (*_progress)[from];

    bool res = false;
    if (std::holds_alternative<append_reply::accepted>(reply.result)) {
        // accepted
        // a follower may have longer log, so cap it with our own log length
        index_t last_idx = std::min(std::get<append_reply::accepted>(reply.result).last_log_index, _log.last_idx());

        logger.trace("append_entries_reply[{}->{}]: accepted match={} last index={}", _my_id, from, progress.match_idx, last_idx);

        progress.match_idx = std::max(progress.match_idx, last_idx);

        // check if any new entry can be committed
        res = check_committed();
    } else {
        // rejected
        append_reply::rejected rejected = std::get<append_reply::rejected>(reply.result);

        logger.trace("append_entries_reply[{}->{}]: rejected match={} index={} non matching term={}, idx for term={}",
                _my_id, from, progress.match_idx, rejected.index, rejected.non_matching_term, rejected.first_idx_for_non_matching_term);

        if (rejected.index <= progress.match_idx) {
            // if rejected index is smaller that matched it means this is a stray reply
            return false;
        }

        index_t n = progress.next_idx;

        if (rejected.non_matching_term) {
            // we got a term mismatch, skip all the entries from next_idx to first_idx_for_non_matching_term
            // that do not have non_matching_term
            for (; n > std::max(_log.start_idx(), rejected.first_idx_for_non_matching_term); n--) {
                if (_log[n].term == rejected.non_matching_term) {
                    logger.trace("append_entries_reply[{}->{}]: first entry with different term {}", _my_id, from, n);
                    n++; // we found a matching entry, now move to the next one
                    break;
                }
            }
        } else {
            // if there is no matching term it means there was a gap (as opposite to term mismatch)
            // so start re-sending from last matched entry
            // FIXME: make it more efficient?
            n = index_t(progress.match_idx + 1);
        }

        progress.next_idx = n;
        logger.trace("replication_fiber[{}->{}]: next_idx={}, match_idx={}", _my_id, from, progress.next_idx, progress.match_idx);
        assert(progress.next_idx != progress.match_idx); // we should not fail to apply an entry next after a matched one
    }

    replicate_to(from);
    return res;
}

void fsm::replicate_to(server_id dst) {
    auto& progress = (*_progress)[dst];

    logger.trace("replicate_to[{}->{}]: called next={} match={}", _my_id, dst, progress.next_idx, progress.match_idx);

    while(progress.next_idx - progress.match_idx < 10) {
        if (progress.next_idx > _log.stable_idx()) {
            logger.trace("replicate_to[{}->{}]: next past stable next={} stable={}", _my_id, dst, progress.next_idx, _log.stable_idx());
            // send out only persisted entries
            return;
        }

        const log_entry& entry = _log[progress.next_idx];
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
            // TODO: send only one entry for now, but we should batch in the future
            std::vector<log_entry_cref>(1, std::cref(entry))
        };


        logger.trace("replicate_to[{}->{}]: send entry idx={}, term={}", _my_id, dst, entry.idx, entry.term);

        // optimistically update next send index. In case a message is lost
        // there will be negative reply that will resend idx
        progress.next_idx++;
        send_append_entries(dst, req);
    }
}

void fsm::replicate() {
    assert(is_leader());
    for (auto server : _current_config.servers) {
        if (server.id != _my_id) {
            replicate_to(server.id);
        }
    }
}

} // end of namespace raft

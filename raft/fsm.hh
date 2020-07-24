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
#pragma once

#include "raft.hh"

namespace raft {

// Leader's view of each follower, including self.
struct follower_progress {
    // Index of the next log entry to send to this server.
    index_t next_idx;
    // Index of the highest log entry known to be replicated to this
    // server.
    index_t match_idx;
};

// A batch of entries to apply to the state machine.
struct apply_batch {
    // Commands to apply.
    std::vector<command_cref> commands;
    // index of the last command in the batch
    index_t idx;
};

// This class represents the Raft log in memory.
// The value of the first index is 1.
// New entries are added at the back.
// Entries are persisted locally after they are added.
// Entries may be dropped from the beginning by snapshotting
// and from the end by a new leader replacing stale entries.
class log {
    // we need something that can be truncated form both sides.
    // std::deque move constructor is not nothrow hence cannot be used
    boost::container::deque<log_entry> _log;
    // the index of the first entry in the log (index starts from 1)
    // will be increased by log gc
    index_t _start_idx = index_t(1);
    // Index of the first stable (persisted) entry in the
    // log.
    index_t _stable_idx = index_t(0);
public:
    log_entry& operator[](size_t i);
    // reserve n additional entries
    void emplace_back(log_entry&& e);
    // Mark all entries up to this index
    // as stable.
    void stable_to(index_t idx);
    // return true if in memory log is empty
    bool empty() const;
    index_t next_idx() const;
    index_t last_idx() const;
    index_t stable_idx() const {
        return _stable_idx;
    }
    void truncate_head(size_t i);
    index_t start_idx() const;
};

// Raft protocol finite state machine
//
// A serious concern that prevented inheriting Scylla Raft from an
// existing C or C++ implementation was the way libraries deal with
// concurrency, and how well network and disk I/O concerns are
// separated from the protocol state machine logic.

// Most libraries address the separation by providing an API to the
// environment of the Raft protocol state machine, such as the
// database, the write ahead log and the RPC to peers.

// The callback based design, while is certainly advantageous
// when compared to tight coupling of Raft components, has some
// drawbacks:

// - some callbacks may be defined in blocking model; e.g.
//  writing log entries to disk, or persisting the current
//  term in the database; Scylla has no blocking IO and
//  would have to emulate it with seastar fibers;
// - the API calls are spread over the state machine
//  implementation, which makes reasoning about the correctness
//  more difficult than need be (what happens if the state
//  machine is accessed concurrently by multiple users,
//  which of these accesses have to be synchronized, and
//  which can be made concurrent; what if the callback
//  fails, is the state machine handling the error correctly?
//  what assumptions about possible types of failures are made?)
// - while it makes testing without a real network or disk possible,
//   it still complicates it, since in order to test the
//   protocol one has to implement meaningful mocks for most of the APIs.
//
// Scylla Raft instead adopts a few design decisions reminding
// Hoare's CSP model:
//
// - the protocol instance is implemented as in-memory state machine
//   with a catch-all API step(messages...). step()
//   handles any kind of input and performs the needed state
//   machine state transitions. It produces Ready object,
//   which encapsulates a list of actions that must be
//   performed until the next Step() call can be made.
// - the time is represented with a logical timer. The client
//   is responsible for periodically involving Tick() method, which
//   advances the state machine time and allows it to track
//   such events as election or heartbeat timeouts.
// - step() uses "template method" design pattern to
//   clearly separate common logic from things specific to
//   leader, follower and candidate roles, which, in turn,
//   are function pointers, updated whenever the FSM
//   transitions to the respective role.
//
// The active agent of the protocol is called instance, and
// provides a facade to the state machine, running send and
// receive fibers, handling I/O and timer events.
struct fsm {
    // id of this node
    server_id _my_id;
    // id of the current leader
    server_id _current_leader;
    // What state the server is in.
    server_state _state = server_state::FOLLOWER;
    // _current_term, _voted_for && _log are persisted in storage
    // latest term the server has seen
    term_t _current_term = term_t(0);
    // candidateId that received vote in current term (or nil if none)
    server_id _voted_for;
    // commit_index && last_applied are volatile state
    // index of highest log entry known to be committed
    index_t _commit_idx = index_t(0);
    // index of highest log entry applied to the state machine
    index_t _last_applied = index_t(0);
    // log entries; each entry contains command for state machine,
    // and term when entry was received by leader
    log _log;

    // A state for each follower, maintained only on the leader.
    std::optional<std::unordered_map<server_id, follower_progress>> _progress;

    // currently committed configuration
    configuration _commited_config;
    // currently used configuration, may be different from committed during configuration change
    configuration _current_config;
public:
    explicit fsm(server_id id, term_t current_term, server_id voted_for, log log);

    bool is_leader() const {
        assert(_state != server_state::LEADER || _my_id == _current_leader);
        return _state == server_state::LEADER;
    }
    bool is_follower() const {
        return _state == server_state::FOLLOWER;
    }
    void check_is_leader() const {
        if (!is_leader()) {
            throw not_leader(_current_leader);
        }
    }

    void become_leader();

    void become_follower(server_id leader);

    void update_current_term(term_t current_term) {
        assert(_state == server_state::FOLLOWER);
        assert(_current_term < current_term);
        _current_term = current_term;
        _voted_for = server_id{};
    }
    // Set cluster configuration, in real app should be taken from log
    void set_configuration(const configuration& config) {
        _current_config = _commited_config = config;
        // We use quorum() - 1 as an index in
        // a sorted array of follower positions to
        // identify which entries are committed.
        assert(quorum() > 0);
    }
    // Calculates current quorum
    size_t quorum() const {
        return _current_config.servers.size() / 2 + 1;
    }
    // Add an entry to in-memory log. The entry has to be
    // committed to the persistent Raft log afterwards.
    const log_entry& add_entry(command command);
    // Called after an added entry is persisted on disk,
    // is called on the leader.
    void stable_to(term_t term, index_t idx);

    // Return entries ready to be applied to the state machine,
    // or an empty optional if there are no such entries.
    std::optional<apply_batch> apply_entries();

    // Update _last_applied index with the index of
    // last applied entry.
    void applied_to(index_t idx) {
        assert(idx > _last_applied);
        assert(idx <= _commit_idx);
        assert(idx <= _log.stable_idx());
        _last_applied = idx;
    }

    // Called when one of the replicas advanced its match index
    // so it may be the case that some entries are committed now.
    // @return true if there are entries that should be committed.
    bool check_committed();
};

} // namespace raft


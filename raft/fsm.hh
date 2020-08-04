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

// Possible leader election outcomes.
enum class vote_result {
    // We haven't got enough responses yet, either because
    // the servers haven't voted or responses failed to arrive.
    VOTE_UNKNOWN,
    // This candidate has won the election
    VOTE_WON,
    // The quorum of servers has voted against this candidate
    VOTE_LOST,
};

// Candidate's state specific to election
struct votes {
    // Number of responses to RequestVote RPC.
    // The candidate always votes for self.
    size_t responded = 1;
    // Number of granted votes.
    // The candidate always votes for self.
    size_t granted = 1;

    vote_result tally_votes(size_t cluster_size) const {
        auto quorum = cluster_size / 2 + 1;
        if (granted >= quorum) {
            return vote_result::VOTE_WON;
        }
        assert(responded <= cluster_size);
        auto unknown = cluster_size - responded;
        return granted + unknown >= quorum ? vote_result::VOTE_UNKNOWN : vote_result::VOTE_LOST;
    }
};

// State of the FSM that needs logging & sending.
struct log_batch {
    std::optional<term_t> term;
    std::optional<server_id> vote;
    std::vector<log_entry> log_entries;
    std::vector<std::pair<server_id, rpc_message>> messages;
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

private:
    void truncate_head(index_t i);
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
    index_t start_idx() const;

    // 3.5
    // Raft maintains the following properties, which
    // together constitute the Log Matching Property:
    // * If two entries in different logs have the same index and
    // term, then they store the same command.
    // * If two entries in different logs have the same index and
    // term, then the logs are identical in all preceding entries.
    //
    // The first property follows from the fact that a leader
    // creates at most one entry with a given log index in a given
    // term, and log entries never change their position in the
    // log. The second property is guaranteed by a consistency
    // check performed by AppendEntries. When sending an
    // AppendEntries RPC, the leader includes the index and term
    // of the entry in its log that immediately precedes the new
    // entries. If the follower does not find an entry in its log
    // with the same index and term, then it refuses the new
    // entries.
    //
    // @retval true  there is a match
    // @retval false log matching property is violated
    bool match_term(index_t idx, term_t term) const;

    // Find the first index with the same term
    // as the term of the index given in the hint, or the first
    // known log index, if all entries from the hint to the start
    // of the index have the same term..
    //
    // Uses linear search from 'hint' back to start of the log.
    //
    // Is used to find the first index of a term on a follower
    // when follower's term for an index position does not
    // match one the leader has at this position.
    //
    // 3.5:
    // When rejecting an AppendEntries request, the
    // follower can include the term of the
    // conflicting entry and the first index it stores for that
    // term. With this information, the leader can
    // decrement nextIndex to bypass all of the conflicting
    // entries in that term; one AppendEntries RPC
    // will be required for each term with conflicting entries,
    // rather than one RPC per entry. Alternatively,
    // the leader can use a binary search approach to find the
    // first entry where the follower’s log differs
    // from its own; this has better worst-case behavior.
    std::pair<index_t, term_t> find_first_idx_of_term(index_t hint) const;

    // Called on a follower to append entries from a leader.
    // @retval return an index of last appended entry
    // Raft log, so we need to persist the log.
    index_t maybe_append(const std::vector<log_entry>& entries);
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
//   is responsible for periodically involving tick() method, which
//   advances the state machine time and allows it to track
//   such events as election or heartbeat timeouts.
// - step() uses "template method" design pattern to
//   clearly separate common logic from things specific to
//   leader, follower and candidate roles, which, in turn,
//   are function pointers, updated whenever the FSM
//   transitions to the respective role.
//
// The active agent of the protocol is called server, and
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

    bool _current_term_is_dirty = false;
    bool _voted_for_is_dirty = false;

    int _election_elapsed = 0;
    // 3.4 Leader election
    // If a follower receives no communication over a period of
    // time called the election timeout, then it assumes there is
    // no viable leader and begins an election to choose a new
    // leader
    const int _election_timeout = 10;
    // A random value in range [election_timeout, 2 * election_timeout)
    int _randomized_election_timeout = 10;
    std::optional<votes> _votes;

    // A state for each follower, maintained only on the leader.
    std::optional<std::unordered_map<server_id, follower_progress>> _progress;
    // Holds all replies to AppendEntries RPC which are not
    // yet sent out. If AppendEntries request is accepted, we must
    // withhold a reply until the respective entry is persisted in
    // the log. Otherwise, e.g. when we receive AppendEntries with
    // an older term, we may reject it immediately.
    // Either way all entries are appended to this queue first.
    //
    // 3.3 Raft Basics
    // If a server receives a request with a stale term number, it
    // rejects the request.
    // TLA+ line 328
    std::vector<std::pair<server_id, rpc_message>> _messages;

    // currently committed configuration
    configuration _commited_config;
    // currently used configuration, may be different from committed during configuration change
    configuration _current_config;

    // Signaled when there is a IO event to process.
    seastar::condition_variable _sm_events;

    bool is_past_election_timeout() const {
        return _election_elapsed > _randomized_election_timeout;
    }

private:
    // A helper to send reply to an append message
    void send_append_reply(server_id to, append_reply reply) {
        _messages.push_back(std::make_pair(to, std::move(reply)));
        _sm_events.signal();
    }

    // A helper to send keepalive
    void send_keepalive(server_id to, keep_alive keep_alive) {
        _messages.push_back(std::make_pair(to, keep_alive));
        _sm_events.signal();
    }

    // A helper to send AppendEntries message
    void send_append_entries(server_id to, append_request_send append) {
        _messages.push_back(std::make_pair(to, append));
        _sm_events.signal();
    }

    // A helper to update FSM current term.
    void update_current_term(term_t current_term) {
        assert(_state == server_state::FOLLOWER);
        assert(_current_term < current_term);
        _current_term = current_term;
        _voted_for = server_id{};
        // No need to mark voted_for as dirty since
        // persisting current_term resets it.
        _current_term_is_dirty = true;

        // Reset the randomized election timeout on each term
        // change, even if we do not plan to campaign during this
        // term: the main purpose of the timeout is to avoid
        // starting our campaign simultaneously with other followers.
        _randomized_election_timeout = _election_timeout + std::rand() % _election_timeout;
    }
    // Calculates current quorum
    size_t quorum() const {
        return _current_config.servers.size() / 2 + 1;
    }

    void check_is_leader() const {
        if (!is_leader()) {
            throw not_leader(_current_leader);
        }
    }

    void become_candidate();
public:
    explicit fsm(server_id id, term_t current_term, server_id voted_for, log log);

    bool is_leader() const {
        assert(_state != server_state::LEADER || _my_id == _current_leader);
        return _state == server_state::LEADER;
    }
    bool is_follower() const {
        return _state == server_state::FOLLOWER;
    }
    void become_leader();

    void become_follower(server_id leader);

    // Set cluster configuration, in real app should be taken from log
    void set_configuration(const configuration& config) {
        _current_config = _commited_config = config;
        // We use quorum() - 1 as an index in
        // a sorted array of follower positions to
        // identify which entries are committed.
        assert(quorum() > 0);
    }
    // Add an entry to in-memory log. The entry has to be
    // committed to the persistent Raft log afterwards.
    const log_entry& add_entry(command command);

    // Return a copy of the entries that need
    // to be logged. When these entries are logged,
    // stable_to() must be called with the last logged
    // term/index. The logged entries are eventually
    // discarded from the state machine after snapshotting.
    std::optional<log_batch> log_entries();

    // Called after an added entry is persisted on disk.
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

    // Called on a follower with a new known leader commit index.
    // Advances the follower's commit index up to all log-stable
    // entries, known to be committed.
    // @retval true _commit_idx was advanced
    bool commit_to(index_t leader_commit_idx);

    // Called to advance virtual clock of the protocol state machine.
    void tick();

    // Common part of all transitions of the protocol state machine.
    void step();

    // controls replication process
    void replicate_to(server_id dst);
    void replicate();

    // returns true if new entries were committed
    bool append_entries_reply(server_id from, append_reply& reply);
    bool append_entries(server_id from, append_request_recv& append_request);
};

} // namespace raft


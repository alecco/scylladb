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
    index_t match_idx = index_t(0);

    enum class state {
        // In this state only one append entry is send until matching index is found
        PROBE,
        // In this state multiple append entries are sent optimistically
        PIPELINE
    };
    state state = state::PROBE;
    // true if a packet was sent already in a probe mode
    bool probe_sent = false;
    // number of in flight still un-acked append entries requests
    size_t in_flight = 0;
    static constexpr size_t max_in_flight = 10;

    // Set when a message is sent to the follower
    // reset on a tick. Used to decide if keep alive is needed.
    bool activity = false;
};

// Possible leader election outcomes.
enum class vote_result {
    // We haven't got enough responses yet, either because
    // the servers haven't voted or responses failed to arrive.
    UNKNOWN,
    // This candidate has won the election
    WON,
    // The quorum of servers has voted against this candidate
    LOST,
};

// Candidate's state specific to election
class votes {
    // Number of responses to RequestVote RPC.
    // The candidate always votes for self.
    size_t _responded = 1;
    // Number of granted votes.
    // The candidate always votes for self.
    size_t _granted = 1;
public:
    void register_vote(bool granted) {
        _responded++;
        if (granted) {
            _granted++;
        }
    }

    vote_result tally_votes(size_t cluster_size) const {
        auto quorum = cluster_size / 2 + 1;
        if (_granted >= quorum) {
            return vote_result::WON;
        }
        assert(_responded <= cluster_size);
        auto unknown = cluster_size - _responded;
        return _granted + unknown >= quorum ? vote_result::UNKNOWN : vote_result::LOST;
    }
};

// State of the FSM that needs logging & sending.
struct log_batch {
    std::optional<term_t> term;
    std::optional<server_id> vote;
    std::optional<index_t> commit_idx;
    std::vector<log_entry> log_entries;
    std::vector<std::pair<server_id, rpc_message>> messages;
};

// A batch of entries to apply to the state machine.
using apply_batch = std::vector<command_cref>;

// 3.3 Raft Basics
// At any given time each server is in one of three states:
// leader, follower, or candidate.
// In normal operation there is exactly one leader and all of the
// other servers are followers. Followers are passive: they issue
// no requests on their own but simply respond to requests from
// leaders and candidates. The leader handles all client requests
// (if a client contacts a follower, the follower redirects it to
// the leader). The third state, candidate, is used to elect a new
// leader.
class follower {};
class candidate {};
class leader {};

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
    // What state the server is in. The default is follower.
    std::variant<follower, candidate, leader> _state;
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

    struct last_observed_state {
        term_t _current_term;
        server_id _voted_for;
        index_t _commit_idx;

        bool is_equal(const fsm& fsm) {
            return _current_term == fsm._current_term && _voted_for == fsm._voted_for &&
                _commit_idx == fsm._commit_idx;
        }

        void advance(const fsm& fsm) {
            _current_term = fsm._current_term;
            _voted_for = fsm._voted_for;
            _commit_idx = fsm._commit_idx;
        }
    } _observed;

    int _election_elapsed = 0;
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
private:
    // Signaled when there is a IO event to process.
    seastar::condition_variable _sm_events;
    // Signaled when there is an entry to apply.
    seastar::condition_variable _apply_entries;
    // Called when one of the replicas advanced its match index
    // so it may be the case that some entries are committed now.
    // Signals relevant events.
    void check_committed();

    bool is_past_election_timeout() const {
        return _election_elapsed > _randomized_election_timeout;
    }

    // A helper to send reply to an append message
    template <typename Message>
    void send_to(server_id to, Message&& m) {
        static_assert(std::is_rvalue_reference<decltype(m)>::value, "must be rvalue");
        _messages.push_back(std::make_pair(to, std::move(m)));
        _sm_events.signal();
    }

    // A helper to update FSM current term.
    void update_current_term(term_t current_term) {
        assert(_current_term < current_term);
        _current_term = current_term;
        _voted_for = server_id{};

        // Reset the randomized election timeout on each term
        // change, even if we do not plan to campaign during this
        // term: the main purpose of the timeout is to avoid
        // starting our campaign simultaneously with other followers.
        _randomized_election_timeout = ELECTION_TIMEOUT + std::rand() % ELECTION_TIMEOUT;
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

    void become_follower(server_id leader);

    // return progress for a follower
    follower_progress& progress_for(server_id dst) {
        assert(is_leader());
        return (*_progress)[dst];
    }

    // controls replication process
    bool can_send_to(const follower_progress& progress);
    void replicate_to(server_id dst, bool allow_empty);
    void replicate();
    void append_entries(server_id from, append_request_recv&& append_request);
    void append_entries_reply(server_id from, append_reply&& reply);

    void request_vote(server_id from, vote_request&& vote_request);
    void request_vote_reply(server_id from, vote_reply&& vote_reply);
public:
    explicit fsm(server_id id, term_t current_term, server_id voted_for, log log);
    fsm() = default;

    bool is_leader() const {
        return std::holds_alternative<leader>(_state);
    }
    bool is_follower() const {
        return std::holds_alternative<follower>(_state);
    }

    // 3.4 Leader election
    // If a follower receives no communication over a period of
    // time called the election timeout, then it assumes there is
    // no viable leader and begins an election to choose a new
    // leader
    static constexpr int ELECTION_TIMEOUT = 10;

    void become_leader();

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
    future<log_batch> log_entries();

    // Called after an added entry is persisted on disk.
    void stable_to(term_t term, index_t idx);

    // Return entries ready to be applied to the state machine,
    // or an empty optional if there are no such entries.
    future<apply_batch> apply_entries();

    // Called on a follower with a new known leader commit index.
    // Advances the follower's commit index up to all log-stable
    // entries, known to be committed.
    void commit_to(index_t leader_commit_idx);

    // Called to advance virtual clock of the protocol state machine.
    void tick();

    // Feed one Raft RPC message into the state machine.
    // Advances the state machine state and generates output
    // messages available through log_entries() and
    // apply_entries().
    template <typename Message>
    void step(server_id from, Message&& msg);

    void stop();
};


template <typename Message>
void fsm::step(server_id from, Message&& msg) {
    static_assert(std::is_rvalue_reference<decltype(msg)>::value, "must be rvalue");

    // Reset election timer.
    _election_elapsed = 0;

    auto visitor = [this, from, msg = std::move(msg)](auto state) mutable {
        using State = decltype(state);

        // 3.3. Raft basics.
        //
        // Current terms are exchanged whenever servers
        // communicate; if one server’s current term is smaller
        // than the other’s, then it updates its current term to
        // the larger value. If a candidate or leader discovers
        // that its term is out of date, it immediately reverts to
        // follower state. If a server receives a request with
        // a stale term number, it rejects the request.
        if (msg.current_term > _current_term) {
			logger.trace("{} [term: {}] received a message with higher term from {} [term: {}]",
				_my_id, _current_term, from, msg.current_term);

            if constexpr (std::is_same_v<Message, append_request_recv>) {
                become_follower(from);
            } else {
                become_follower(server_id{});
            }
            update_current_term(msg.current_term);

        } else if (msg.current_term < _current_term) {
            if constexpr (std::is_same_v<Message, append_request_recv>) {
                // Instructs the leader to step down.
                append_reply reply{_current_term, append_reply::rejected{msg.prev_log_idx, _log.last_idx()}};
                send_to(from, std::move(reply));
            } else {
                // Ignore other cases
                logger.trace("{} [term: {}] ignored a message with lower term from {} [term: {}]",
                    _my_id, _current_term, from, msg.current_term);
            }
            return;
        }

        if constexpr (std::is_same_v<Message, append_request_recv>) {
            // Got AppendEntries RPC from self
            assert((!std::is_same_v<State, leader>));
            // 3.4 Leader Election
            // While waiting for votes, a candidate may receive an AppendEntries
            // RPC from another server claiming to be leader. If the
            // leader’s term (included in its RPC) is at least as large as the
            // candidate’s current term, then the candidate recognizes the
            // leader as legitimate and returns to follower state.
            if constexpr (std::is_same_v<State, candidate>) {
                become_follower(from);
            }
            append_entries(from, std::move(msg));
        } else if constexpr (std::is_same_v<Message, append_reply>) {
            if constexpr (!std::is_same_v<State, leader>) {
                // Ignore stray reply if we're not a leader.
                return;
            }
            append_entries_reply(from, std::move(msg));
        } else if constexpr (std::is_same_v<Message, vote_request>) {
            request_vote(from, std::move(msg));
        } else if constexpr (std::is_same_v<Message, vote_reply>) {
            if constexpr (!std::is_same_v<State, candidate>) {
                // Ignore stray reply if we're not a candidate.
                return;
            }
            request_vote_reply(from, std::move(msg));
        }
    };

    std::visit(visitor, _state);
}

} // namespace raft


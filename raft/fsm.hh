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
public:
    explicit fsm(server_id id);

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
    void become_leader() {
        assert(_state != server_state::LEADER);
        _state = server_state::LEADER;
        _current_leader = _my_id;
    }
    void become_follower(server_id leader) {
        assert(_state != server_state::FOLLOWER);
        _current_leader = leader;
        _state = server_state::FOLLOWER;
    }
};

} // namespace raft


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

#include <map>
#include <seastar/core/pipe.hh>
#include "fsm.hh"

namespace raft {

class rpc_server_interface {
public:
    virtual ~rpc_server_interface() {};

    // This function is called by append_entries RPC
    virtual void append_entries(server_id from, append_request_recv append_request) = 0;

    // This function is called by append_entries_reply RPC
    virtual void append_entries_reply(server_id from, append_reply reply) = 0;

    // This function is called to handle RequestVote RPC.
    virtual void request_vote(server_id from, vote_request vote_request) = 0;
    // Handle response to RequestVote RPC
    virtual void request_vote_reply(server_id from, vote_reply vote_reply) = 0;

    // Apply incoming snapshot, future resolves when application is complete
    virtual future<> apply_snapshot(server_id from, install_snapshot snp) = 0;
};

enum class wait_type {
    committed,
    applied
};

class node {
public:
    virtual ~node() {}
    // Adds command to replicated log
    // Returned future is resolved depending on wait_type parameter:
    //  'committed' - when the entry is committed
    //  'applied'   - when the entry is applied (happens after it is committed)
    // The function has to be called on a leader, throws not_a_leader exception otherwise.
    // May fail because of internal error or because leader changed and an entry was replaced
    // by another leader. In the later case dropped_entry exception will be returned.
    virtual future<> add_entry(command command, wait_type type) = 0;

    // Adds new server to a cluster. If a node is already a member
    // of the cluster does nothing Provided node_info is passed to
    // rpc::new_node() on each node in a cluster as it learns
    // about joining node. Connection info can be passed there.
    // Can be called on a leader only, otherwise throws not_a_leader.
    // Cannot be called until previous add/remove server completes
    // otherwise conf_change_in_progress exception is returned.
    virtual future<> add_server(server_id id, bytes node_info, clock_type::duration timeout) = 0;

    // Removes a server from the cluster. If the server is not a member
    // of the cluster does nothing. Can be called on a leader only
    // otherwise throws not_a_leader.
    // Cannot be called until previous add/remove server completes
    // otherwise conf_change_in_progress exception is returned.
    virtual future<> remove_server(server_id id, clock_type::duration timeout) = 0;

    // Load persisted state and start background work that needs
    // to run for this Raft server to function; The object cannot
    // be used until the returned future is resolved.
    virtual future<> start() = 0;

    // Stop this Raft server, all submitted but not completed
    // operations will get an error and callers will not be able
    // to know if they succeeded or not. If this server was
    // a leader it will relinquish its leadership and cease
    // replication.
    virtual future<> abort() = 0;

    // This function needs to be called before attempting read
    // from the local state machine. The read can proceed only if
    // the future returned by the function resolved successfully.
    // If called not on a leader it  not_a_leader error. After calling
    // this function the result of all completed
    // add_entries(wait_type::applied) can be observed by direct
    // access to the local state machine.
    virtual future<> read_barrier() = 0;

    // Ad hoc functions for testing

    virtual void make_me_leader() = 0;
};

std::unique_ptr<node> create_server(server_id uuid, std::unique_ptr<rpc> rpc, std::unique_ptr<state_machine> state_machine,
        std::unique_ptr<storage> storage);

} // namespace raft


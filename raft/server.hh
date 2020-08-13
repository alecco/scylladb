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
#include "raft.hh"

namespace raft {

// A single uniquely identified participant of the Raft group.
class server {
public:
    explicit server(server_id uuid, std::unique_ptr<rpc> rpc,
        std::unique_ptr<state_machine> state_machine,
        std::unique_ptr<storage> storage);

    server(server&&) = delete;

    enum class wait_type {
        committed,
        applied
    };

    // Adds command to replicated log
    // Returned future is resolved depending on wait_type parameter:
    //  'committed' - when the entry is committed
    //  'applied'   - when the entry is applied (happens after it is commited)
    // The function has to be called on a leader, throws not_a_leader exception otherwise.
    // May fail because of internal error or because leader changed and an entry was replaced
    // by another leader. In the later case dropped_entry exception will be returned.
    future<> add_entry(command command, wait_type type);

    // This function is called by append_entries RPC
    void append_entries(server_id from, append_request_recv append_request);

    // This function is called by append_entries_reply RPC
    void append_entries_reply(server_id from, append_reply reply);

    // This function is called to handle RequestVote RPC.
    void request_vote(server_id from, vote_request vote_request);
    // Handle response to RequestVote RPC
    void request_vote_reply(server_id from, vote_reply vote_reply);

    // Adds new server to a cluster. If a node is already a member
    // of the cluster does nothing Provided node_info is passed to
    // rpc::new_node() on each node in a cluster as it learns
    // about joining node. Connection info can be passed there.
    // Can be called on a leader only, otherwise throws not_a_leader.
    // Cannot be called until previous add/remove server completes
    // otherwise conf_change_in_progress exception is returned.
    future<> add_server(server_id id, bytes node_info, clock_type::duration timeout);

    // Removes a server from the cluster. If the server is not a member
    // of the cluster does nothing. Can be called on a leader only
    // otherwise throws not_a_leader.
    // Cannot be called until previous add/remove server completes
    // otherwise conf_change_in_progress exception is returned.
    future<> remove_server(server_id id, clock_type::duration timeout);

    // Load persisted state and start background work that needs
    // to run for this Raft server to function; The object cannot
    // be used until the returned future is resolved.
    future<> start();

    // Stop this Raft server, all submitted but not completed
    // operations will get an error and callers will not be able
    // to know if they succeeded or not. If this server was
    // a leader it will relinquish its leadership and cease
    // replication.
    future<> abort();

    // This function needs to be called before attempting read
    // from the local state machine. The read can proceed only if
    // the future returned by the function resolved successfully.
    // If called not on a leader it  not_a_leader error. After calling
    // this function the result of all completed
    // add_entries(wait_type::applied) can be observed by direct
    // access to the local state machine.
    future<> read_barrier();

private:
    std::unique_ptr<rpc> _rpc;
    std::unique_ptr<state_machine> _state_machine;
    std::unique_ptr<storage> _storage;
    // id of this server
    server_id _id;
};

} // namespace raft


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

#include "fsm.hh"

namespace raft {

// A single uniquely identified participant of the Raft group.
// Thread-safety: safe to use within a single Seastar shard,
// calls from different Seastar shards must be synchronized.
class server {
public:
    explicit server(fsm fsm, std::unique_ptr<rpc> rpc, std::unique_ptr<state_machine> state_machine, std::unique_ptr<storage> storage);
    server(server&&) = delete;

    // Adds command to replicated log
    // Returned future is resolved when command is committed (but not necessary applied yet)
    // The function has to be called on a leader, throws otherwise
    // May fail because of internal error or because leader changed and an entry was replaced
    // by another leader
    future<> add_entry(command command);

    // This function is called by append_entries RPC and a reply is forwarded to a remote server
    // Returned future is resolved when either request is rejected or data is persisted
    future<append_reply> append_entries(server_id from, append_request_recv&& append_request);

    // This function is called by request vote RPC and a reply is forwarded to a remote server
    future<vote_reply> request_vote(server_id from, vote_request&& vote_request);

    // Adds new server to a cluster. If a node is already a member of the cluster does nothing
    // Provided node_info is passed to rpc::new_node() on each node in a cluster as it learns about
    // joining node. Connection info can be passed there.
    // Can be called on a leader only otherwise throws
    future<> add_server(server_id id, bytes node_info, clock_type::duration timeout);

    // Removes a server from a cluster. If a node is not a member of the cluster does nothing
    // Can be called on a leader only otherwise throws
    future<> remove_server(server_id id, clock_type::duration timeout);

    // Load persisted state and starts background work that needs
    // to run for this raft server to function; The object cannot
    // be used until the returned future is resolved.
    future<> start();

    // Stop this raft server, all submitted, but not completed
    // operations will get an error and callers will not be able
    // to know if they succeeded or not. If this server was
    // a leader it will relinquish its leadership and cease
    // replication.
    future<> stop();

    // Ad hoc functions for testing

    future<> make_me_leader();
private:
    std::unique_ptr<rpc> _rpc;
    std::unique_ptr<state_machine> _state_machine;
    std::unique_ptr<storage> _storage;
    // Protocol deterministic finite-state machine
    fsm _fsm;

    // the sate that is valid only on leader
    struct leader_state {
        // signaled on a leader each time an entry is added to the log
        seastar::condition_variable _log_entry_added;
        // on a leader holds futures of all replication fibers
        std::vector<future<>> _replicatoin_fibers;
        // status of a keepalive fiber
        future<> keepalive_status = make_ready_future<>();
    };

    std::optional<leader_state> _leader_state;

    struct commit_status {
        term_t term; // term the entry was added with
        promise<> committed; // notify commit even here
    };

    // entries that have a waiter that needs to be notified when committed
    std::map<index_t, commit_status> _awaited_commits;

    // constantly replicate the log to a given node.
    // Started when a server becomes a leader
    // Stopped when a server stopped being a leader
    future<> replication_fiber(server_id id, follower_progress& state);

    // Called to commit entries (on a leader or otherwise).
    void commit_entries();

    // each leadership transition is serialized by this future
    future<> _leadership_transition = make_ready_future();

    // Called when a node wins an election
    future<> start_leadership();

    // Called when a node stops being a leader
    // a future resolves when all the leader background work is stopped
    future<> stop_leadership();

    // This fibers persists unstable log entries on disk.
    future<> log_fiber();

    // This fiber runs in the background and applies committed entries.
    future<> applier_fiber();

    // Signaled when there is an entry to persist in the log
    seastar::condition_variable _log_entries;
    // signaled when there is an entry to apply
    seastar::condition_variable _apply_entries;
    future<> _applier_status = make_ready_future<>();
    future<> _log_status = make_ready_future<>();

    future<> keepalive_fiber();
};

} // namespace raft


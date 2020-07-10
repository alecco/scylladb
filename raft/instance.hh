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
#include "raft.hh"

namespace raft {

class instance {
public:
    explicit instance(node_id id, std::unique_ptr<rpc> rpc, std::unique_ptr<state_machine> state_machine, std::unique_ptr<storage> storage);
    instance(instance&&) = delete;

    // Returns current leader for this raft group
    node_id get_current_leader() const {
        return _current_leader;
    }

    // Adds command to replicated log
    // Returned future is resolved when command is committed (but not necessary applied yet)
    // The function has to be called on a leader, throws otherwise
    // May fail because of internal error or because leader changed and an entry was replaced
    // by another leader
    future<> add_entry(command command);

    // This function is called by append_entries rpc and a reply is forwarded to a remote instance
    // Returned future is resolved when either request is rejected or data is persisted
    future<append_reply> append_entries(node_id from, append_request_recv&& append_request);

    // This function is called by request vote rpc and a reply is forwarded to a remote instance
    future<vote_reply> request_vote(node_id from, vote_request&& vote_request);

    // Adds new instance to a cluster. If a node is already a member of the cluster does nothing
    // Provided node_info is passed to rpc::new_node() on each node in a cluster as it learns about
    // joining node. Connection info can be passed there.
    // Can be called on a leader only otherwise throws
    future<> add_node(node_id id, bytes node_info, clock_type::duration timeout);

    // Removes a node from a cluster. If a node is not a member of the cluster does nothing
    // Can be called on a leader only otherwise throws
    future<> remove_node(node_id id, clock_type::duration timeout);

    // Load persisted state and starts background work that need to run for raft instance to function;
    // The raft instance cannot be used untill the returned future is resolved
    future<> start();

    // stop this raft instance, all submitted, but not completed operation will get an error
    // and a caller will not be able to know if they succeeded or not. If an instance was a leader
    // it will relingiush its leadership and cease replication
    future<> stop();

    // Ad hoc functions for testing

    // Set cluster configuration, in real app should be taken from log
    void set_config(configuration config);
    future<> make_me_leader();
    void set_committed(index_t idx);
private:
    std::unique_ptr<rpc> _rpc;
    std::unique_ptr<state_machine> _state_machine;
    std::unique_ptr<storage> _storage;

    // id of this node
    node_id _my_id;
    // id of a current leader
    node_id _current_leader;
    // currently committed configuration
    configuration _commited_config;
    // currently used configuration, may be different from committed during configuration change
    configuration _current_config;

    // commit_index && last_applied are volatile state
    // index of highest log entry known to be committed
    index_t _commit_index = index_t(0);
    // index of highest log entry applied to the state machine
    index_t _last_applied =index_t(0);

    // _current_term, _voted_for && _log are persisted in storage
    // latest term instance has seen
    term_t _current_term = term_t(0);
    // candidateId that received vote in current term (or null if none)
    std::optional<node_id> _voted_for;
    // log entries; each entry contains command for state machine,
    // and term when entry was received by leader
    log _log;

    struct leader_per_node_state {
        // index of the next log entry to send to that serve
        index_t next_idx;
        // index of highest log entry known to be replicated on the node
        index_t match_idx;
    };

    // the sate that is valid only on leader
    struct leader_state {
        // signaled on a leader each time an entry is added to the log
        seastar::condition_variable _log_entry_added;
        // a state for each follower
        std::unordered_map<node_id, leader_per_node_state> _nodes_state;
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

    enum class state : uint8_t {
        LEADER,
        FOLLOWER,
        CANDIDATE,
    };

    // What state the node is
    state _state = state::FOLLOWER;

    bool is_leader() const {
        assert(_state != state::LEADER || _my_id == _current_leader);
        return _state == state::LEADER;
    }

    // constantly replicate the log to a given node.
    // Started when a node becomes a leader
    // Stopped when a node stopped been a leader
    future<> replication_fiber(node_id node, leader_per_node_state&);

    // called when one of the replicas advanced its match index
    // so it may be the case that some entries are committed now
    void check_committed();

    // calculates current quorum
    size_t quorum() {
        return _current_config.nodes.size() / 2 + 1;
    }

    // called when next entry is committed (on a leader or otherwise)
    void commit_entries(index_t);

    // each leadership transition is serialized by this future
    future<> _leadership_transition = make_ready_future();

    // called when a node wins an election
    future<> become_leader();

    // called when a node stops been a leader
    // a future resolves when all the leader background work is stopped
    future<> drop_leadership(state);

    // called when the node become a follower
    void become_follower();

    // set and persists current term
    future<> set_current_term(term_t term);

    // this fibers run in a background and applies commited entries
    future<> applier_fiber();
    // signaled when there is an entry to apply
    seastar::condition_variable _apply_entries;
    future<> _applier_status = make_ready_future<>();

    future<> keepalive_fiber();
};

} // namespace raft


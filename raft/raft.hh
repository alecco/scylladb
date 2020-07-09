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

#include <map>
#include <vector>
#include <unordered_map>
#include <functional>
#include <boost/container/deque.hpp>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/condition-variable.hh>
#include "bytes_ostream.hh"
#include "utils/UUID.hh"

namespace raft {
// Keeps user defined command. A user is responsible to serialize a state machine operation
// into it before passing to raft and deserialize in apply() before applying
using command = bytes_ostream;
using command_cref = std::reference_wrapper<const command>;

namespace internal {

template<typename Tag>
class typed_uint64 {
    uint64_t _val;
public:
    typed_uint64() : _val(0) {}
    explicit typed_uint64(uint64_t v) : _val(v) {}
    typed_uint64(const typed_uint64&) = default;
    typed_uint64(typed_uint64&&) = default;
    typed_uint64& operator=(const typed_uint64&) = default;
    auto operator<=>(const typed_uint64&) const = default;

    uint64_t operator()() const {
        return _val;
    };
    operator uint64_t() const {
        return _val;
    }
    typed_uint64& operator++() { // pre increment
        ++_val;
        return *this;
    }
    typed_uint64 operator++(int) { // post increment
        uint64_t v = _val++;
        return typed_uint64(v);
    }
    typed_uint64& operator--() { // pre decrement
        --_val;
        return *this;
    }
    typed_uint64 operator--(int) { // post decrement
        uint64_t v = _val--;
        return typed_uint64(v);
    }
    typed_uint64 operator+(const typed_uint64& o) const {
        return typed_uint64(_val + o._val);
    }
    typed_uint64 operator-(const typed_uint64& o) const {
        return typed_uint64(_val - o._val);
    }
};

template<typename Tag>
struct generic_id {
    utils::UUID id;
    bool operator==(const generic_id& o) const {
        return id == o.id;
    }
};
template<typename Tag>
std::ostream& operator<<(std::ostream& os, const generic_id<Tag>& id) {
    os << id.id;
    return os;
}

}
}

namespace std {

template<typename Tag>
struct hash<raft::internal::generic_id<Tag>> {
    size_t operator()(const raft::internal::generic_id<Tag>& id) const {
        return hash<utils::UUID>()(id.id);
    }
};
}

namespace raft {

// This is user provided id for a snapshot
using snapshot_id = internal::generic_id<struct shapshot_id_tag>;
// Unique identifier of a node in a raft group
using node_id = internal::generic_id<struct node_id_tag>;

using term_t = internal::typed_uint64<struct term_tag>;
using index_t = internal::typed_uint64<struct index_tag>;

using clock_type = lowres_clock;

struct node {
    node_id id;
    bytes info;
};

struct configuration {
    std::vector<node> nodes;
};

struct log_entry {
    term_t term;
    index_t index;
    std::variant<command, configuration> data;
};

// this class represents raft log in memory
// note that first value index is 1
// new entries are added at the back
// entries may be dropped form the beginning by snapshotting
// and from the end by new leader replacing stale entries
class log {
    // we need something that can be truncated form both sides.
    // std::deque move constructor is not nothrow hence cannot be used
    boost::container::deque<log_entry> _log;
    // prevents concurrent write to the log
    // it is unique_ptr to make log class movable (smeaophore is not)
    std::unique_ptr<seastar::semaphore> _log_lock = std::make_unique<seastar::semaphore>(1);
    // the index of the first entry in the log (index starts from 1)
    // will be increased by log gc
    index_t _log_starting_index = index_t(1);
public:
    future<seastar::semaphore_units<>> lock();
    log_entry& operator[](size_t i);
    // reserve n additional entries
    void ensure_capacity(size_t n);
    void emplace_back(log_entry&& e);
    // return true if in memory log is empty
    bool empty() const;
    index_t next_idx() const;
    index_t last_idx() const;
    void truncate_head(size_t i);
};

struct error : public std::runtime_error {
    error(std::string error) : std::runtime_error(error) {}
};

struct not_leader : public error {
    node_id leader;
    not_leader(node_id l) : error("Not a leader"), leader(l) {}
};

struct dropped_entry : public error {
    dropped_entry() : error("Entry was dropped because of a leader change") {}
};

struct stopped_error : public error {
    stopped_error() : error("Raft instance is stooped") {}
};

struct snapshot {
    // Index and term of last entry in the snapshot
    index_t index;
    term_t term;
    // The committed configuration in the snapshot
    configuration config;
    // Id of the snapshot.
    snapshot_id id;
};

using log_entry_cref = std::reference_wrapper<const log_entry>;

class rpc;
class storage;
class state_machine;

class instance {
    struct append_request_base {
        // leader's term
        term_t current_term;
        // so follower can redirect clients
        // In practice we do not need it since we should know sender's id anyway
        node_id leader_id;
        // index of log entry immediately preceding new ones
        index_t prev_log_index;
        // term of prev_log_index entry
        term_t prev_log_term;
        // leader's commit_index
        index_t leader_commit;
    };
public:
    struct append_request_send : public append_request_base {
        // log entries to store (empty for heartbeat; may send more than one for efficiency)
        std::vector<log_entry_cref> entries;
    };
    struct append_request_recv : public append_request_base {
        // same as for append_request_send but unlike it here the message owns the entries
        std::vector<log_entry> entries;
    };
    struct append_reply {
        // current term, for leader to update itself
        term_t current_term;
        // 'true' if all entries were successfully appended
        // 'false' if a receivers term is larger or there was a mismatch in index/term
        bool result;
    };

    // this is an extension of Raft used for keepalive aggregation between multiple groups
    struct keep_alive {
        // leader's term
        term_t current_term;
        // so follower can redirect clients
        // here it has to be included since this will be sent not
        // as point to point message but as part of an aggregated one.
        node_id leader_id;
        // leader's commit_index
        index_t leader_commit;
    };

    struct vote_request {
        // candidate’s term
        term_t term;
        // candidate requesting vote
        node_id candidate_id;
        // index of candidate's last log entry
        index_t last_log_idx;
        // term of candidate's last log entry
        term_t last_log_term;
        // true if pre-vote
        // bool pre_vote
    };

    struct vote_reply {
        // current term, for candidate to update itself
        term_t term;
        // true means candidate received vote
        bool vote_granted;
    };

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

class state_machine {
public:
    virtual ~state_machine() {}
    // This is called after entries are committed (replicated to at least quorum of nodes).
    // Multiple entries can be committed simultaneously.
    // Will be eventually called on all replicas.
    // Raft owns the data since it may be still replicating.
    // Raft will not call another apply until the retuned future will not become ready.
    virtual future<> apply(const std::vector<command_cref> command) = 0;

    // The function suppose to take a snapshot of a state machine
    // To be called during log compaction or when a leader brings
    // a lagging follower up-to-date
    virtual future<snapshot_id> take_snaphot() = 0;

    // The function drops a snapshot with a provided id
    virtual void drop_snapshot(snapshot_id id) = 0;

    // reload state machine from a snapshot id
    // To be used by a restarting node or by a follower that
    // catches up to a leader
    virtual future<> load_snapshot(snapshot_id id) = 0;
};

class rpc {
protected:
    // Pointer to the instance. Needed for passing rpc messages.
    instance* _instance = nullptr;
public:
    virtual ~rpc() {}

    // Send a snapshot snap to a node node_id.
    // A returned future is resolved when snapshot is sent and successfully applied
    // by a receiver
    virtual future<> send_snapshot(node_id node_id, snapshot snap) = 0;

    // Sends provided append_request to supplied node and waits for a reply
    virtual future<instance::append_reply> send_append_entries(node_id id, const instance::append_request_send& append_request) = 0;

    // Sends vote requests and returns vote reply
    virtual future<instance::vote_reply> send_request_vote(node_id id, const instance::vote_request& vote_request) = 0;

    // This is an extension of Raft used for keepalive aggregation between multiple groups
    // This RPC does not return anything since it will be aggregated for many groups
    // but this means that it cannot reply with larger term and convert a leader that sends it
    // to a follower. A new leader that detects stale leader by processing this message needs to
    // contact it explicitly by issuing empty send_append_entries call.
    virtual void send_keepalive(node_id id, instance::keep_alive keep_alive) = 0;

    // When new node is learn this function is called with the info about the node
    virtual void add_node(node_id id, bytes node_info) = 0;

    // When a node is removed from local config this call is executed
    virtual void remove_node(node_id id) = 0;
private:
    void set_instance(raft::instance& instance) { _instance = &instance; }
    friend instance;
};

class storage {
public:
    virtual ~storage() {}
    // Persist given term and resets vote atomically
    virtual future<> store_term(term_t term) = 0;

    // Load persisted term
    virtual future<term_t> load_term() = 0;

    // Persist given vote
    virtual future<> store_vote(node_id vote) = 0;

    // Load persisted vote
    virtual future<std::optional<node_id>> load_vote() = 0;

    // Persist given snapshot and drops all but 'preserve_log_entries'
    // entries from the raft log starting from the beginning
    // This will rewrite previously persisted snapshot
    virtual future<> store_snapshot(snapshot snap, size_t preserve_log_entries) = 0;

    // Load a saved snapshot
    // This only loads it into memory, but does not apply yet
    // To apply call 'state_machine::load_snapshot(snapshot::id)'
    virtual future<snapshot> load_snapshot() = 0;

    // Persist given log entries
    virtual future<> store_log_entries(const std::vector<log_entry>& entries) = 0;

    // Persist given log entry
    virtual future<> store_log_entry(const log_entry& entry) = 0;

    // Load saved raft log
    virtual future<log> load_log() = 0;

    // Truncate all entries with index greater that idx in the log
    // and persist it
    virtual future<> truncate_log(index_t idx) = 0;
};

} // namespace raft


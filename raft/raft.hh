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

#include <vector>
#include <functional>
#include <boost/container/deque.hpp>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/future.hh>
#include <seastar/util/log.hh>
#include "bytes_ostream.hh"
#include "utils/UUID.hh"

namespace raft {
// Keeps user defined command. A user is responsible to serialize
// a state machine operation into it before passing to raft and
// deserialize in apply() before applying.
using command = bytes_ostream;
using command_cref = std::reference_wrapper<const command>;

extern seastar::logger logger;

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
    explicit operator bool() const { return _val != 0; }

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
    friend std::ostream& operator<<(std::ostream& os, const typed_uint64<Tag>& u) {
        os << u._val;
        return os;
    }
};

template<typename Tag>
struct tagged_id {
    utils::UUID id;
    bool operator==(const tagged_id& o) const {
        return id == o.id;
    }
    // The default constructor sets the id to nil, which is
    // guaranteed to not match any valid id.
    bool is_nil() const {
        return id.get_least_significant_bits() == 0 && id.get_most_significant_bits() == 0;
    }
    explicit operator bool() const { return !is_nil(); }
};

template<typename Tag>
std::ostream& operator<<(std::ostream& os, const tagged_id<Tag>& id) {
    os << id.id;
    return os;
}

} // end of namespace internal
} // end of namespace raft

namespace std {

template<typename Tag>
struct hash<raft::internal::tagged_id<Tag>> {
    size_t operator()(const raft::internal::tagged_id<Tag>& id) const {
        return hash<utils::UUID>()(id.id);
    }
};
} // end of namespace std

namespace raft {

// This is user provided id for a snapshot
using snapshot_id = internal::tagged_id<struct shapshot_id_tag>;
// Unique identifier of a server in a Raft group
using server_id = internal::tagged_id<struct server_id_tag>;

using term_t = internal::typed_uint64<struct term_tag>;
using index_t = internal::typed_uint64<struct index_tag>;

using clock_type = lowres_clock;

struct server_address {
    server_id id;
    // Opaque connection properties
    bytes info;
};

struct configuration {
    std::vector<server_address> servers;

    configuration(std::initializer_list<server_id> ids) {
        servers.reserve(ids.size());
        for (auto&& id : ids) {
            servers.emplace_back(server_address{std::move(id)});
        }
    }
    configuration() = default;
};

struct log_entry {
    // Dummy entry is used when a leader needs to commit an entry
    // (after leadership change for instance)but there is nothing
    // else to commit.
    struct dummy {};
    term_t term;
    index_t idx;
    std::variant<command, configuration, dummy> data;
};

using log_entry_ptr = seastar::lw_shared_ptr<const log_entry>;

struct error : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct not_leader : public error {
    server_id leader;
    explicit not_leader(server_id l) : error("Not a leader"), leader(l) {}
};

struct dropped_entry : public error {
    dropped_entry() : error("Entry was dropped because of a leader change") {}
};

struct stopped_error : public error {
    stopped_error() : error("Raft instance is stopped") {}
};

struct snapshot {
    // Index and term of last entry in the snapshot
    index_t idx = index_t(0);
    term_t term = term_t(0);
    // The committed configuration in the snapshot
    configuration config;
    // Id of the snapshot.
    snapshot_id id;
};

using log_entry_cref = std::reference_wrapper<const log_entry>;

struct append_request_base {
    // The leader's term.
    term_t current_term;
    // So that follower can redirect clients
    // In practice we do not need it since we should know sender's id anyway.
    server_id leader_id;
    // Index of the log entry immediately preceding new ones
    index_t prev_log_idx;
    // Term of prev_log_idx entry.
    term_t prev_log_term;
    // The leader's commit_idx.
    index_t leader_commit_idx;
};

struct append_request_send : public append_request_base {
    // Log entries to store (empty for heartbeat; may send more
    // than one for efficiency).
    std::vector<log_entry_cref> entries;
};
struct append_request_recv : public append_request_base {
    // Same as for append_request_send but unlike it here the
    // message owns the entries.
    std::vector<log_entry> entries;
};
struct append_reply {
    struct rejected {
        // Index of non matching entry that caused the request
        // to be rejected.
        index_t non_matching_idx;
        // Last index in the follower's log, can be used to find next
        // matching index more efficiently.
        index_t last_idx;
    };
    struct accepted {
        // Last entry that was appended (may be smaller than max log index
        // in case follower's log is longer and appended entries match).
        index_t last_new_idx;
    };
    // Current term, for leader to update itself.
    term_t current_term;
    std::variant<rejected, accepted> result;
};

// This is an extension of Raft used for keepalive aggregation
// between multiple groups.
struct keep_alive {
    // The leader's term.
    term_t current_term;
    // So that the follower can redirect clients.
    // Here it has to be included since this will be sent not as
    // a point to point message but as part of an aggregated one.
    server_id leader_id;
    // The leader's commit_idx.
    index_t leader_commit_idx;
};

struct vote_request {
    // The candidate’s term.
    term_t current_term;
    // The index of the candidate's last log entry.
    index_t last_log_idx;
    // The term of the candidate's last log entry.
    term_t last_log_term;
};

struct vote_reply {
    // Current term, for the candidate to update itself.
    term_t current_term;
    // True means the candidate received a vote.
    bool vote_granted;
};

using rpc_message = std::variant<keep_alive, append_request_send, append_reply, vote_request, vote_reply, snapshot>;

// This class represents the Raft log in memory.
//
// The value of the first index is 1.
// New entries are added at the back.
//
// Entries are persisted locally after they are added.  Entries may be
// dropped from the beginning by snapshotting and from the end by
// a new leader replacing stale entries. Any exception thrown by
// any function leaves the log in a consistent state.
class log {
    // Snapshot of the prefix of the log.
    snapshot _snapshot;
    // We need something that can be truncated form both sides.
    // std::deque move constructor is not nothrow hence cannot be used
    boost::container::deque<log_entry_ptr> _log;
    // Index of the last stable (persisted) entry in the log.
    index_t _stable_idx = index_t(0);

private:
    void truncate_head(index_t i);
    log_entry_ptr& get_entry(index_t);
public:
    log() = default;
    log(snapshot snp) : _snapshot(std::move(snp)) {}
    log_entry_ptr& operator[](size_t i);
    // Add an entry to the log.
    void emplace_back(log_entry&& e);
    // Mark all entries up to this index as stable.
    void stable_to(index_t idx);
    // Return true if in memory log is empty.
    bool empty() const;
    // 3.6.1 Election restriction.
    // The voter denies its vote if its own log is more up-to-date
    // than that of the candidate.
    bool is_up_to_date(index_t idx, term_t term) const;
    index_t next_idx() const;
    index_t last_idx() const;
    index_t stable_idx() const {
        return _stable_idx;
    }
    index_t start_idx() const;
    term_t last_term() const;

    // The function returns current snapshot state of the log
    const snapshot& get_snapshot() {
        return _snapshot;
    }

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
    // @retval disengaged optional - there is a match
    // @retval non matching term - the follower's log doesn't
    //                             match the leader's.
    std::optional<term_t> match_term(index_t idx, term_t term) const;

    // Called on a follower to append entries from a leader.
    // @retval return an index of last appended entry
    index_t maybe_append(std::vector<log_entry>&& entries);

    friend std::ostream& operator<<(std::ostream& os, const log& l);
};

class rpc;
class storage;

// Any of the functions may return an error, but it will kill the
// raft instance that uses it.
class state_machine {
public:
    virtual ~state_machine() {}
    // This is called after entries are committed (replicated to
    // at least quorum of servers).  Multiple entries can be
    // committed simultaneously.
    // Will be eventually called on all replicas.
    // Raft owns the data since it may be still replicating.
    // Raft will not call another apply until the retuned future
    // will not become ready.
    virtual future<> apply(std::vector<command_cref> command) = 0;

    // The function suppose to take a snapshot of a state machine
    // To be called during log compaction or when a leader brings
    // a lagging follower up-to-date
    virtual future<snapshot_id> take_snaphot() = 0;

    // The function drops a snapshot with a provided id
    virtual void drop_snapshot(snapshot_id id) = 0;

    // reload state machine from a snapshot id
    // To be used by a restarting server or by a follower that
    // catches up to a leader
    virtual future<> load_snapshot(snapshot_id id) = 0;

    // stops the state machine instance by aborting the work
    // that can be aborted and waiting for all the rest to complete
    // any unfinished apply/snapshot operation may return an error after
    // this function is called
    virtual future<> abort() = 0;
};

class server;

// It is safe for for rpc implementation to drop any message.
// Error returned by send function will be ignored. All send_()
// functions can be called concurrently, returned future should be
// waited only for back pressure purposes.
class rpc {
protected:
    // Pointer to the server. Needed for passing RPC messages.
    server* _server = nullptr;
public:
    virtual ~rpc() {}

    // Send a snapshot snap to a server server_id.
    // A returned future is resolved when snapshot is sent and
    // successfully applied by a receiver.
    virtual future<> send_snapshot(server_id server_id, const snapshot& snap) = 0;

    // Send provided append_request to the supplied server, does
    // not wait for reply. The returned future resolves when
    // message is sent. It does not mean it was received.
    virtual future<> send_append_entries(server_id id, const append_request_send& append_request) = 0;

    // Send a reply to an append_request. The returned future
    // resolves when message is sent. It does not mean it was
    // received.
    virtual future<> send_append_entries_reply(server_id id, const append_reply& reply) = 0;

    // Send a vote request.
    virtual future<> send_vote_request(server_id id, const vote_request& vote_request) = 0;

    // Sends a reply to a vote request.
    virtual future<> send_vote_reply(server_id id, const vote_reply& vote_reply) = 0;

    // This is an extension of Raft used for keepalive aggregation
    // between multiple groups This RPC does not return anything
    // since it will be aggregated for many groups but this means
    // that it cannot reply with larger term and convert a leader
    // that sends it to a follower. A new leader that detects
    // stale leader by processing this message needs to contact it
    // explicitly by issuing empty send_append_entries call.
    virtual void send_keepalive(server_id id, const keep_alive& keep_alive) = 0;

    // When a new server is learn this function is called with the
    // info about the server.
    virtual void add_server(server_id id, bytes server_info) = 0;

    // When a server is removed from local config this call is
    // executed.
    virtual void remove_server(server_id id) = 0;

    // Stop the RPC instance by aborting the work that can be
    // aborted and waiting for all the rest to complete any
    // unfinished send operation may return an error after this
    // function is called.
    virtual future<> abort() = 0;
private:
    void set_server(raft::server& server) { _server = &server; }
    friend server;
};

// This class represents persistent storage state. If any of the
// function returns an error the Raft instance will be aborted.
class storage {
public:
    virtual ~storage() {}
    // Persist given term and vote.
    // Can be called concurrently with other save-* functions in
    // the storage and with itself but an implementation has to
    // make sure that the result is linearisable.
    virtual future<> store_term_and_vote(term_t term, server_id vote) = 0;

    // Load persisted term and vote.
    // Called during Raft server initialization only, is not run
    // in parallel with store.
    virtual future<std::pair<term_t, server_id>> load_term_and_vote() = 0;

    // Persist given snapshot and drop all but 'preserve_log_entries'
    // entries from the Raft log starting from the beginning.
    // This can overwrite a previously persisted snapshot.
    // Is called only after the previous invocation completes.
    // In other words, it's the caller's responsibility to serialize
    // calls to this function. Can be called in parallel with
    // store_log_entries() but snap.index should belong to an already
    // persisted entry.
    virtual future<> store_snapshot(const snapshot& snap, size_t preserve_log_entries) = 0;

    // Load a saved snapshot.
    // This only loads it into memory, but does not apply yet To
    // apply call 'state_machine::load_snapshot(snapshot::id)'
    // Called during Raft server initialization only, should not
    // run in parallel with store.
    virtual future<snapshot> load_snapshot() = 0;

    // Persist given log entries.
    // Can be called without waiting for previous call to resolve,
    // but internally all writes should be serialized info forming
    // one contiguous log that holds entries in order of the
    // function invocation.
    virtual future<> store_log_entries(const std::vector<log_entry_ptr>& entries) = 0;

    // Load saved Raft log. Called during Raft server
    // initialization only, should not run in parallel with store.
    virtual future<log> load_log() = 0;

    // Truncate all entries with an index greater or equal that
    // the index in the log and persist the truncation. Can be
    // called in parallel with store_log_entries() but internally
    // should be linearized vs store_log_entries():
    // store_log_entries() called after truncate_log() should wait
    // for truncation to complete internally before persisting its
    // entries.
    virtual future<> truncate_log(index_t idx) = 0;

    // Stop the storage instance by aborting the work that can be
    // aborted and waiting for all the rest to complete any
    // unfinished store/load operation may return an error after
    // this function is called.
    virtual future<> abort() = 0;
};

inline auto short_id(raft::server_id id) {
    return id.id.get_least_significant_bits();
}

} // namespace raft


/*
 * Copyright (C) 2021-present ScyllaDB
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

#define BOOST_TEST_NO_MAIN

#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>
#include <seastar/testing/random.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "xx_hasher.hh"
#include "test/raft/helpers.hh"
#include "test/lib/eventually.hh"

// Test Raft library with declarative test definitions
//
//  Each test can be defined by (struct test_case):
//      .nodes                       number of nodes
//      .total_values                how many entries to append to leader nodes (default 100)
//      .initial_term                initial term # for setup
//      .initial_leader              what server is leader
//      .initial_states              initial logs of servers
//          .le                      log entries
//      .initial_snapshots           snapshots present at initial state for servers
//      .updates                     updates to execute on these servers
//          entries{x}               add the following x entries to the current leader
//          new_leader{x}            elect x as new leader
//          partition{a,b,c}         Only servers a,b,c are connected
//          partition{a,leader{b},c} Only servers a,b,c are connected, and make b leader
//          set_config{a,b,c}        Change configuration on leader
//          set_config{a,b,c}        Change configuration on leader
//          check_rpc_config{a,cfg}  Check rpc config of a matches
//          check_rpc_config{[],cfg} Check rpc config multiple nodes matches
//
//      run_test
//      - Creates the servers and initializes logs and snapshots
//        with hasher/digest and tickers to advance servers
//      - Processes updates one by one
//      - Appends remaining values
//      - Waits until all servers have logs of size of total_values entries
//      - Verifies hash
//      - Verifies persisted snapshots
//
//      Tests are run also with 20% random packet drops.
//      Two test cases are created for each with the macro
//          RAFT_TEST_CASE(<test name>, <test case>)

using namespace std::chrono_literals;
using namespace std::placeholders;

extern seastar::logger tlogger;

class hasher_int : public xx_hasher {
public:
    using xx_hasher::xx_hasher;
    void update(int val) noexcept {
        xx_hasher::update(reinterpret_cast<const char *>(&val), sizeof(val));
    }
    static hasher_int hash_range(int max) {
        hasher_int h;
        for (int i = 0; i < max; ++i) {
            h.update(i);
        }
        return h;
    }
};

struct snapshot_value {
    hasher_int hasher;
    raft::index_t idx;
};

struct initial_state {
    raft::server_address address;
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    raft::snapshot snapshot;
    snapshot_value snp_value;
    raft::server::configuration server_config = raft::server::configuration{.append_request_threshold = 200};
};

// For verbosity in test declaration (i.e. node_id{x})
struct node_id {
    size_t id;
};

std::vector<raft::server_id> to_raft_id_vec(std::vector<node_id> nodes);
raft::server_address_set address_set(std::vector<node_id> nodes);

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
struct entries {
    size_t n;
};
struct new_leader {
    size_t id;
};
struct leader {
    size_t id;
};
using partition = std::vector<std::variant<leader,int>>;

// Disconnect 2 servers both ways
struct two_nodes {
    size_t first;
    size_t second;
};
struct disconnect : public two_nodes {};

struct stop {
    size_t id;
};

struct reset {
    size_t id;
    initial_state state;
};

struct wait_log {
    std::vector<size_t> local_ids;
    wait_log(size_t local_id) : local_ids({local_id}) {}
    wait_log(std::initializer_list<size_t> local_ids) : local_ids(local_ids) {}
};

struct set_config_entry {
    size_t node_idx;
    bool can_vote;

    set_config_entry(size_t idx, bool can_vote = true)
        : node_idx(idx), can_vote(can_vote)
    {}
};
using set_config = std::vector<set_config_entry>;

struct config {
    std::vector<node_id> curr;
    std::vector<node_id> prev;
    operator raft::configuration() {
        auto current = address_set(curr);
        auto previous = address_set(prev);
        return raft::configuration{current, previous};
    }
};

using rpc_address_set = std::vector<node_id>;

struct check_rpc_config {
    std::vector<node_id> nodes;
    rpc_address_set addrs;
    check_rpc_config(node_id node, rpc_address_set addrs) : nodes({node}), addrs(addrs) {}
    check_rpc_config(std::vector<node_id> nodes, rpc_address_set addrs) : nodes(nodes), addrs(addrs) {}
};

struct check_rpc_added {
    std::vector<node_id> nodes;
    size_t expected;
    check_rpc_added(node_id node, size_t expected) : nodes({node}), expected(expected) {}
    check_rpc_added(std::vector<node_id> nodes, size_t expected) : nodes(nodes), expected(expected) {}
};

struct check_rpc_removed {
    std::vector<node_id> nodes;
    size_t expected;
    check_rpc_removed(node_id node, size_t expected) : nodes({node}), expected(expected) {}
    check_rpc_removed(std::vector<node_id> nodes, size_t expected) : nodes(nodes), expected(expected) {}
};

using rpc_reset_counters = std::vector<node_id>;

struct tick {
    uint64_t ticks;
};

using update = std::variant<entries, new_leader, partition, disconnect, stop, reset, wait_log,
      set_config, check_rpc_config, check_rpc_added, check_rpc_removed, rpc_reset_counters,
      tick>;

struct log_entry {
    unsigned term;
    std::variant<int, raft::configuration> data;
};

struct initial_log {
    std::vector<log_entry> le;
};

struct initial_snapshot {
    raft::snapshot snap;
};

struct test_case {
    const size_t nodes;
    const size_t total_values = 100;
    uint64_t initial_term = 1;
    const size_t initial_leader = 0;
    const std::vector<struct initial_log> initial_states;
    const std::vector<struct initial_snapshot> initial_snapshots;
    const std::vector<raft::server::configuration> config;
    const std::vector<update> updates;
    size_t get_first_val();
};

std::mt19937 random_generator();
int rand() noexcept;

// Lets assume one snapshot per server
using snapshots = std::unordered_map<raft::server_id, snapshot_value>;
using persisted_snapshots = std::unordered_map<raft::server_id, std::pair<raft::snapshot, snapshot_value>>;

// application of a snaphot with that id will be delayed until snapshot_sync is signaled
extern raft::snapshot_id delay_apply_snapshot;
// sending of a snaphot with that id will be delayed until snapshot_sync is signaled
extern raft::snapshot_id delay_send_snapshot;

template <typename ClockType>
class raft_cluster {
    using apply_fn = std::function<size_t(raft::server_id id, const std::vector<raft::command_cref>& commands, lw_shared_ptr<hasher_int> hasher)>;
    class state_machine;
    class persistence;
    class connected;
    class failure_detector;
    class rpc;
    struct test_server {
        std::unique_ptr<raft::server> server;
        state_machine* sm;
        raft_cluster::rpc* rpc;
    };
    std::vector<test_server> _servers;
    std::unique_ptr<connected> _connected;
    std::unique_ptr<snapshots> _snapshots;
    std::unique_ptr<persisted_snapshots> _persisted_snapshots;
    size_t _apply_entries;
    size_t _next_val;
    bool _packet_drops;
    bool _prevote;
    apply_fn _apply;
    std::unordered_set<size_t> _in_configuration;   // Servers in current configuration
    std::vector<seastar::timer<lowres_clock>> _tickers;
    size_t _leader;
    std::vector<initial_state> get_states(test_case test, bool prevote);
public:
    raft_cluster(test_case test,
            apply_fn apply,
            size_t apply_entries, size_t first_val, size_t first_leader,
            bool prevote, bool packet_drops);
    // No copy
    raft_cluster(const raft_cluster&) = delete;
    raft_cluster(raft_cluster&&) = default;
    future<> stop_server(size_t id);
    future<> reset_server(size_t id, initial_state state); // Reset a stopped server
    size_t size() {
        return _servers.size();
    }
    future<> start_all();
    future<> stop_all();
    future<> wait_all();
    void tick_all();
    void disconnect(size_t id, std::optional<raft::server_id> except = std::nullopt);
    void connect_all();
    void elapse_elections();
    future<> elect_new_leader(size_t new_leader);
    future<> free_election();
    template <typename Clock> void init_raft_tickers();
    void pause_tickers();
    void restart_tickers();
    void cancel_ticker(size_t id);
    void set_ticker_callback(size_t id) noexcept;
    future<> add_entries(size_t n);
    future<> add_remaining_entries();
    future<> wait_log(size_t follower);
    future<> wait_log(::wait_log followers);
    future<> wait_log_all();
    future<> change_configuration(::set_config sc);
    future<> check_rpc_config(::check_rpc_config cc);
    void check_rpc_added(::check_rpc_added expected) const;
    void check_rpc_removed(::check_rpc_removed expected) const;
    void rpc_reset_counters(::rpc_reset_counters nodes);
    future<> reconfigure_all();
    future<> partition(::partition p);
    future<> tick(::tick t);
    future<> stop(::stop server);
    future<> reset(::reset server);
    void disconnect(::disconnect nodes);
    void verify();
private:
    test_server create_server(size_t id, initial_state state);
};

template <typename Clock>
void raft_cluster::init_raft_tickers() {
    _tickers.resize(_servers.size());
    // Only start tickers for servers in configuration
    for (auto s: _in_configuration) {
        _tickers[s].arm_periodic(tick_delta);
        _tickers[s].set_callback([&, s] {
            _servers[s].server->tick();
        });
    }
}

class raft_cluster::state_machine : public raft::state_machine {
    raft::server_id _id;
    apply_fn _apply;
    size_t _apply_entries;
    size_t _seen = 0;
    promise<> _done;
    snapshots* _snapshots;
public:
    lw_shared_ptr<hasher_int> hasher;
    state_machine(raft::server_id id, apply_fn apply, size_t apply_entries,
            snapshots* snapshots):
        _id(id), _apply(std::move(apply)), _apply_entries(apply_entries), _snapshots(snapshots),
        hasher(make_lw_shared<hasher_int>()) {}
    future<> apply(const std::vector<raft::command_cref> commands) override;

    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> abort() override;
    future<> done();
};

class raft_cluster::persistence : public raft::persistence {
    raft::server_id _id;
    initial_state _conf;
    snapshots* _snapshots;
    persisted_snapshots* _persisted_snapshots;
public:
    persistence(raft::server_id id, initial_state conf, snapshots* snapshots,
            persisted_snapshots* persisted_snapshots) : _id(id),
            _conf(std::move(conf)), _snapshots(snapshots),
            _persisted_snapshots(persisted_snapshots) {}
    persistence() {}
    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) { return seastar::sleep(1us); }
    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) {
        (*_persisted_snapshots)[_id] = std::make_pair(snap, (*_snapshots)[_id]);
        tlogger.debug("sm[{}] persists snapshot {}", _id, (*_snapshots)[_id].hasher.finalize_uint64());
        return make_ready_future<>();
    }
    future<raft::snapshot> load_snapshot() override {
        return make_ready_future<raft::snapshot>(_conf.snapshot);
    }
    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) { return seastar::sleep(1us); };
    virtual future<raft::log_entries> load_log() {
        raft::log_entries log;
        for (auto&& e : _conf.log) {
            log.emplace_back(make_lw_shared(std::move(e)));
        }
        return make_ready_future<raft::log_entries>(std::move(log));
    }
    virtual future<> truncate_log(raft::index_t idx) { return make_ready_future<>(); }
    virtual future<> abort() { return make_ready_future<>(); }
};

struct raft_cluster::connected {
    struct connection {
       raft::server_id from;
       raft::server_id to;
       bool operator==(const connection &o) const {
           return from == o.from && to == o.to;
       }
    };

    struct hash_connection {
        std::size_t operator() (const connection &c) const {
            return std::hash<utils::UUID>()(c.from.id);
        }
    };

    // Map of from->to disconnections
    std::unordered_set<connection, hash_connection> disconnected;
    size_t n;
    connected(size_t n) : n(n) { }
    // Cut connectivity of two servers both ways
    void cut(raft::server_id id1, raft::server_id id2) {
        disconnected.insert({id1, id2});
        disconnected.insert({id2, id1});
    }
    // Isolate a server
    void disconnect(raft::server_id id, std::optional<raft::server_id> except = std::nullopt) {
        for (size_t other = 0; other < n; ++other) {
            auto other_id = to_raft_id(other);
            // Disconnect if not the same, and the other id is not an exception
            // disconnect(0, except=1)
            if (id != other_id && !(except && other_id == *except)) {
                cut(id, other_id);
            }
        }
    }
    // Re-connect a node to all other nodes
    void connect(raft::server_id id) {
        for (auto it = disconnected.begin(); it != disconnected.end(); ) {
            if (id == it->from || id == it->to) {
                it = disconnected.erase(it);
            } else {
                ++it;
            }
        }
    }
    void connect_all() {
        disconnected.clear();
    }
    bool operator()(raft::server_id id1, raft::server_id id2) {
        // It's connected if both ways are not disconnected
        return !disconnected.contains({id1, id2}) && !disconnected.contains({id1, id2});
    }
};

class raft_cluster::failure_detector : public raft::failure_detector {
    raft::server_id _id;
    connected* _connected;
public:
    failure_detector(raft::server_id id, connected* connected) : _id(id), _connected(connected) {}
    bool is_alive(raft::server_id server) override {
        return (*_connected)(server, _id);
    }
};

class raft_cluster::rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
    connected* _connected;
    snapshots* _snapshots;
    bool _packet_drops;
    raft::server_address_set _known_peers;
    uint32_t _servers_added = 0;
    uint32_t _servers_removed = 0;
public:
    rpc(raft::server_id id, connected* connected, snapshots* snapshots,
            bool packet_drops) : _id(id), _connected(connected), _snapshots(snapshots),
            _packet_drops(packet_drops) {
        net[_id] = this;
    }
    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id id, const raft::install_snapshot& snap, seastar::abort_source& as);
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request& append_request);
    virtual void send_append_entries_reply(raft::server_id id, const raft::append_reply& reply);
    virtual void send_vote_request(raft::server_id id, const raft::vote_request& vote_request);
    virtual void send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply);
    virtual void send_timeout_now(raft::server_id id, const raft::timeout_now& timeout_now);
    virtual void add_server(raft::server_id id, bytes node_info);
    virtual void remove_server(raft::server_id id);
    virtual future<> abort();
    static void reset_network();

    const raft::server_address_set& known_peers() const;
    void reset_counters();
    uint32_t servers_added() const;
    uint32_t servers_removed() const;
};

std::vector<raft::log_entry> create_log(std::vector<log_entry> list, unsigned start_idx);
size_t apply_changes(raft::server_id id, const std::vector<raft::command_cref>& commands,
        lw_shared_ptr<hasher_int> hasher);

future<> run_test(test_case test, bool prevote, bool packet_drops);

void replication_test(struct test_case test, bool prevote, bool packet_drops);

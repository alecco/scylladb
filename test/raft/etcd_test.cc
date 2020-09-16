#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"


// Test Raft library with declarative test definitions
//
//  For each test defined by
//      (replication_tests)
//      - Name
//      - Number of servers (nodes)
//      - Current term
//      - Initial leader
//      - Initial states for each server (log entries)
//      - Updates to be procesed
//          - append to log (trickles from leader to the rest)
//          - leader change
//          - configuration change
//
//      (run_test)
//      - Create the servers and initialize
//      - Create expected final log state
//      - Process updates one by one
//      - Wait until all servers have logs of size of expected entries
//      - Check server logs
//

using namespace std::chrono_literals;
using namespace std::placeholders;

static seastar::logger tlogger("test");

struct snapshot_value {
    int value = -1;
};

std::mt19937 random_generator() {
    std::random_device rd;
    // In case of errors, replace the seed with a fixed value to get a deterministic run.
    auto seed = rd();
    std::cout << "Random seed: " << seed << "\n";
    return std::mt19937(seed);
}

int rand() {
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    static thread_local auto gen = random_generator();

    return dist(gen);
}

bool drop_replication = false;

// lets assume one snapshot per server
std::unordered_map<raft::server_id, snapshot_value> snapshots;
std::unordered_map<raft::server_id, int> sums;

class state_machine : public raft::state_machine {
public:
    using apply_fn = std::function<future<>(raft::server_id id, promise<>&, const std::vector<raft::command_cref>& commands)>;
private:
    raft::server_id _id;
    apply_fn _apply;
    promise<> _done;
public:
    state_machine(raft::server_id id, apply_fn apply) : _id(id), _apply(std::move(apply)) {}
    future<> apply(const std::vector<raft::command_cref> commands) override {
        return _apply(_id, _done, commands);
    }
    future<raft::snapshot_id> take_snaphot() override {
        snapshots[_id].value = sums[_id];
        return make_ready_future<raft::snapshot_id>(raft::snapshot_id());
    }
    void drop_snapshot(raft::snapshot_id id) override {
        snapshots.erase(_id);
    }
    future<> load_snapshot(raft::snapshot_id id) override {
        sums[_id] = snapshots[_id].value;
         return make_ready_future<>();
    };
    future<> abort() override { return make_ready_future<>(); }

    future<> done() {
        return _done.get_future();
    }
};

struct initial_state {
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    raft::snapshot snapshot;
    snapshot_value snp_value;
    raft::configuration config;    // TODO
    state_machine::apply_fn apply;
};

class storage : public raft::storage {
    initial_state _conf;
public:
    storage(initial_state conf) : _conf(std::move(conf)) {}
    storage() {}
    future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override { co_return seastar::sleep(1us); }
    future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) override { return make_ready_future<>(); }
    future<raft::snapshot> load_snapshot() override {
        return make_ready_future<raft::snapshot>(_conf.snapshot);
    }
    future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) override { co_return seastar::sleep(1us); };
    future<raft::log_entries> load_log() override {
        raft::log_entries log;
        for (auto&& e : _conf.log) {
            log.emplace_back(make_lw_shared(std::move(e)));
        }
        return make_ready_future<raft::log_entries>(std::move(log));
    }
    future<> truncate_log(raft::index_t idx) override { return make_ready_future<>(); }
    future<> abort() override { return make_ready_future<>(); }
};

class rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
public:
    rpc(raft::server_id id) : _id(id) {
        net[_id] = this;
    }
    virtual future<> send_snapshot(raft::server_id id, const raft::install_snapshot& snap) {
        snapshots[id] = snapshots[_id];
        return net[id]->_server->apply_snapshot(_id, std::move(snap));
    }
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request_send& append_request) {
        if (drop_replication && !(rand() % 5)) {
            return make_ready_future<>();
        }
        raft::append_request_recv req;
        req.current_term = append_request.current_term;
        req.leader_id = append_request.leader_id;
        req.prev_log_idx = append_request.prev_log_idx;
        req.prev_log_term = append_request.prev_log_term;
        req.leader_commit_idx = append_request.leader_commit_idx;
        for (auto&& e: append_request.entries) {
            req.entries.push_back(e);
        }
        net[id]->_server->append_entries(_id, std::move(req));
        //co_return seastar::sleep(1us);
        return make_ready_future<>();
    }
    virtual future<> send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
        if (drop_replication && !(rand() % 5)) {
            return make_ready_future<>();
        }
        net[id]->_server->append_entries_reply(_id, std::move(reply));
        return make_ready_future<>();
    }
    virtual future<> send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
        net[id]->_server->request_vote(_id, std::move(vote_request));
        return make_ready_future<>();
    }
    virtual future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
        net[id]->_server->request_vote_reply(_id, std::move(vote_reply));
        return make_ready_future<>();
    }
    virtual void add_server(raft::server_id id, bytes node_info) {}
    virtual void remove_server(raft::server_id id) {}
    virtual future<> abort() { return make_ready_future<>(); }
};

std::unordered_map<raft::server_id, rpc*> rpc::net;

std::pair<std::unique_ptr<raft::node>, state_machine*>
create_raft_server(raft::server_id uuid, initial_state state) {

    auto sm = std::make_unique<state_machine>(uuid, std::move(state.apply));
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid);
    auto mstorage = std::make_unique<storage>(state);
    auto raft = raft::create_server(uuid, std::move(mrpc), std::move(sm), std::move(mstorage));

    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<raft::node>, state_machine*>>> create_cluster(std::vector<initial_state> states) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<raft::node>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        auto uuid = utils::UUID(0, i);
        config.servers.push_back(raft::server_address{uuid});
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = config.servers[i];
        states[i].snapshot.config = config;
        snapshots[s.id] = states[i].snp_value;
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, states[i])).first;
        co_await raft.start();
    }

    co_return std::move(rafts);
}

struct log_entry {
    unsigned term;
    int value;
};

std::vector<raft::log_entry> create_log(std::initializer_list<log_entry> list, unsigned start_idx) {
    std::vector<raft::log_entry> log;

    unsigned i = start_idx;
    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e.value);
        log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(i++), std::move(command)});
    }

    return log;
}

template <typename T>
std::vector<raft::command> create_commands(std::vector<T> list) {
    std::vector<raft::command> commands;
    commands.reserve(list.size());

    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e);
        commands.push_back(std::move(command));
    }

    return commands;
}

future<> apply_changes(std::vector<int> &res, size_t expected, raft::server_id id, promise<>& done,
        const std::vector<raft::command_cref>& commands) {
    tlogger.debug("sm::apply_changes[{}] got {} entries", id, commands.size());

    for (auto&& d : commands) {
        auto is = ser::as_input_stream(d);
        int n = ser::deserialize(is, boost::type<int>());
        res.push_back(n);
        tlogger.debug("{}: apply_changes {}", id, n);
    }
    if (res.size() >= expected) {
        done.set_value();
    }
    return make_ready_future<>();
};

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
using entries = std::vector<int>;
using new_leader = int;
// TODO: config change
using update = std::variant<entries, new_leader>;

struct initial_log {
    std::initializer_list<log_entry> le;
    unsigned start_idx;
};

struct test_case {
    const std::string name;
    const size_t nodes;
    uint64_t initial_term;
    const std::optional<uint64_t> initial_leader;
    const std::vector<struct initial_log> initial_states;
    const std::vector<std::pair<raft::snapshot, int>> initial_snapshots;
    const std::vector<update> updates;
    const std::optional<std::vector<int>> expected;
};

// Run test case (name, nodes, leader, initial logs, updates)
future<int> run_test(test_case test) {
    std::vector<initial_state> states(test.nodes);       // Server initial states
    std::vector<std::vector<int>> committed(test.nodes); // Actual outputs for each server

    size_t leader;
    if (test.initial_leader) {
        leader = *test.initial_leader;
    } else {
        // TODO: trigger election (deterministic)
        leader = 0;
    }

    states[leader].term = raft::term_t{test.initial_term};

    // Build expected result log as initial leader log and subsequent updates
    std::vector<int> expected;                           // Expected output for Raft
    if (test.expected) {
        expected = *test.expected;
    } else {
        if (leader < test.initial_states.size()) {
            // Ignore index as it's internal and not part of output
            for (auto log_initializer: test.initial_states[leader].le) {
                log_entry le(log_initializer);
                if (le.term > test.initial_term) {
                    break;
                }
                expected.push_back(le.value);
            }
        }
        for (auto update: test.updates) {
            if (std::holds_alternative<entries>(update)) {
                auto updates = std::get<entries>(update);
                expected.insert(expected.end(), updates.begin(), updates.end());
            }
        }
    }
    size_t expected_entries = expected.size();

    // Server initial logs, etc
    for (size_t i = 0; i < states.size(); ++i) {
        if (i < test.initial_states.size()) {
            auto state = test.initial_states[i];
            states[i].log = create_log(state.le, state.start_idx);
        } else {
            states[i].log = {};
        }
        if (i < test.initial_snapshots.size()) {
            states[i].snapshot = test.initial_snapshots[i].first;
            states[i].snp_value.value = test.initial_snapshots[i].second;
        }
        states[i].apply = std::bind(apply_changes, std::ref(committed[i]), expected_entries, _1, _2, _3);
    }

    auto rafts = co_await create_cluster(states);

    rafts[leader].first->make_me_leader();

    // Process all updates in order
    for (auto update: test.updates) {
        if (std::holds_alternative<entries>(update)) {
            auto updates = std::get<entries>(update);
            std::vector<raft::command> commands = create_commands<int>(updates);
            co_await seastar::parallel_for_each(commands, [&] (const raft::command cmd) {
                tlogger.debug("Adding command entry on leader {}", leader);
                return rafts[leader].first->add_entry(std::move(cmd), raft::wait_type::committed);
            });
        } else if (std::holds_alternative<new_leader>(update)) {
            leader = std::get<new_leader>(update);

            // co_await new_leader.read_barrier();
            co_await rafts[leader].first->elect_me_leader();
        }
    }

    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    for (auto& r: rafts) {
        co_await r.first->abort(); // Stop servers
    }

    // Check final state of committed is fine
    for (size_t i = 0; i < committed.size(); ++i) {
        if (committed[i] != expected) {
            co_return 1;
        }
    }

    co_return 0;
}

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;

    seastar::app_template::config cfg;
    seastar::app_template app(cfg);

    std::vector<test_case> replication_tests = {
        // 1 nodes, 2 client entries, automatic expected result
        {.name = "simple_1_auto", .nodes = 1, .initial_term = 1, .initial_leader = 0,
         .initial_states = {}, .updates = {entries{0,1}},
        },
        // 1 nodes, 2 client entries, custom expected result
        {.name = "simple_1_expected", .nodes = 1, .initial_term = 1, .initial_leader = 0,
         .initial_states = {},
         .updates = {entries{0,1}},
         .expected = {{0,1}}},
        // 1 nodes, 1 leader entry, 2 client entries
        {.name = "simple_1_pre", .nodes = 1, .initial_term = 1, .initial_leader = 0,
         .initial_states = {{{{1,0}}, 1}},
         .updates = {entries{1,2}},},
        // 2 nodes, 1 leader entry, 2 client entries
        {.name = "simple_2_pre", .nodes = 2, .initial_term = 1, .initial_leader = 0,
         .initial_states = {{{{1,0}}, 1}},
         .updates = {entries{1,2}},},
        // 2 nodes, 1 leader entry, 2 client entries, change leader, 2 client entries
        {.name = "simple_02_pre_chg", .nodes = 2, .initial_term = 1, .initial_leader = 0,
         .initial_states = {{{{1,0}}, 1}},
         .updates = {entries{1,2},new_leader{1},entries{3,4}},},
        // 3 nodes, follower has spurious entry
        {.name = "simple_3_spurious", .nodes = 3, .initial_term = 2, .initial_leader = 0,
         .initial_states = {{{{1,0},{1,1},{1,2}}, 1},
                            {{{2,10}}, 1}},
         .updates = {entries{3,4}},},
        // 3 nodes, term 2, follower has spurious entry, multiple leader changes
        {.name = "simple_3_spurious_multi_leaders", .nodes = 3, .initial_term = 2, .initial_leader = 0,
         .initial_states = {{},{{{1,10},{2,20}}, 1}},
         .updates = {entries{0,1},new_leader{1},entries{2,3},new_leader{2},entries{4,5}}},

        // 2 nodes, term 2, leader has 1 entry, follower has 3 spurious entry
        {.name = "simple_2_spurious", .nodes = 2, .initial_term = 2, .initial_leader = 0,
         .initial_states = {{{{1,0},}, 1},
                            {{{2,10},{2,20},{2,30}}, 1}},
         .updates = {entries{1,2}},},
        // 2 nodes, term 2, leader has 1 entry, follower has 1 good and 3 spurious entries
        {.name = "simple_2_follower_4_1", .nodes = 2, .initial_term = 3, .initial_leader = 0,
         .initial_states = {{{{1,0},{1,1}}, 1},
                            {{{1,0},{2,20},{2,30},{2,40}}, 1}},
         .updates = {entries{2,3}},},
        // A follower and a leader have matching logs but leader's is shorter
        // 2 nodes, term 2, leader has 2 entries, follower has same 2 and 2 extra entries (good)
        {.name = "simple_2_short_leader", .nodes = 2, .initial_term = 3, .initial_leader = 0,
         .initial_states = {{{{1,0},{1,1}}, 1},
                            {{{1,0},{1,1},{1,2},{1,3}}, 1}},
         .updates = {entries{4,5}},},
        // A follower and a leader have no common entries
        // 2 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries
        {.name = "follower_not_matching", .nodes = 2, .initial_term = 3, .initial_leader = 0,
         .initial_states = {{{{1,0},{1,1},{1,2}}, 1},
                            {{{2,10},{2,11},{2,12}}, 1}},
         .updates = {entries{3,4}},},
        // A follower and a leader have one common entry
        // 2 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries
        {.name = "follower_one_common", .nodes = 2, .initial_term = 4, .initial_leader = 0,
         .initial_states = {{{{1,0},{1,1},{1,2}}, 1},
                            {{{1,0},{2,11},{2,12},{2,13}}, 1}},
         .updates = {entries{3,4}},},
        // A follower and a leader have 2 common entries in different terms
        // 2 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries
        {.name = "follower_one_common", .nodes = 2, .initial_term = 5, .initial_leader = 0,
         .initial_states = {{{{1,0},{2,1},{3,2},{3,3}}, 1},
                            {{{1,0},{2,1},{2,12},{2,13}}, 1}},
         .updates = {entries{4,5}},},

        // 2 nodes, leader with 6 entries, initial snapshot
        {.name = "simple_snapshot", .nodes = 2, .initial_term = 1, .initial_leader = 0,
         .initial_states = {{{{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}, 11}},
         .initial_snapshots = {{{.idx = raft::index_t(10),
                                 .term = raft::term_t(1),
                                 .id = utils::UUID(0, 0)},
                                ((10 - 1) * 10)/2}},
         .updates = {entries{7,8}}},
    };

    return app.run(argc, argv, [&replication_tests] () -> future<int> {
        std::stringstream ss;
        for (auto test: replication_tests) {
            if (co_await run_test(test) != 0) {
                co_return 1; // Fail
            }
        }
        co_return 0;
    });
}


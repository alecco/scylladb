#include <fmt/format.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"

using namespace std::chrono_literals;
using namespace std::placeholders;

static seastar::logger tlogger("test");

struct snapshot_value {
    int value = -1;
};

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
        return make_ready_future<raft::snapshot_id>(raft::snapshot_id());
    }
    void drop_snapshot(raft::snapshot_id id) override {}
    future<> load_snapshot(raft::snapshot_id id) override {
        return make_ready_future<>();
    };
    future<> abort() override {
        return make_ready_future<>();
    }

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
    raft::configuration config;
    state_machine::apply_fn apply;
};


class storage : public raft::storage {
    initial_state _conf;
public:
    storage(initial_state conf) : _conf(std::move(conf)) {}
    storage() {}
    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) { co_return seastar::sleep(1us); }
    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) { return make_ready_future<>(); }
    virtual future<raft::snapshot> load_snapshot() override {
        return make_ready_future<raft::snapshot>(_conf.snapshot);
    }
    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) { co_return seastar::sleep(1us); };
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

class rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
public:
    rpc(raft::server_id id) : _id(id) {
        net[_id] = this;
    }
    virtual future<> send_snapshot(raft::server_id id, const raft::install_snapshot& snap) override {
        // snapshots[id] = snapshots[_id];
        return net[id]->_server->apply_snapshot(_id, std::move(snap));
    }
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request_send& append_request) {
fmt::print("XXX [{}] send_append_entries: to {}\n", short_id(_id), short_id(id));
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
        net[id]->_server->append_entries_reply(_id, std::move(reply));
        return make_ready_future<>();
    }
    virtual future<> send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
fmt::print("XXX [{}] send_vote_request: to {}\n", short_id(_id), short_id(id));
        net[id]->_server->request_vote(_id, std::move(vote_request));
        return make_ready_future<>();
    }
    virtual future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
fmt::print("XXX [{}] send_vote_reply: to {}\n", short_id(_id), short_id(id));
        net[id]->_server->request_vote_reply(_id, std::move(vote_reply));
        return make_ready_future<>();
    }
    void send_keepalive(raft::server_id id, const raft::keep_alive& keep_alive) {
        raft::append_request_recv req;
        req.current_term = keep_alive.current_term;
        req.leader_id = keep_alive.leader_id;
        req.prev_log_idx = raft::index_t(0);
        req.prev_log_term = raft::term_t(0);
        req.leader_commit_idx = keep_alive.leader_commit_idx;
        net[id]->_server->append_entries(_id, std::move(req));
    }
    virtual void add_server(raft::server_id id, bytes node_info) {}
    virtual void remove_server(raft::server_id id) {}
    virtual future<> abort() { return make_ready_future<>(); }
};

class server : public raft::server {
public:
    using raft::server::server;
    // Force election
    void election_timeout() {
        for (int i = 0; i < 2 * raft::ELECTION_TIMEOUT.count(); i++) {
            if (_fsm->is_candidate()) {
                break;
            }
            _fsm->tick();
        }
    }

    future<> make_leader() {
        election_timeout();
        // TODO: async wait properly
        do {
            co_await seastar::sleep(150us);
        } while (!_fsm->is_leader());
    }
};

std::unordered_map<raft::server_id, rpc*> rpc::net;

std::pair<std::unique_ptr<server>, state_machine*>
create_raft_server(raft::server_id uuid, initial_state state) {

    auto sm = std::make_unique<state_machine>(uuid, std::move(state.apply));
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid);
    auto mstorage = std::make_unique<storage>(state);
    auto raft = std::make_unique<server>(uuid, std::move(mrpc), std::move(sm), std::move(mstorage));

    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<server>, state_machine*>>> create_cluster(std::vector<initial_state> states) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<server>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        auto uuid = utils::UUID(0, i);
        config.servers.push_back(raft::server_address{uuid});
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = config.servers[i];
        states[i].config = config;
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, states[i])).first;
        co_await raft.start();
    }

    co_return std::move(rafts);
}

struct log_entry {
    unsigned term;
    int value;
};

std::vector<raft::log_entry> create_log(std::initializer_list<log_entry> list) {
    std::vector<raft::log_entry> log;

    unsigned i = 0;
    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e.value);
        log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(++i), std::move(command)});
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
// fmt::print("apply_changes: start\n");
std::vector<int> vals;
for (auto&& d : commands) {
    auto is = ser::as_input_stream(d);
    int n = ser::deserialize(is, boost::type<int>());
    vals.push_back(n);
}
fmt::print("apply_changes[{}]: got {} entries: {}\n", short_id(id), commands.size(), vals);
    for (auto&& d : commands) {
        auto is = ser::as_input_stream(d);
        int n = ser::deserialize(is, boost::type<int>());
        res.push_back(n);
// fmt::print("apply_changes[{}]: pushed {}\n", short_id(id), n);
        tlogger.debug("{}: apply_changes {}", id, n);
    }
// fmt::print("apply_changes[{}]: committed {} expected {}\n", short_id(id), res.size(), expected);
    if (res.size() >= expected) {
// fmt::print("apply_changes[{}]: done\n", short_id(id));
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

struct test_case {
    const std::string name;
    const size_t nodes;
    raft::term_t term;
    const size_t leader;
    const std::vector<std::initializer_list<log_entry>> initial_states;
    // const std::vector<raft::command> updates;
    const std::vector<update> updates;
    test_case(std::string name, size_t nodes, int term, size_t leader,
            std::vector<std::initializer_list<log_entry>> initial_states,
            std::vector<update> updates) : name(name),
            nodes(nodes), term(raft::term_t(term)), leader(leader), initial_states(initial_states), updates(updates) {
        assert(leader < nodes && "Leader higher than total nodes");
    }
};

// Run test case (name, nodes, leader, initial logs, updates)
future<int> run_test(test_case test) {

    std::vector<initial_state> states(test.nodes);       // Server initial states
    std::vector<int> expected;                           // Expected output for Raft
    std::vector<std::vector<int>> committed(test.nodes); // Actual outputs for each server
    states[test.leader].term = test.term;
fmt::print("run_test: {} servers {} term {} leader {} initial log size {}\n", test.name, test.nodes, test.term, test.leader, test.leader < test.initial_states.size()? test.initial_states[test.leader].size() : 0);
    if (test.leader < test.initial_states.size()) {
        for (auto log_initializer: test.initial_states[test.leader]) {
            log_entry le(log_initializer);
            if (le.term > test.term) {
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
    size_t expected_entries = expected.size();

    // Server initial logs
    for (size_t i = 0; i < states.size(); ++i) {
        if (i < test.initial_states.size()) {
            states[i].log = create_log(test.initial_states[i]);
        } else {
            states[i].log = {};
        }
        states[i].apply = std::bind(apply_changes, std::ref(committed[i]), expected_entries, _1, _2, _3);
    }

    auto rafts = co_await create_cluster(states);

    auto& leader = *rafts[test.leader].first;
    co_await leader.make_leader();

    // Process all updates in order
    // XXX here pick
    for (auto update: test.updates) {
        //
        if (std::holds_alternative<entries>(update)) {
            auto updates = std::get<entries>(update);
            std::vector<raft::command> commands = create_commands<int>(updates);
            co_await seastar::parallel_for_each(commands, [&] (const raft::command cmd) {
                tlogger.debug("Adding command entry on leader");
fmt::print("XXX adding new command\n");
                return leader.add_entry(std::move(cmd), raft::server::wait_type::committed);
            });
        } else if (std::holds_alternative<new_leader>(update)) {
            // auto new_leader_id = std::get<new_leader>(update);
fmt::print("XXX new leader\n");
// fmt::print("XXX new leader {} {}\n", new_leader_id, rafts.size());
            // auto& new_leader = *rafts[new_leader_id].first;

            // co_await new_leader.read_barrier();
            // XXX new_leader.make_me_leader();
        }
    }

// fmt::print("run_test: {} done, waiting\n", test.name);
    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    for (auto& r: rafts) {
        co_await r.first->abort(); // Stop servers
    }

    // Check final state of committed is fine
// fmt::print("run_test: {} checking committed\n", test.name);
    for (size_t i = 0; i < committed.size(); ++i) {
fmt::print("run_test: {}  server[{}] {} vs expected {} \n", test.name, i, committed[i], expected);
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
        // name, servers, current term, leader, initial logs (each), updates
#if 1
        {"simple_1_1_0_e*_1", 1, 1, 0, {{}},
            {entries{1,2}}},
        {"simple_1_1_0_1_2", 1, 1, 0, {{{1,10}}},
            {entries{1,2}},},
        {"simple_2_1_0_1_2", 2, 1, 0, {{{1,10}}},
            {entries{1,2},new_leader{1}},},
#endif
        {"simple_2_1_1_1_2", 2, 1, 1, {{},{{1,10}}},
            {entries{1,2}},},
#if 1
        {"simple_3_2_1_1_2", 3, 2, 1, {{{1,10}}},
            {entries{1,2}},},
#endif
    };

    return app.run(argc, argv, [&replication_tests] () -> future<int> {
        std::stringstream ss;
        for (auto test: replication_tests) {
            if (co_await run_test(test) != 0) {
fmt::print("main: test {} failed\n", test.name);
                co_return 1; // Fail
            }
fmt::print("------------------------------------------------------\n");
        }
fmt::print("main: done\n");
        co_return 0;
    });
}


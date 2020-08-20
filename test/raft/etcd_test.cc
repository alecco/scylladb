#include <fmt/format.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include <sstream>

using namespace std::chrono_literals;
using namespace std::placeholders;

static seastar::logger tlogger("test");

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
    virtual future<raft::snapshot> load_snapshot() { return make_ready_future<raft::snapshot>(raft::snapshot()); }
    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) { co_return seastar::sleep(1us); };
    virtual future<raft::log> load_log() {
        raft::log log;
        for (auto&& e : _conf.log) {
            log.emplace_back(std::move(e));
        }
        log.stable_to(raft::index_t(_conf.log.size()));
        return make_ready_future<raft::log>(std::move(log));
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
    virtual future<> send_snapshot(raft::server_id server_id, const raft::snapshot& snap) { return make_ready_future<>(); }
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request_send& append_request) {
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
        net[id]->_server->request_vote(_id, std::move(vote_request));
        return make_ready_future<>();
    }
    virtual future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
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

std::unordered_map<raft::server_id, rpc*> rpc::net;

std::pair<std::unique_ptr<raft::server>, state_machine*>
create_raft_server(raft::server_id uuid, const raft::configuration& config, initial_state state) {

    auto sm = std::make_unique<state_machine>(uuid, std::move(state.apply));
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid);
    auto mstorage = std::make_unique<storage>(state);
    auto raft = std::make_unique<raft::server>(uuid, std::move(mrpc), std::move(sm), std::move(mstorage));
    raft->set_configuration(config);

    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>>> create_cluster(std::vector<initial_state> states) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        auto uuid = utils::UUID(0, i);
        config.servers.push_back(raft::server_address{uuid});
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = config.servers[i];
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, config, states[i])).first;
        co_await raft.start();
    }

    co_return std::move(rafts);
}

struct log_entry {
    unsigned term;
    int value;
};

std::vector<raft::log_entry> create_log(const std::initializer_list<log_entry> list) {
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

using update = std::variant<raft::command, raft::configuration>;

struct test_case {
    const std::string name;
    const size_t nodes;
    raft::term_t term;
    const size_t leader;
    const std::vector<std::initializer_list<log_entry>> initial_states;
    const std::vector<raft::command> updates;
    test_case(std::string name, size_t nodes, int term, size_t leader,
            std::vector<std::initializer_list<log_entry>> initial_states,
            std::vector<raft::command> updates) : name(name),
            nodes(nodes), term(raft::term_t(term)), leader(leader), initial_states(initial_states), updates(updates) {
        assert(leader < nodes && "Leader higher than total nodes");
    }
};

// Run test case (name, nodes, leader, initial logs, updates)
future<int> run_test(test_case test) {

    std::vector<initial_state> states(test.nodes);
    states[test.leader].term = test.term;
// fmt::print("run_test: {} leader {} initial log size {}\n", test.name, test.leader, test.leader < test.initial_states.size()? test.initial_states[test.leader].size() : 0);
    std::vector<std::vector<int>> committed(test.nodes);
    size_t expected = test.updates.size();
if (test.leader < test.initial_states.size()) {
    expected += test.initial_states[test.leader].size();
}
    for (size_t i = 0; i < states.size(); ++i) {
        if (i < test.initial_states.size()) {
            states[i].log = create_log(test.initial_states[i]);
        } else {
            states[i].log = {};
        }
        states[i].apply = std::bind(apply_changes, std::ref(committed[i]), expected, _1, _2, _3);
    }

    auto rafts = co_await create_cluster(states);

    auto& leader = *rafts[test.leader].first;
    leader.make_me_leader();

    // Process all updates in order
    co_await seastar::parallel_for_each(test.updates, [&] (const update u) {
            raft::command cmd = std::get<raft::command>(u);
            tlogger.debug("Adding command entry on leader");
            return leader.add_entry(std::move(cmd), raft::server::wait_type::committed);
        return make_ready_future<>();
    });

// fmt::print("run_test: {} done, waiting\n", test.name);
    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    for (auto& r: rafts) {
        co_await r.first->abort(); // Stop servers
    }

    // Check final state of committed is fine
    auto leader_committed = committed[test.leader];
// fmt::print("run_test: {} checking committed\n", test.name);
// fmt::print("run_test: {} leader {} result {}\n", test.name, test.leader, committed[test.leader]);
    for (size_t i = 0; i < committed.size(); ++i) {
        if (i != test.leader) {
fmt::print("run_test: {}        {} result {}\n", test.name, i, committed[i]);
            if (committed[i] != leader_committed) {
                co_return 1;
            }
        }
    }

    co_return 0;
}

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;

    seastar::app_template::config cfg;
    seastar::app_template app(cfg);

    std::vector<test_case> replication_tests = {
        // name, servers, current term, leader, initial logs (each), commands to send to leader
#if 0
        {"non_empty_leader_log", 2, 1, 1, {{},{{1, 0}, {1, 1}, {1, 3}}}, create_commands<int>({4}) },
        {"simple1", 1, 1, 0, {{}}, create_commands<int>({1}),},
        {"simple2", 1, 1, 0, {{{1,10}}}, create_commands<int>({1,2,3}),},
        {"simple2", 2, 1, 0, {{{1,10}}}, create_commands<int>({1,2,3}),},
#endif
        {"simple3", 3, 1, 1, {{{1,10},{1,20},{1,30}}}, create_commands<int>({1,2,3}),},
        {"simple_many", 3, 1, 1, {{{1,10},{2,20},{3,30}}}, create_commands<int>({1,2,3}),},
    };

    return app.run(argc, argv, [&replication_tests] () -> future<int> {
        std::stringstream ss;
        for (auto test: replication_tests) {
            if (co_await run_test(test) != 0) {
fmt::print("main: test {} failed\n", test.name);
                co_return 1; // Fail
            }
        }
fmt::print("main: done\n");
        co_return 0;
    });
}


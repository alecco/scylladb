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
create_raft_server(raft::server_id uuid, state_machine::apply_fn apply,
        const raft::configuration& config, initial_state state) {

    auto sm = std::make_unique<state_machine>(uuid, std::move(apply));
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid);
    auto mstorage = std::make_unique<storage>(state);
    auto raft = std::make_unique<raft::server>(uuid, std::move(mrpc), std::move(sm), std::move(mstorage));
    raft->set_configuration(config);

    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>>> create_cluster(std::vector<initial_state> states, state_machine::apply_fn apply) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        auto uuid = utils::UUID(0, i);
        config.servers.push_back(raft::server_address{uuid});
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = config.servers[i];
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, apply, config, states[i])).first;
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

const int MARK_DONE = INT_MIN;
long int count = 0;

future<> apply(raft::server_id id, promise<>& done, const std::vector<raft::command_cref>& commands) {
        tlogger.debug("sm::apply got {} entries", commands.size());
        for (auto&& d : commands) {
            auto is = ser::as_input_stream(d);
            int n = ser::deserialize(is, boost::type<int>());
            if (n == MARK_DONE) {
                done.set_value();
                break;
            }
            tlogger.debug("{}: apply {}", id, n);
        }
        return make_ready_future<>();
};

future<> apply_count(long int end, raft::server_id id, promise<>& done, const std::vector<raft::command_cref>& commands) {
        tlogger.debug("sm::apply_count got {} entries", commands.size());
        for (auto&& d : commands) {
            auto is = ser::as_input_stream(d);
            int n = ser::deserialize(is, boost::type<int>());
            if (n == MARK_DONE) {
fmt::print("apply_count: {} command DONE\n", id);
                done.set_value();
                break;
            }
else
fmt::print("apply_count: {} command {}\n", id, n);
            tlogger.debug("{}: apply_count {}", id, n);
        }
        return make_ready_future<>();
};

long int sum = 0;

future<> apply_sum(long int end, raft::server_id id, promise<>& done, const std::vector<raft::command_cref>& commands) {
        tlogger.debug("sm::apply_sum got {} entries", commands.size());
        for (auto&& d : commands) {
            auto is = ser::as_input_stream(d);
            int n = ser::deserialize(is, boost::type<int>());
            if (n == MARK_DONE) {
                done.set_value();
                break;
            }
            tlogger.debug("{}: apply_sum {}", id, n);
        }
        return make_ready_future<>();
};

using update = std::variant<raft::command, raft::configuration>;

// Run test with n raft servers
// giving each initial log states (with just ints starting from 0)
// and starting at position start_itr
future<int> test_helper(const std::vector<initial_state> initial_states,
        const std::vector<update> updates, const state_machine::apply_fn apply,
        const int start_leader = 0) {

    auto rafts = co_await create_cluster(initial_states, apply);

    auto& leader = *rafts[start_leader].first;
    leader.make_me_leader();

    // Process all updates in order
    co_await seastar::do_for_each(updates, [&] (const update u) {
        if (std::holds_alternative<raft::command>(u)) {
            raft::command cmd = std::get<raft::command>(u);
            // XXX int val = ser::deserialize(cmd, boost::type<int>());
            tlogger.debug("Adding command entry on leader");
fmt::print("test_helper: adding command\n");
            return leader.add_entry(std::move(cmd), raft::server::wait_type::committed);
        }
        return make_ready_future<>();
    });

    raft::command command;
    ser::serialize(command, MARK_DONE);
    co_await leader.add_entry(std::move(command), raft::server::wait_type::committed);

    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    co_return 1;
}


future<int> test_simple_commit_count(size_t size) {
    long int end = 10;  // Count till ten
    std::vector<initial_state> states(3);
    states[0].term = raft::term_t(2);
    states[2].log = create_log({{1, 10}, {1, 20}, {1, 30}});
    std::vector<update> updates = { };
    for (int i: std::views::iota(1, 10)) {
        raft::command command;
        ser::serialize(command, i);
// fmt::print("test_simple_commit_count: adding {}\n", i);
        updates.push_back(std::move(command));
    }
    auto apply = std::bind(apply_count, end, _1, _2, _3); // XXX , _1),
    return test_helper(std::move(states), std::move(updates), apply);
}


int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;

    seastar::app_template::config cfg;
    seastar::app_template app(cfg);

    return app.run(argc, argv, [] () -> future<int> {
        std::stringstream ss;
        auto ret = co_await test_simple_commit_count(2);
fmt::print("main: done\n");
        co_return ret;
    });
}


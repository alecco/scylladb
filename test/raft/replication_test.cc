#include <fmt/format.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include "raft/server.hh"
#include "serializer.hh"
#include "serializer_impl.hh"

using namespace std::chrono_literals;

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
    virtual future<> apply(const std::vector<raft::command_cref> commands) {
        return _apply(_id, _done, commands);
    }
    virtual future<raft::snapshot_id> take_snaphot() { return make_ready_future<raft::snapshot_id>(raft::snapshot_id()); }
    virtual void drop_snapshot(raft::snapshot_id id) {}
    virtual future<> load_snapshot(raft::snapshot_id id) { return make_ready_future<>(); };
    virtual future<> stop() { return make_ready_future<>(); }

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
    virtual future<> store_term(raft::term_t term) { co_return seastar::sleep(1us); }
    virtual future<> store_vote(raft::server_id vote) { return make_ready_future<>(); }
    virtual future<> store_snapshot(raft::snapshot snap, size_t preserve_log_entries) { return make_ready_future<>(); }
    virtual future<raft::snapshot> load_snapshot() { return make_ready_future<raft::snapshot>(raft::snapshot()); }
    virtual future<> store_log_entries(const std::vector<raft::log_entry>& entries) { co_return seastar::sleep(1us); };
    virtual future<> store_log_entry(const raft::log_entry& entry) { co_return seastar::sleep(1us); }
    virtual future<> truncate_log(raft::index_t idx) { return make_ready_future<>(); }
    virtual future<> stop() { return make_ready_future<>(); }
};

class rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
public:
    rpc(raft::server_id id) : _id(id) {
        net[_id] = this;
    }
    virtual future<> send_snapshot(raft::server_id server_id, raft::snapshot snap) { return make_ready_future<>(); }
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
    virtual future<> send_append_entries_reply(raft::server_id id, raft::append_reply reply) {
        net[id]->_server->append_entries_reply(_id, std::move(reply));
        return make_ready_future<>();
    }
    virtual future<> send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
        net[id]->_server->request_vote(_id, vote_request);
        return make_ready_future<>();
    }
    virtual future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
        net[id]->_server->reply_vote(_id, vote_reply);
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
    virtual future<> stop() { return make_ready_future<>(); }
};

std::unordered_map<raft::server_id, rpc*> rpc::net;

std::pair<std::unique_ptr<raft::server>, state_machine*>
create_raft_server(raft::server_id uuid, state_machine::apply_fn apply,
        const raft::configuration& config, initial_state state) {

    raft::log log;
    for (auto&& e : state.log) {
        log.emplace_back(std::move(e));
    }
    if (state.log.size()) {
        log.stable_to(raft::index_t(state.log.size()));
    }

    auto sm = std::make_unique<state_machine>(uuid, std::move(apply));
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid);
    auto mstorage = std::make_unique<storage>(state);
    raft::fsm fsm{uuid, state.term, state.vote, std::move(log)};
    fsm.set_configuration(config);
    auto raft = std::make_unique<raft::server>(std::move(fsm), std::move(mrpc), std::move(sm), std::move(mstorage));
    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>>> create_cluster(std::vector<initial_state> states, state_machine::apply_fn apply) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        auto uuid = utils::make_random_uuid();
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

constexpr int itr = 100;
std::unordered_map<raft::server_id, int> sums;

future<> apply(raft::server_id id, promise<>& done, const std::vector<raft::command_cref>& commands) {
        tlogger.debug("sm::apply got {} entries", commands.size());
        for (auto&& d : commands) {
            auto is = ser::as_input_stream(d);
            int n = ser::deserialize(is, boost::type<int>());
            tlogger.debug("{}: apply {}", id, n);
            auto it = sums.find(id);
            if (it == sums.end()) {
                sums[id] = 0;
            }
            sums[id] += n;
        }
        if (sums[id] == ((itr - 1) * itr)/2) {
            done.set_value();
        }
        return make_ready_future<>();
};


future<> test_helper(std::vector<initial_state> states, int start_itr = 0) {
    auto rafts = co_await create_cluster(states, apply);

    auto& leader = *rafts[0].first;
    leader.make_me_leader();

    co_await seastar::parallel_for_each(std::views::iota(start_itr, itr), [&] (int i) {
            tlogger.debug("Adding entry {} on a leader", i);
            raft::command command;
            ser::serialize(command, i);
            return leader.add_entry(std::move(command));
    });

    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    for (auto& r: rafts) {
        co_await r.first->stop();
    }

    sums.clear();
    co_return;
}

future<> test_simple_replication(size_t size) {
    return test_helper(std::vector<initial_state>(size));
}

// initially a leader has non empty log
future<> test_replicate_non_empty_leader_log() {
    // 2 nodes, leader has entries in his log
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(1);
    states[0].log = create_log({{1, 0}, {1, 1}, {1, 2}, {1, 3}});

    // start iterations from 4 since o4 entry is already in the log
    return test_helper(std::move(states), 4);
}

// test special case where prev_index = 0 because the leader's log is empty
future<> test_replace_log_leaders_log_empty() {
    // current leaders term is 2 and empty log
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(3);
    states[0].term = raft::term_t(2);
    states[2].log = create_log({{1, 10}, {1, 20}, {1, 30}});

    return test_helper(std::move(states));
}

// two nodes, leader has one entry, follower has 3, existing entries do not match
future<> test_replace_log_leaders_log_not_empty() {
    // current leaders term is 2 and the log has one entry
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(3);
    states[0].log = create_log({{1, 0}});
    states[1].log = create_log({{2, 10}, {2, 20}, {2, 30}});

    // start iterations from 1 since one entry is already in the log
    return test_helper(std::move(states), 1);
}

// two nodes, leader has 2 entries, follower has 4, index=1 matches index=2 does not
future<> test_replace_log_leaders_log_not_empty_2() {
    // current leader's term is 2 and the log has one entry
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(3);
    states[0].log = create_log({{1, 0}, {1, 1}});
    states[1].log = create_log({{1, 0}, {2, 20}, {2, 30}, {2, 40}});

    // start iterations from 2 since 2 entries are already in the log
    return test_helper(std::move(states), 2);
}

// a follower and a leader have matching logs but leader's is shorter
future<> test_replace_log_leaders_log_not_empty_3() {
    // current leaders term is 2 and the log has one entry
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(2);
    states[0].log = create_log({{1, 0}, {1, 1}});
    states[1].log = create_log({{1, 0}, {1, 1}, {1, 2}, {1, 3}});

    // start iterations from 2 since 2 entries are already in the log
    return test_helper(std::move(states), 2);
}

// a follower and a leader have no common entries
future<> test_replace_no_common_entries() {
    // current leaders term is 2 and the log has one entry
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(3);
    states[0].log = create_log({{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {1, 6}});
    states[1].log = create_log({{2, 10}, {2, 11}, {2, 12}, {2, 13}, {2, 14}, {2, 15}, {2, 16}});

    // start iterations from 7 since 7 entries are already in the log
    return test_helper(std::move(states), 7);
}

// a follower and a leader have one common entry
future<> test_replace_one_common_entry() {
    // current leaders term is 2 and the log has one entry
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(4);
    states[0].log = create_log({{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {3, 6}});
    states[1].log = create_log({{1, 0}, {2, 11}, {2, 12}, {2, 13}, {2, 14}, {2, 15}, {2, 16}});

    // start iterations from 7 since 7 entries are already in the log
    return test_helper(std::move(states), 7);
}

// a follower and a leader have t1i common entry in different terms
future<> test_replace_two_common_entry_different_terms() {
    // current leaders term is 2 and the log has one entry
    // one of the follower have three entries that should be replaced
    std::vector<initial_state> states(2);
    states[0].term = raft::term_t(5);
    states[0].log = create_log({{1, 0}, {2, 1}, {3, 2}, {3, 3}, {3, 4}, {3, 5}, {4, 6}});
    states[1].log = create_log({{1, 0}, {2, 1}, {2, 12}, {2, 13}, {2, 14}, {2, 15}, {2, 16}});

    // start iterations from 7 since 7 entries are already in the log
    return test_helper(std::move(states), 7);
}

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;

    seastar::app_template::config cfg;
    seastar::app_template app(cfg);

    using test_fn = std::function<future<>()>;

    test_fn tests[] =  {
        std::bind(test_simple_replication, 1),
        std::bind(test_simple_replication, 2),
        test_replicate_non_empty_leader_log,
        test_replace_log_leaders_log_empty,
        test_replace_log_leaders_log_not_empty,
        test_replace_log_leaders_log_not_empty_2,
        test_replace_log_leaders_log_not_empty_3,
        test_replace_no_common_entries,
        test_replace_one_common_entry,
        test_replace_two_common_entry_different_terms
    };

    return app.run(argc, argv, [&tests] () -> future<> {
        int i = 0;
        for (auto& t : tests) {
            tlogger.debug("test: {}", i++);
            co_await t();
        }
    });
}


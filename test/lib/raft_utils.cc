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

#include "test/lib/raft_utils.hh"
#include <boost/test/unit_test.hpp>

using raft::term_t, raft::index_t, raft::server_id;

// Helper for visitors
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
// TODO: remove this deduction guidance once it's not needed (C++20)
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

class connected_nodes {
    std::unordered_set<unsigned> _nodes; // Nodes currently disconnected
public:
    void disconnect(unsigned id) {
        _nodes.insert(id);
    }
    void reconnect(unsigned id) {
        _nodes.erase(id);
    }
    bool operator()(unsigned id) {
        return _nodes.find(id) == _nodes.end();
    }
    void disconnect(server_id id) {
        _nodes.insert(id.id.get_least_significant_bits() - 1);
    }
    void reconnect(server_id id) {
        _nodes.erase(id.id.get_least_significant_bits() - 1);
    }
    bool operator()(server_id id) {
        return _nodes.find(id.id.get_least_significant_bits()) == _nodes.end();
    }
};

void election_threshold(seastar::lw_shared_ptr<raft::fsm> fsm) {
    for (int i = 0; i <= raft::ELECTION_TIMEOUT.count(); i++) {
        fsm->tick();
    }
}

void election_timeout(seastar::lw_shared_ptr<raft::fsm> fsm) {
    for (int i = 0; i <= 2 * raft::ELECTION_TIMEOUT.count(); i++) {
        fsm->tick();
    }
}

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return std::move(command);
}

struct failure_detector: public raft::failure_detector {
    connected_nodes& _connected;
    bool is_alive(server_id from) override {
        return _connected(from);
    }
    failure_detector(connected_nodes& map) : _connected(map) { }
};

void fill_log_entries(std::vector<log_input> entries, unsigned start_idx,
        raft::log_entries& log_entries) {

    unsigned i = start_idx;
    for (auto e : entries) {
        raft::command command;
        ser::serialize(command, e.value);
        log_entries.emplace_back(seastar::make_lw_shared<raft::log_entry>(
                    raft::log_entry{term_t{e.term}, index_t{i++}, std::move(command)}));
    }
}

// Runs a configured test case
// NOTE: creates ids for number of nodes, and fsms are first few as also specified
class Test {
protected:
    unsigned _nodes;
    std::string _name;
    unsigned _initial_term;
    std::optional<unsigned> initial_leader;
    std::vector<std::vector<log_entry>> initial_logs;
    std::vector<step> _steps;

    int _next_val = 1;    // TODO: make consecutive from log

    term_t _current_term;
    connected_nodes _connected; 
    std::vector<failure_detector> fds;
    std::vector<seastar::lw_shared_ptr<raft::fsm>> _fsms;

    void _run_test() {
fmt::print("Test {}\n", _name);
        for (auto& [actions, expect]: _steps) {
            // Actions
            for (auto& action: actions) {
                std::visit(overloaded{
                    [&](simple_action& action) {
                        switch (action) {
                        case receptive_all:
                            for (auto fsm: _fsms) {
                                election_timeout(fsm);
                            }
                                break;
                        }
                    },
                    [&](struct candidate& action) {
                        election_threshold(_fsms[action.id]);
                    },
                    [&](struct receptive& action) {
                        election_timeout(_fsms[action.id]);
                    },
                    [&](struct elect& action) {
                        unsigned candidate = action.id;
                        server_id candidate_id{utils::UUID(0, action.id + 1)};
                        make_fsms_receptive();
                        // Make defined fsm candidate, force votes
                        // NOTE: we skip doing vote handling at other fsms for now
                        election_timeout(_fsms[candidate]);
                        BOOST_CHECK(_fsms[candidate]->is_candidate());
                        auto output = _fsms[candidate]->get_output();
                        _current_term = output.term;
                        for (auto& [id, msg] : output.messages) {
                            auto vreq = get_req<raft::vote_request>(msg);
                            fsm_step(candidate, id, raft::vote_reply{vreq.current_term, true});
                        }
                        BOOST_CHECK(_fsms[candidate]->is_leader());
                        // Finally, handle dummy entry propagation
                        output = _fsms[candidate]->get_output();
                        get_req<raft::log_entry::dummy>(output.log_entries[0]->data);
                        output = _fsms[candidate]->get_output();
                        BOOST_CHECK(output.messages.size() == _nodes - 1);
                        for (auto& [id, msg] : output.messages) {
                            // Get request for one node
                            auto areq = get_req<raft::append_request>(msg);
                            BOOST_CHECK(areq.entries.size() == 1);
                            raft::log_entry_ptr areq_lep =  areq.entries.back();
                            BOOST_CHECK(areq_lep->term == _current_term);
                            get_req<raft::log_entry::dummy>(areq_lep->data);
                            unsigned dst_id = id.id.get_least_significant_bits() - 1;
                            if (dst_id < _fsms.size()) {
                                // Propagate and get reply from other fsms
                                fsm_step(dst_id, candidate_id, std::move(areq));
                                auto follower_output = _fsms[dst_id]->get_output();
                                BOOST_CHECK(follower_output.messages.size() == 1);
                                auto& [reply_dst, reply] = follower_output.messages.back();
                                BOOST_CHECK(reply_dst == candidate_id);
                                auto arep = get_req<raft::append_reply>(reply);
                                get_req<raft::append_reply::accepted>(arep.result);
                                fsm_step(candidate, id, std::move(arep));
                            } else {
                                // Reply from virtual nodes
                                fsm_step(candidate, id,
                                        raft::append_reply{_current_term, areq_lep->idx,
                                                raft::append_reply::accepted{areq_lep->idx}});
                            }
                        }
                        output = _fsms[candidate]->get_output();
                        BOOST_CHECK(output.committed.size() == 1); // Dummy committed
                    },
                    [&](struct disconnect& d) {
                        for (auto id: d.ids) {
                            _connected.disconnect(id);
                        }
                    },
                    [&](struct reconnect& r) {
                        for (auto id: r.ids) {
                            _connected.reconnect(id);
                        }
                    },
                    [&](struct entries& entries) {
fmt::print("ENTRIES server {} [{}, {}]\n", entries.server.id, _next_val, _next_val + entries.n - 1);
                        for (auto i = _next_val; i < _next_val + entries.n; i++) {
                            raft::command cmd = create_command(i);
                            _fsms[entries.server.id]->add_entry(std::move(cmd));
                        }
                        _next_val += entries.n;
                    },
                }, action);
            }

            // Expected
            for (auto& e: expect) {
                auto id = e.server;
                auto output = _fsms[id]->get_output();
                if (e.follower) {
                    BOOST_CHECK(_fsms[id]->is_follower());
                }
                if (e.candidate) {
                    BOOST_CHECK(_fsms[id]->is_candidate());
                }
                if (e.leader) {
                    BOOST_CHECK(_fsms[id]->is_leader());
                }
                if (e.term) {
                    BOOST_CHECK(output.term == e.term);
                }
                if (e.entries.size() > 0) {
                    BOOST_CHECK(output.log_entries.size() == e.entries.size());
                    for (size_t le = 0; le < e.entries.size(); le++) {
                        auto expected = e.entries[le];
                        auto actual = output.log_entries[le];
                        BOOST_CHECK(actual->term == term_t{expected.term});
                        BOOST_CHECK(actual->idx == index_t{expected.idx});
                        std::visit(overloaded{
                            [&](struct cmd& expected_cmd) {
                                raft::command cmd;
                                BOOST_REQUIRE_NO_THROW(cmd = std::get<raft::command>(actual->data));
                                auto is = ser::as_input_stream(cmd);
                                int val = ser::deserialize(is, boost::type<int>());
                            },
                            [&](struct configuration& cfg) {
                                BOOST_REQUIRE_NO_THROW(std::get<raft::configuration>(actual->data));
                                // XXX
                            },
                            [&](struct dummy& dummy) {
                                BOOST_REQUIRE_NO_THROW(std::get<raft::log_entry::dummy>(actual->data));
                            },
                        }, e.entries[le].data);
                    }
                }
                if (e.messages.size() > 0) {
fmt::print("    [{}] messages {} output messages {}\n", id, e.messages.size(), output.messages.size());
                    BOOST_CHECK(output.messages.size() == e.messages.size());
                    for (size_t le = 0; le < e.messages.size(); le++) {
                        auto expected = e.messages[le];
                        auto& [id, msg] = output.messages[le];
                        BOOST_CHECK(id == server_id{utils::UUID(0, expected.dst + 1)});
                    }
//using rpc_message = std::variant<append_request, append_reply, vote_request, vote_reply, install_snapshot, snapshot_reply>;
for (auto& [id, msg]: output.messages) {
    fmt::print("        type {}\n", msg.index());
}
                }
            }
        }

    }
    template<typename T, typename S>
    T get_req(S obj) {
        T ret;
        // NOTE: catching exception here hides caller line
        BOOST_REQUIRE_NO_THROW(ret = std::get<T>(obj));
        return ret;
    }
    void make_fsms_receptive() {
        for (auto fsm: _fsms) {
            election_threshold(fsm);
            auto output = fsm->get_output();  // ignore output (i.e. vote requests)
        }
    }
    template <typename Message>
    void fsm_step(unsigned dst, server_id src, Message&& msg) {
        if (_connected(dst) && _connected(src)) {
            _fsms[dst]->step(src, std::move(msg));
        }
    }
public:
    Test(test_case test) : _nodes(test.nodes), _name(test.name),
            _initial_term(test.initial_term), _steps(std::move(test.steps)) {
        for (unsigned n = 0; n < test.fsms; n++) {
            fds.emplace_back(failure_detector{_connected});
        }

        raft::configuration cfg;
        raft::fsm_config fsm_cfg{.append_request_threshold = 1};
        cfg.current.reserve(_nodes);
        std::vector<server_id> ids;
        for (unsigned s = 0; s < _nodes; s++) {
            ids.emplace_back(server_id{utils::UUID(0, s + 1)});
            cfg.current.emplace(raft::server_address{ids[s]});
        }
        for (unsigned f = 0; f < test.fsms; f++) {
            raft::log_entries log_entries;
            if (test.initial_logs.size() > f) {
                fill_log_entries(test.initial_logs[f], 1, log_entries);
            }
            raft::log log{raft::snapshot{.config = cfg}, log_entries};
            _fsms.push_back(seastar::make_lw_shared<raft::fsm>(ids[f], term_t{_initial_term},
                        server_id{}, std::move(log), fds.at(f), fsm_cfg));
        }
        _run_test();
    }
};


void Tester::_test(test_case test) {
    Test t{std::move(test)};
}


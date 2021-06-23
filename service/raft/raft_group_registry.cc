/*
 * Copyright (C) 2020-present ScyllaDB
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
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/raft/schema_raft_state_machine.hh"
#include "service/raft/raft_gossip_failure_detector.hh"

#include "message/messaging_service.hh"
#include "cql3/query_processor.hh"
#include "gms/gossiper.hh"
#include "gms/inet_address_serializer.hh"
#include "db/system_keyspace.hh"

#include <seastar/core/smp.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>

namespace service {

logging::logger rslog("raft_group_registry");

raft_group_registry::raft_group_registry(netw::messaging_service& ms, gms::gossiper& gs, sharded<cql3::query_processor>& qp)
    : _ms(ms), _gossiper(gs), _qp(qp), _fd(make_shared<raft_gossip_failure_detector>(gs, *this))
{
}

void raft_group_registry::init_rpc_verbs() {
    auto handle_raft_rpc = [this] (
            const rpc::client_info& cinfo,
            const raft::group_id& gid, raft::server_id from, raft::server_id dst, auto handler) {
        return container().invoke_on(shard_for_group(gid),
                [addr = netw::messaging_service::get_source(cinfo).addr, from, dst, gid, handler] (raft_group_registry& self) mutable {
            // Update the address mappings for the rpc module
            // in case the sender is encountered for the first time
            auto& rpc = self.get_rpc(gid);
            // The address learnt from a probably unknown server should
            // eventually expire
            self.update_address_mapping(from, std::move(addr), true);
            // Execute the actual message handling code
            return handler(rpc);
        });
    };

    _ms.register_raft_send_snapshot([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::install_snapshot snp) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, snp = std::move(snp)] (raft_rpc& rpc) mutable {
            return rpc.apply_snapshot(std::move(from), std::move(snp));
        });
    });

    _ms.register_raft_append_entries([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
           raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_request append_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, append_request = std::move(append_request)] (raft_rpc& rpc) mutable {
            rpc.append_entries(std::move(from), std::move(append_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_append_entries_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_reply reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, reply = std::move(reply)] (raft_rpc& rpc) mutable {
            rpc.append_entries_reply(std::move(from), std::move(reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_request([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_request vote_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_request] (raft_rpc& rpc) mutable {
            rpc.request_vote(std::move(from), std::move(vote_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_reply vote_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_reply] (raft_rpc& rpc) mutable {
            rpc.request_vote_reply(std::move(from), std::move(vote_reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_timeout_now([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::timeout_now timeout_now) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, timeout_now] (raft_rpc& rpc) mutable {
            rpc.timeout_now_request(std::move(from), std::move(timeout_now));
            return make_ready_future<>();
        });
    });

    auto peer_exchange_impl = [this](const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::peer_list peers) -> future<raft::peer_exchange> {

        return container().invoke_on(0 /* group 0 is on shard 0 */, [peers = std::move(peers)] (
                raft_group_registry& self) -> future<raft::peer_exchange> {

            return self.peer_exchange(std::move(peers));
        });
    };

    _ms.register_raft_peer_exchange(peer_exchange_impl);

    auto cas_or_bounce = [this](raft::group_id gid, auto cas) -> future<raft::success_or_bounce> {
        raft::server& server = get_server(gid);
        try {
            co_await server.cas(cas);
            co_return raft::success_or_bounce{};
        } catch (const std::exception& e) {
            if (! raft::is_transient_error(e)) {
                throw;
            }
        }
        auto bounce = co_await guess_leader(gid);
        co_return raft::success_or_bounce{.bounce = bounce};
    };

    auto raft_add_server_impl = [this, cas_or_bounce] (const rpc::client_info& cinfo,
        rpc::opt_time_point timeout, raft::group_id gid, raft::server_address addr) {

        return container().invoke_on(shard_for_group(gid), [gid, addr, cas_or_bounce] (
                raft_group_registry& self) -> future<raft::success_or_bounce> {

            return cas_or_bounce(gid, [addr] (const raft::server& server) -> future<raft::server_address_set> {
                auto configuration = server.get_configuration().current;
                configuration.emplace(std::move(addr));
                co_return configuration;
            });
        });
    };

    _ms.register_raft_add_server(raft_add_server_impl);

    auto raft_remove_server_impl = [this, cas_or_bounce] (const rpc::client_info& cinfo,
        rpc::opt_time_point timeout, raft::group_id gid, raft::server_id sid) {

        return container().invoke_on(shard_for_group(gid), [gid, sid, cas_or_bounce] (
                raft_group_registry& self) -> future<raft::success_or_bounce> {

            return cas_or_bounce(gid, [sid] (const raft::server& server) -> future<raft::server_address_set> {
                auto configuration = server.get_configuration().current;
                configuration.erase(raft::server_address{.id = sid});
                co_return configuration;
            });
        });
    };

    _ms.register_raft_remove_server(raft_remove_server_impl);
}

future<> raft_group_registry::uninit_rpc_verbs() {
    return when_all_succeed(
        _ms.unregister_raft_send_snapshot(),
        _ms.unregister_raft_append_entries(),
        _ms.unregister_raft_append_entries_reply(),
        _ms.unregister_raft_vote_request(),
        _ms.unregister_raft_vote_reply(),
        _ms.unregister_raft_timeout_now(),
        _ms.unregister_raft_peer_exchange(),
        _ms.unregister_raft_add_server(),
        _ms.unregister_raft_remove_server()
    ).discard_result();
}

future<> raft_group_registry::stop_servers() {
    std::vector<future<>> stop_futures;
    stop_futures.reserve(_servers.size());
    for (auto& entry : _servers) {
        stop_futures.emplace_back(entry.second.server->abort());
    }
    co_await when_all_succeed(stop_futures.begin(), stop_futures.end());
}

seastar::future<> raft_group_registry::init() {
    // Load or initialize a Raft server id. Avoid races on
    // different shards by always querying shard 0.
    _my_addr = co_await container().invoke_on(0, [] (raft_group_registry& self) -> future<raft::server_address> {
        return with_semaphore(self._my_addr_sem, 1, [&self] () -> future<raft::server_address> {
            // Workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95111
            auto self_ptr = &self;
            if (self_ptr->_my_addr.id == raft::server_id{}) {
                self_ptr->_my_addr.id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
                if (self_ptr->_my_addr.id == raft::server_id{}) {
                    self_ptr->_my_addr.id = raft::server_id::create_random_id();
                    co_await db::system_keyspace::set_raft_server_id(self_ptr->_my_addr.id.id);
                }
                self_ptr->_my_addr.info = ser::serialize_to_buffer<bytes>(self_ptr->_gossiper.get_broadcast_address());
            }
            co_return self_ptr->_my_addr;
        });
    });
    // Once a Raft server starts, it soon times out
    // and starts an election, so RPC must be ready by
    // then to send VoteRequest messages.
    co_return init_rpc_verbs();
}

seastar::future<> raft_group_registry::uninit() {
    _abort_source.request_abort();
    co_await when_all_succeed(
        _shutdown_gate.close(),
        uninit_rpc_verbs(),
        stop_servers()
    ).discard_result();
}

raft_group_registry::server_for_group& raft_group_registry::get_server_for_group(raft::group_id gid) {
    auto it = _servers.find(gid);
    if (it == _servers.end()) {
        throw raft_group_not_found(gid);
    }
    return it->second;
}

raft_rpc& raft_group_registry::get_rpc(raft::group_id gid) {
    return get_server_for_group(gid).rpc;
}

raft::server& raft_group_registry::get_server(raft::group_id gid) {
    return *(get_server_for_group(gid).server);
}

raft_group_registry::server_for_group raft_group_registry::create_server_for_group(raft::group_id gid) {

    auto rpc = std::make_unique<raft_rpc>(_ms, *this, gid, _my_addr.id);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(_qp.local(), gid);
    auto state_machine = std::make_unique<schema_raft_state_machine>();
    auto server = raft::create_server(_my_addr.id, std::move(rpc), std::move(state_machine),
            std::move(storage), _fd, raft::server::configuration());

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<ticker_type>([srv = server.get()] { srv->tick(); });

    return server_for_group{
        .gid = std::move(gid),
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
    };
}

future<> raft_group_registry::start_server_for_group(server_for_group new_grp) {
    auto gid = new_grp.gid;
    auto [it, inserted] = _servers.emplace(std::move(gid), std::move(new_grp));

    if (!inserted) {
        on_internal_error(rslog, format("Attempt to add the second instance of raft server with the same gid={}", gid));
    }
    auto& grp = it->second;
    try {
        // start the server instance prior to arming the ticker timer.
        // By the time the tick() is executed the server should already be initialized.
        co_await grp.server->start();
    } catch (...) {
        // remove server from the map to prevent calling `abort()` on a
        // non-started instance when `raft_group_registry::uninit` is called.
        _servers.erase(it);
        on_internal_error(rslog, std::current_exception());
    }

    grp.ticker->arm_periodic(tick_interval);
}

unsigned raft_group_registry::shard_for_group(const raft::group_id& gid) const {
    return 0; // schema raft server is always owned by shard 0
}

gms::inet_address raft_group_registry::get_inet_address(raft::server_id id) const {
    auto it = _srv_address_mappings.find(id);
    if (!it) {
        on_internal_error(rslog, format("Destination raft server not found with id {}", id));
    }
    return *it;
}

void raft_group_registry::update_address_mapping(raft::server_id id, gms::inet_address addr, bool expiring) {
    _srv_address_mappings.set(id, std::move(addr), expiring);
}

void raft_group_registry::remove_address_mapping(raft::server_id id) {
    _srv_address_mappings.erase(id);
}

future<raft::server_address>
raft_group_registry::guess_leader(raft::group_id gid) {
    raft::server& server = get_server(gid);
    co_return co_await with_gate(_shutdown_gate, [this, &server] () -> future<raft::server_address> {
        try {
            co_await server.read_barrier();
            co_return _my_addr;
        } catch (std::exception& e) {
            if (auto n_a_l = dynamic_cast<raft::not_a_leader*>(&e)) {
                if (n_a_l->leader != raft::server_id{}) {
                    auto it = _srv_address_mappings.find(n_a_l->leader);
                    if (it) {
                        auto addr = raft::server_address{
                            .id = n_a_l->leader,
                            .info = ser::serialize_to_buffer<bytes>(*it)
                        };
                        co_return addr;
                    }
                }
                // Fall through into the round-robin mode if there is
                // no mapping for this raft server id or of there is
                // no stable leader.
            } else if (! raft::is_transient_error(e)) {
                throw;
            }
            // We're in a state of uncertainty, an election is
            // in progress.
            std::set<raft::server_address> sorted(
                server.get_configuration().current.begin(),
                server.get_configuration().current.end());
            assert(!sorted.empty());
            auto it = sorted.find(_my_addr);
            // The group is put into registry on this server only
            // after a majority commits the configuration change,
            // but not necessarily this server was part of this
            // majority. Hence, when round-robining over the list
            // of servers we may arrive to a server which doesn't
            // yet have itself as part of the config. In that case
            // skip over to the first server which is part of the
            // config.
            if (it == sorted.end() || ++it == sorted.end()) {
                co_return *sorted.begin();
            }
            co_return *it;
        }
    });
}

future<raft::leader_info>
raft_group_registry::discover_leader() {
    std::vector<raft::server_address> seeds;
    seeds.reserve(_gossiper.get_seeds().size());
    for (auto& seed : _gossiper.get_seeds()) {
        if (seed == _gossiper.get_broadcast_address()) {
            continue;
        }
        seeds.push_back({.info = ser::serialize_to_buffer<bytes>(seed)});
    }
    _group0 = raft::discovery{_my_addr, std::move(seeds)};
    auto& discovery = std::get<raft::discovery>(_group0);
    auto clear_discovery = defer([this] {
        _group0 = raft::no_discovery{};
    });

    struct tracker {
        explicit tracker(raft::discovery::output output_arg) : output(std::move(output_arg)) {}
        raft::discovery::output output;
        promise<std::optional<raft::leader_info>> leader;
        bool is_set = false;
        void set_value(std::optional<raft::leader_info> opt_leader) {
            is_set = true;
            leader.set_value(std::move(opt_leader));
        }
        void set_exception() {
            is_set = true;
            leader.set_exception(std::current_exception());
        }
    };
    // Send peer information to all known peers. If replies
    // discover new peers, send peer information to them as well.
    // As soon as we get a leader information form any peer,
    // return it. If there is no chosen leader, collect replies
    // from all peers, then make this node the leader if it has
    // the smallest id. Otherwise sleep and keep pinging peers
    // till some other node becomes a leader and shares its leader_info
    // with us.
    while (true) {
        auto tracker = make_lw_shared<struct tracker>(discovery.get_output());
        if (std::holds_alternative<raft::discovery::i_am_leader>(tracker->output)) {
            co_return raft::leader_info{.addr = _my_addr};
        }
        if (std::holds_alternative<raft::discovery::pause>(tracker->output)) {
            co_await seastar::sleep_abortable(std::chrono::milliseconds(100), _abort_source);
            continue;
        }
        auto& request_list = std::get<raft::discovery::request_list>(tracker->output);
        auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{100};
        (void) parallel_for_each(request_list, [this, tracker, timeout, &discovery] (
            std::pair<raft::server_address, raft::peer_list>& req) -> future<> {
            netw::msg_addr peer(ser::deserialize_from_buffer(req.first.info,
                    boost::type<gms::inet_address>{}));
            co_await with_gate(_shutdown_gate,
                [this, tracker, &discovery, peer, timeout, from = std::move(req.first), msg = std::move(req.second)] () -> future<> {

                auto reply = co_await _ms.send_raft_peer_exchange(peer, timeout, std::move(msg));
                // Check if this loop iteration has completed already
                // before accessing discovery, which may be gone.
                if (tracker->is_set) {
                    co_return;
                }
                if (std::holds_alternative<raft::peer_list>(reply.info)) {
                    discovery.response(from, std::move(std::get<raft::peer_list>(reply.info)));
                } else if (std::holds_alternative<raft::leader_info>(reply.info)) {
                    tracker->set_value(std::move(std::get<raft::leader_info>(reply.info)));
                }
            });
        }).then_wrapped([tracker] (future<> f) -> future<> {
            // When the leader is discovered, silence all
            // errors.
            if (tracker->is_set) {
                co_return;
            }
            // Silence all runtime errors, such as host
            // unreachable. Propagate rpc and system errors up.
            try {
                co_await std::move(f);
                tracker->set_value({});
            } catch (std::exception& e) {
                if (dynamic_cast<std::runtime_error*>(&e) == nullptr) {
                    tracker->set_exception();
                } else {
                    rslog.debug("discovery failed to send message: {}", e);
                    tracker->set_value({});
                }
            }
        });
        if (auto leader = co_await tracker->leader.get_future()) {
            co_return *leader;
        }
    }
}

// Handle bounce response from the peer and redirect RPC
// to the bounced peer, until success.
// XXX handle shutdown
future<> raft_group_registry::rpc_until_success(raft::server_address addr, auto rpc) {
    while (true) {
        auto success_or_bounce = co_await with_gate(_shutdown_gate, [this, &addr, &rpc] () ->
                future<raft::success_or_bounce> {
            auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000};
            netw::msg_addr peer(ser::deserialize_from_buffer(addr.info, boost::type<gms::inet_address>{}));
            try {
                co_return co_await rpc(peer, timeout);
            } catch (std::exception& e) {
                if (dynamic_cast<std::runtime_error*>(&e) != nullptr) {
                    // Re-try with the same peer.
                    co_return raft::success_or_bounce{.bounce = addr};
                } else {
                    throw;
                }
            }
        });
        if (!success_or_bounce.bounce) {
            break;
        }
        addr = *success_or_bounce.bounce;
        co_await seastar::sleep_abortable(std::chrono::milliseconds(100), _abort_source);
    }
}

future<> raft_group_registry::join_raft_group0() {
    assert(this_shard_id() == 0);
    assert(_my_addr.id != raft::server_id{});
    raft::group_id group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
    if (group0_id != raft::group_id{}) {
        co_await start_server_for_group(create_server_for_group(group0_id));
        _group0 = group0_id;
        co_return;
    }
    raft::server_address leader;

    auto li = co_await discover_leader();
    group0_id = li.group0_id;
    leader = li.addr;

    raft::configuration initial_configuration;
    if (leader.id == _my_addr.id) {
         // Time-based ordering for groups identifiers may be
         // useful to provide linearisability between group
         // operations. Currently it's unused.
        group0_id = raft::group_id{utils::UUID_gen::get_time_UUID()};
        initial_configuration.current.emplace(_my_addr);
    }
    auto grp = create_server_for_group(group0_id);
    co_await grp.server->bootstrap(std::move(initial_configuration));
    co_await start_server_for_group(std::move(grp));
    if (leader.id != _my_addr.id) {
        co_await rpc_until_success(leader, [this, group0_id] (auto peer, auto timeout)
                -> future<raft::success_or_bounce> {
            co_return co_await _ms.send_raft_add_server(peer, timeout, group0_id, _my_addr);
        });
    }
    co_await db::system_keyspace::set_raft_group0_id(group0_id.id);
    // Allow peer_exchange() RPC to access group 0 only after group0_id is persisted.
    _group0 = group0_id;
}

future<> raft_group_registry::leave_raft_group0() {
    assert(this_shard_id() == 0);
    if (_my_addr.id == raft::server_id{}) {
        // Nothing to do
        co_return;
    }
    raft::group_id group0_id;
    raft::server_address leader;
    if (std::holds_alternative<raft::group_id>(_group0)) {
        group0_id = std::get<raft::group_id>(_group0);
    } else {
        group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
        if (group0_id == raft::group_id{}) {
            auto li = co_await discover_leader();
            group0_id = li.group0_id;
            leader = li.addr;
            if (leader.id == _my_addr.id) {
                co_return;
            }
        }
        co_await start_server_for_group(create_server_for_group(group0_id));
    }
    leader = co_await guess_leader(group0_id);
    co_await rpc_until_success(leader, [this, group0_id] (auto peer, auto timeout)
            -> future<raft::success_or_bounce> {
        co_return co_await _ms.send_raft_remove_server(peer, timeout, group0_id, _my_addr.id);
    });
}

future<raft::peer_exchange> raft_group_registry::peer_exchange(raft::peer_list peers) {
    return std::visit([this, peers = std::move(peers)] (auto&& d) -> future<raft::peer_exchange> {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, raft::no_discovery>) {
            // Discovery not started or we're persisting the
            // leader information locally.
            co_return raft::peer_exchange{raft::no_discovery{}};
        } else if constexpr (std::is_same_v<T, raft::discovery>) {
            // Use discovery to produce a response
            co_return raft::peer_exchange{d.request(std::move(peers))};
        } else if constexpr (std::is_same_v<T, raft::group_id>) {
            // Obtain leader information from existing Group 0 server.
            auto addr = co_await guess_leader(std::get<raft::group_id>(_group0));
            co_return raft::peer_exchange{raft::leader_info{
                .group0_id = std::get<raft::group_id>(_group0),
                .addr = addr
            }};
        }
    }, _group0);
}

} // end of namespace service

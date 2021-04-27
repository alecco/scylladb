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
#include "service/raft/raft_gossip_failure_detector.hh"
#include "message/messaging_service.hh"
#include "serializer_impl.hh"


#include <seastar/core/coroutine.hh>

namespace service {

logging::logger rslog("raft_group_registry");

raft_group_registry::raft_group_registry(netw::messaging_service& ms, gms::gossiper& gossiper)
    : _ms(ms), _fd(make_shared<raft_gossip_failure_detector>(gossiper, _srv_address_mappings))
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
            self._srv_address_mappings.set(from, std::move(addr), true);
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

    _ms.register_raft_read_quorum([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum read_quorum) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum] (raft_rpc& rpc) mutable {
            rpc.read_quorum_request(std::move(from), std::move(read_quorum));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_read_quorum_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum_reply read_quorum_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum_reply] (raft_rpc& rpc) mutable {
            rpc.read_quorum_reply(std::move(from), std::move(read_quorum_reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_execute_read_barrier_on_leader([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from] (raft_rpc& rpc) mutable {
            return rpc.execute_read_barrier(from);
        });
    });

    auto raft_add_server_impl = [this] (const rpc::client_info& cinfo,
            rpc::opt_time_point timeout, raft::group_id gid, raft::server_address addr)
            -> future<raft_success_or_bounce> {

        return container().invoke_on(shard_for_group(gid), [gid, addr] (
                raft_group_registry& self) -> future<raft_success_or_bounce> {

            rslog.info("Adding node to Raft Group 0 {}",
            ser::deserialize_from_buffer(addr.info, boost::type<gms::inet_address>{}));
            raft::server& server = self.get_server(gid);
            try {
                co_await with_semaphore(self._cfg_sem, 1, [&server, addr] () -> future<> {
                    auto cfg = server.get_configuration().current;
                    cfg.emplace(std::move(addr));
                    co_await server.set_configuration(cfg);
                });
                co_return raft_success_or_bounce{};
            } catch (const std::exception& e) {
                if (! raft::is_transient_error(e)) {
                    throw;
                }
            }
            auto bounce = co_await self.guess_leader(gid);
            co_return raft_success_or_bounce{.bounce = bounce};
        });
    };

    _ms.register_raft_add_server(raft_add_server_impl);

    auto raft_remove_server_impl = [this] (const rpc::client_info& cinfo,
        rpc::opt_time_point timeout, raft::group_id gid, raft::server_id sid) -> future<raft_success_or_bounce> {

        return container().invoke_on(shard_for_group(gid), [gid, sid] (
                raft_group_registry& self) -> future<raft_success_or_bounce> {

            rslog.info("Removing node to Raft Group 0 {}", sid);
            raft::server& server = self.get_server(gid);
            try {
                co_await with_semaphore(self._cfg_sem, 1, [&server, sid] () -> future<> {
                    auto cfg = server.get_configuration().current;
                    cfg.erase(raft::server_address{.id = sid});
                    co_await server.set_configuration(cfg);
                });
                co_return raft_success_or_bounce{};
            } catch (const std::exception& e) {
                if (! raft::is_transient_error(e)) {
                    throw;
                }
            }
            auto bounce = co_await self.guess_leader(gid);
            co_return raft_success_or_bounce{.bounce = bounce};
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
        _ms.unregister_raft_read_quorum(),
        _ms.unregister_raft_read_quorum_reply(),
        _ms.unregister_raft_execute_read_barrier_on_leader(),
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

seastar::future<> raft_group_registry::start() {
    // Once a Raft server starts, it soon times out
    // and starts an election, so RPC must be ready by
    // then to send VoteRequest messages.
    co_return init_rpc_verbs();
}

seastar::future<> raft_group_registry::stop() {
    _cfg_sem.broken();
    co_await when_all_succeed(
        _shutdown_gate.close(),
        uninit_rpc_verbs(),
        stop_servers()
    ).discard_result();
}

raft_server_for_group& raft_group_registry::server_for_group(raft::group_id gid) {
    auto it = _servers.find(gid);
    if (it == _servers.end()) {
        throw raft_group_not_found(gid);
    }
    return it->second;
}

raft_rpc& raft_group_registry::get_rpc(raft::group_id gid) {
    return server_for_group(gid).rpc;
}

raft::server& raft_group_registry::get_server(raft::group_id gid) {
    return *(server_for_group(gid).server);
}

raft::server& raft_group_registry::group0() {
    return *(server_for_group(*_group0_id).server);
}

future<> raft_group_registry::start_server_for_group(raft_server_for_group new_grp) {
    auto gid = new_grp.gid;
    auto [it, inserted] = _servers.emplace(std::move(gid), std::move(new_grp));

    if (!inserted) {
        on_internal_error(rslog, format("Attempt to add the second instance of raft server with the same gid={}", gid));
    }
    if (_servers.size() == 1 && this_shard_id() == 0) {
        _group0_id = gid;
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

    grp.ticker->arm_periodic(raft_tick_interval);
}

unsigned raft_group_registry::shard_for_group(const raft::group_id& gid) const {
    return 0; // schema raft server is always owned by shard 0
}

future<raft::server_address>
raft_group_registry::guess_leader(raft::group_id gid) {
    raft::server& server = get_server(gid);
    co_return co_await with_gate(_shutdown_gate, [this, &server] () -> future<raft::server_address> {
        try {
            co_await server.read_barrier();
            co_return _srv_address_mappings.get_server_address(server.id());
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
            auto it = sorted.find(raft::server_address{.id = server.id()});
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

} // end of namespace service

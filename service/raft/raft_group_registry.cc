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
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>

namespace service {

logging::logger rslog("raft_group_registry");

raft_group_registry::raft_group_registry(netw::messaging_service& ms, gms::gossiper& gs, sharded<cql3::query_processor>& qp)
    : _ms(ms), _gossiper(gs), _qp(qp), _fd(make_shared<raft_gossip_failure_detector>(gs, *this))
{
    (void) _gossiper;
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

            return make_ready_future<raft::peer_exchange>();
        });
    };

    _ms.register_raft_peer_exchange(peer_exchange_impl);

    auto raft_add_server_impl = [this] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_address addr) {

        return container().invoke_on(shard_for_group(gid), [gid, addr] (
                raft_group_registry& self) {

            return make_ready_future<raft::success_or_bounce>();
        });
    };

    _ms.register_raft_add_server(raft_add_server_impl);

    auto raft_remove_server_impl = [this] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id sid) {

        return container().invoke_on(shard_for_group(gid), [gid, sid] (
                raft_group_registry& self) {

            return make_ready_future<raft::success_or_bounce>();
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
            if (self._my_addr.id == raft::server_id{}) {
                self._my_addr.id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
                if (self._my_addr.id == raft::server_id{}) {
                    self._my_addr.id = raft::server_id::create_random_id();
                    co_await db::system_keyspace::set_raft_server_id(self._my_addr.id.id);
                }
                self._my_addr.info = ser::serialize_to_buffer<bytes>(self._gossiper.get_broadcast_address());
            }
            co_return self._my_addr;
        });
    });
    // Once a Raft server starts, it soon times out
    // and starts an election, so RPC must be ready by
    // then to send VoteRequest messages.
    co_return init_rpc_verbs();
}

seastar::future<> raft_group_registry::uninit() {
    return uninit_rpc_verbs().then([this] {
        return stop_servers();
    });
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

} // end of namespace service

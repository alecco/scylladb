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
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_gossip_failure_detector.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/raft/schema_raft_state_machine.hh"

#include "message/messaging_service.hh"
#include "cql3/query_processor.hh"
#include "gms/gossiper.hh"
#include "db/system_keyspace.hh"

#include <seastar/core/smp.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>

namespace service {

raft_group0::raft_group0(raft_group_registry& raft_gr,
        netw::messaging_service& ms,
        gms::gossiper& gs,
        cql3::query_processor& qp,  // XXX here
        service::migration_manager& mm)
    : _raft_gr(raft_gr), _ms(ms), _gossiper(gs), _qp(qp), _mm(mm)
{
}

seastar::future<raft::server_address> raft_group0::load_or_create_my_addr() {
    assert(this_shard_id() == 0);
    raft::server_address my_addr;
    my_addr.id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (my_addr.id == raft::server_id{}) {
        my_addr.id = raft::server_id::create_random_id();
        co_await db::system_keyspace::set_raft_server_id(my_addr.id.id);
    }
    my_addr.info = ser::serialize_to_buffer<bytes>(_gossiper.get_broadcast_address());
    co_return my_addr;
}

raft_server_for_group raft_group0::create_server_for_group(raft::group_id gid,
        raft::server_address my_addr) {

    _raft_gr.address_map().set(my_addr.id,
        ser::deserialize_from_buffer(my_addr.info, boost::type<gms::inet_address>{}),
        false);
    auto rpc = std::make_unique<raft_rpc>(_ms, _raft_gr.address_map(), gid, my_addr.id);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    // XXX here
    auto storage = std::make_unique<raft_sys_table_storage>(_qp, gid);
    auto state_machine = std::make_unique<schema_raft_state_machine>();
    auto server = raft::create_server(my_addr.id, std::move(rpc), std::move(state_machine),
            std::move(storage), _raft_gr.failure_detector(), raft::server::configuration());

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });

    return raft_server_for_group{
        .gid = std::move(gid),
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
    };
}

future<raft_leader_info>
raft_group0::discover_leader(raft::server_address my_addr) {
    std::vector<raft::server_address> seeds;
    seeds.reserve(_gossiper.get_seeds().size());
    for (auto& seed : _gossiper.get_seeds()) {
        if (seed == _gossiper.get_broadcast_address()) {
            continue;
        }
        seeds.push_back({.info = ser::serialize_to_buffer<bytes>(seed)});
    }
    _group0 = discovery{my_addr, std::move(seeds)};
    service::discovery& discovery = std::get<service::discovery>(_group0);
    auto clear_discovery = defer([this] {
        _group0 = std::monostate{};
    });

    struct tracker {
        explicit tracker(discovery::output output_arg) : output(std::move(output_arg)) {}
        discovery::output output;
        promise<std::optional<raft_leader_info>> leader;
        bool is_set = false;
        void set_value(std::optional<raft_leader_info> opt_leader) {
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
        if (std::holds_alternative<discovery::i_am_leader>(tracker->output)) {
            co_return raft_leader_info{.addr = my_addr};
        }
        if (std::holds_alternative<discovery::pause>(tracker->output)) {
            co_await seastar::sleep_abortable(std::chrono::milliseconds(100), _abort_source);
            continue;
        }
        auto& request_list = std::get<discovery::request_list>(tracker->output);
        auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{100};
        (void) parallel_for_each(request_list, [this, tracker, timeout, &discovery] (
            std::pair<raft::server_address, discovery::peer_list>& req) -> future<> {
            netw::msg_addr peer(ser::deserialize_from_buffer(req.first.info,
                    boost::type<gms::inet_address>{}));
            co_await with_gate(_shutdown_gate,
                [this, tracker, &discovery, peer, timeout, from = std::move(req.first), msg = std::move(req.second)] () -> future<> {

                rslog.info("Sending discovery message to {}", peer);
                auto reply = co_await _ms.send_raft_peer_exchange(peer, timeout, std::move(msg));
                // Check if this loop iteration has completed already
                // before accessing discovery, which may be gone.
                if (tracker->is_set) {
                    co_return;
                }
                if (std::holds_alternative<discovery::peer_list>(reply.info)) {
                    discovery.response(from, std::move(std::get<discovery::peer_list>(reply.info)));
                } else if (std::holds_alternative<raft_leader_info>(reply.info)) {
                    tracker->set_value(std::move(std::get<raft_leader_info>(reply.info)));
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
future<> raft_group0::rpc_until_success(raft::server_address addr, auto rpc) {
    while (true) {
        auto success_or_bounce = co_await with_gate(_shutdown_gate, [this, &addr, &rpc] () ->
                future<raft_success_or_bounce> {
            auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000};
            netw::msg_addr peer(ser::deserialize_from_buffer(addr.info, boost::type<gms::inet_address>{}));
            try {
                co_return co_await rpc(peer, timeout);
            } catch (std::exception& e) {
                if (dynamic_cast<std::runtime_error*>(&e) != nullptr) {
                    // Re-try with the same peer.
                    co_return raft_success_or_bounce{.bounce = addr};
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

future<> raft_group0::join_raft_group0() {
    assert(this_shard_id() == 0);
    auto my_addr = co_await load_or_create_my_addr();
    raft::group_id group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
    if (group0_id != raft::group_id{}) {
        co_await _raft_gr.start_server_for_group(create_server_for_group(group0_id, my_addr));
        _group0 = group0_id;
        co_return;
    }
    raft::server_address leader;

    auto li = co_await discover_leader(my_addr);
    group0_id = li.group0_id;
    leader = li.addr;

    raft::configuration initial_configuration;
    if (leader.id == my_addr.id) {
         // Time-based ordering for groups identifiers may be
         // useful to provide linearisability between group
         // operations. Currently it's unused.
        group0_id = raft::group_id{utils::UUID_gen::get_time_UUID()};
        initial_configuration.current.emplace(my_addr);
    }
    auto grp = create_server_for_group(group0_id, my_addr);
    co_await grp.server->bootstrap(std::move(initial_configuration));
    co_await _raft_gr.start_server_for_group(std::move(grp));
    if (leader.id != my_addr.id) {
        co_await rpc_until_success(leader, [this, group0_id, my_addr] (auto peer, auto timeout)
                -> future<raft_success_or_bounce> {
            rslog.info("Sending add node RPC to {}", peer);
            co_return co_await _ms.send_raft_add_server(peer, timeout, group0_id, my_addr);
        });
    }
    co_await db::system_keyspace::set_raft_group0_id(group0_id.id);
    // Allow peer_exchange() RPC to access group 0 only after group0_id is persisted.
    _group0 = group0_id;
}

future<> raft_group0::leave_raft_group0() {
    assert(this_shard_id() == 0);
    raft::server_address my_addr;
    my_addr.id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (my_addr.id == raft::server_id{}) {
        // Nothing to do
        co_return;
    }
    my_addr.info = ser::serialize_to_buffer<bytes>(_gossiper.get_broadcast_address());
    raft::group_id group0_id;
    raft::server_address leader;
    if (std::holds_alternative<raft::group_id>(_group0)) {
        group0_id = std::get<raft::group_id>(_group0);
    } else {
        group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
        if (group0_id == raft::group_id{}) {
            auto li = co_await discover_leader(my_addr);
            group0_id = li.group0_id;
            leader = li.addr;
            if (leader.id == my_addr.id) {
                co_return;
            }
        }
        co_await _raft_gr.start_server_for_group(create_server_for_group(group0_id, my_addr));
    }
    leader = co_await _raft_gr.guess_leader(group0_id);
    co_await rpc_until_success(leader, [this, group0_id, my_addr] (auto peer, auto timeout)
            -> future<raft_success_or_bounce> {
        co_return co_await _ms.send_raft_remove_server(peer, timeout, group0_id, my_addr.id);
    });
}

future<raft_peer_exchange> raft_group0::peer_exchange(discovery::peer_list peers) {
    return std::visit([this, peers = std::move(peers)] (auto&& d) -> future<raft_peer_exchange> {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            // Discovery not started or we're persisting the
            // leader information locally.
            co_return raft_peer_exchange{std::monostate{}};
        } else if constexpr (std::is_same_v<T, discovery>) {
            // Use discovery to produce a response
            co_return raft_peer_exchange{d.request(std::move(peers))};
        } else if constexpr (std::is_same_v<T, raft::group_id>) {
            // Obtain leader information from existing Group 0 server.
            auto addr = co_await _raft_gr.guess_leader(std::get<raft::group_id>(_group0));
            co_return raft_peer_exchange{raft_leader_info{
                .group0_id = std::get<raft::group_id>(_group0),
                .addr = addr
            }};
        }
    }, _group0);
}

} // end of namespace service


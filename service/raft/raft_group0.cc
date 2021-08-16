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

future<group0_info>
raft_group0::discover_group0(raft::server_address my_addr) {
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
        promise<std::optional<group0_info>> g0_info;
        bool is_set = false;
        void set_value(std::optional<group0_info> opt_g0_info) {
            is_set = true;
            g0_info.set_value(std::move(opt_g0_info));
        }
        void set_exception() {
            is_set = true;
            g0_info.set_exception(std::current_exception());
        }
    };
    // Send peer information to all known peers. If replies
    // discover new peers, send peer information to them as well.
    // As soon as we get a Raft Group 0 member information form
    // any peer, return it. If there is no Group 0, collect
    // replies from all peers, then, if this server has the smallest
    // id, make a new Group 0 with this server as the only member.
    // Otherwise sleep and
    // keep pinging peers till some other node creates a group
    // and shares its group 0 id and peer address with us.
    while (true) {
        auto tracker = make_lw_shared<struct tracker>(discovery.get_output());
        if (std::holds_alternative<discovery::i_am_leader>(tracker->output)) {
            co_return group0_info{
                // Time-based ordering for groups identifiers may be
                // useful to provide linearisability between group
                // operations. Currently it's unused.
                .group0_id = raft::group_id{utils::UUID_gen::get_time_UUID()},
                .addr = my_addr
            };
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
            auto pause_shutdown = _shutdown_gate.hold();

            rslog.info("Sending discovery message to {}", peer);
            auto reply = co_await _ms.send_group0_peer_exchange(peer, timeout, std::move(req.second));
            // Check if this loop iteration has completed already
            // before accessing discovery, which may be gone.
            if (tracker->is_set) {
                co_return;
            }
            if (std::holds_alternative<discovery::peer_list>(reply.info)) {
                discovery.response(req.first, std::move(std::get<discovery::peer_list>(reply.info)));
            } else if (std::holds_alternative<group0_info>(reply.info)) {
                tracker->set_value(std::move(std::get<group0_info>(reply.info)));
            }
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
        if (auto g0_info = co_await tracker->g0_info.get_future()) {
            co_return *g0_info;
        }
    }
}

future<> raft_group0::join_group0() {
    assert(this_shard_id() == 0);
    auto my_addr = co_await load_or_create_my_addr();
    raft::group_id group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
    if (group0_id != raft::group_id{}) {
        co_await _raft_gr.start_server_for_group(create_server_for_group(group0_id, my_addr));
        _group0 = group0_id;
        co_return;
    }
    raft::server* server = nullptr;
    while (true) {
        auto g0_info = co_await discover_group0(my_addr);
        if (server && group0_id != g0_info.group0_id) {
            // Subsequent discovery returned a different group 0 id?!
            throw std::runtime_error("Can't add server to two clusters. Please check your seeds don't overlap");
        }
        group0_id =  g0_info.group0_id;
        if (server == nullptr) {
            // This is the first time the discovery is run.
            raft::configuration initial_configuration;
            if (g0_info.addr.id == my_addr.id) {
                // We should start a new group.
                initial_configuration.current.emplace(my_addr);
            }
            auto grp = create_server_for_group(group0_id, my_addr);
            server = grp.server.get();
            co_await grp.server->bootstrap(std::move(initial_configuration));
            co_await _raft_gr.start_server_for_group(std::move(grp));
        }
        if (server->get_configuration().can_vote(my_addr.id)) {
            // True if we started a new group or completed a configuration change
            // initiated earlier.
            break;
        }
        std::vector<raft::server_address> add_set;
        add_set.push_back(my_addr);
        auto pause_shutdown = _shutdown_gate.hold();
        auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000};
        netw::msg_addr peer(ser::deserialize_from_buffer(g0_info.addr.info, boost::type<gms::inet_address>{}));
        try {
            co_await _ms.send_group0_modify_config(peer, timeout, group0_id, add_set, {});
            break;
        } catch (std::runtime_error& e) {
            // Retry
        }
        // Try again after a pause
        co_await seastar::sleep_abortable(std::chrono::milliseconds(100), _abort_source);
    }
    co_await db::system_keyspace::set_raft_group0_id(group0_id.id);
    // Allow peer_exchange() RPC to access group 0 only after group0_id is persisted.
    _group0 = group0_id;
}

future<> raft_group0::leave_group0() {
    assert(this_shard_id() == 0);
    raft::server_address my_addr;
    my_addr.id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (my_addr.id == raft::server_id{}) {
        // Nothing to do
        co_return;
    }
    std::vector<raft::server_id> del_set;
    del_set.push_back(my_addr.id);
    auto pause_shutdown = _shutdown_gate.hold();
    if (std::holds_alternative<raft::group_id>(_group0)) {
        co_return co_await _raft_gr.group0().modify_config({}, del_set);
    }
    auto g0_info = co_await discover_group0(my_addr);
    if (g0_info.addr.id == my_addr.id) {
        co_return;
    }
    netw::msg_addr peer(ser::deserialize_from_buffer(g0_info.addr.info, boost::type<gms::inet_address>{}));
    auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000000};
    co_return co_await _ms.send_group0_modify_config(peer, timeout, g0_info.group0_id, {}, del_set);
}

future<group0_peer_exchange> raft_group0::peer_exchange(discovery::peer_list peers) {
    return std::visit([this, peers = std::move(peers)] (auto&& d) -> future<group0_peer_exchange> {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            // Discovery not started or we're persisting the
            // leader information locally.
            co_return group0_peer_exchange{std::monostate{}};
        } else if constexpr (std::is_same_v<T, discovery>) {
            // Use discovery to produce a response
            co_return group0_peer_exchange{d.request(std::move(peers))};
        } else if constexpr (std::is_same_v<T, raft::group_id>) {
            // Even if in follower state, return own address: the
            // incoming RPC will then be bounced to the leader.
            co_return group0_peer_exchange{group0_info{
                .group0_id = std::get<raft::group_id>(_group0),
                .addr = _raft_gr.address_map().get_server_address(_raft_gr.group0().id())
            }};
        }
    }, _group0);
}

} // end of namespace service


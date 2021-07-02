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
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/abort_source.hh>

#include "message/messaging_service_fwd.hh"
#include "gms/inet_address.hh"
#include "raft/raft.hh"
#include "raft/discovery.hh"
#include "raft/server.hh"
#include "service/raft/raft_address_map.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace gms {

class gossiper;

} // namespace gms

namespace service {

class raft_rpc;
class raft_gossip_failure_detector;

struct raft_group_not_found: public raft::error {
    raft::group_id gid;
    raft_group_not_found(raft::group_id gid_arg)
            : raft::error(format("Raft group {} not found", gid_arg)), gid(gid_arg)
    {}
};

// An entry in the group registry
struct raft_server_for_group {
    raft::group_id gid;
    std::unique_ptr<raft::server> server;
    std::unique_ptr<raft_ticker_type> ticker;
    raft_rpc& rpc;
};

// This class is responsible for creating, storing and accessing raft servers.
// It also manages the raft rpc verbs initialization.
//
// `peering_sharded_service` inheritance is used to forward requests
// to the owning shard for a given raft group_id.
class raft_group_registry : public seastar::peering_sharded_service<raft_group_registry> {
public:
private:
    seastar::gate _shutdown_gate;
    seastar::abort_source _abort_source;
    netw::messaging_service& _ms;
    gms::gossiper& _gossiper;
    sharded<cql3::query_processor>& _qp;
    // Shard-local failure detector instance shared among all raft groups
    shared_ptr<raft_gossip_failure_detector> _fd;
    // Raft Server ID of this server. Valid after init().
    raft::server_address _my_addr;
    // Serialize read/write to _my_addr on a given shard
    semaphore _my_addr_sem{1};
    // Status of leader discovery. Initially there is no group 0,
    // and the variant contains no state. During initial cluster
    // bootstrap a discovery object is created, which is then
    // substituted by group0 id when a leader is discovered or
    // created.
    std::variant<raft::no_discovery, raft::discovery, raft::group_id> _group0;

    // Raft servers along with the corresponding timers to tick each instance.
    // Currently ticking every 100ms.
    std::unordered_map<raft::group_id, raft_server_for_group> _servers;
    // inet_address:es for remote raft servers known to us
    raft_address_map<> _srv_address_mappings;

    void init_rpc_verbs();
    seastar::future<> uninit_rpc_verbs();
    seastar::future<> stop_servers();

    raft_server_for_group create_server_for_group(raft::group_id id);

    raft_server_for_group& server_for_group(raft::group_id id);
    // A helper function to get the current Raft group leader.
    // Checks if this server is a leader, and if it's the
    // case, returns its address.
    // Otherwise one of the two cases are possible:
    // 1. This is a follower of a stable leader.
    //    Returns the address of this leader.
    // 2. This server is not a leader and not a follower of an
    //    existing leader. E.g. election is in progress or this server
    //    lost leadership just recently and is a follower with no
    //    leader. Instead of waiting for election to finish, orders
    //    the configuration lexicographically and returns the next
    //    server address. This helps avoid a deadlock if e.g. this
    //    server is partitioned away from the majority. Instead of
    //    waiting for election to finish, perhaps indefinitely, we
    //    switch to another server.
    //
    // Internal errors are propagated up unchanged.
    //
    future<raft::server_address> guess_leader(raft::group_id gid);

    // A helper function to discover Raft Group 0 leader in
    // absence of running group 0 server.
    future<raft::leader_info> discover_leader();
    // A helper to run Raft RPC in a loop handling bounces
    // and trying different leaders until it succeeds.
    future<> rpc_until_success(raft::server_address addr, auto rpc);
public:

    raft_group_registry(netw::messaging_service& ms, gms::gossiper& gs, sharded<cql3::query_processor>& qp);
    // To integrate Raft service into the boot procedure, the
    // initialization is split into 2 steps:
    // - in sharded::start(), we construct an instance of
    // raft_group_registry on each shard. The RPC is not available
    // yet and no groups are created. The created object is
    // useless but it won't crash on use.
    // - in init(), which must be invoked explicitly on each
    // shard after the query processor and database have started,
    // we boot all existing groups from the local system tables
    // and start RPC
    seastar::future<> init();
    // Must be invoked explicitly on each shard to stop this service.
    seastar::future<> uninit();

    raft_rpc& get_rpc(raft::group_id gid);

    // Find server for group by group id. Throws exception if
    // there is no such group.
    raft::server& get_server(raft::group_id gid);

    // Start raft server instance, store in the map of raft servers and
    // arm the associated timer to tick the server.
    future<> start_server_for_group(raft_server_for_group grp);
    unsigned shard_for_group(const raft::group_id& gid) const;

    // Join this node to the cluster-wide Raft group
    // Called during bootstrap. Is idempotent - it
    // does nothing if already done, or resumes from the
    // unifinished state if aborted. The result is that
    // raft service has group 0 running.
    future<> join_raft_group0();

    // Remove the node from the cluster-wide raft group.
    // This procedure is idempotent. In case of replace node,
    // it removes the replaced node from the group, since
    // it can't do it by itself (it's dead).
    future<> leave_raft_group0();

    // Handle peer_exchange RPC
    future<raft::peer_exchange> peer_exchange(raft::peer_list peers);
};

} // end of namespace service

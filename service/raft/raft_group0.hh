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
#pragma once
#include "service/raft/raft_group_registry.hh"
#include "raft/discovery.hh"

namespace cql3 { class query_processor; }

namespace gms { class gossiper; }

namespace service {

class migration_manager;

class raft_group0 {
public:
    seastar::gate _shutdown_gate;
    seastar::abort_source _abort_source;
    raft_group_registry& _raft_gr;
    netw::messaging_service& _ms;
    gms::gossiper& _gossiper;
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    // Status of leader discovery. Initially there is no group 0,
    // and the variant contains no state. During initial cluster
    // bootstrap a discovery object is created, which is then
    // substituted by group0 id when a leader is discovered or
    // created.
    std::variant<raft::no_discovery, raft::discovery, raft::group_id> _group0;

    raft_server_for_group create_server_for_group(raft::group_id id, raft::server_address my_addr);
    future<raft::server_address> load_or_create_my_addr();
public:
    raft_group0(service::raft_group_registry& raft_gr,
        netw::messaging_service& ms,
        gms::gossiper& gs,
        cql3::query_processor& qp,
        migration_manager& mm);

    future<> abort() {
        _abort_source.request_abort();
        return _shutdown_gate.close();
    }

    // A helper function to discover Raft Group 0 leader in
    // absence of running group 0 server.
    future<raft::leader_info> discover_leader(raft::server_address my_addr);
    // A helper to run Raft RPC in a loop handling bounces
    // and trying different leaders until it succeeds.
    future<> rpc_until_success(raft::server_address addr, auto rpc);

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

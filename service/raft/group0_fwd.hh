/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iosfwd>
#include "raft/raft.hh"
#include "gms/inet_address.hh"

namespace service {

// Address of a discovery peer
struct discovery_peer {
    raft::server_id id;
    gms::inet_address ip;
    bool operator==(const discovery_peer& rhs) const {
        return id == rhs.id;
    }

    bool operator==(const raft::server_id& rhs) const {
        return id == rhs;
    }

    bool operator<(const discovery_peer& rhs) const {
        return id < rhs.id;
    }
    discovery_peer(raft::server_id id_, gms::inet_address ip_)
        : id(id_)
        , ip(ip_)
    {}
};

// Used in a bootstrapped Scylla cluster, provides group  0
// identifier and the current group leader address.
struct group0_info {
    raft::group_id group0_id;
    raft::server_id id;
    gms::inet_address ip;
    bool operator==(const group0_info& rhs) const {
        return rhs.group0_id == group0_id && rhs.id == id && rhs.ip == ip;
    }
};

// If the peer has no cluster discovery running, it returns
// std::monostate, which means the caller needs to retry
// contacting this server after a pause. Otherwise it returns
// its leader data or a list of peers.
struct group0_peer_exchange {
    std::variant<std::monostate, group0_info, std::vector<discovery_peer>> info;
};

enum class group0_upgrade_state : uint8_t {
    // In recovery state group 0 is disabled.
    // The node does not attempt any Raft/group 0 related operations, including upgrade.
    recovery = 0,

    // In `use_pre_raft_procedures` state the node performs schema changes without using group 0,
    // using the 'old ways': by updating its local schema tables and attempting to push the change to others
    // through direct RPC. If necessary, changes are later propagated with gossip.
    use_pre_raft_procedures = 1,

    // In `synchronize` state the node rejects any attempts for changing the schema.
    // Schema changes may still arrive from other nodes for some time. However, if no failures occur
    // during the upgrade procedure, eventually all nodes should enter `synchronize` state. Then
    // the nodes ensure that schema is synchronized across the entire cluster before entering `use_post_raft_procedures`.
    synchronize = 2,

    // In `use_post_raft_procedures` state the upgrade is finished. The node performs schema changes
    // using group 0, i.e. by constructing appropriate Raft commands and sending them to the Raft group 0 cluster.
    use_post_raft_procedures = 3,
};

inline constexpr uint8_t group0_upgrade_state_last = 3;

std::ostream& operator<<(std::ostream&, group0_upgrade_state);

} // namespace service

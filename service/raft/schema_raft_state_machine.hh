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

#include "raft/raft.hh"
#include "utils/UUID_gen.hh"
#include "canonical_mutation.hh"
#include "service/raft/raft_state_machine.hh"

namespace service {
class migration_manager;

struct schema_raft_command {
    // Mutations of schema tables (such as `system_schema.keyspaces`, `system_schema.tables` etc.)
    // Describe a DDL statement (keyspace/table/type create/drop/update etc.)
    std::vector<canonical_mutation> mutations;

    // Each state of the Raft schema state machine has a unique ID (which is a timeuuid).
    //
    // There is only one state of the schema state machine to which this schema change can be correctly applied:
    // the state which was used to validate the change and compute the mutations.
    //
    // When the change is computed, we read the state ID from the state machine and save it in the command
    // (`prev_state_id`).
    //
    // When we apply the change (in `state_machine::apply`), we verify that `prev_state_id` is still equal to the machine's state ID.
    //
    // If not, it means there was a concurrent schema update which invalidated our change;
    // in that case we won't apply our mutations, effectively making the command a no-op.
    // The creator of the change must recompute it using the new state and retry (or find that the schema update
    // they are trying to perform is no longer valid in the context of this new state).
    //
    // Otherwise we update the state ID (`new_state_id`).
    //
    // Exception: if `prev_state_id == utils::UUID{}`, we skip the verification step.
    // This is used to apply schema changes unconditionally for internal distributed tables
    // and for code that wasn't yet fully converted to use Raft-based schema changes.
    utils::UUID prev_state_id;
    utils::UUID new_state_id;

    // Address of the creator of this command. For debugging.
    gms::inet_address creator_addr;
};

// Raft state machine implementation for managing schema changes.
// NOTE: schema raft server is always instantiated on shard 0.
class schema_raft_state_machine : public raft_state_machine {
    migration_manager& _mm;
public:
    schema_raft_state_machine(migration_manager& mm) : _mm(mm) {}
    future<> apply(std::vector<raft::command_cref> command) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> transfer_snapshot(gms::inet_address from, raft::snapshot_id snp) override;
    future<> abort() override;
};

} // end of namespace service

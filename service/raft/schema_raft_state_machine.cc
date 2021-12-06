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
#include "service/raft/schema_raft_state_machine.hh"
#include <seastar/core/coroutine.hh>
#include "service/migration_manager.hh"
#include "message/messaging_service.hh"
#include "canonical_mutation.hh"
#include "schema_mutations.hh"
#include "frozen_schema.hh"
#include "serialization_visitors.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/schema_raft_state_machine.dist.hh"
#include "idl/schema_raft_state_machine.dist.impl.hh"
#include "service/migration_manager.hh"
#include "db/system_keyspace.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"

namespace service {

static logging::logger slogger("schema_raft_sm");

static future<> update_schema_state_id(cql3::query_processor& qp, utils::UUID new_state_id) {
    // SCHEMA_RAFT_HISTORY table exists since raft feature is enabled (or `apply` wouldn't be called)
    co_await qp.execute_internal(
        format(
            "INSERT INTO system.{} (key, schema_id) VALUES ('history', ?)",
            db::system_keyspace::v3::SCHEMA_RAFT_HISTORY),
        {new_state_id});
}

future<utils::UUID> migration_manager::get_schema_state_id(cql3::query_processor& qp) {
    if (!_raft_gr.is_enabled()) {
        co_return utils::UUID{};
    }

    auto rs = co_await qp.execute_internal(
        format(
            "SELECT schema_id FROM system.{} WHERE key = 'history' LIMIT 1",
            db::system_keyspace::v3::SCHEMA_RAFT_HISTORY));
    assert(rs);
    if (rs->empty()) {
        co_return utils::UUID{};
    }
    co_return rs->one().get_as<utils::UUID>("schema_id");
}

future<bool> migration_manager::was_schema_change_applied(cql3::query_processor& qp, utils::UUID state_id) {
    if (!_raft_gr.is_enabled()) {
        co_return true;
    }

    auto rs = co_await qp.execute_internal(
        format(
            "SELECT schema_id FROM system.{} WHERE key = 'history' AND schema_id = ?",
            db::system_keyspace::v3::SCHEMA_RAFT_HISTORY),
        {state_id});
    assert(rs);
    co_return !rs->empty();
}

utils::UUID generate_schema_state_id(utils::UUID prev_state_id) {
    auto ts = api::new_timestamp();
    if (prev_state_id != utils::UUID{}) {
        auto lower_bound = utils::UUID_gen::micros_timestamp(prev_state_id);
        if (ts <= lower_bound) {
            ts = lower_bound + 1;
        }
    }
    return utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::microseconds{ts});
}

future<> schema_raft_state_machine::apply(std::vector<raft::command_cref> command) {
    slogger.trace("apply() is called");
    for (auto&& c : command) {
        auto is = ser::as_input_stream(c);
        auto cmd = ser::deserialize(is, boost::type<schema_raft_command>{});

        slogger.trace("schema raft cmd prev state ID {} new state ID {}", cmd.prev_state_id, cmd.new_state_id);

        auto schema_state_id = co_await _mm.get_schema_state_id(_qp);
        if (cmd.prev_state_id != utils::UUID{} && cmd.prev_state_id != schema_state_id) {
            // This command used obsolete state. Make it a no-op.
            slogger.trace("schema raft cmd prev state id {} different than current state id {}", cmd.prev_state_id, schema_state_id);
            co_return;
        } else if (cmd.prev_state_id == utils::UUID{}) {
            slogger.trace("unconditional schema modification {}", cmd.new_state_id);
        }

        // We assume that `cmd.mutations` were constructed using schema state which was observed *after* `cmd.prev_state_id` was obtained.
        // It is now important that we apply the mutations *before* we update the schema state ID.

        // TODO: ensure that either all schema mutations are applied and the state ID is updated, or none of this happens.
        // We need to use a write-ahead-entry which contains all this information and make sure it's replayed during restarts?

        co_await _mm.merge_schema_from(netw::messaging_service::msg_addr(std::move(cmd.creator_addr)), std::move(cmd.mutations));

        co_await update_schema_state_id(_qp, cmd.new_state_id);
    }
}

future<raft::snapshot_id> schema_raft_state_machine::take_snapshot() {
    return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
}

void schema_raft_state_machine::drop_snapshot(raft::snapshot_id id) {
    (void) id;
}

future<> schema_raft_state_machine::load_snapshot(raft::snapshot_id id) {
    return make_ready_future<>();
}

future<> schema_raft_state_machine::transfer_snapshot(gms::inet_address from, raft::snapshot_descriptor snp) {
    slogger.trace("transfer snapshot from {} index {} snp id {}", from, snp.idx, snp.id);
    return _mm.submit_migration_task(from, false);
}

future<> schema_raft_state_machine::abort() {
    return make_ready_future<>();
}

} // end of namespace service

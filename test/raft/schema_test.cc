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

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>

#include "types.hh"
#include "utils/UUID_gen.hh"   // XXX ?
#include "types/list.hh"

#include "test/lib/cql_test_env.hh"
#include "cql3/query_processor.hh"

// XXX from cdc_test
static std::vector<std::vector<bytes_opt>> to_bytes(const cql_transport::messages::result_message::rows& rows) {
    auto rs = rows.rs().result_set().rows();
    std::vector<std::vector<bytes_opt>> results;
    for (auto it = rs.begin(); it != rs.end(); ++it) {
        results.push_back(*it);
    }
    return results;
}


SEASTAR_TEST_CASE(test_create_table_updates_timestampuuid) {
    return do_with_cql_env([] (cql_test_env& env) -> future<> {
        cql3::query_processor& qp = env.local_qp();
        auto timeuuid_list_type = list_type_impl::get_instance(timeuuid_type, false);

        auto get_timeuuid = [&] (const sstring& cf) -> future<utils::UUID> {

            co_await env.execute_cql(
                format("CREATE TABLE ks.{} (pk int PRIMARY KEY, c INT);", cf));

            auto msg = co_await env.execute_cql("SELECT current_timeuuid, previous_timeuuid "
                    "FROM system_schema.scylla_tables WHERE keyspace_name = 'system';");
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            auto results = to_bytes(*rows);
            BOOST_REQUIRE(results.size() == 1);   // Static columns
            auto& row = results.front();
            BOOST_REQUIRE(row.size() == 2);
            BOOST_REQUIRE(row[0].has_value());
            auto val_timeuuid = timeuuid_type->deserialize(*row[0]);
            BOOST_REQUIRE(!val_timeuuid.is_null());

//            BOOST_REQUIRE(row[1].has_value());
// XXX check timeuuid list also
fmt::print("\n TTT timeuuid list has value? {}\n", row[1].has_value());
//             auto val_listtimeuuid = timeuuid_list_type->deserialize(*row[1]);
// fmt::print("\n\n\n timeuuid list is null? {}\n\n\n", val_listtimeuuid.is_null());
// auto& column_info_v = rows->rs().result_set().get_metadata().get_names();
// fmt::print("\n\n\n result column names size {}\n\n\n", column_info_v.size());
//             BOOST_REQUIRE(!val_listtimeuuid.is_null());

            co_return value_cast<utils::UUID>(val_timeuuid);
        };

        utils::UUID timeuuid_1 = co_await get_timeuuid("t1");
        utils::UUID timeuuid_2 = co_await get_timeuuid("t2");
fmt::print("\n TTT timestamps:\n1: {}\n2: {}\n", timeuuid_1.timestamp(), timeuuid_2.timestamp());
        BOOST_REQUIRE(timeuuid_1.timestamp() < timeuuid_2.timestamp());

    }, raft_cql_test_config());
}

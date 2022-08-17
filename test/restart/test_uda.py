#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test UDAs across restart.
"""
import sys
import pytest


# Import fixtures from topology
from test.topology.conftest import manager, pytest_addoption

# Use the util.py library from ../cql-pytest:
from test.cql_pytest.util import new_test_keyspace, new_test_table, new_function, new_aggregate



@pytest.mark.asyncio
async def test_uda_restart(manager, test_keyspace):
    """Test UDA survives server restart"""
    servers = await manager.servers()
    schema = 'id bigint primary key'
    with new_test_table(manager.cql, test_keyspace, schema) as table:
        for i in range(8):
            manager.cql.execute(f"INSERT INTO {table} (id) VALUES ({10**i})")
        avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
        div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
        with new_function(manager.cql, test_keyspace, avg_partial_body) as avg_partial, new_function(manager.cql, test_keyspace, div_body) as div_fun:
            custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
            with new_aggregate(manager.cql, test_keyspace, custom_avg_body) as custom_avg:
                await manager.server_restart(servers[0])   # Restart server before using aggregate
                custom_res = [row for row in manager.cql.execute(f"SELECT {test_keyspace}.{custom_avg}(id) AS result FROM {table}")]
                avg_res = [row for row in manager.cql.execute(f"SELECT avg(id) AS result FROM {table}")]
                assert custom_res == avg_res

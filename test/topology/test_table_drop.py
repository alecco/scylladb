#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
from cassandra.protocol import InvalidRequest                            # type: ignore


@pytest.mark.asyncio
async def test_table_drop(manager):
    cql = manager.cql
    assert cql is not None

    keyspace_name = "tk"
    cql.execute(f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = "
                 "{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
    for i in range(1000):

        table_name = f"{keyspace_name}.t_{i}"
        await cql.run_async(f"CREATE TABLE {table_name} (pk text PRIMARY KEY, col1 text, col2 float)")
        # XXX: this fails occassionally with cassandra.WriteFailure
        await cql.run_async(f"INSERT INTO {table_name} (pk, col1, col2) VALUES (%s, %s, %s)",
                            parameters=["1", "1", 1])
        await cql.run_async(f"DROP TABLE {table_name}")
        with pytest.raises(InvalidRequest, match='unconfigured table'):
            await cql.run_async(f"SELECT * FROM {table_name}")


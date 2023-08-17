#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import time
import pytest
from cassandra.protocol import InvalidRequest, ReadFailure                         # type: ignore
from cassandra.query import SimpleStatement                                        # type: ignore
from test.topology.util import wait_for_token_ring_and_group0_consistency


# Simple test of schema helper
@pytest.mark.asyncio
async def test_new_table(manager, random_tables):
    cql = manager.cql
    assert cql is not None
    table = await random_tables.add_table(ncolumns=5)
    # Before performing data queries, make sure that the token ring
    # has converged (we're using ring_delay = 0).
    # Otherwise the queries may pick wrong replicas.
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 60)
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" \
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    pk_col = table.columns[0]
    ck_col = table.columns[1]
    vals = [pk_col.val(1), ck_col.val(1)]
    res = await cql.run_async(f"SELECT * FROM {table} WHERE {pk_col}=%s AND {ck_col}=%s",
                              parameters=vals)
    assert len(res) == 1
    assert list(res[0])[:2] == vals
    await random_tables.drop_table(table)
    # NOTE: On rare occasions the exception is ReadFailure
    with pytest.raises((InvalidRequest, ReadFailure),
                       match='(unconfigured table|failed to execute read)'):
        await cql.run_async(f"SELECT * FROM {table}")
    await random_tables.verify_schema()

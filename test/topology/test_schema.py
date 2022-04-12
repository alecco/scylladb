#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
from cassandra.protocol import InvalidRequest                            # type: ignore


# Simple test of schema helper
@pytest.mark.asyncio
async def test_new_table(cql, tables):
    table = await tables.add_table(ncolumns=5)
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" +
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    res = await cql.run_async(f"SELECT * FROM {table} "
                              f"WHERE {table.columns[0]}='1' "
                              f"AND {table.columns[1]}='1'")
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']
    await tables.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table}")
    await tables.verify_schema()


# Simple test of schema helper with alter
@pytest.mark.asyncio
async def test_alter_verify_schema(cql, tables):
    """Verify table schema"""
    await tables.add_tables(ntables=4, ncolumns=5)
    await tables.verify_schema()
    # Manually remove a column
    table = tables[0]
    await cql.run_async(f"ALTER TABLE {table} DROP {table.columns[-1].name}")
    with pytest.raises(AssertionError, match='Column'):
        await tables.verify_schema()

#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
from cassandra.protocol import InvalidRequest                            # type: ignore


# Simple test of schema helper
@pytest.mark.asyncio
async def test_new_table(cql, random_tables):
    table = await random_tables.add_table(ncolumns=5)
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" +
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    res = await cql.run_async(f"SELECT * FROM {table} "
                              f"WHERE {table.columns[0]}='1' "
                              f"AND {table.columns[1]}='1'")
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']
    await random_tables.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table}")
    await random_tables.verify_schema()


# Simple test of schema helper with alter
@pytest.mark.asyncio
async def test_alter_verify_schema(cql, random_tables):
    """Verify table schema"""
    await random_tables.add_tables(ntables=4, ncolumns=5)
    await random_tables.verify_schema()
    # Manually remove a column
    table = random_tables[0]
    await cql.run_async(f"ALTER TABLE {table} DROP {table.columns[-1].name}")
    with pytest.raises(AssertionError, match='Column'):
        await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_new_table_insert_one(cql, random_tables):
    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()
    col = table.columns[0].name
    res = [row for row in await cql.run_async(f"SELECT * FROM {table} WHERE pk='1' AND {col}='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']


@pytest.mark.asyncio
async def test_drop_column(cql, random_tables):
    """Drop a random column from a table"""
    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()
    await table.drop_column()
    res = (await cql.run_async(f"SELECT * FROM {table} WHERE pk='1'"))[0]
    assert len(res) == 4
    await table.drop_column()
    res = (await cql.run_async(f"SELECT * FROM {table} WHERE pk='1'"))[0]
    assert len(res) == 3
    await random_tables.verify_schema(table)


@pytest.mark.asyncio
async def test_add_index(cql, random_tables):
    """Add and drop an index"""
    table = await random_tables.add_table(ncolumns=5)
    with pytest.raises(AssertionError, match='PK'):
        await table.add_index(0)
    with pytest.raises(AssertionError, match='CK'):
        await table.add_index(1)
    await table.add_index(2)
    await random_tables.verify_schema(table)

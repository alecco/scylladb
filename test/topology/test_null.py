# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

from cassandra.protocol import InvalidRequest
from pylib.util import random_string
import pytest


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_delete_empty_string_key(cql, keyspace):
    s = random_string()
    # An empty-string clustering *is* allowed:
    await cql.run_async(f"DELETE FROM {keyspace.tables[0].full_name} WHERE pk='{s}' AND c_01=''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        await cql.run_async(f"DELETE FROM {keyspace.tables[0].full_name} WHERE pk='' AND c_01='{s}'")


@pytest.mark.asyncio
@pytest.mark.ntables(0)
async def test_new_table_insert_one(cql, keyspace):
    table = await keyspace.create_table()
    await table.insert_seq()
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']
    await keyspace.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table.full_name}")


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_drop_column(cql, keyspace):
    """Drop a random column from a table"""
    ncols = len(keyspace.tables[0].columns)
    await keyspace.tables[0].insert_seq()
    await keyspace.tables[0].drop_column()
    assert ncols - 1 == len(keyspace.tables[0].columns)
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res[0]) == ncols - 1


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_table_same_name(cql, keyspace):
    """Insert to a table, drop it, create new one with same schema, check"""
    table = keyspace.tables[0]
    await table.insert_seq()
    await keyspace.drop_table(table)
    cql.cluster.refresh_schema_metadata()
    table_meta = cql.cluster.metadata.keyspaces[keyspace.name].tables
    print("XXX meta 1 {table_meta}", file=sys.stderr) # XXX
    await keyspace.create_table(table)
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res) == 0


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_table_same_name(cql, keyspace):
    """Drop a random column from a table"""
    await keyspace.tables[0].insert_seq()
    await keyspace.tables[0].drop_column()
    assert ncols - 1 == len(keyspace.tables[0].columns)
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res[0]) == ncols - 1


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_alter_table_compaction(cql, keyspace):
    """Alter table compaction"""
    ncols = len(keyspace.tables[0].columns)
    await keyspace.tables[0].insert_seq()
    await keyspace.tables[0].drop_column()
    assert ncols - 1 == len(keyspace.tables[0].columns)
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res[0]) == ncols - 1
    cql.cluster.refresh_schema_metadata()

    table_meta = cql.cluster.metadata.keyspaces[keyspace.name].tables
    missing_tables = [f"new_table_{n}" for n in range(RANGE) if f"new_table_{n}" not in table_meta]
    if missing_tables:
        print(f"Missing tables: {', '.join(missing_tables)}", file=sys.stderr)
        logger.error(f"Missing tables: {', '.join(missing_tables)}")

    not_indexed = [f"index_me_{n}" for n in range(RANGE) if len(table_meta[f"index_me_{n}"].indexes) != 7]
    if not_indexed:
        print(f"Not indexed tables: {', '.join(not_indexed)}", file=sys.stderr)
        logger.error(f"Not indexed tables: {', '.join(not_indexed)}")

    expected_cols = sorted(['id', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7'])
    errors = []
    for n in range(RANGE):
        altered = table_meta[f"alter_me_{n}"]
        if sorted(altered.columns) != expected_cols:
            errors.append(f"alter_me_{n}: {', '.join(list(altered.columns))}")

    if errors:
        logger.error("Errors found:\n{0}".format('\n'.join(errors)))
        raise Exception("Schema errors found, check log")
    else:
        logger.error("No Errors found, try again")


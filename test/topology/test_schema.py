# Copyright 2022-present ScyllaDB
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
import pytest


@pytest.mark.asyncio
async def test_verify_schema(keyspace):
    """Verify table schema"""
    await keyspace.verify_schema()


@pytest.mark.asyncio
@pytest.mark.ntables(0)
async def test_new_table(cql, keyspace):
    table = await keyspace.create_table()
    val = "'1'"
    await cql.run_async(f"INSERT INTO {table.full_name} ({','.join(c for c in table.columns)})" +
                        f"VALUES ({','.join([val] * len(table.columns))})")
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']
    await keyspace.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table.full_name}")

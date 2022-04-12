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

import pytest
from cassandra.protocol import InvalidRequest
from pylib.schema_helper import get_schema


@pytest.mark.asyncio
async def test_new_table(cql):
    tables = await get_schema("delete_empty_string_key", cql, ntables=1, ncolumns=5)
    table = tables[0]
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" +
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    res = [row for row in await cql.run_async(f"SELECT * FROM {table} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']
    await tables.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table}")
    tables.verify_schema()


@pytest.mark.asyncio
async def test_verify_schema(cql):
    """Verify table schema"""
    tables = await get_schema("delete_empty_string_key", cql, ntables=4, ncolumns=5)
    await tables.verify_schema()
    # Manually remove a column
    table = tables[0]
    await cql.run_async(f"ALTER TABLE {table} DROP {table.columns[-1].name}")
    with pytest.raises(AssertionError, match='Column'):
        await tables.verify_schema()

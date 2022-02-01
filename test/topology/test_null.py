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
from pylib.util import random_string, unique_name
import pytest


@pytest.fixture()
async def table1(cql, keyspace):
    table = keyspace.name + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p text, c text, v text, primary key (p, c))")
    yield table
    await cql.run_async("DROP TABLE " + table)


@pytest.mark.asyncio
async def test_delete_empty_string_key(cql, table1):
    s = random_string()
    # An empty-string clustering *is* allowed:
    await cql.run_async(f"DELETE FROM {table1} WHERE p='{s}' AND c=''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        await cql.run_async(f"DELETE FROM {table1} WHERE p='' AND c='{s}'")


@pytest.mark.asyncio
async def test_xxx(keyspace, table1):
    """Verify table schema"""
    await keyspace.verify_schema(table=table1)

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
@pytest.mark.ntables(1)
async def test_insert_one(cql, keyspace):
    """Insert a row in a table"""
    await keyspace.tables[0].insert_seq()
    res = [row for row in await cql.run_async(f"SELECT * FROM {keyspace.tables[0].full_name} WHERE pk='1' AND c_01='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']

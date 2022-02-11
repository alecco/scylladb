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

import asyncio
from cassandra.protocol import InvalidRequest
import pytest
from conftest import Column, UUIDType, IntType
import logging


logger = logging.getLogger('schema-test')


# https://issues.apache.org/jira/browse/CASSANDRA-10250
# - Create 20 new tables
# - Drop 7 columns one at time across 20 tables
# - Add 7 columns one at time across 20 tables
# - Add one column index on each of the 7 columns on 20 tables
@pytest.mark.skip(reason="this will fail until we have safe schema changes with Raft")
@pytest.mark.asyncio
@pytest.mark.ntables(0)
async def test_cassandra_issue_10250(tables):

    RANGE = 20

    tables_a = []
    tables_i = []
    for n in range(RANGE):
        # alter_me: id uuid, s1 int, ..., s7 int
        tables_a.append(tables.new_table(columns=[Column(name="id", ctype=UUIDType),
                                         *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 8)]],
                                         pks=1, name=f"alter_me_{n}"))
        # index_me: id uuid, c1 int, ..., c7 int
        tables_i.append(tables.new_table(columns=[Column(name="id", ctype=UUIDType),
                                         *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 8)]],
                                         pks=1, name=f"index_me_{n}"))
    await tables.create_tables()

    aws = []
    # Drops all the c* columns and adds s* columns, one each at a time
    for n in range(RANGE):
        table = tables.new_table(columns=[Column(name="id", ctype=UUIDType),
                                 *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 4)]],
                                 pks=1, name=f"new_table_{n}")
        aws.append(table.create())
        for a in range(1, 8):
            # Note: removing column after adding to keep at least one value column in Table
            # alter table alter_me_{n} add c{a} int
            aws.append(tables_a[n].add_column(name=f"c{a}", ctype=IntType)) # XXX ??? a/n?
            # alter table alter_me_{n} drop s{a}  value columns
            aws.append(tables_a[n].drop_column(1))
            # create index ix_index_me_{0}_c{1} on index_me_{0} (c{1})
            aws.append(tables_i[a].add_index(f"c{a}", f"ix_index_me_{n}_c{a}"))

    await asyncio.gather(*aws)
    await asyncio.sleep(20)
    logger.debug(f"verifing schema status")
    await tables.verify_schema()

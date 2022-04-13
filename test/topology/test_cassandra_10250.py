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
import pytest
from pylib.schema_helper import Table, Column, UUIDType, IntType         # type: ignore
import logging


logger = logging.getLogger('schema-test')


# #1207 Schema changes are not linearizable, so concurrent updates may lead to
# inconsistent schemas
# This is the same issue exposed in https://issues.apache.org/jira/browse/CASSANDRA-10250
# and this is a port of that repro.
# How this repro works:
# - Creates 20+20 tables to alter and 20 tables to index
# - In parallel run 20 * create table, and drop/add column and index of previous 2 tables
@pytest.mark.asyncio
async def test_cassandra_issue_10250(cql, tables, fails_without_raft):
    RANGE = 20
    tables_a = []
    tables_i = []
    # Create sequentially 20+20 tables, 20 to alter later, 20 to index later.
    for n in range(RANGE):
        # alter_me: id uuid, s1 int, ..., s7 int
        tables_a.append(await tables.add_table(columns=[Column(name="id", ctype=UUIDType),
                                               *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 8)]],
                                               pks=1, name=f"alter_me_{n}"))
        # index_me: id uuid, c1 int, ..., c7 int
        tables_i.append(await tables.add_table(columns=[Column(name="id", ctype=UUIDType),
                                               *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 8)]],
                                               pks=1, name=f"index_me_{n}"))

    # Create a bunch of futures to run in parallel (in aws list)
    #   - alter a table in the _a list by adding a column
    #   - alter a table in the _a list by removing a column
    #   - index a table in the _i list
    aws = []
    for n in range(RANGE):
        table = tables.add_table(columns=[Column(name="id", ctype=UUIDType),
                                 *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 4)]], pks=1)
        aws.append(table)
        for a in range(1, 8):
            # Note: removing column after adding to keep at least one non-PK column in the table
            # alter table alter_me_{n} add c{a} int
            aws.append(tables_a[n].add_column(name=f"c{a}", ctype=IntType))
            # alter table alter_me_{n} drop s{a}  value columns
            aws.append(tables_a[n].drop_column(1))
            # create index ix_index_me_{0}_c{1} on index_me_{0} (c{1})
            aws.append(tables_i[n].add_index(f"c{a}", f"ix_index_me_{n}_c{a}"))

    # Run everything in parallel
    await asyncio.gather(*aws)
    await asyncio.sleep(20)  # Sleep 20 seconds to settle like in the original Cassandra issue repro
    logger.debug("verifing schema status")
    # When bug happens, deleted columns are still there (often) and/or new columns are missing (rarely)
    await tables.verify_schema()

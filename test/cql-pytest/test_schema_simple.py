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
import itertools
import logging
import random
import sys  # XXX
import uuid

# TODO:
#   - alter tables
#       - change table properties
#       - change compaction strategy
#   - alter keyspace
#       - add/remove tables
#       - UDTS (??)
#       - alter keyspace with rf, replication, class strategy
#       - switch multi-dc and back
#       - Create/Delete/Alter GSI, SI, MV
#   - schema + topology changes
#
#   CHECK cols are same
#
#
#  rebase against forced push

#   - check data integrity (SELECT)
#       - keep track of count?
#       - value count at table-level? not column
#       - but how to track removed cols?
#           - maybe list with [seed, [cols]]
#   - random values
#       - keep track of inserted (random seed, not random value?)

# coroutines doing things concurrently
# generator (like kamil's randomized)  gen of tests
#
# add column a1, drop column a1, add column a2, drop column a2


logger = logging.getLogger('schema-test')


async def cql_execute(cql, cql_query, parameters=None, log=True):
    if log:
        logger.debug(f"Running CQL: {cql_query}")
    print(f"XXX cql_execute(): cql_query={cql_query}, parameters={parameters}", file=sys.stderr)  # XXX
    cql.execute(cql_query, parameters=parameters)


class ColumnType():
    def nextval(self, seed):
        """Return next value for this type"""
        pass

    def rndv(self):
        """Return a random value for this type"""
        pass


class IntType(ColumnType):
    def __init__(self):
        self.name = 'int'

    def nextval(self, seed):
        return seed

    def rndv(self, a=1, b=100):
        return random.random(a, b)


class TextType(ColumnType):
    def __init__(self, length=10):
        self.length = length
        self.name = 'text'

    def nextval(self, seed):
        return str(seed)

    def rndv(self):
        return "XXX"  # XXX


class FloatType(ColumnType):
    def __init__(self):
        self.name = 'float'

    def nextval(self, seed):
        return float(seed)

    def rndv(self):
        return random.random()


class UUIDType(ColumnType):
    def __init__(self):
        self.name = 'uuid'

    def nextval(self, seed):
        return uuid.UUID(f"{{00000000-0000-0000-0000-{seed:012}}}")

    def rndv(self):
        return uuid.uuid4()


class Column():
    newid = itertools.count(start=1).__next__

    # TODO: add random collections and user-defined type
    def __init__(self, name=None, ctype=None):
        """A column definition.
           If no type given picks a simple type (no collection or user-defined)"""
        self.id = Column.newid()
        self.next_count = itertools.count(start=1)
        if name is not None:
            self.name = name
        else:
            self.name = f"c_{self.id:05}"
        if ctype is not None:
            self.ctype = ctype()
        else:
            self.ctype = random.choice([IntType, TextType, FloatType, UUIDType])()
        print(f"XXX new column {self.name} type {self.ctype.name}", file=sys.stderr)  # XXX

    @property
    def cql(self):
        return f"{self.name} {self.ctype.name}"

    @property
    def nextval(self):
        return self.ctype.nextval(self.next_count.__next__())

    def rndv(self):
        return self.ctype.rndv()


# TODO: should it also load from CQL `DESC TABLE ks.name` and parse output?
class Table():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql, keyspace_name, name=None, cols=None, pks=1):
        """Set up a new table definition from column definitions.
           If column definitions not specified pick a random number of columns with random types.
           By default first column is Primary Key (pk) or the first pks columns"""
        self.id = Table.newid()
        self.cql = cql
        self.keyspace_name = keyspace_name
        self.name = name if name is not None else f"t_{self.id:04}"
        self.full_name = keyspace_name + "." + self.name
        # TODO: assumes primary key is composed of first self.pks columns
        self.pks = pks

        if cols is not None:
            self.columns = cols
            assert pks < len(cols) + 1, f"Fewer columns {len(cols)} than needed {pks + 1}"
            logger.error(f"XXX Table got {len(self.columns)} pre-defined columns")
        else:
            self.columns = []
            print(f"XXX adding columns for table {self.full_name}", file=sys.stderr)
            # TODO: handle minimum amount of columns in tests
            for i in range(random.randint(pks + 2, 4)):
                # create a column for each PK and at least 1 value column
                self.columns.append(Column())
            logger.debug(f"XXX Table picked {len(self.columns)} random columns")

        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([self.columns[i].name for i in range(self.pks, len(self.columns))])

        # Track inserted columns with [<seed value>, [value columns]]
        self.inserted = []

    async def create(self):
        # XXX no fstring but %s ?
        await cql_execute(self.cql, f"CREATE TABLE {self.full_name} (" + ", ".join(c.cql for c in self.columns) +
                    ", primary key(" + ",".join(c.name for c in self.columns[:self.pks]) + "))")

    async def drop(self):
        await cql_execute(self.cql, f"DROP TABLE {self.full_name}")

    async def add_column(self, col=None):
        col = col if col is not None else Column()
        await cql_execute(self.cql, f"ALTER TABLE {self.full_name} ADD {col.cql}")
        self.columns.append(col)
        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([self.columns[i].name for i in range(self.pks, len(self.columns))])

    async def remove_column(self, column=None):
        if column is None:
            pos = random.randint(self.pks + 1, len(self.columns) - 1)
            print(f"XXX remove_column rnd {pos}/{len(self.columns)}")  # XXX
            col = self.columns[pos]
        elif type(column) is int:
            pos = column
            col = self.columns.index(column)
        else:
            col = column
            pos = self.columns.index(column)
        assert pos > self.pks, f"Cannot remove PK column {pos} {col.name}"
        assert len(self.columns) - 1 > self.pks, f"Cannot remove last value column {pos} {col.name}"
        await cql_execute(self.cql, f"ALTER TABLE {self.full_name} DROP {col.name}")
        del self.columns[pos]
        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([self.columns[i].name for i in range(self.pks, len(self.columns))])

    async def insert_next(self):
        # XXX make proper format args duh
        await cql_execute(self.cql, f"INSERT INTO {self.full_name} (" + self.all_col_names + ") VALUES (" +
                    ", ".join(["%s"] * len(self.columns)) + ")", parameters=[c.nextval for c in self.columns])

    # TODO: insert random, track existing random values in column


class Keyspace():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql, replication_strategy, replication_factor, ntables):
        self.id = Keyspace.newid()
        self.name = f"ks_{self.id:04}"
        self.replication_strategy = replication_strategy
        self.replication_factor = replication_factor
        self.tables = [Table(cql, self.name) for _ in range(ntables)]
        self.cql = cql
        self.create()

    async def create(self):
        await cql_execute(self.cql, f"CREATE KEYSPACE {self.name} WITH REPLICATION = {{ 'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication_factor} }}")
        [t.create() for t in self.tables]

    async def drop(self):
        [t.drop() for t in self.tables]
        await cql_execute(self.cql, f"DROP KEYSPACE {self.name}")

    async def drop_random_table(self):
        table = random.choice(self.tables)
        await table.drop()
        return table

    @property
    def random_table(self):
        return random.choice(self.tables)


# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in a test. The keyspace is created with RF=2
# and destroyed after each test (not reused).
@pytest.fixture()
def keyspace(request, cql):
    marker_tables = request.node.get_closest_marker("ntables")
    ntables = marker_tables.args[0] if marker_tables is not None else 10
    marker_rstrategy = request.node.get_closest_marker("replication_strategy")
    rstrategy = marker_rstrategy.args[0] if marker_rstrategy is not None else "SimpleStrategy"
    marker_rf = request.node.get_closest_marker("replication_factor")
    # TODO: pick default RF from number of available nodes
    rf = marker_rf.args[0] if marker_rf is not None else 1
    ks = Keyspace(cql, rstrategy, rf, ntables)
    yield ks
    ks.drop()

def test_multi_add_one_column(keyspace):
    keyspace.random_table.add_column()


@pytest.mark.ntables(1)
def test_one_add_column_insert_100_drop_column(keyspace):
    col = keyspace.tables[0].add_column()
    for _ in range(100):
        keyspace.tables[0].insert_next()
    keyspace.tables[0].remove_column(col)


def test_multi_add_column_insert_100_drop_column(keyspace):
    table = keyspace.random_table
    col = table.add_column()
    for _ in range(100):
        table.insert_next()
    table.remove_column(col)


@pytest.mark.ntables(1)
def test_one_remove_one_column(keyspace):
    keyspace.tables[0].remove_column()


def test_multi_remove_one_column(keyspace):
    keyspace.random_table.remove_column()

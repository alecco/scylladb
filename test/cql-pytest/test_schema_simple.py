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


def cql_execute(cql, cql_query, parameters=None, log=True):
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

    def __init__(self, cql, keyspace, name=None, cols=None, pks=1):
        """Set up a new table definition from column definitions.
           If column definitions not specified pick a random number of columns with random types.
           By default first column is Primary Key (pk) or the first pks columns"""
        self.id = Table.newid()
        self.cql = cql
        self.keyspace = keyspace
        self.name = name if name is not None else f"t_{self.id:04}"
        self.full_name = keyspace + "." + self.name
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

    def create(self):
        cql_execute(self.cql, f"CREATE TABLE {self.full_name} (" + ", ".join(c.cql for c in self.columns) +
                    ", primary key(" + ",".join(c.name for c in self.columns[:self.pks]) + "))")

    def drop(self):
        cql_execute(self.cql, f"DROP TABLE {self.full_name}")

    def add_column(self, col=None):
        col = col if col is not None else Column()
        cql_execute(self.cql, f"ALTER TABLE {self.full_name} ADD {col.cql}")
        self.columns.append(col)
        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([self.columns[i].name for i in range(self.pks, len(self.columns))])

    def remove_column(self, column=None):
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
        cql_execute(self.cql, f"ALTER TABLE {self.full_name} DROP {col.name}")
        del self.columns[pos]
        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([self.columns[i].name for i in range(self.pks, len(self.columns))])

    def insert_next(self):
        cql_execute(self.cql, f"INSERT INTO {self.full_name} (" + self.all_col_names + ") VALUES (" +
                    ", ".join(["%s"] * len(self.columns)) + ")", parameters=[c.nextval for c in self.columns])
        cql_execute(self.cql, f"INSERT INTO {self.full_name} (" + self.all_col_names + ") VALUES (" +

    # TODO: insert random, track existing random values in column


class Tables():
    def __init__(self, cql, keyspace, tables=1, create=True):
        if type(tables) is int:
            self.tables = [Table(cql, keyspace) for _ in range(tables)]
        else:
            self.tables = tables
        if create:
            [t.create() for t in self.tables]

    def drop_all(self):
        [t.drop() for t in self.tables]

    def drop_random(self):
        table = random.choice(self.tables)
        table.drop()
        return table

    @property
    def random_table(self):
        return random.choice(self.tables)


# Create a set of tables per test
@pytest.fixture()
def table(cql, test_keyspace):
    table = Table(cql, test_keyspace)
    table.create()
    yield table
    table.drop()


# Create a set of tables per test
@pytest.fixture()
def tables_10(cql, test_keyspace):
    tables = Tables(cql, test_keyspace, tables=10)
    yield tables
    tables.drop_all()


def test_multi_add_one_column(cql, tables_10):
    tables_10.random_table.add_column()


def test_one_add_column_insert_100_drop_column(cql, table):
    col = table.add_column()
    for _ in range(100):
        table.insert_next()
    table.remove_column(col)


def test_multi_add_column_insert_100_drop_column(cql, tables_10):
    table = tables_10.random_table
    col = table.add_column()
    for _ in range(100):
        tables_10.random_table.insert_next()
    table.remove_column(col)


def test_one_remove_one_column(cql, table):
    table.remove_column()


def test_multi_remove_one_column(cql, tables_10):
    tables_10.random_table.remove_column()

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


# XXX ?
# # All test coroutines will be treated as marked.
# pytestmark = pytest.mark.asyncio

# Range for default initial table value columns
MAX_INITIAL_VALUE_COLS = 5
MIN_INITIAL_VALUE_COLS = 3

# TODO:
#   - CREATE INDEX
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
    return cql.execute(cql_query, parameters=parameters)


class ValueType():
    def nextval(self, seed):
        """Return next value for this type"""
        pass

    def rndv(self):
        """Return a random value for this type"""
        pass


class IntType(ValueType):
    def __init__(self):
        self.name = 'int'

    def nextval(self, seed):
        return seed

    def rndv(self, a=1, b=100):
        return random.random(a, b)


class TextType(ValueType):
    def __init__(self, length=10):
        self.length = length
        self.name = 'text'

    def nextval(self, seed):
        return str(seed)

    def rndv(self):
        return "XXX"  # XXX


class FloatType(ValueType):
    def __init__(self):
        self.name = 'float'

    def nextval(self, seed):
        return float(seed)

    def rndv(self):
        return random.random()


class UUIDType(ValueType):
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

    def nextval(self, seed):
        return self.ctype.nextval(seed)

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

        if type(cols) is list:
            self.columns = cols
            assert pks < len(cols) + 1, f"Fewer columns {len(cols)} than needed {pks + 1}"
            print(f"XXX Table got {len(self.columns)} pre-defined columns", file=sys.stderr)
        else:
            self.columns = []
            if type(cols) is int:
                ncols = cols
            else:
                ncols = random.randint(pks + MIN_INITIAL_VALUE_COLS, MAX_INITIAL_VALUE_COLS)
            print(f"XXX adding {ncols} columns for table {self.full_name}", file=sys.stderr)
            # TODO: handle minimum amount of columns in tests
            for _ in range(ncols):
                # create a column for each PK and at least 1 value column
                self.columns.append(Column())
            print(f"XXX Table picked {len(self.columns)} random columns", file=sys.stderr)

        self.removed_columns = []

        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([col.name for col in self.columns[:self.pks]])

        self.next_val_seed_count = itertools.count(start=1)
        # Index id is independent from column id as columns might be re-indexed later
        self.next_idx_id_count = itertools.count(start=1)

        # Map of current indexes (idx_id:col_id)
        self.indexes = {}

        # Track inserted row with [<seed value>, [value columns]]
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
        return col

    async def remove_column(self, column=None):
        # XXX what about index on the column?
        if column is None:
            pos = random.randint(self.pks + 1, len(self.columns) - 1)
            col = self.columns[pos]
            print(f"XXX remove_column rnd {col.name} {pos}/{len(self.columns)}")  # XXX
        elif type(column) is int:
            pos = column
            col = self.columns.index(column)
        elif type(column) is str:
            try:
                pos, col = next((pos, col) for pos, col in enumerate(self.columns) if col.name == column)
            except StopIteration:
                raise Exception(f"Column {column} not found in table {self.name}")
        else:
            assert type(column) is Column, f"can not remove unknown type {type(column)}"
            col = column
            pos = self.columns.index(column)
        assert pos >= self.pks, f"Cannot remove PK column {pos} {col.name}"
        assert len(self.columns) - 1 > self.pks, f"Cannot remove last value column {pos} {col.name}"
        await cql_execute(self.cql, f"ALTER TABLE {self.full_name} DROP {col.name}")
        del self.columns[col]
        self.removed_columns.append(col)
        self.all_col_names = ", ".join([c.name for c in self.columns])
        self.valcol_names = ", ".join([col.name for col in self.columns[:self.pks]])
        return col

    async def insert_next(self):
        seed = self.next_val_seed_count.__next__()
        return await cql_execute(self.cql, f"INSERT INTO {self.full_name} ({self.all_col_names}) " +
                    f"VALUES ({', '.join(['%s'] * len(self.columns)) })",
                    parameters=[c.nextval(seed) for c in self.columns])

    def idx_name(self, idx_id, col_id):
        return f"{self.name}_{col_id:04}_{idx_id:04}"

    # TODO: custom index name (change tracking)
    async def create_index(self, column=None):
        """Create a secondary index on a value column and return its id"""
        # XXX check if index already exists!
        if column is None:
            col = self.columns[random.randint(self.pks, len(self.columns) - 1)]
        elif type(column) is str:
            try:
                col = next(col for col in self.columns if col.name == column)
            except StopIteration:
                raise Exception(f"Column {column} to index not found in table {self.name}")
        elif type(column) is int:
            assert column < len(self.columns), f"column {column} to index not found in table {self.name}"
            assert column >= self.pks, f"column {column} to index must be a value column"
            col = self.columns[column]
        elif type(column) is Column:
            assert column in self.columns, f"column {column.name} to index must be present"
            assert self.columns.index(column) >= self.pks, "column to index must be present"

        idx_id = self.next_idx_id_count.__next__()
        await cql_execute(self.cql, f"CREATE INDEX {self.idx_name(idx_id, col.id)} ON {self.full_name} ({col.name})")
        self.indexes[idx_id] = col.id
        return idx_id

    async def drop_index(self, idx_id=None):
        if idx_id is None:
            idx_id, col_id = self.indexes.popitem()  # random enough
        else:
            assert idx_id in self.indexes, "index to drop {idx_id} must exist"
            col_id = self.indexes.pop(idx_id)
        await cql_execute(self.cql, f"DROP INDEX {self.idx_name(idx_id, col.id)}")

    # TODO: insert random, track existing random values in column


class Keyspace():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql, replication_strategy, replication_factor, ntables, ncols):
        self.id = Keyspace.newid()
        self.name = f"ks_{self.id:04}"
        self.replication_strategy = replication_strategy
        self.replication_factor = replication_factor
        self.tables = [Table(cql, self.name, cols=ncols) for _ in range(ntables)]
        self.cql = cql

    async def create(self):
        await cql_execute(self.cql, f"CREATE KEYSPACE {self.name} WITH REPLICATION = {{ 'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication_factor} }}")
        [await t.create() for t in self.tables]

    async def drop(self):
        [await t.drop() for t in self.tables]
        await cql_execute(self.cql, f"DROP KEYSPACE {self.name}")

    async def drop_random_table(self):
        table = random.choice(self.tables)
        await table.drop()
        return table

    @property
    def random_table(self):
        return random.choice(self.tables)

    def get_table(self, name):
        return next(t for t in self.tables if t.name == name)

    def new_table(self, name):
        raise Exception("Not implemented")
        self.tables.append(Table())

    def drop_table(self, name):
        raise Exception("Not implemented")
        pos, t = next((pos, t) for pos, t in enumerate(self.tables) if t.name = name)
        del self.tables[pos]
        self.removed_tables.append(t)


# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in a test. The keyspace is created with RF=2
# and destroyed after each test (not reused).
@pytest.fixture()
async def keyspace(request, cql):
    marker_tables = request.node.get_closest_marker("ntables")
    ntables = marker_tables.args[0] if marker_tables is not None else 10
    marker_ncols = request.node.get_closest_marker("ncols")
    ncols = marker_ncols.args[0] if marker_ncols is not None else None
    marker_rstrategy = request.node.get_closest_marker("replication_strategy")
    rstrategy = marker_rstrategy.args[0] if marker_rstrategy is not None else "SimpleStrategy"
    marker_rf = request.node.get_closest_marker("replication_factor")
    # TODO: pick default RF from number of available nodes
    rf = marker_rf.args[0] if marker_rf is not None else 1
    ks = Keyspace(cql, rstrategy, rf, ntables, ncols)
    await ks.create()
    yield ks
    await ks.drop()

async def insert_rows(table, n):
    for _ in range(n):
        await table.insert_next()

# async def test_multi_add_one_column(keyspace):
#     await keyspace.random_table.add_column()
# 
# #@pytest.mark.ntables(1)
# #async def test_xxx(cql, keyspace):
# #    # XXX
# #    #ret = cql.execute("SELECT data_center FROM system.local")
# #    #ret = cql.execute("DESCRIBE SCHEMA")
# #    #for r in ret:
# #    #    print(f"XXX: {r}", file=sys.stderr)
# #    # XXX Row(data_center='datacenter1')
# #    raise Exception("XXX")
# 
# @pytest.mark.ntables(1)
# async def test_one_add_column_insert_100_drop_column(keyspace):
#     col = await keyspace.tables[0].add_column()
#     await insert_rows(keyspace.tables[0], 100)
#     await keyspace.tables[0].remove_column(col)
#     # XXX check
# 
# 
# async def test_multi_add_column_insert_100_drop_column(keyspace):
#     table = keyspace.random_table
#     col = await table.add_column()
#     await insert_rows(table, 100)
#     await table.remove_column(col)
# 
# 
# async def test_multi_remove_one_column(keyspace):
#     await keyspace.random_table.remove_column()
# 
# 
# async def test_multi_add_remove_index(keyspace):
#     idx_id = await keyspace.random_table.create_index()


# https://issues.apache.org/jira/browse/CASSANDRA-10250
# - Create 20 new tables
# - Drop 7 columns one at time across 20 tables
# - Add 7 columns one at time across 20 tables
# - Add one column index on each of the 7 columns on 20 tables
@pytest.mark.asyncio  # XXX needed?
@pytest.mark.ntables(0)
async def test_cassandra_issue_10250(event_loop, cql, keyspace):

    assert len(keyspace.tables) == 0, "Keyspace not empty"
    for n in range(20):
        # alter_me: id uuid, s1 int, ..., s7 int
        ta = Table(cql, keyspace.name, name=f"alter_me_{n}", cols=[Column(name="id", ctype=UUIDType),
                    *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 8)]])
        await ta.create()
        # index_me: id uuid, c1 int, ..., c7 int
        ti = Table(cql, keyspace.name, name=f"index_me_{n}", cols=[Column(name="id", ctype=UUIDType),
                    *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 8)]])
        await ti.create()
        keyspace.tables.extend([ta, ti])

    aws = []
    for n in range(20):
        tn = Table(cql, keyspace.name, name=f"new_table_{n}", cols=[Column(name="id", ctype=UUIDType),
                    *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 5)]])
        aws.append(tn.create())  # XXX is this correct?
        keyspace.tables.append(tn)
        for a in range(1, 8):
            # cmds.append(("alter table alter_me_{0} drop s{1};".format(n, a), ()))
            aws.append(keyspace.get_table(f"alter_me_{n}").remove_column(f"s{a}"))

            # cmds.append(("alter table alter_me_{0} add c{1} int;".format(n, a), ()))
            aws.append(keyspace.get_table(f"alter_me_{n}").add_column(Column(name=f"c{a}", ctype=IntType)))

            # cmds.append(("create index ix_index_me_{0}_c{1} on index_me_{0} (c{1});".format(n, a), ())
            aws.append(keyspace.get_table(f"index_me_{n}").create_index(column=f"c{a}"))

    #results = execute_concurrent(session, cmds, concurrency=100, raise_on_first_error=True)
    # XXX XXX XXX run in executor? threads? processes?
    res = await asyncio.gather(*aws)   # XXX is this correct?
    print(f"XXX res {res}")

    logger.debug("sleeping 20 to make sure things are settled")
    await asyncio.sleep(20)

    logger.debug(f"verifing schema status")
    cql.cluster.refresh_schema_metadata()


    #print(f"XXX keyspaces keys {cql.cluster.metadata.keyspaces.keys()} ks {keyspace.name}", file=sys.stderr) # XXX remove

    table_meta = cql.cluster.metadata.keyspaces[keyspace.name].tables
    errors = []
    for n in range(20):
        if "new_table_{0}".format(n) not in table_meta:
            errors.append("table is missing: new_table_{0}".format(n))
        if 7 != len(table_meta["index_me_{0}".format(n)].indexes):
            errors.append("index_me_{0} expected indexes ix_index_me_c0->7, got: {1}".format(n, sorted(list(table_meta["index_me_{0}".format(n)].indexes))))
        altered = table_meta["alter_me_{0}".format(n)]
        for col in altered.columns:
            if not col.startswith("c") and col != "id":
                 errors.append("alter_me_{0} column[{1}] does not start with c and should have been dropped: {2}".format(n, col, sorted(list(altered.columns))))
        if 8 != len(altered.columns):
            errors.append("alter_me_{0} expected c1 -> c7, id, got: {1}".format(n, sorted(list(altered.columns))))

    if 0 != len(errors):
        print("Errors found:\n{0}".format("\n".join(errors)), file=sys.stderr)
        logger.debug("Errors found:\n{0}".format("\n".join(errors)))
    else:
        print("No Errors found, try again", file=sys.stderr)
        logger.debug("No Errors found, try again")
    raise Exception("XXX") # XXX keep log

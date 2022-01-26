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
import concurrent.futures
import pytest
from functools import partial
import itertools
import logging
import random
import sys
import threading
import time
import uuid


# Range for default initial table value columns
MAX_INITIAL_VALUE_COLS = 5
MIN_INITIAL_VALUE_COLS = 3

# TODO:
#
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

@pytest.fixture()
async def thread_pool():
    return concurrent.futures.ThreadPoolExecutor()


async def run_blocking_tasks(event_loop, executor, to_run):
    blocking_tasks = [event_loop.run_in_executor(executor, f) for f in to_run]
    completed, pending = await asyncio.wait(blocking_tasks)
    return [t.result() for t in completed]


def cql_execute(cql, cql_query, parameters=None):
    logger.debug(f"Running CQL: {cql_query}")
    #print(f"XXX cql_execute(): cql_query={cql_query}, parameters={parameters}", file=sys.stderr)  # XXX
    ret = cql.execute(cql_query, parameters=parameters)
    return ret


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
        # print(f"XXX new column {self.name} type {self.ctype.name}", file=sys.stderr)  # XXX

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
            # XXX print(f"XXX Table got {len(self.columns)} pre-defined columns", file=sys.stderr)
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

        self.lock = threading.Lock()

    def create(self):
        # XXX no fstring but %s ?
        cql_execute(self.cql, f"CREATE TABLE {self.full_name} (" + ", ".join(c.cql for c in self.columns) +
                    ", primary key(" + ",".join(c.name for c in self.columns[:self.pks]) + "))")

    def drop(self):
        cql_execute(self.cql, f"DROP TABLE {self.full_name}")

    def add_column(self, col=None):
        col = col if col is not None else Column()
        cql_execute(self.cql, f"ALTER TABLE {self.full_name} ADD {col.cql}")
        with self.lock:
            self.columns.append(col)
            self.all_col_names = ", ".join([c.name for c in self.columns])
            self.valcol_names = ", ".join([self.columns[i].name for i in range(self.pks, len(self.columns))])
        return col

    def remove_column(self, column=None):
        with self.lock:
            if column is None:
                col = random.choice(self.columns)
                print(f"XXX remove_column rnd {col.name} {pos}/{len(self.columns)}")  # XXX
            elif type(column) is int:
                assert column >= self.pks, f"Cannot remove PK column {pos} {col.name}"
                col = self.columns[column]
            elif type(column) is str:
                try:
                    col = next(col for col in self.columns if col.name == column)
                except StopIteration:
                    raise Exception(f"Column {column} not found in table {self.name}")
            else:
                assert type(column) is Column, f"can not remove unknown type {type(column)}"
                assert column in self.columns, f"column {column.name} not present"
                col = column
            assert len(self.columns) - 1 > self.pks, f"Cannot remove last value column {pos} {col.name}"

        cql_execute(self.cql, f"ALTER TABLE {self.full_name} DROP {col.name}")

        try:
            with self.lock:
                self.columns.remove(col)
        except Exception as exc:
            with self.lock:
                logger.error(f"remove_column UNSUCCESSFUL {exc}: col {col.name} {pos}/{len(self.columns)}")
        with self.lock:
            self.removed_columns.append(col)
            self.all_col_names = ", ".join([c.name for c in self.columns])
            self.valcol_names = ", ".join([col.name for col in self.columns[:self.pks]])
        return col

    def insert_next(self):
        with self.lock:
            seed = self.next_val_seed_count.__next__()
        return cql_execute(self.cql, f"INSERT INTO {self.full_name} ({self.all_col_names}) " +
                    f"VALUES ({', '.join(['%s'] * len(self.columns)) })",
                    parameters=[c.nextval(seed) for c in self.columns])

    def idx_name(self, idx_id, col_id):
        return f"{self.name}_{col_id:04}_{idx_id:04}"

    # TODO: custom index name (change tracking)
    def create_index(self, column=None):
        """Create a secondary index on a value column and return its id"""
        with self.lock:
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
        cql_execute(self.cql, f"CREATE INDEX {self.idx_name(idx_id, col.id)} ON {self.full_name} ({col.name})")
        with self.lock:
            self.indexes[idx_id] = col.id
        return idx_id

    def drop_index(self, idx_id=None):
        with self.lock:
            if idx_id is None:
                idx_id, col_id = self.indexes.popitem()  # random enough
            else:
                assert idx_id in self.indexes, "index to drop {idx_id} must exist"
                col_id = self.indexes.pop(idx_id)
        cql_execute(self.cql, f"DROP INDEX {self.idx_name(idx_id, col.id)}")

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
        self.lock = threading.Lock()

    def create(self):
        cql_execute(self.cql, f"CREATE KEYSPACE {self.name} WITH REPLICATION = {{ 'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication_factor} }}")

    def create_tables(self):
        [t.create() for t in self.tables]

    def drop_tables(self):
        [t.drop() for t in self.tables]

    def drop(self):
        cql_execute(self.cql, f"DROP KEYSPACE {self.name}")

    def drop_random_table(self):
        with self.lock:
            table = random.choice(self.tables)
        table.drop()
        return table

    @property
    def random_table(self):
        with self.lock:
            return random.choice(self.tables)

    def get_table(self, name):
        with self.lock:
            return next(t for t in self.tables if t.name == name)

    def new_table(self, name):
        raise Exception("Not implemented")
        #self.tables.append(Table())

    def drop_table(self, name):
        raise Exception("Not implemented")
        #pos, t = next((pos, t) for pos, t in enumerate(self.tables) if t.name = name)
        #del self.tables[pos]
        #self.removed_tables.append(t)


# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in a test. The keyspace is created with RF=2
# and destroyed after each test (not reused).
@pytest.fixture()
def keyspace(event_loop, request, cql):
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
    ks.create()
    ks.create_tables()
    yield ks
    ks.drop()

def insert_rows(table, n):
    for _ in range(n):
        table.insert_next()

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
@pytest.mark.ntables(0)
def test_cassandra_issue_10250(event_loop, thread_pool, cql, keyspace):
    from cassandra.concurrent import execute_concurrent  # XXX will this clash with asyncio

    RANGE = 20
    for n in range(RANGE):
        # alter_me: id uuid, s1 int, ..., s7 int
        ta = Table(cql, keyspace.name, name=f"alter_me_{n}", cols=[Column(name="id", ctype=UUIDType),
                    *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 8)]])
        ta.create()
        # index_me: id uuid, c1 int, ..., c7 int
        ti = Table(cql, keyspace.name, name=f"index_me_{n}", cols=[Column(name="id", ctype=UUIDType),
                    *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 8)]])
        ti.create()
        keyspace.tables.extend([ta, ti])

    if False:
        to_run = []
        for n in range(RANGE):
            tn = Table(cql, keyspace.name, name=f"new_table_{n}", cols=[Column(name="id", ctype=UUIDType),
                        *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 5)]])
            to_run.append(tn.create)
            for a in range(1, 8):
                # cmds.append(("alter table alter_me_{0} drop s{1};".format(n, a), ()))
                to_run.append(partial(keyspace.get_table(f"alter_me_{n}").remove_column, f"s{a}"))

                # cmds.append(("alter table alter_me_{0} add c{1} int;".format(n, a), ()))
                to_run.append(partial(keyspace.get_table(f"alter_me_{n}").add_column, Column(name=f"c{a}", ctype=IntType)))

                # cmds.append(("create index ix_index_me_{0}_c{1} on index_me_{0} (c{1});".format(n, a), ())
                to_run.append(partial(keyspace.get_table(f"index_me_{n}").create_index, column=f"c{a}"))

            res = event_loop.run_until_complete(run_blocking_tasks(event_loop, thread_pool, to_run))
    else:
        cql.execute(f"use {keyspace.name}")
        cmds = []
        for n in range(20):
            cmds.append(("create table new_table_{0} (id uuid primary key, c1 int, c2 int, c3 int, c4 int);".format(n), ()))
            for a in range(1, 8):
                cmds.append(("alter table alter_me_{0} drop s{1};".format(n, a), ()))
                cmds.append(("alter table alter_me_{0} add c{1} int;".format(n, a), ()))
                cmds.append(("create index ix_index_me_{0}_c{1} on index_me_{0} (c{1});".format(n, a), ()))
        res = execute_concurrent(cql, cmds, concurrency=100, raise_on_first_error=True)

    logger.debug("sleeping 20 seconds to make sure things are settled")
    time.sleep(20)

    logger.debug(f"verifing schema status")
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


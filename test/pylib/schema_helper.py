#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import asyncio
import itertools
import random
import uuid


# Default initial values
DEFAULT_DCRF = 3        # Replication Factor for this_dc


new_keyspace_id = itertools.count(start=1).__next__


class ValueType():
    def val(self, seed):
        """Return next value for this type"""
        pass


class IntType(ValueType):
    def __init__(self):
        self.name = 'int'

    def val(self, seed):
        return seed


class TextType(ValueType):
    def __init__(self):
        self.name = 'text'

    def val(self, seed):
        return str(seed)


class FloatType(ValueType):
    def __init__(self):
        self.name = 'float'

    def val(self, seed):
        return float(seed)


class UUIDType(ValueType):
    def __init__(self):
        self.name = 'uuid'

    def val(self, seed):
        return uuid.UUID(f"{{00000000-0000-0000-0000-{seed:012}}}")


class Column():
    def __init__(self, name, ctype=None):
        """A column definition.
           If no type given picks a simple type (no collection or user-defined)"""
        self.name = name
        if ctype is not None:
            self.ctype = ctype()
        else:
            self.ctype = random.choice([IntType, TextType, FloatType, UUIDType])()

        self.cql = f"{self.name} {self.ctype.name}"

    def val(self, seed):
        return self.ctype.val(seed)

    def __str__(self):
        return self.name


class Table():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql, keyspace, ncolumns=None, columns=None, pks=2, name=None):
        """Set up a new table definition from column definitions.
           If column definitions not specified pick a random number of columns with random types.
           By default there will be 4 columns with first column as Primary Key"""
        self.id = Table.newid()
        self.cql = cql
        self.keyspace = keyspace
        self.name = name if name is not None else f"t_{self.id:02}"
        self.full_name = keyspace + "." + self.name
        self.next_clustering_id = itertools.count(start=1).__next__
        self.next_value_id = itertools.count(start=1).__next__
        # TODO: assumes primary key is composed of first self.pks columns
        self.pks = pks

        if columns is not None:
            assert len(columns) > pks, "Not enough value columns provided"
            self.columns = columns
        else:
            assert ncolumns > pks, "Not enough value columns provided"
            # Primary key pk, clustering columns c_xx, value columns v_xx
            self.columns = [Column("pk", ctype=TextType)]
            self.columns += [Column(f"c_{self.next_clustering_id():02}", ctype=TextType) for i in range(1, pks)]
            self.columns += [Column(f"v_{self.next_value_id():02}") for i in range(1, ncolumns - pks + 1)]

        # Counter for sequential values to insert
        self.next_seq = itertools.count(start=1).__next__

    @property
    def all_col_names(self):
        return ", ".join([c.name for c in self.columns])

    async def create(self):
        await self.cql.run_async(f"CREATE TABLE {self.full_name} (" + ", ".join(f"{c.cql}" for c in self.columns) +
                                 ", primary key(" + ", ".join(c.name for c in self.columns[:self.pks]) + "))")

    async def drop(self):
        await self.cql.run_async(f"DROP TABLE {self.full_name}")

    async def insert_seq(self):
        """Insert a row of next sequential values"""
        seed = self.next_seq()
        await self.cql.run_async(f"INSERT INTO {self.full_name} ({self.all_col_names}) " +
                                 f"VALUES ({', '.join(['%s'] * len(self.columns)) })",
                                 parameters=[c.val(seed) for c in self.columns])

    def __str__(self):
        return self.full_name


class TestTables():
    """Create tables for a test in a given keyspace"""
    def __init__(self, test_name, cql, keyspace, ntables: int, ncolumns: int):
        self.test_name = test_name
        self.cql = cql
        self.keyspace = keyspace
        self.tables = [Table(cql, keyspace, ncolumns) for _ in range(ntables)]
        self.removed_tables = []

    def new_table(self, ncolumns=None, columns=None, pks=2, name=None):
        table = Table(self.cql, self.keyspace, ncolumns=ncolumns, columns=columns, pks=pks, name=name)
        self.tables.append(table)
        return table

    def __getitem__(self, pos):
        return self.tables[pos]

    def append(self, table):
        return self.tables.append(table)

    def extend(self, tables):
        return self.tables.extend(tables)

    async def create_tables(self):
        await asyncio.gather(*(t.create() for t in self.tables))

    async def drop_table(self, table):
        if type(table) is str:
            table = next(t for t in self.tables if table in [t.name, t.full_name])
        else:
            assert type(table) is Table, f"Invalid table type {type(table)}"
        await table.drop()
        self.tables.remove(table)
        self.removed_tables.append(table)
        return table

    async def drop_all_tables(self):
        """Drop all current tables"""
        await asyncio.gather(*(t.drop() for t in self.tables))
        self.removed_tables.extend(self.tables)

    async def verify_schema(self, table=None):
        """Verify keyspace table schema"""
        if type(table) is Table:
            tables = {table.name}
            q = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}' " \
                f"AND table_name = '{table.name}'"
        elif type(table) is str:
            if table.startswith(f"{self.keyspace}."):
                table = table[len(self.keyspace) + 1:]
            tables = {table}
            q = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}' " \
                f"AND table_name = '{table}'"
        else:
            tables = set(t.name for t in self.tables)
            q = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}'"

        res = {row.table_name for row in await self.cql.run_async(q)}
        assert not tables - res, f"Tables {tables - res} not present"

        for table_name in tables:
            table = next(t for t in self.tables if t.name == table_name)
            cols = {c.name: c for c in table.columns}
            c_pos = {c.name: i for i, c in enumerate(table.columns)}
            res = {row.column_name: row for row in await self.cql.run_async(
                    f"SELECT column_name, position, kind, type FROM system_schema.columns "
                    f"WHERE keyspace_name = '{self.keyspace}' AND table_name = '{table_name}'")}
            assert res.keys() == cols.keys(), f"Column names for {table_name} do not match " \
                                              "{', '.join(res.keys())} {', '.join(cols.keys())}"
            for c_name, c in res.items():
                pos = c_pos[c_name]
                col = cols[c_name]
                assert c.type == col.ctype.name, f"Column {c_name} type does not match {c.type} {col.ctype.name}"
                if pos == 0:
                    kind = "partition_key"
                    schema_pos = 0
                elif pos < table.pks:
                    kind = "clustering"
                    schema_pos = 0
                else:
                    kind = "regular"
                    schema_pos = -1
                assert c.kind == kind, f"Column {c_name} kind does not match {c.kind} {kind}"
                assert c.position == schema_pos, f"Column {c_name} position {c.position} does not match {schema_pos}"


# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will figure
# out the name of the current DC, so it can be used in NetworkTopologyStrategy.
async def this_dc(cql):
    res = await cql.run_async("SELECT data_center FROM system.local")
    return res[0][0]


async def create_keyspace(cql):
    keyspace_name = f"k_{new_keyspace_id():02}"
    dc = await this_dc(cql)
    await cql.run_async(f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = "
                        f"{{ 'class' : 'NetworkTopologyStrategy', '{dc}' : '{DEFAULT_DCRF}' }}")
    return keyspace_name


async def get_schema(test_name, cql, ntables: int, ncolumns: int):
    """Creates a keyspace and specified table schema.
       Since clusters are destroyed per test run it does not bother to drop anything."""
    tables = TestTables(test_name, cql, await create_keyspace(cql), ntables, ncolumns)
    await tables.create_tables()
    return tables

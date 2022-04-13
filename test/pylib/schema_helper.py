#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

from __future__ import annotations
from abc import ABCMeta
import asyncio
import itertools
import random
import uuid
from typing import Type, List, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from cassandra.cluster import Session as CassandraSession            # type: ignore


# Default initial values
DEFAULT_DCRF = 3        # Replication Factor for this_dc


new_keyspace_id = itertools.count(start=1).__next__


class ColumnNotFound(Exception):
    pass


class ValueType(metaclass=ABCMeta):
    name: str = ""

    def val(self, seed: int):
        """Return next value for this type"""
        pass


class IntType(ValueType):
    def __init__(self):
        self.name: str = 'int'

    def val(self, seed: int) -> int:
        return seed


class TextType(ValueType):
    def __init__(self):
        self.name: str = 'text'

    def val(self, seed) -> str:
        return str(seed)


class FloatType(ValueType):
    def __init__(self):
        self.name: str = 'float'

    def val(self, seed: int) -> float:
        return float(seed)


class UUIDType(ValueType):
    def __init__(self):
        self.name: str = 'uuid'

    def val(self, seed: int) -> uuid.UUID:
        return uuid.UUID(f"{{00000000-0000-0000-0000-{seed:012}}}")


class Column():
    def __init__(self, name: str, ctype: Type[ValueType] = None):
        """A column definition.
           If no type given picks a simple type (no collection or user-defined)"""
        self.name: str = name
        if ctype is not None:
            self.ctype = ctype()
        else:
            self.ctype = random.choice([IntType, TextType, FloatType, UUIDType])()

        self.cql: str = f"{self.name} {self.ctype.name}"

    def val(self, seed):
        return self.ctype.val(seed)

    def __str__(self):
        return self.name


class Table():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql: CassandraSession, keyspace: str, ncolumns=None, columns=None, pks=2, name=None):
        """Set up a new table definition from column definitions.
           If column definitions not specified pick a random number of columns with random types.
           By default there will be 4 columns with first column as Primary Key"""
        self.id: int = Table.newid()
        self.cql = cql
        self.keyspace = keyspace
        self.name: str = name if name is not None else f"t_{self.id:02}"
        self.full_name: str = keyspace + "." + self.name
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

        self.removed_columns: List[Column] = []
        # Counter for sequential values to insert
        self.next_seq = itertools.count(start=1).__next__
        self.next_idx_id = itertools.count(start=1).__next__
        self.indexes = []
        self.removed_indexes = []

    @property
    def all_col_names(self) -> str:
        return ", ".join([c.name for c in self.columns])

    async def create(self) -> asyncio.Future:
        return await self.cql.run_async(f"CREATE TABLE {self.full_name} ("
                                        + ", ".join(f"{c.cql}" for c in self.columns)
                                        + ", primary key("
                                        + ", ".join(c.name for c in self.columns[:self.pks]) + "))")

    async def drop(self) -> asyncio.Future:
        return await self.cql.run_async(f"DROP TABLE {self.full_name}")

    async def add_column(self, name: str = None, ctype: Type[ValueType] = None, column: Column = None):
        if column is not None:
            assert type(column) is Column, "Wrong column type to add_column"
        else:
            name = name if name is not None else f"c_{self.next_clustering_id():02}"
            ctype = ctype if ctype is not None else TextType
            column = Column(name, ctype=ctype)
        self.columns.append(column)
        await self.cql.run_async(f"ALTER TABLE {self.full_name} ADD {column.name} {column.ctype.name}")

    async def drop_column(self, column: Union[Column, str] = None):
        if column is None:
            col = random.choice(self.columns[self.pks:])
        elif type(column) is int:
            assert column >= self.pks, f"Cannot remove {self.name} PK column at pos {column}"
            col = self.columns[column]
        elif type(column) is str:
            try:
                col = next(col for col in self.columns if col.name == column)
            except StopIteration:
                raise ColumnNotFound(f"Column {column} not found in table {self.name}")
        else:
            assert type(column) is Column, f"can not remove unknown type {type(column)}"
            assert column in self.columns, f"column {column.name} not present"
            col = column
        assert len(self.columns) - 1 > self.pks, f"Cannot remove last value column {col.name} from {self.name}"
        self.columns.remove(col)
        self.removed_columns.append(col)
        await self.cql.run_async(f"ALTER TABLE {self.full_name} DROP {col.name}")

    async def insert_seq(self):
        """Insert a row of next sequential values"""
        seed = self.next_seq()
        await self.cql.run_async(f"INSERT INTO {self.full_name} ({self.all_col_names}) " +
                                 f"VALUES ({', '.join(['%s'] * len(self.columns)) })",
                                 parameters=[c.val(seed) for c in self.columns])

    async def add_index(self, column, name=None):
        if type(column) is int:
            assert column >= self.pks, f"Idex on {'PK' if column == 0 else 'CK'} column"
            col_name = self.columns[column].name
        elif type(column) is str:
            col_name = next(c for c in self.columns if c.name == column)
        else:
            assert type(column) is Column, "Wrong column type to add_column"
            assert column in self.columns
            col_name = column.name

        name = name if name is not None else f"{self.name}_{col_name}_{self.next_idx_id():02}"
        await self.cql.run_async(f"CREATE INDEX {name} on {self.full_name} ({col_name})")
        self.indexes.append(name)

    async def drop_index(self, index):
        if type(index) is int:
            index = self.indexes[index]
        else:
            assert type(index) is str, "Need integer or name for index drop"
        self.removed_indexes.append(index)
        await self.cql.run_async(f"DROP INDEX {self.keyspace}.{index}")

    def __str__(self):
        return self.full_name


class TestTables():
    """Create tables for a test in a given keyspace"""
    def __init__(self, test_name: str, cql: CassandraSession, keyspace: str, ntables: int, ncolumns: int):
        self.test_name = test_name
        self.cql = cql
        self.keyspace = keyspace
        self.tables: List[Table] = [Table(cql, keyspace, ncolumns) for _ in range(ntables)]
        self.removed_tables: List[Table] = []

    def new_table(self, ncolumns: int = None, columns: List[Column] = None, pks: int = 2, name: str = None) -> Table:
        table = Table(self.cql, self.keyspace, ncolumns=ncolumns, columns=columns, pks=pks, name=name)
        self.tables.append(table)
        return table

    def __getitem__(self, pos: int) -> Table:
        return self.tables[pos]

    def append(self, table: Table) -> None:
        self.tables.append(table)

    def extend(self, tables: List[Table]) -> None:
        self.tables.extend(tables)

    async def create_tables(self) -> None:
        await asyncio.gather(*(t.create() for t in self.tables))

    async def drop_table(self, table: Table) -> Table:
        if type(table) is str:
            table = next(t for t in self.tables if table in [t.name, t.full_name])
        else:
            assert type(table) is Table, f"Invalid table type {type(table)}"
        await table.drop()
        self.tables.remove(table)
        self.removed_tables.append(table)
        return table

    async def drop_all_tables(self) -> None:
        """Drop all current tables"""
        await asyncio.gather(*(t.drop() for t in self.tables))
        self.removed_tables.extend(self.tables)

    async def verify_schema(self, table: Union[Table, str] = None) -> None:
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

        res1 = {row.table_name for row in await self.cql.run_async(q)}
        assert not tables - res1, f"Tables {tables - res1} not present"

        for table_name in tables:
            table = next(t for t in self.tables if t.name == table_name)
            cols = {c.name: c for c in table.columns}
            c_pos = {c.name: i for i, c in enumerate(table.columns)}
            res2 = {row.column_name: row for row in await self.cql.run_async(
                    f"SELECT column_name, position, kind, type FROM system_schema.columns "
                    f"WHERE keyspace_name = '{self.keyspace}' AND table_name = '{table_name}'")}
            assert res2.keys() == cols.keys(), f"Column names for {table_name} do not match " \
                                               "{', '.join(res2.keys())} {', '.join(cols.keys())}"
            for c_name, c in res2.items():
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
async def this_dc(cql: CassandraSession):
    res = await cql.run_async("SELECT data_center FROM system.local")
    return res[0][0]


async def create_keyspace(cql: CassandraSession):
    keyspace_name = f"k_{new_keyspace_id():02}"
    dc = await this_dc(cql)
    await cql.run_async(f"CREATE KEYSPACE {keyspace_name} WITH REPLICATION = "
                        f"{{ 'class' : 'NetworkTopologyStrategy', '{dc}' : '{DEFAULT_DCRF}' }}")
    return keyspace_name


async def get_schema(test_name: str, cql: CassandraSession, ntables: int, ncolumns: int):
    """Creates a keyspace and specified table schema.
       Since clusters are destroyed per test run it does not bother to drop anything."""
    tables = TestTables(test_name, cql, await create_keyspace(cql), ntables, ncolumns)
    await tables.create_tables()
    return tables

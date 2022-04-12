#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file provides helpers for table creation and schema changes.
"""Provides helpers for CQL table management.

Classes:
    Tables
    TestTables
"""


from __future__ import annotations
from abc import ABCMeta
import asyncio
import itertools
import random
import uuid
from typing import Type, List, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from cassandra.cluster import Session as CassandraSession            # type: ignore


new_keyspace_id = itertools.count(start=1).__next__


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

    def __str__(self):
        return self.full_name


class TestTables():
    """Create tables for a test in a given keyspace"""
    def __init__(self, test_name: str, cql: CassandraSession, keyspace: str):
        self.test_name = test_name
        self.cql = cql
        self.keyspace = keyspace
        self.tables: List[Table] = []
        self.removed_tables: List[Table] = []

    async def add_tables(self, ntables: int = 1, ncolumns: int = 5) -> None:
        tables = [Table(self.cql, self.keyspace, ncolumns) for _ in range(ntables)]
        await asyncio.gather(*(t.create() for t in tables))
        self.tables.extend(tables)

    async def add_table(self, ncolumns: int = None, columns: List[Column] = None, pks: int = 2, name: str = None) -> Table:
        table = Table(self.cql, self.keyspace, ncolumns=ncolumns, columns=columns, pks=pks, name=name)
        await table.create()
        self.tables.append(table)
        return table

    def __getitem__(self, pos: int) -> Table:
        return self.tables[pos]

    def append(self, table: Table) -> None:
        self.tables.append(table)

    def extend(self, tables: List[Table]) -> None:
        self.tables.extend(tables)

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
                                               f"expected ({', '.join(cols.keys())}) " \
                                               f"got ({', '.join(res2.keys())})"
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

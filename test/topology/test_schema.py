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

from enum import Enum
import pytest
import random
from cassandra.protocol import InvalidRequest                            # type: ignore
from pylib.schema_helper import get_schema                               # type: ignore
from typing import List, TYPE_CHECKING
if TYPE_CHECKING:
    from test.pylib.harness import HarnessCli                            # type: ignore
    from test.pylib.schema_helper import TestTables                      # type: ignore
from sys import stderr


async def get_nodes(harness):   # XXX
    nodes = await harness.nodes()
    print(f"XXX test nodes {nodes}", file=stderr)  # XXX
    return await harness.nodes()


@pytest.mark.asyncio
async def test_new_table(cql):
    tables = await get_schema("new_table", cql, ntables=1, ncolumns=5)
    table = tables[0]
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" +
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    res = [row for row in await cql.run_async(f"SELECT * FROM {table} "
                                              "WHERE pk='1' AND c_01='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']
    await tables.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table}")
    tables.verify_schema()


@pytest.mark.asyncio
async def test_verify_schema(cql):
    """Verify table schema"""
    tables = await get_schema("verify_schema", cql, ntables=4, ncolumns=5)
    await tables.verify_schema()
    # Manually remove a column
    table = tables[0]
    await cql.run_async(f"ALTER TABLE {table} DROP {table.columns[-1].name}")
    with pytest.raises(AssertionError, match='Column'):
        await tables.verify_schema()


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_new_table_insert_one(cql):
    tables = await get_schema("new_table_insert_one", cql, ntables=1, ncolumns=5)
    table = tables[0]
    await table.insert_seq()
    col = table.columns[0].name
    res = [row for row in await cql.run_async(f"SELECT * FROM {table} WHERE pk='1' AND {col}='1'")]
    assert len(res) == 1
    assert list(res[0])[:2] == ['1', '1']


@pytest.mark.asyncio
async def test_drop_column(cql):
    """Drop a random column from a table"""
    tables = await get_schema("drop_column", cql, ntables=1, ncolumns=5)
    table = tables[0]
    await table.insert_seq()
    await table.drop_column()
    await tables.verify_schema(table)


@pytest.mark.asyncio
async def test_add_index(cql):
    """Add and drop an index"""
    tables = await get_schema("add_index", cql, ntables=1, ncolumns=3)
    table = tables[0]
    with pytest.raises(AssertionError, match='PK'):
        await table.add_index(0)
    with pytest.raises(AssertionError, match='CK'):
        await table.add_index(1)
    await table.add_index(2)
    await tables.verify_schema(table)


class ChangesTrack():
    def __init__(self, end_schema_changes, end_topology_changes, end_time):
        self.end_schema_changes = end_schema_changes
        self.end_topology_changes = end_topology_changes
        self.end_time = False
        self.current_schema = 0
        self.current_topology_changes = 0
        self.current_time = 0

    def __bool__(self):
        return self.end_schema >= self.current_schema and \
               self.end_topology_changes >= self.current_topology_changes and self.end_time


async def random_schema_changes(n: int, tables: TestTables, started_nodes: List[str], track: ChangesTrack,
                                rf: int, max_nodes: int = 8):
    for _ in range(n):
        pass  # XXX


async def random_topology_changes(n: int, harness: HarnessCli, started_nodes: List[str], rf: int, max_nodes: int = 8):
    """Random topology changes while keeping minimum RF
       Operations are remove, add, stop, and start node.
       A set of running nodes must be provided.
    """
    stopped_nodes: List[str] = []

    class NodeOp(Enum):
        ADD = 0
        REMOVE = 1
        START = 2
        STOP = 3

    def random_node_rf_safe():
        nn = len(started_nodes) + len(stopped_nodes)
        pos = random.randint(0, min(nn, rf) - 1)
        if pos < len(started_nodes):
            node_id = started_nodes.pop(pos)
        else:
            node_id = stopped_nodes.pop(pos - len(started_nodes))
        return node_id

    for _ in range(n):
        # Pick valid topology changes at this stage
        if len(started_nodes) + len(stopped_nodes) < max_nodes:
            ops = [NodeOp.ADD]
        if len(started_nodes) > rf:
            ops.append(NodeOp.REMOVE)
            ops.append(NodeOp.STOP)
        if len(stopped_nodes):
            ops.append(NodeOp.START)

        op = random.choice(ops)
        if op == NodeOp.ADD:
            node_id = await harness.node_add()
            print(f"XXX adding  node {node_id}", file=stderr)  # XXX
            started_nodes.append(node_id)
        elif op == NodeOp.REMOVE:
            node_id = random_node_rf_safe()
            print(f"XXX removing node {node_id}", file=stderr)  # XXX
            await harness.node_remove(node_id)
        elif op == NodeOp.START:
            node_id = random.choice(stopped_nodes)
            print(f"XXX starting node {node_id}", file=stderr)  # XXX
            await harness.node_start(node_id)
            stopped_nodes.remove(node_id)
            started_nodes.append(node_id)
        elif op == NodeOp.STOP:
            node_id = random_node_rf_safe()
            print(f"XXX stopping node {node_id}", file=stderr)  # XXX
            await harness.node_stop(node_id)


@pytest.mark.asyncio
async def test_random_schema_topology(cql, harness):
    tables = await get_schema("new_table_insert_one", cql, ntables=10, ncolumns=10)
    """Run random topology and schema changes in parallel"""
    # XXX
    # Pick random topology changes
    started_nodes = await get_nodes(harness)
    # XXX calculate max nodes or get from global params
    # XXX get RF ??
    topology_changes = random_topology_changes(harness, started_nodes, rf)
    schema_changes = []
    await tables.verify_schema(table)

    await harness.nodes_add(3)
    nodes = await get_nodes(harness)
    tables = await get_schema("add_index", cql, ntables=1, ncolumns=3)
    table = tables[0]
    with pytest.raises(AssertionError, match='PK'):
        await table.add_index(0)
    with pytest.raises(AssertionError, match='CK'):
        await table.add_index(1)
    await table.add_index(2)
    await tables.verify_schema(table)

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
from typing import Set, TYPE_CHECKING
if TYPE_CHECKING:
    from test.pylib.harness import HarnessCli                            # type: ignore
from sys import stderr


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


def random_topology_change(n: int, harness: HarnessCli, started_nodes: List[str], stopped_nodes: List[str], rf: int, max_nodes: int):
    """Random topology changes while keeping minimum RF
       Operations are remove, add, stop, and start node.
       A set of running nodes must be provided.
    """
    class NodeOp(Enum):
        ADD = 0
        REMOVE = 1
        START = 2
        STOP = 3
    for _ in range(n):
        if len(started_nodes) + len(stopped_nodes) < max_nodes:
            ops = [NodeOp.ADD]
        if len(stopped_nodes):
            ops = [NodeOp.START]
        if len(stopped_nodes) <= rf:
        # XXX can stop if started nodes > rf
        if len(nodes) > rf:
            ops.append[1]     # Remove
        if len(stopped_nodes) <= rf:
            bit_ops = 1    # Only do add and start node (ops 0, 1)
        else:
            bit_ops = 2    # Can also do stop and remove node (ops 2, 3)
        op = random.getrandbits(bit_ops)
        # XXX remove carefully to keep RF (e.g. stopped node)
        match random.choice(ops):
            case NodeOp.ADD:
                print(f"XXX adding  node {len(nodes)}", file=stderr)  # XXX
                node_id = await harness.node_add()
                nodes.add(node_id)
            case NodeOp.REMOVE:
                # XXX careful with RF (else pick stopped?)
                print(f"XXX removing node {node_id}", file=stderr)  # XXX
                await harness.node_remove(random.choice(nodes))
                nodes.remove(node_id)
                pass
            case NodeOp.START:
                node_id = random.choice(stopped_nodes)
                print(f"XXX starting node {node_id}", file=stderr)  # XXX
                await harness.node_start(node_id)
                stopped_nodes.remove(node_id)
            case NodeOp.STOP:
                # XXX careful with RF (else pick stopped?)
                node_id = random.choice(nodes - stopped_nodes)
                print(f"XXX starting node {node_id}", file=stderr)  # XXX
                await harness.node_start(node_id)
                stopped_nodes.remove(node_id)
        elif op == 2:                               # Remove
        elif op == 3:                               # Stop
            print(f"XXX stopping node {node_id}", file=stderr)  # XXX
            node_id = random.choice(nodes)
            await harness.node_stop(node_id)
            stopped_nodes.add(node_id)


@pytest.mark.asyncio
async def test_random_schema_topology(cql, harness):
    """Run random topology and schema changes in parallel"""
    # XXX
    # Pick random topology changes
    topology_changes = [random_topology_change(harness, rf) for _ in range(20)]
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

# XXX XXX XXX debug
#
import asyncio
from enum import Enum
import pytest
import random
from time import time
from datetime import timedelta
from typing import List
from test.pylib.harness_cli import HarnessCli                            # type: ignore
from test.pylib.schema_helper import TestTables                      # type: ignore
from sys import stderr   # XXX
from pylib.schema_helper import get_schema                               # type: ignore


async def get_nodes(harness):   # XXX
    nodes = await harness.nodes()
    print(f"XXX test nodes {nodes}", file=stderr)  # XXX
    return nodes


class ChangesTrack():
    """Track if there were enough schema and topology changes and specified time elapsed"""
    def __init__(self, end_schema_changes: int, end_topology_changes: int, end_time: float):
        self.end_schema_changes = end_schema_changes
        self.end_topology_changes = end_topology_changes
        self.end_time = end_time
        self.schema_changes = 0
        self.topology_changes = 0

    def __bool__(self):
        ret = self.schema_changes < self.end_schema_changes and \
               self.topology_changes < self.end_topology_changes and time() < self.end_time # XXX
        print(f"XXX ChangesTrack {ret}", file=stderr)  # XXX
        return ret


async def random_schema_changes(tables: TestTables, started_nodes: List[str], changes: ChangesTrack,
                                rf: int):
    while changes:
        changes.schema_changes += 1
        # XXX


async def random_topology_changes(harness: HarnessCli, started_nodes: List[str], changes: ChangesTrack,
                                  rf: int, max_nodes: int = 8):
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

    while changes:
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
        changes.topology_changes += 1


@pytest.mark.asyncio
async def test_harness_xxx(cql, harness):
    nodes = await get_nodes(harness)
    tables = await get_schema("new_table_insert_one", cql, ntables=10, ncolumns=10)
    changes = ChangesTrack(10, 10, time() + 0.2)  # 200 ms
    await asyncio.gather(random_topology_changes(harness, nodes, changes, 3, 10),  # XXX 10 nodes
                         random_schema_changes(tables, nodes, changes, 3))
    print("XXX test_xxx DONE", file=stderr)  # XXX
    raise Exception("XXX")

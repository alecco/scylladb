#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import random
from pylib.schema_helper import get_schema                               # type: ignore
from typing import Set, TYPE_CHECKING
if TYPE_CHECKING:
    from test.pylib.harness import HarnessCli                            # type: ignore
from sys import stderr



async def get_nodes(harness):   # XXX
    nodes = await harness.nodes()
    print(f"XXX test nodes {nodes}", file=stderr)  # XXX
    return await harness.nodes()


@pytest.mark.asyncio
async def test_add_node_add_column(cql, harness):
    tables = await get_schema("add_node", cql, ntables=1, ncolumns=5)
    await harness.node_add()
    await tables[0].add_column()
    await tables.verify_schema()


@pytest.mark.asyncio
async def test_stop_node_add_column(cql, harness):
    """Add a node, stop an original node, add a column"""
    nodes = await harness.nodes()
    tables = await get_schema("add_node", cql, ntables=1, ncolumns=5)
    await harness.node_add()
    await harness.node_stop(nodes[1])  # XXX
    await tables[0].add_column()
    await tables.verify_schema()


@pytest.mark.asyncio
async def test_restart_node_add_column(cql, harness):
    """Add a node, stop an original node, add a column"""
    nodes = await harness.nodes()
    tables = await get_schema("add_node", cql, ntables=1, ncolumns=5)
    await harness.node_restart(nodes[1])
    await tables[0].add_column()
    await tables.verify_schema()


@pytest.mark.asyncio
async def test_remove_node_add_column(cql, harness):
    """Add a node, remove an original node, add a column"""
    nodes = await harness.nodes()
    tables = await get_schema("add_node", cql, ntables=1, ncolumns=5)
    await harness.node_add()
    await harness.node_remove(nodes[1])
    await tables[0].add_column()
    await tables.verify_schema()


@pytest.mark.asyncio
async def test_decomission_node_add_column(cql, harness):
    """Add a node, decomission an original node, add a column"""
    nodes = await harness.nodes()
    tables = await get_schema("add_node", cql, ntables=1, ncolumns=5)
    await harness.node_add()
    await harness.node_decommission(nodes[1])
    await tables[0].add_column()
    await tables.verify_schema()


@pytest.mark.asyncio
async def test_replace_node_add_column(cql, harness):
    """Add a node, replace an original node, add a column"""
    nodes = await harness.nodes()
    tables = await get_schema("add_node", cql, ntables=1, ncolumns=5)
    await harness.node_replace(nodes[1])
    await tables[0].add_column()
    await tables.verify_schema()

#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
import random
from sys import stderr


#@pytest.mark.asyncio
#async def test_add_node_add_column(cql, cluster, tables):
#    table = await tables.add_table(ncolumns=5)
#    await cluster.node_add()
#    await table.add_column()
#    await tables.verify_schema()


@pytest.mark.asyncio
async def test_stop_node_add_column(cql, cluster, tables):
    """Add a node, stop an original node, add a column"""
    nodes = await cluster.nodes()
    table = await tables.add_table(ncolumns=5)
    await cluster.node_add()
    await cluster.node_stop(nodes[1])
    await table.add_column()
    await tables.verify_schema()


#@pytest.mark.asyncio
#async def test_restart_node_add_column(cql, cluster, tables):
#    """Add a node, stop an original node, add a column"""
#    nodes = await cluster.nodes()
#    table = await tables.add_table(ncolumns=5)
#    await cluster.node_restart(nodes[1])
#    await table.add_column()
#    await tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_remove_node_add_column(cql, cluster, tables):
#    """Add a node, remove an original node, add a column"""
#    nodes = await cluster.nodes()
#    table = await tables.add_table(ncolumns=5)
#    await cluster.node_add()
#    await cluster.node_remove(nodes[1])
#    await table.add_column()
#    await tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_decomission_node_add_column(cql, cluster, tables):
#    """Add a node, decomission an original node, add a column"""
#    nodes = await cluster.nodes()
#    table = await tables.add_table(ncolumns=5)
#    await cluster.node_add()
#    await cluster.node_decommission(nodes[1])
#    await table.add_column()
#    await tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_replace_node_add_column(cql, cluster, tables):
#    """Add a node, replace an original node, add a column"""
#    nodes = await cluster.nodes()
#    table = await tables.add_table(ncolumns=5)
#    await cluster.node_replace(nodes[1])
#    await table.add_column()
#    await tables.verify_schema()

#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Simple tests for consistency of schema changes while there are topology changes.
"""
import pytest


@pytest.mark.asyncio
async def test_1(cluster_api, random_tables):
    await cluster_api.node_add()
    raise Exception("XXX")

@pytest.mark.asyncio
async def test_2(cluster_api, random_tables):
    await cluster_api.node_add()


#@pytest.mark.asyncio
#async def test_add_node_add_column(cluster_api, random_tables):
#    """Add a node and then add a column to a table and verify"""
#    table = await random_tables.add_table(ncolumns=5)
#    await cluster_api.node_add()
#    await table.add_column()
#    await random_tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_stop_node_add_column(cluster_api, random_tables):
#    """Add a node, stop an original node, add a column"""
#    nodes = await cluster_api.nodes()
#    table = await random_tables.add_table(ncolumns=5)
#    await cluster_api.node_add()
#    await cluster_api.node_stop(nodes[1])
#    await table.add_column()
#    await random_tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_restart_node_add_column(cluster_api, random_tables):
#    """Add a node, stop an original node, add a column"""
#    nodes = await cluster_api.nodes()
#    table = await random_tables.add_table(ncolumns=5)
#    await cluster_api.node_restart(nodes[1])
#    await table.add_column()
#    await random_tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_remove_node_add_column(cluster_api, random_tables):
#    """Add a node, remove an original node, add a column"""
#    nodes = await cluster_api.nodes()
#    table = await random_tables.add_table(ncolumns=5)
#    await cluster_api.node_add()
#    await cluster_api.node_remove(nodes[1])
#    await table.add_column()
#    await random_tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_decomission_node_add_column(cluster_api, random_tables):
#    """Add a node, decomission an original node, add a column"""
#    nodes = await cluster_api.nodes()
#    table = await random_tables.add_table(ncolumns=5)
#    await cluster_api.node_add()
#    await cluster_api.node_decommission(nodes[1])
#    await table.add_column()
#    await random_tables.verify_schema()
#
#
#@pytest.mark.asyncio
#async def test_replace_node_add_column(cluster_api, random_tables):
#    """Add a node, replace an original node, add a column"""
#    nodes = await cluster_api.nodes()
#    table = await random_tables.add_table(ncolumns=5)
#    await cluster_api.node_replace(nodes[1])
#    await table.add_column()
#    await random_tables.verify_schema()

#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import pytest


@pytest.mark.asyncio
async def test_add_node_add_column(harness, random_tables):
    """Add a node and then add a column to a table and verify"""
    table = await random_tables.add_table(ncolumns=5)
    await harness.node_add()
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_restart_node_add_column(harness, random_tables):
    """Add a node, stop an original node, add a column"""
    nodes = await harness.nodes()
    table = await random_tables.add_table(ncolumns=5)
    await harness.node_restart(nodes[1])
    await table.add_column()
    await random_tables.verify_schema()

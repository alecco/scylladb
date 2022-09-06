#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import pytest
import sys      # XXX
import asyncio  # XXX


#@pytest.mark.asyncio
#async def test_add_server_add_column(manager, random_tables):
#    """Add a node and then add a column to a table and verify"""
#    table = await random_tables.add_table(ncolumns=5)
#    await manager.server_add()
#    await table.add_column()
#    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_stop_server_add_column(manager, random_tables):
    """Add a node, stop an original node, add a column"""
    servers = await manager.servers()
    table = await random_tables.add_table(ncolumns=5)
    # server_id = await manager.server_add()
    # asyncio.sleep(1)                                                           # XXX
    # print(f"\nXXX test_stop... addded server {server_id}", file=sys.stderr)    # XXX
    # print(f"\nXXX test_stop... stopping server {servers[1]}", file=sys.stderr) # XXX
    # await manager.server_stop(servers[1])
    # asyncio.sleep(10)                                                           # XXX
    #print(f"\nXXX test_stop... add column", file=sys.stderr)                   # XXX
    #await table.add_column()
    #print(f"\nXXX test_stop... add column DONE", file=sys.stderr)              # XXX
    await random_tables.verify_schema()


#@pytest.mark.asyncio
#async def test_restart_server_add_column(manager, random_tables):
#    """Add a node, stop an original node, add a column"""
#    servers = await manager.servers()
#    table = await random_tables.add_table(ncolumns=5)
#    ret = await manager.server_restart(servers[1])
#    await table.add_column()
#    await random_tables.verify_schema()


#@pytest.mark.asyncio
#async def test_remove_server_add_column(manager, random_tables):
#    """Add a node, remove an original node, add a column"""
#    servers = await manager.servers()
#    table = await random_tables.add_table(ncolumns=5)
#    await manager.server_add()
#    await manager.server_remove(servers[1])
#    await table.add_column()
#    await random_tables.verify_schema()

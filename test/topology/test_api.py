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
@pytest.mark.skip(reason="debug")
async def test_server_id(manager):
    servers = await manager.servers()
    server_uuid = await manager.get_server_uuid(servers[0])
    raise Exception(f"XXX {servers[0]} UUID: {server_uuid}")


@pytest.mark.asyncio
#@pytest.mark.skip(reason="debug")
async def test_remove_server(manager):
    servers = await manager.servers()
    server_uuid = await manager.get_server_uuid(servers[1])   # get uuid [1] (.2)
    await manager.server_remove(servers[1], server_uuid)      # Remove [1] (.2) on [0]


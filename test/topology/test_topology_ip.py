#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import time
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency
import pytest


@pytest.mark.asyncio
async def test_replace_different_ip(manager: ManagerClient) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_different_ip_using_host_id(manager: ManagerClient) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip(manager: ManagerClient) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = False)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

@pytest.mark.asyncio
async def test_replace_reuse_ip_using_host_id(manager: ManagerClient) -> None:
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True, use_host_id = True)
    await manager.server_add(replace_cfg)
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

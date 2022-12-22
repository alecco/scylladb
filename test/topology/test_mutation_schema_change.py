
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test repro of failure to store mutation with schema change and a server down
"""
import pytest
import logging
from test.pylib.rest_client import ScyllaRESTAPIClient, inject_error


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_mutation_schema_change(manager, random_tables):
    """
        1. build a cluster of 3 nodes
        2. inject failure into paxos apply on one node
        3. execute paxos write
        4. alter table add column
        5. replace the node which failed to apply a mutation
        6. drop another node, not the one you were replacing (stopping it is just fine)
        7. perform serial read of the partition -> this will lead to repair,
           which should try to load old mapping, which is gone onthe replaced node.
    """
    table = await random_tables.add_table(ncolumns=5)

    servers = await manager.running_servers()
    # async with inject_error(manager.api, servers[0].ip_addr, 'read_cas_request_apply', one_shot=True):
    async with inject_error(manager.api, servers[0].ip_addr, 'read_cas_request_apply', one_shot=True):
        async with inject_error(manager.api, servers[1].ip_addr, 'read_cas_request_apply', one_shot=True):
            async with inject_error(manager.api, servers[2].ip_addr, 'read_cas_request_apply', one_shot=True):
                await table.insert_row(if_not_exists = True)    # Insert a row
    raise Exception("CUEC")  # XXX
    # await manager.remove_node(servers[0].server_id, servers[1].server_id)   # Remove 1 (on 0)
    # await table.add_column()
    # await manager.server_stop_gracefully(servers[1].server_id)              # stop   1

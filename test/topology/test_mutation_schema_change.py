
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test repro of failure to store mutation with schema change and a server down
"""
import logging
from test.pylib.rest_client import inject_error
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_mutation_schema_change(manager, random_tables):
    """
        1. build a cluster of 3 nodes
        2. inject failure into paxos apply on one node (XXX learn?)
        3. execute paxos write
        4. alter table add column
        5. replace the node which failed to apply a mutation
        6. drop another node, not the one you were replacing (stopping it is just fine)
        7. perform serial read of the partition -> this will lead to repair, which should try
           to load old mapping, which is gone on the replaced node.
    """
    servers = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5)
    manager.driver_close()
    await manager.server_stop_gracefully(servers[0].server_id)          # Stop  [0]
    await manager.driver_connect()
    for srv in [1, 2]:
        await manager.api.set_logger_level(servers[srv].ip_addr, "paxos", "trace")
        await manager.api.set_logger_level(servers[srv].ip_addr, "raft_group0", "trace")

    ROWS = 5
    seeds = [t.next_seq() for _ in range(ROWS)]
    async with inject_error(manager.api, servers[1].ip_addr, 'paxos_error_before_learn',
                            one_shot=False):
        for seed in seeds:
            stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
                   f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
                   f"IF NOT EXISTS"
            logger.warning(f"---------------- {seed} -------------------------\n{stmt}\n")
            await manager.cql.run_async(stmt, parameters=[c.val(seed) for c in t.columns])  # FIRST
            await t.add_column()

    manager.driver_close()           # CLOSE
    await manager.server_stop_gracefully(servers[1].server_id)    # Stop  B  (C stays)
    # XXX here we want A to get the schema change from C's snapshot
    logger.warning("---------------------------------- STARTING A -----------------------------------------")  # XXX
    await manager.server_start(servers[0].server_id)              # Start A again  (C leader)
    await manager.driver_connect()   # CONNECT
    # logger.warning("---------------------------------- BEFORE SLEEP -----------------------------------------")  # XXX
    # await asyncio.sleep(5) # XXX SLEEP
    # manager._driver_update()                                      # driver update endpoints
    for seed in seeds:
        await manager.cql.run_async(f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
                                    f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
                                    f"IF NOT EXISTS",
                                    parameters=[c.val(seed) for c in t.columns])  # SECOND

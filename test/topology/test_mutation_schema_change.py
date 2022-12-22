
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
        1. shut down 1 node,  A
        2. do LWT operation,  (B C)
        3. change schema twice,  (so it cannot recreate a history)   2 add columns
        4. shut down B, then start A   [K: C becomes leader, replicates it's Raft log to A]
        5. then do LWT operation on the same key
    """
    servers = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5)
    manager.driver_close()
    await manager.server_stop_gracefully(servers[0].server_id)          # Stop  A
    await manager.driver_connect()
    for srv in [1, 2]:
        await manager.api.set_logger_level(servers[srv].ip_addr, "paxos", "trace")
        await manager.api.set_logger_level(servers[srv].ip_addr, "raft", "trace")

    seeds = [t.next_seq() for _ in range(10)]
    async with inject_error(manager.api, servers[1].ip_addr, 'group0_force_snapshot', one_shot=False):
        async with inject_error(manager.api, servers[2].ip_addr, 'group0_force_snapshot', one_shot=False):
            for seed in seeds:
                logger.warning(f"---------------- {seed} -------------------------")  # XXX
                stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
                       f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
                       f"IF NOT EXISTS"
                # XXX Fails here after 2 or 3
                await manager.cql.run_async(stmt, parameters=[c.val(seed) for c in t.columns])  # FIRST
                await t.add_column()

    manager.driver_close()
    await manager.server_stop_gracefully(servers[1].server_id)    # Stop  B  (C stays)
    logger.warning("----------- STARTING A --------------")  # XXX
    await manager.server_start(servers[0].server_id)              # Start A again  (C leader)
    await manager.driver_connect()   # CONNECT
    for seed in seeds:
        await manager.cql.run_async(f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
                                    f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
                                    f"IF NOT EXISTS",
                                    parameters=[c.val(seed) for c in t.columns])  # SECOND


#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test repro of failure to store mutation with schema change and a server down
"""
import asyncio
import logging
from test.pylib.rest_client import inject_error
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_mutation_schema_change(manager, random_tables):
    """
        Cluster A, B, C
        create table
        C is down
        change schema
        do lwt write
        change schema
        B down, C up
        do lwt write to the same key
    """
    servers = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5)
    manager.driver_close()
    logger.warning("----- STOPPING [2] -----")
    await manager.server_stop_gracefully(servers[2].server_id)          # Stop  [2]
    await manager.driver_connect()

    await t.add_column()
    ROWS = 1
    seeds = [t.next_seq() for _ in range(ROWS)]
    for seed in seeds:
        stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
               f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
               f"IF NOT EXISTS"
        logger.debug("----- FIRST INSERT: %s -----\n%s\n", seed, stmt)
        await manager.cql.run_async(stmt, parameters=[c.val(seed) for c in t.columns])  # FIRST
    await t.add_column()

    manager.driver_close()
    logger.warning("----- STOPPING [1] -----")
    await manager.server_stop_gracefully(servers[1].server_id)    # Stop  [1]
    logger.warning("----- STARTING [2] -----")
    await manager.server_start(servers[2].server_id)              # Start [2] again
    await manager.driver_connect()
    # await asyncio.sleep(1)
    for seed in seeds:
        stmt = f"UPDATE {t} "                        \
               f"SET   {t.columns[3].name} = %s "  \
               f"WHERE {t.columns[0].name} = %s "  \
               f"IF    {t.columns[3].name} = %s"
        logger.warning("----- SECOND INSERT: %s -----\n%s\n", seed, stmt)
        await manager.cql.run_async(stmt, parameters=[t.columns[3].val(seed + 1), # v_01 = seed + 1
                                                      t.columns[0].val(seed),     # pk = seed
                                                      t.columns[3].val(seed)])    # v_01 == seed

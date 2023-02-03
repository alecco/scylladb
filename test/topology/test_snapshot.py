#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test snapshot transfer by forcing threshold and performing schema changes
"""
import asyncio
import logging
from test.pylib.rest_client import inject_error_one_shot, inject_error
import pytest
from cassandra.query import SimpleStatement              # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_snapshot(manager, random_tables):
    """
        Cluster A, B, C
        with reduced snapshot threshold create table, do several schema changes, insert
        start a new server, stop A B C, check if it sees the table schema correctly with an insert.
    """
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5, pks=1)

    # Reduce the snapshot thresholds
    errs = [inject_error_one_shot(manager.api, s.ip_addr, 'raft_server_snapshot_reduce_threshold')
            for s in [server_a, server_b, server_c]]
    await asyncio.gather(*errs)
    # error injection is in Raft io_thread code path, wait for it to run
    await asyncio.sleep(.01)

    for i in range(5):
        await t.add_column()

    manager.driver_close()
    server_d = await manager.server_add()
    logger.info("Started D %s", server_d)

    logger.info("Stopping A %s, B %s, and C %s", server_a, server_b, server_c)
    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id)
                           for s in [server_a, server_b, server_c]])

    logger.info("Driver connecting to C %s", server_c)
    await manager.driver_connect()

    await random_tables.verify_schema()

    manager.driver_close()
    logger.info("Starting A %s", server_a)
    await manager.server_start(server_a.server_id)
    logger.info("Starting B %s", server_b)
    await manager.server_start(server_b.server_id)
    await manager.driver_connect()
    logger.info("Test DONE")

    await manager.mark_dirty()

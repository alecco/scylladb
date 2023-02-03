#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Reproducer for a failure during lwt operation due to missing of a column mapping in schema history table.
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
        stop C
        with reduced snapshot threshold create table, do several schema changes, insert
        start C again and check if it sees the table schema correctly with an insert.
    """
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5, pks=1)

    logger.debug("Stopping C %s", server_c)
    manager.driver_close()
    await manager.server_stop_gracefully(server_c.server_id)
    await manager.driver_connect()

    # Reduce the snapshot threshold to 3, trailing 2 on A and B
    errs = [inject_error_one_shot(manager.api, s.ip_addr, 'raft_server_snapshot_reduce_threshold')
            for s in [server_a, server_b]]
    await asyncio.gather(*errs)
    await asyncio.sleep(.01)  # wait for io_thread to run

    for i in range(5):
        await t.add_column()

    manager.driver_close()
    logger.debug("Starting C %s", server_c)
    await manager.server_start(server_c.server_id)

    logger.debug("Stopping A %s and B %s", server_a, server_b)
    await manager.server_stop_gracefully(server_a.server_id)
    await manager.server_stop_gracefully(server_b.server_id)

    logger.debug("Driver connecting to C %s", server_c)
    await manager.driver_connect()

    await random_tables.verify_schema()

    logger.debug("Starting A %s and B %s", server_a, server_b)
    await manager.server_start(server_a.server_id)
    await manager.server_start(server_b.server_id)

    # Restore snapshot threshold for A and B
    errs = [inject_error_one_shot(manager.api, s.ip_addr, 'raft_server_snapshot_restore_threshold')
            for s in [server_a, server_b]]
    await asyncio.gather(*errs)
    await asyncio.sleep(.01)  # wait for io_thread to run

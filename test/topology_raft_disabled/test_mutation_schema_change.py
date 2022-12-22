
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
from cassandra.cluster import Cluster, ConsistencyLevel  # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement              # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Issue #10770")
async def test_mutation_schema_change(manager, random_tables):
    """
        Cluster A, B, C
        create table
        stop C
        change schema + do lwt write + change schema
        stop B
        start C
        do lwt write to the same key through A
    """
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5)
    manager.driver_close()
    await inject_error_one_shot(manager.api, server_a.ip_addr,
                                'raft_server_snapshot_reduce_threshold')
    await asyncio.sleep(2)  # XXX wait for io_thread to run

    logger.warning("----- STOPPING C -----")
    await manager.server_stop_gracefully(server_c.server_id)          # Stop  C
    await manager.driver_connect()

    async with inject_error(manager.api, server_b.ip_addr, 'paxos_error_before_learn',
                            one_shot=False):
        await t.add_column()
        ROWS = 1
        seeds = [t.next_seq() for _ in range(ROWS)]
        stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
               f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
               f"IF NOT EXISTS"
        query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
        for seed in seeds:
            logger.debug("----- FIRST INSERT: %s -----\n", seed)
            await manager.cql.run_async(query, parameters=[c.val(seed) for c in t.columns])  # FIRST
        await t.add_column()

    manager.driver_close()

    logger.warning("----- STOPPING B -----")
    await manager.server_stop_gracefully(server_b.server_id)
    logger.warning("----- STARTING C -----")
    await manager.server_start(server_c.server_id)

    await asyncio.sleep(10)

    logger.debug("driver connecting to C")
    manager.ccluster = manager.con_gen([server_c.ip_addr], manager.port, manager.use_ssl)
    manager.cql = manager.ccluster.connect()

    stmt = f"UPDATE {t} "                        \
           f"SET   {t.columns[3].name} = %s "  \
           f"WHERE {t.columns[0].name} = %s "  \
           f"IF    {t.columns[3].name} = %s"
    query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
    for seed in seeds:
        logger.warning("----- UPDATE: %s -----\n", seed)
        await manager.cql.run_async(query, parameters=[t.columns[3].val(seed + 1), # v_01 = seed + 1
                                                       t.columns[0].val(seed),     # pk = seed
                                                       t.columns[3].val(seed)])    # v_01 == seed

    await inject_error_one_shot(manager.api, server_a.ip_addr,
                                'raft_server_snapshot_restore_threshold')
    await asyncio.sleep(.1)


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Issue #10770")
async def test_mutation_schema_change_restart(manager, random_tables):
    """
        Cluster A, B, C
        create table
        stop C
        change schema + do lwt write + change schema
        stop B
        restart A
        start C
        do lwt write to the same key through A
    """
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5)
    manager.driver_close()
    await inject_error_one_shot(manager.api, server_a.ip_addr,
                                'raft_server_snapshot_reduce_threshold')
    await asyncio.sleep(2)  # XXX wait for io_thread to run

    logger.warning("----- STOPPING C -----")
    await manager.server_stop_gracefully(server_c.server_id)          # Stop  C
    await manager.driver_connect()

    await inject_error_one_shot(manager.api, server_a.ip_addr,
                                'raft_server_reduce_threshold')
    async with inject_error(manager.api, server_b.ip_addr, 'paxos_error_before_learn',
                            one_shot=False):
        await t.add_column()
        ROWS = 1
        seeds = [t.next_seq() for _ in range(ROWS)]
        stmt = f"INSERT INTO {t} ({','.join(c.name for c in t.columns)}) " \
               f"VALUES ({', '.join(['%s'] * len(t.columns))}) "           \
               f"IF NOT EXISTS"
        query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
        for seed in seeds:
            logger.debug("----- FIRST INSERT: %s -----\n", seed)
            await manager.cql.run_async(query, parameters=[c.val(seed) for c in t.columns])  # FIRST
        await t.add_column()

    manager.driver_close()

    logger.warning("----- STOPPING B -----")
    await manager.server_stop_gracefully(server_b.server_id)
    logger.warning("----- RESTARTING A -----")
    await manager.server_restart(server_a.server_id)
    logger.warning("----- STARTING C -----")
    await manager.server_start(server_c.server_id)

    await asyncio.sleep(10)

    logger.debug("driver connecting to A")
    manager.ccluster = manager.con_gen([server_a.ip_addr], manager.port, manager.use_ssl)
    manager.cql = manager.ccluster.connect()

    stmt = f"UPDATE {t} "                        \
           f"SET   {t.columns[3].name} = %s "  \
           f"WHERE {t.columns[0].name} = %s "  \
           f"IF    {t.columns[3].name} = %s"
    query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
    for seed in seeds:
        logger.warning("----- UPDATE: %s -----\n", seed)
        await manager.cql.run_async(query, parameters=[t.columns[3].val(seed + 1), # v_01 = seed + 1
                                                       t.columns[0].val(seed),     # pk = seed
                                                       t.columns[3].val(seed)])    # v_01 == seed

    await inject_error_one_shot(manager.api, server_a.ip_addr,
                                'raft_server_snapshot_restore_threshold')
    await asyncio.sleep(.1)

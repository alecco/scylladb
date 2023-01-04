
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
import time
from typing import Optional
from test.pylib.rest_client import ScyllaRESTAPIClient, inject_error
from test.pylib.manager_client import IPAddress
from test.pylib.util import wait_for
import pytest
from cassandra.cluster import Cluster, ConsistencyLevel  # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement              # type: ignore # pylint: disable=no-name-in-module


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
    server_a, server_b, server_c = await manager.running_servers()
    t = await random_tables.add_table(ncolumns=5)
    manager.driver_close()
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
    await manager.server_stop_gracefully(server_b.server_id)    # Stop  B
    logger.warning("----- STARTING C -----")
    await manager.server_start(server_c.server_id)              # Start A again
    await manager.driver_connect()

    # await asyncio.sleep(10)
    server_c_gen = await manager.api.get_gossip_generation_number(server_a.ip_addr, server_c.ip_addr)
    logging.info(f"Gossip generation number of {server_c} seen from {server_a}: {server_c_gen}")
    # Before continuing, ensure that A node has gossiped with C after C has restarted.
    # Then we know that A node learned about srv1
    logging.info(f"Waiting until A gossips with C")
    await wait_for_gossip_gen_increase(manager.api, server_c_gen, server_a.ip_addr, server_c.ip_addr, time.time() + 60)

    stmt = f"UPDATE {t} "                        \
           f"SET   {t.columns[3].name} = %s "  \
           f"WHERE {t.columns[0].name} = %s "  \
           f"IF    {t.columns[3].name} = %s"
    query = SimpleStatement(stmt, consistency_level=ConsistencyLevel.ONE)
    for seed in seeds:
        logger.warning("----- SECOND INSERT: %s -----\n", seed)
        await manager.cql.run_async(query, parameters=[t.columns[3].val(seed + 1), # v_01 = seed + 1
                                                       t.columns[0].val(seed),     # pk = seed
                                                       t.columns[3].val(seed)])    # v_01 == seed


async def wait_for_gossip_gen_increase(api: ScyllaRESTAPIClient, gen: int, node_ip: IPAddress,
                                       target_ip: IPAddress, deadline: float):
    """Wait until the generation number of `target_ip` increases above `gen` from the point of view of `node_ip`.
       Can be used to wait until `node_ip` gossips with `target_ip` after `target_ip` was restarted
       by saving the generation number of `target_ip` before restarting it and then calling this function
       (nodes increase their generation numbers when they restart).
    """
    async def gen_increased() -> Optional[int]:
        curr_gen = await api.get_gossip_generation_number(node_ip, target_ip)
        if curr_gen <= gen:
            logging.info(f"Gossip generation number of {target_ip} is {curr_gen} <= {gen} according to {node_ip}")
            return None
        return curr_gen
    gen = await wait_for(gen_increased, deadline)
    logging.info(f"Gossip generation number of {target_ip} is reached {gen} according to {node_ip}")

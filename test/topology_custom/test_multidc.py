#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test functionality on the cluster with different values of the --smp parameter on the nodes.
"""
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
from test.topology.util import wait_for_token_ring_and_group0_consistency
import pytest

logger = logging.getLogger(__name__)

# Checks a cluster boot/operations in multi-dc environment
@pytest.mark.asyncio
async def test_multidc(manager: ManagerClient, random_tables: RandomTables) -> None:

    logger.info(f'Creating a new node')
    await manager.server_add(config={'snitch': 'GossipingPropertyFileSnitch'},
     propertyFile={
         'dc': 'dc1',
         'rack': 'myrack'
     })
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)
    logger.info(f'Creating new tables')
    await random_tables.add_tables(ntables=3, ncolumns=3)
    await random_tables.verify_schema()

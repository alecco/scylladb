#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Harness client.
   Communicates with Harness server via socket.
   Provides helper methods to test cases.
   Manages driver refresh when cluster is cycled.
"""

from __future__ import annotations                                         # type: ignore
import asyncio                                                             # type: ignore
from datetime import datetime, timedelta                                   # type: ignore
from typing import Callable, List, Optional, TYPE_CHECKING
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore
if TYPE_CHECKING:
    from cassandra.cluster import Session as CassandraSession              # type: ignore
    from cassandra.cluster import Cluster as CassandraCluster              # type: ignore


class HarnessOpException(Exception):
    """Operational exception with remote API"""

class HarnessTimeoutException(Exception):
    """Harness client timed out while waiting on remote API"""

class HarnessCli():
    """Helper Harness API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Harness server is listening
        con_gen (Callable): generator function for CQL driver connection to a cluster
    """
    def __init__(self, sock_path: str,
                 con_gen: Optional[Callable[[List[str], int], CassandraSession]] = None):
        self.sock_path = sock_path
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)
        self.con_gen = con_gen
        self.dirty: bool = False
        self.cluster_nodes: List[str] = []    # List of nodes of current cluster
        self.cluster_cql_port: int = 9042
        self.cluster_wait_timeout: int = 30   # Wait time until cluster is up
        self.retry_sleep: float = .1          # Retry seconds to check if cluster is up
        self.ccluster: Optional[CassandraCluster] = None
        self.cql: Optional[CassandraSession] = None

    async def _request_and_check(self, url: str, msg: str, expect: Optional[str]=None) -> str:
        resp = await self.session.get(url)
        if resp.status >= 500:
            raise HarnessOpException(msg)
        text = await resp.text()
        if expect is not None:
            assert text == expect, f"Request to Harness failed, got {text}, expected {expect}"
        return text

    async def _wait_for_cluster(self) -> None:
        """Waits until cluster is up"""
        until = datetime.now() + timedelta(seconds=self.cluster_wait_timeout)
        while datetime.now() < until:
            await self._request_and_check("http://localhost/cluster/up",
                                          "Harness server error while checking for cluster up")
            await asyncio.sleep(self.retry_sleep)
        raise HarnessTimeoutException("Timeout waiting for cluster to start")

    def _driver_close(self) -> None:
        if self.ccluster is not None:
            self.ccluster.shutdown()
            self.ccluster = None
        self.cql = None

    async def _driver_connect(self) -> None:
        if self.con_gen is not None:
            self.ccluster = self.con_gen(await self.nodes(), await self.cql_port())
            self.cql = self.ccluster.connect()

    # Make driver update endpoints from remote connection
    def _driver_update(self) -> None:
        if self.ccluster is not None:
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def before_test(self, test_name: str) -> None:
        """Before a test starts check if cluster needs cycling and update driver connection"""
        if await self.is_dirty():
            self._driver_close()  # Close driver connection to old cluster
        await self._request_and_check(f"http://localhost/cluster/before_test/{test_name}",
                                      f"Could not get a new cluster for test {test_name}")
        await self.is_dirty()
        if self.cql is None:
            # TODO: if cluster is not up yet due to taking long and HTTP timeout, wait for it
            # await self._wait_for_cluster()
            await self._driver_connect()  # Connect driver to new cluster

    async def after_test(self, test_name: str) -> None:
        """Tell harness this test finished"""
        await self._request_and_check(f"http://localhost/cluster/after_test/{test_name}",
                                      f"Could not tell harness test {test_name} ended")

    async def is_up(self) -> bool:
        """Check if Harness server is up"""
        ret = await self._request_and_check(f"http://localhost/cluster/up",
                                            f"Could not get if harness is up")
        return await ret == "UP"

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        self.dirty = True
        await self._request_and_check("http://localhost/cluster/mark-dirty",
                                      "Could not mark cluster dirty")

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        dirty = await self._request_and_check("http://localhost/cluster/is-dirty",
                                              "Could not check if cluster is dirty")
        self.dirty = (dirty == "True")
        return self.dirty

    async def stop(self) -> None:
        """Stop all active servers immediately (kill)"""
        self._driver_close()
        self.dirty = True
        await self._request_and_check("http://localhost/cluster/stop", "Error stopping cluster")

    async def stop_gracefully(self) -> None:
        """Stop all active servers gracefully"""
        self._driver_close()
        self.dirty = True
        await self._request_and_check("http://localhost/cluster/stop_gracefully",
                                     "Could not stop active servers gracefully")

    async def start(self) -> None:
        """Start the cluster (if not running already)"""
        await self._request_and_check("http://localhost/cluster/start", "Could not start cluster")

    async def cql_port(self) -> int:
        """Get CQL port for cluster servers"""
        resp = await self._request_and_check("http://localhost/cluster/port",
                                             "Error getting CQL port from cluster")
        return int(resp)

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._request_and_check("http://localhost/cluster/replicas",
                                             "Error getting replicas (RF) from cluster")
        return int(resp)

    async def nodes(self) -> List[str]:
        """Get list of running nodes"""
        host_list = await self._request_and_check("http://localhost/cluster/nodes",
                                                  "Error getting list of nodes")
        self.cluster_nodes = host_list.split(',')
        return self.cluster_nodes

    async def node_stop(self, node_id: str) -> bool:
        """Stop specified node"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{node_id}/stop",
                                            f"Error stopping server {node_id}")
        return ret == "OK"

    async def node_start(self, node_id: str) -> bool:
        """Start specified node"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{node_id}/start",
                                            f"Error starting server {node_id}")
        return ret == "OK"

    async def node_restart(self, node_id: str) -> bool:
        """Restart specified node"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{node_id}/restart",
                                            f"Error restarting server {node_id}")
        self._driver_update()
        return ret == "OK"

    async def node_add(self) -> str:
        """Add a new node"""
        self.dirty = True
        ret = await self._request_and_check("http://localhost/cluster/addnode",
                                             "Error adding server")
        self._driver_update()
        return ret

    async def node_remove(self, node_id: str) -> None:
        """Remove a specified node"""
        self.dirty = True
        await self._request_and_check(f"http://localhost/cluster/removenode/{node_id}",
                                     f"Failed to remove node {node_id}")
        self._driver_update()

    async def node_decommission(self, node_id: str) -> None:
        """Decommission a specified node"""
        self.dirty = True
        await self._request_and_check(f"http://localhost/cluster/decommission/{node_id}",
                                     f"Failed to decommission node {node_id}")
        self._driver_update()

    async def node_replace(self, node_id: str) -> None:
        """Replace a specified node with a new one"""
        self.dirty = True
        await self._request_and_check(f"http://localhost/cluster/replace/{node_id}",
                                     f"Failed to replace node {node_id}")
        self._driver_update()

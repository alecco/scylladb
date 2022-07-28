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

from typing import List, Optional, Callable
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore
from cassandra.cluster import Session as CassandraSession                  # type: ignore
from cassandra.cluster import Cluster as CassandraCluster                  # type: ignore


class HarnessOpException(Exception):
    """Operational exception with remote API"""

class HarnessCli():
    """Helper Harness API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Harness server is listening
        con_gen (Callable): generator function for CQL driver connection to a cluster
    """
    conn: aiohttp.UnixConnector
    session: aiohttp.ClientSession
    dirty: bool

    def __init__(self, sock_path: str,
                 con_gen: Optional[Callable[[List[str], int], CassandraSession]] = None) -> None:
        self.con_gen = con_gen
        self.ccluster: Optional[CassandraCluster] = None
        self.cql: Optional[CassandraSession] = None
        # API
        self.sock_path = sock_path

    async def start(self):
        """Setup connection to Harness server"""
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def driver_connect(self) -> None:
        """Connect to cluster"""
        if self.con_gen is not None:
            self.ccluster = self.con_gen(await self.servers(), 9042)
            self.cql = self.ccluster.connect()

    def driver_close(self) -> None:
        """Disconnect from cluster"""
        if self.ccluster is not None:
            self.ccluster.shutdown()
            self.ccluster = None
        self.cql = None

    # Make driver update endpoints from remote connection
    def _driver_update(self) -> None:
        if self.ccluster is not None:
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def before_test(self, test_name: str) -> None:
        """Before a test starts check if cluster needs cycling and update driver connection"""
        dirty = await self.is_dirty()
        if dirty:
            self.driver_close()  # Close driver connection to old cluster
        await self._request_and_check(f"http://localhost/cluster/before_test/{test_name}",
                                      f"Could not get a new cluster for test {test_name}")
        if self.cql is None:
            # TODO: if cluster is not up yet due to taking long and HTTP timeout, wait for it
            # await self._wait_for_cluster()
            await self.driver_connect()  # Connect driver to new cluster

    async def after_test(self, test_name: str) -> None:
        """Tell harness this test finished"""
        await self._request_and_check(f"http://localhost/cluster/after_test/{test_name}",
                                      f"Could not tell harness test {test_name} ended")

    async def _request_and_check(self, url: str, msg: str, expect: Optional[str]=None) -> str:
        resp = await self.session.get(url)
        if resp.status >= 500:
            raise HarnessOpException(msg)
        text = await resp.text()
        if expect is not None:
            assert text == expect, f"Request to Harness failed, got {text}, expected {expect}"
        return text

    async def is_harness_up(self) -> bool:
        """Check if Harness server is up"""
        ret = await self._request_and_check("http://localhost/up",
                                            "Could not get if harness is up")
        return ret == "True"

    async def is_cluster_up(self) -> bool:
        """Check if cluster is up"""
        ret = await self._request_and_check("http://localhost/cluster/up",
                                            "Could not get if cluster is up")
        return ret == "True"

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        dirty = await self._request_and_check("http://localhost/cluster/is-dirty",
                                              "Could not check if cluster is dirty")
        return dirty == "True"

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._request_and_check("http://localhost/cluster/replicas",
                                             "Error getting replicas (RF) from cluster")
        return int(resp)

    async def servers(self) -> List[str]:
        """Get list of running servers"""
        host_list = await self._request_and_check("http://localhost/cluster/servers",
                                                  "Error getting list of servers")
        return host_list.split(',')

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        self.dirty = True
        await self._request_and_check("http://localhost/cluster/mark-dirty",
                                      "Could not mark cluster dirty")

    async def server_stop(self, server_id: str) -> bool:
        """Stop specified node"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{server_id}/stop",
                                            f"Error stopping server {server_id}")
        return ret == "OK"

    async def server_stop_gracefully(self, server_id: str) -> bool:
        """Stop specified node gracefully"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{server_id}/stop_gracefully",
                                            f"Error stopping server {server_id}")
        return ret == "OK"

    async def server_start(self, server_id: str) -> bool:
        """Start specified node"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{server_id}/start",
                                            f"Error starting server {server_id}")
        return ret == "OK"

    async def server_restart(self, server_id: str) -> bool:
        """Restart specified node"""
        self.dirty = True
        ret = await self._request_and_check(f"http://localhost/cluster/node/{server_id}/restart",
                                            f"Error restarting server {server_id}")
        self._driver_update()
        return ret == "OK"

    async def server_add(self) -> str:
        """Add a new node"""
        self.dirty = True
        server_id = await self._request_and_check("http://localhost/cluster/addnode",
                                                "Error adding server")
        self._driver_update()
        return server_id

    async def server_remove(self, server_id: str) -> None:
        """Remove a specified node"""
        self.dirty = True
        await self._request_and_check(f"http://localhost/cluster/removenode/{server_id}",
                                     f"Failed to remove node {server_id}")
        self._driver_update()

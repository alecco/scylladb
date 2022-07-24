#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Manager client.
   Communicates with Manager server via socket.
   Provides helper methods to test cases.
   Manages driver refresh when cluster is cycled.
"""

from typing import List, Optional
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore


class ManagerOpException(Exception):
    """Operational exception with remote API"""

class ManagerClient():
    """Helper Manager API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Manager server is listening
    """
    conn: aiohttp.UnixConnector
    session: aiohttp.ClientSession

    def __init__(self, sock_path: str) -> None:
        self.sock_path = sock_path

    async def start(self):
        """Setup connection to Manager server"""
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def _request(self, url: str, msg: str) -> str:
        resp = await self.session.get(url)
        if resp.status >= 500:
            raise ManagerOpException(msg)
        return await resp.text()

    async def is_manager_up(self) -> bool:
        """Check if Manager server is up"""
        ret = await self._request("http://localhost/up", "Could not get if manager is up")
        return ret == "True"

    async def is_cluster_up(self) -> bool:
        """Check if cluster is up"""
        ret = await self._request("http://localhost/cluster/up", "Could not get if cluster is up")
        return ret == "True"

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        dirty = await self._request("http://localhost/cluster/is-dirty",
                                    "Could not check if cluster is dirty")
        return dirty == "True"

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._request("http://localhost/cluster/replicas",
                                   "Error getting replicas (RF) from cluster")
        return int(resp)

    async def servers(self) -> List[str]:
        """Get list of running servers"""
        host_list = await self._request("http://localhost/cluster/servers",
                                        "Error getting list of servers")
        return host_list.split(',')

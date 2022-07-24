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

from typing import List, Optional
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore


class HarnessOpException(Exception):
    """Operational exception with remote API"""

class HarnessCli():
    """Helper Harness API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Harness server is listening
    """
    def __init__(self, sock_path: str) -> None:
        # API
        self.sock_path = sock_path
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

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

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        await self._request_and_check("http://localhost/cluster/mark-dirty",
                                      "Could not mark cluster dirty")

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        dirty = await self._request_and_check("http://localhost/cluster/is-dirty",
                                              "Could not check if cluster is dirty")
        return dirty == "True"

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
        return host_list.split(',')

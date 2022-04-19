#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Cluster harness client helper"""

from __future__ import annotations                                         # type: ignore
from typing import List, Optional, TYPE_CHECKING                           # type: ignore
import aiohttp.web                                                         # type: ignore
import aiohttp                                                             # type: ignore
if TYPE_CHECKING:
    from cassandra.cluster import ControlConnection                        # type: ignore


class ClusterOpException(Exception):
    pass

class ClusterCli():
    """Helper Harness API client"""
    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def _request_and_check(self, url: str, msg: str):
        resp = await self.session.get(url)
        if resp.status >= 500:
            raise ClusterOpException(msg)
        return resp

    async def stop(self) -> None:
        """Stop all active servers immediately (kill)"""
        await self._request_and_check("http://localhost/cluster/stop", "Error stopping cluster")

    async def stop_gracefully(self) -> None:
        """Stop all active servers gracefully"""
        await self._request_and_check("http://localhost/cluster/stop_gracefully",
                                     "Could not stop active servers gracefully")

    async def start(self) -> None:
        """Start the cluster (if not running already)"""
        await self._request_and_check("http://localhost/cluster/start", "Could not start cluster")

    async def cql_port(self) -> int:
        """Get CQL port for cluster servers"""
        resp = await self._request_and_check("http://localhost/cluster/cql-port",
                                             "Error getting CQL port from cluster")
        return int(await resp.text())

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._request_and_check("http://localhost/cluster/replicas",
                                             "Error getting replicas (RF) from cluster")
        return int(await resp.text())

    async def mark_dirty(self) -> None:
        """Manually mark this cluster dirty.
           To be used when a server was modified outside of this API."""
        await self._request_and_check("http://localhost/cluster/mark-dirty",
                                      "Could not mark cluster dirty")

    async def nodes(self) -> List[str]:
        """Get list of running nodes"""
        resp = await self._request_and_check("http://localhost/cluster/nodes",
                                             "Error getting list of nodes")
        host_list = await resp.text()
        return host_list.split(',')

    async def node_stop(self, node_id: str) -> bool:
        """Stop specified node"""
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/stop")
        ret = await resp.text()
        return ret == "OK"

    async def node_start(self, node_id: str) -> bool:
        """Start specified node"""
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/start")
        ret = await resp.text()
        return ret == "OK"

    async def node_restart(self, node_id: str) -> bool:
        """Restart specified node"""
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/restart")
        ret = await resp.text()
        return ret == "OK"

    async def node_add(self) -> str:
        """Add a new node"""
        resp = await self.session.get("http://localhost/cluster/addnode")
        return await resp.text()

    async def node_remove(self, node_id: str) -> None:
        """Remove a specified node"""
        await self._request_and_check(f"http://localhost/cluster/removenode/{node_id}",
                                     f"Failed to remove node {node_id}")

    async def node_decommission(self, node_id: str) -> None:
        """Decommission a specified node"""
        await self._request_and_check(f"http://localhost/cluster/decommission/{node_id}",
                                     f"Failed to decommission node {node_id}")

    async def node_replace(self, node_id: str) -> None:
        """Replace a specified node with a new one"""
        await self._request_and_check(f"http://localhost/cluster/replace/{node_id}",
                                     f"Failed to replace node {node_id}")

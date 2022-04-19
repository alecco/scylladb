#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations                                         # type: ignore
import aiohttp.web                                                         # type: ignore
import aiohttp                                                             # type: ignore
from typing import List                                                    # type: ignore


class ClusterCli():
    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def stop(self) -> aiohttp.ClientResponse:
        return await self.session.get("http://localhost/cluster/stop")

    async def stop_gracefully(self) -> aiohttp.ClientResponse:
        return await self.session.get("http://localhost/cluster/stop_gracefully")

    async def start(self) -> aiohttp.ClientResponse:
        return await self.session.get("http://localhost/cluster/start")

    async def cql_port(self) -> int:
        resp = await self.session.get("http://localhost/cluster/cql-port")
        return int(await resp.text())

    async def replicas(self) -> int:
        resp = await self.session.get("http://localhost/cluster/replicas")
        return int(await resp.text())

    async def mark_dirty(self) -> aiohttp.ClientResponse:
        return await self.session.get("http://localhost/cluster/mark-dirty")

    async def nodes(self) -> List[str]:
        resp = await self.session.get("http://localhost/cluster/nodes")
        host_list = await resp.text()
        return host_list.split(',')

    async def node_stop(self, node_id: str) -> bool:
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/stop")
        ret = await resp.text()
        return ret == "OK"

    async def node_start(self, node_id: str) -> bool:
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/start")
        ret = await resp.text()
        return ret == "OK"

    async def node_restart(self, node_id: str) -> bool:
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/restart")
        ret = await resp.text()
        return ret == "OK"

    async def node_add(self) -> str:
        resp = await self.session.get("http://localhost/cluster/addnode")
        return await resp.text()

    async def node_remove(self, node_id: str) -> aiohttp.ClientResponse:
        return await self.session.get(f"http://localhost/cluster/removenode/{node_id}")

    async def node_decommission(self, node_id: str) -> bool:
        resp = await self.session.get(f"http://localhost/cluster/decommission/{node_id}")
        ret = await resp.text()
        return ret == "OK"

    async def node_replace(self, node_id: str) -> bool:
        resp = await self.session.get(f"http://localhost/cluster/replace/{node_id}")
        ret = await resp.text()
        return ret == "OK"

#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations
import aiohttp.web


class ClusterCli():
    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def stop(self):
        resp = await self.session.get("http://localhost/cluster/stop")
        return resp

    async def start(self):
        resp = await self.session.get("http://localhost/cluster/start")
        return resp

    async def cql_port(self):
        resp = await self.session.get("http://localhost/cluster/cql-port")
        return int(await resp.text())

    async def replicas(self):
        resp = await self.session.get("http://localhost/cluster/replicas")
        return int(await resp.text())

    async def mark_dirty(self):
        resp = await self.session.get("http://localhost/cluster/mark-dirty")
        return resp

    async def nodes(self):
        resp = await self.session.get("http://localhost/cluster/nodes")
        host_list = await resp.text()
        return host_list.split(',')

    async def node_stop(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/stop")
        ret = await resp.text()
        return ret == "OK"

    async def node_start(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/start")
        ret = await resp.text()
        return ret == "OK"

    async def node_restart(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/node/{node_id}/restart")
        ret = await resp.text()
        return ret == "OK"

    async def node_add(self):
        resp = await self.session.get("http://localhost/cluster/addnode")
        return await resp.text()

    async def node_remove(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/removenode/{node_id}")
        # return await resp.text()

    async def node_decommission(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/decommission/{node_id}")
        ret = await resp.text()
        return ret == "OK"

    async def node_replace(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/replace/{node_id}")
        ret = await resp.text()
        return ret == "OK"

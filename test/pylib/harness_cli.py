#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations
import aiohttp.web

import sys  # XXX debug prints


class HarnessCli():
    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        # print(f"XXX HarnessCli() {sock_path}", file=sys.stderr)  # XXX
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def stop(self):
        # print(f"XXX going to STOP to {self.sock_path}/stop", file=sys.stderr)  # XXX
        resp = await self.session.get("http://localhost/cluster/stop")
        # print(f"XXX STOP resp {resp.status}", file=sys.stderr)  # XXX
        return resp

    async def start(self):
        # print(f"XXX going to START to {self.sock_path}/start", file=sys.stderr)  # XXX
        resp = await self.session.get("http://localhost/cluster/start")
        # print(f"XXX START resp {resp.status}", file=sys.stderr)  # XXX
        return resp

    async def nodes(self):
        print("XXX HarnessCli get nodes", file=sys.stderr)  # XXX
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
        print(f"XXX HarnessCli.node_add() res status {resp.status}", file=sys.stderr)  # XXX
        return await resp.text()

    async def node_remove(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/removenode/{node_id}")
        txt = await resp.text()  # XXX
        print(f"XXX HarnessCli.node_remove(node_id) done {txt}", file=sys.stderr)  # XXX
        return txt  # XXX
        # return await resp.text()

    async def node_decommission(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/decommission/{node_id}")
        ret = await resp.text()
        return ret == "OK"

    async def node_replace(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/replace/{node_id}")
        ret = await resp.text()
        return ret == "OK"

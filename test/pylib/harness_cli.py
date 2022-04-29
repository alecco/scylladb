#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from test import PythonTestSuite                                     # type: ignore
import aiohttp.web
from test.pylib.harness import Harness

#  stop - stop entire cluster
#  start - start entire cluster
#  nodes - list cluster nodes (their server ids
#  node/<id>/stop - stop node
#  node/<id>/start - start node
#  node/<id>/restart - restart node
#  addnode - add a new node and return its id
#  removenode/<id> - remove node by id
#  decommission/<id> - decommission node by id
#  replacenode/<id> - replace node by id, and return new node id.

import sys  # XXX

class HarnessCli():
    def __init__(self, sock_path):
        self.sock_path = sock_path
        #print(f"XXX HarnessCli() {sock_path}", file=sys.stderr)  # XXX
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def stop(self):
        #print(f"XXX going to STOP to {self.sock_path}/stop", file=sys.stderr)  # XXX
        resp = await self.session.get(f"http://localhost/cluster/stop")
        #print(f"XXX STOP resp {resp.status}", file=sys.stderr)  # XXX
        return resp

    async def start(self):
        #print(f"XXX going to START to {self.sock_path}/start", file=sys.stderr)  # XXX
        resp = await self.session.get(f"http://localhost/cluster/start")
        #print(f"XXX START resp {resp.status}", file=sys.stderr)  # XXX
        return resp

    async def nodes(self):
        resp = await self.session.get(f"http://localhost/cluster/nodes")
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
        resp = await self.session.get(f"http://localhost/cluster/addnode")
        print(f"XXX HarnessCli.node_add() res status {resp.status}", file=sys.stderr)  # XXX
        return await resp.text()

    async def node_remove(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/removenode/{node_id}")
        txt = await resp.text() # XXX
        print(f"XXX HarnessCli.node_remove() done {txt}", file=sys.stderr)  # XXX
        return txt # XXX
        #return await resp.text()

    async def node_decommission(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/decommission/{node_id}")
        ret = await resp.text()
        return ret == "OK"

    async def node_replace(self, node_id: str):
        resp = await self.session.get(f"http://localhost/cluster/replace/{node_id}")
        ret = await resp.text()
        return ret == "OK"

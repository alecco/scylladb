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
        print(f"XXX HarnessCli() {sock_path}", file=sys.stderr)  # XXX
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def nodes(self):
        resp = await self.session.get(f"http://localhost/cluster/nodes")
        host_list = await resp.text()
        print(f"XXX host list {host_list.split()}", file=sys.stderr)  # XXX
        return host_list.split(',')

    async def stop(self):
        print(f"XXX going to STOP to {self.sock_path}/stop", file=sys.stderr)  # XXX
        resp = await self.session.get(f"http://localhost/cluster/stop")
        print(f"XXX STOP resp {resp.status}", file=sys.stderr)  # XXX
        return resp

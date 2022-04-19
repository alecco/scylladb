#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations
import asyncio
import aiohttp.web
from random import randint
import uuid
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from test import PythonTestSuite                                     # type: ignore

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


class Harness():
    def __init__(self, suite: PythonTestSuite):
        self.name = str(uuid.uuid1())
        self.suite = suite
        self.servers = {}
        self.app = aiohttp.web.Application()
        self.setup_routes()
        self.runner = aiohttp.web.AppRunner(self.app)
        self.sock_path = f"{suite.options.tmpdir}/harness_sock_{randint(1000000,9999999)}"
        # print(f"RestAPI '{self.name}' going to listen on {self.sock_path}")  # XXX
        self.dirty = False

    def endpoint(self):
        # print(f"XXX Harness endpoint() {next(iter(self.servers.keys()))} <<<<")
        return next(iter(self.servers.keys()))

    def seed(self):
        # print(f"XXX Harness seed() {next(iter(self.servers.keys())) if self.servers else None} <<<<")
        return next(iter(self.servers.keys())) if self.servers else None

    def __getitem__(self, pos: int):
        return next(server for i, server in enumerate(self.servers.values()) if i == pos)

    async def add_server(self):
        server = self.suite.create_server(self.name, self.seed())
        await server.install_and_start()
        self.servers[server.host] = server
        return server.host

    async def setup_and_run(self):  # XXX XXX XXX call this??
        await self.runner.setup()
        site = aiohttp.web.UnixSite(self.runner, path=self.sock_path)  # XXX path
        # print(f"RestAPI listening on {site.name} ...")  # XXX
        await site.start()

    def setup_routes(self):
        self.app.router.add_get('/', self.index)
        self.app.router.add_get('/cluster/nodes', self.cluster_nodes)
        self.app.router.add_get('/cluster/stop', self.cluster_stop)
        self.app.router.add_get('/cluster/start', self.cluster_start)
        self.app.router.add_get('/cluster/node/{id}/stop', self.cluster_node_stop)
        self.app.router.add_get('/cluster/node/{id}/start', self.cluster_node_start)
        self.app.router.add_get('/cluster/node/{id}/restart', self.cluster_node_restart)
        self.app.router.add_get('/cluster/addnode', self.cluster_node_add)
        self.app.router.add_get('/cluster/removenode/{id}', self.cluster_node_remove)
        self.app.router.add_get('/cluster/decommission/{id}', self.cluster_node_decommission)
        self.app.router.add_get('/cluster/replacenode/{id}', self.cluster_node_replace)

    async def index(self, request):
        return aiohttp.web.Response(text="OK")

    async def cluster_nodes(self, request):
        print("XXX RestAPI nodes")  # XXX
        return aiohttp.web.Response(text=f"{','.join(sorted(self.servers.keys()))}")

    async def cluster_stop(self, request):
        await asyncio.gather(*(server.stop() for server in self.servers.values()))
        return aiohttp.web.Response(text="OK")

    async def cluster_start(self, request):
        await asyncio.gather(*(server.start() for server in self.servers.values()))
        return aiohttp.web.Response(text="OK")

    async def cluster_node_stop(self, request):
        node_id = request.match_info['id']
        server = self.servers.get(node_id, None)
        if server is None:
            return aiohttp.web.Response(status=500, text=f"Host {node_id} not found")
        await server.stop()
        return aiohttp.web.Response(text="OK")

    async def cluster_node_start(self, request):
        node_id = request.match_info['id']
        server = self.servers.get(node_id, None)
        if server is None:
            return aiohttp.web.Response(status=500, text=f"Host {node_id} not found")
        await server.start()
        return aiohttp.web.Response(text="OK")

    async def cluster_node_restart(self, request):
        node_id = request.match_info['id']
        server = self.servers.get(node_id, None)
        if server is None:
            return aiohttp.web.Response(status=500, text=f"Host {node_id} not found")
        await server.stop()
        await server.start()
        return aiohttp.web.Response(text="OK")

    async def cluster_node_add(self, request):
        node_id = await self.add_server()
        return aiohttp.web.Response(text=node_id)

    async def cluster_node_remove(self, request):
        node_id = request.match_info['id']
        server = self.servers.get(node_id, None)
        print(f"XXX RestAPI REMOVING {node_id}")  # XXX
        if server is None:
            print(f"RestAPI ERROR REMOVING {node_id}")  # XXX
            return aiohttp.web.Response(status=500, text=f"Host {node_id} not found")
        print(f"XXX RestAPI node_remove stopping {node_id}")  # XXX
        await server.stop()
        print(f"XXX RestAPI node_remove uninstalling {node_id}")  # XXX
        await server.uninstall()
        del self.servers[node_id]
        print(f"XXX RestAPI node_remove done {node_id}")  # XXX
        return aiohttp.web.Response(text="OK")

    async def cluster_node_decommission(self, request):
        # node_id = request.match_info['id']
        return aiohttp.web.Response(status=500, text="Not implemented")

    async def cluster_node_replace(self, request):
        old_node_id = request.match_info['id']
        await self.cluster_node_stop(old_node_id)
        del self.servers[old_node_id]
        new_node_id = await self.add_server()
        return aiohttp.web.Response(text=new_node_id)

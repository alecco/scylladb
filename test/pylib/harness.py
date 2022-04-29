#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations
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
        self.hosts = {}
        self.app = aiohttp.web.Application()
        self.setup_routes()
        self.runner = aiohttp.web.AppRunner(self.app)
        self.sock_path = f"{suite.options.tmpdir}/harness_sock_{randint(1000000,9999999)}"
        #print(f"RestAPI '{self.name}' going to listen on {self.sock_path}")  # XXX
        self.dirty = False

    def endpoint(self):
        #print(f"XXX Harness endpoint() {next(iter(self.hosts.keys()))} <<<<")
        return next(iter(self.hosts.keys()))

    def seed(self):
        #print(f"XXX Harness seed() {next(iter(self.hosts.keys())) if self.hosts else None} <<<<")
        return next(iter(self.hosts.keys())) if self.hosts else None

    async def add_server(self):
        server = self.suite.create_server(self.name, self.seed())
        await server.install_and_start()
        self.hosts[server.host] = server

    async def setup_and_run(self):  # XXX XXX XXX call this??
        await self.runner.setup()
        site = aiohttp.web.UnixSite(self.runner, path=self.sock_path)  # XXX path
        #print(f"RestAPI listening on {site.name} ...")  # XXX
        await site.start()

    def setup_routes(self):
        self.app.router.add_get('/', self.index)
        self.app.router.add_get('/cluster/nodes', self.cluster_nodes)
        self.app.router.add_get('/cluster/stop', self.cluster_stop)
        self.app.router.add_get('/cluster/start', self.cluster_start)
        self.app.router.add_get('/cluster/node/{id}/stop', self.cluster_node_stop)
        self.app.router.add_get('/cluster/node/{id}/start', self.cluster_node_start)
        self.app.router.add_get('/cluster/node/{id}/restart', self.cluster_node_restart)
        self.app.router.add_get('/cluster/addnode', self.cluster_addnode)
        self.app.router.add_get('/cluster/removenode/{id}', self.cluster_removenode)
        self.app.router.add_get('/cluster/decommission/{id}', self.cluster_decommission)
        self.app.router.add_get('/cluster/replacenode/{id}', self.cluster_replacenode)

    async def index(self, request):
        return aiohttp.web.Response(text="OK")

    async def cluster_nodes(self, request):
        return aiohttp.web.Response(text=f"{','.join(self.hosts.keys())}")

    async def cluster_stop(self, request):
        return aiohttp.web.Response(text="OK")

    async def cluster_start(self, request):
        return aiohttp.web.Response(text="OK")

    async def cluster_node_stop(self, request):
        node_id = request.match_info['id']
        return aiohttp.web.Response(text="OK")

    async def cluster_node_start(self, request):
        node_id = request.match_info['id']
        return aiohttp.web.Response(text="OK")

    async def cluster_node_restart(self, request):
        node_id = request.match_info['id']
        return aiohttp.web.Response(text="OK")

    async def cluster_addnode(self, request):
        return aiohttp.web.Response(text="7")

    async def cluster_removenode(self, request):
        node_id = request.match_info['id']
        return aiohttp.web.Response(text="OK")

    async def cluster_decommission(self, request):
        node_id = request.match_info['id']
        return aiohttp.web.Response(text="OK")

    async def cluster_replacenode(self, request):
        node_id = request.match_info['id']
        return aiohttp.web.Response(text="OK")

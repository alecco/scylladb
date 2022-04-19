#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# XXX
#import asyncio
#import logging
#import os
#import pathlib
#import re
#import shutil
#import time
#import aiohttp
#from typing import Optional, Any
#from cassandra.auth import PlainTextAuthProvider        # type: ignore
#from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore


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

import asyncio
from aiohttp import web

class RestAPI():
    def __init__(self):
        loop = asyncio.get_event_loop()
        # XXX add stuff to the loop
        # ...
        # set up aiohttp - like run_app, but non-blocking
        #runner = aiohttp.web.AppRunner(app)
        #loop.run_until_complete(runner.setup())
        #site = aiohttp.web.TCPSite(runner)
        #loop.run_until_complete(site.start())

        #self.app = web.Application()
        #self.setup_routes(app)
        # XXX web.run_app(app)

    async def index(request):
        return web.Response(text='Hello (single) Aiohttp!')
    
    def setup_routes(app):
        # app.router.add_get('/', index)
        app.router.add_get('/cluster/stop', cluster_stop)  # - stop entire cluster
        app.router.add_get('/cluster/start', cluster_start)  # - start entire cluster
        app.router.add_get('/cluster/nodes', cluster_nodes)  # - list cluster nodes (their server ids
        app.router.add_get('/cluster/node/{id}/stop', cluster_node_stop)  # - stop node
        app.router.add_get('/cluster/node/{id}/start', cluster_node_start)  # - start node
        app.router.add_get('/cluster/node/{id}/restart', cluster_node_restart)  # - restart node
        app.router.add_get('/cluster/addnode', cluster_addnode)  # - add a new node and return its id
        app.router.add_get('/cluster/removenode/{id}', cluster_removenode)  # - remove node by id
        app.router.add_get('/cluster/decommission/{id}', cluster_decommission)  # - decommission node by id
        app.router.add_get('/cluster/replacenode/{id}', cluster_replacenode)  # - replace node by id, and return new node id.
    

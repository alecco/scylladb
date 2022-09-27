#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
from abc import ABCMeta, abstractmethod
import asyncio
import logging
import os.path
from time import time
from typing import Optional
import aiohttp


logger = logging.getLogger(__name__)


class RESTClientBase(metaclass=ABCMeta):
    """Base class for REST API operations"""

    def __init__(self, connector: aiohttp.BaseConnector = None):
        self.session: aiohttp.ClientSession = aiohttp.ClientSession(connector = connector)

    async def close(self) -> None:
        """End session"""
        await self.session.close()

    @abstractmethod
    def _resource_uri(self, resource: str, host: Optional[str] = None) -> str:
        return ""

    async def get(self, resource: str, host: Optional[str] = None) -> aiohttp.ClientResponse:
        """Fetch remote resource or raise"""
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        resp = await self.session.get(self._resource_uri(resource, host))
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"status code: {resp.status}, body text: {text}")
        return resp

    async def get_text(self, resource: str, host: Optional[str] = None) -> str:
        """Fetch remote resource text response or raise"""
        resp = await self.get(resource, host)
        return await resp.text()

    async def post(self, resource: str, host: str, params: Optional[dict[str, str]] = None) \
            -> aiohttp.ClientResponse:
        """Post to remote resource or raise"""
        resp = await self.session.post(self._resource_uri(resource, host), params=params)
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"status code: {resp.status}, body text: {text}, "
                               f"resource {resource} params {params}")
        return resp

    async def put_json(self, resource: str, json: dict, host: Optional[str] = None) \
            -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.request(method="PUT", url=self._resource_uri(resource, host),
                                          json=json)


class UnixRESTClient(RESTClientBase):
    """An async helper for REST API operations using AF_UNIX socket"""

    def __init__(self, sock_path: str):
        self.sock_name: str = os.path.basename(sock_path)
        super().__init__(aiohttp.UnixConnector(path=sock_path))

    def _resource_uri(self, resource: str, host: Optional[str] = None) -> str:
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        #       host parameter is ignored
        return f"http+unix://{self.sock_name}{resource}"


class TCPRESTClient(RESTClientBase):
    """An async helper for REST API operations"""

    def __init__(self, port: int):
        self.port: int = port
        super().__init__()

    def _resource_uri(self, resource: str, host: Optional[str] = None) -> str:
        return f"http://{host}:{self.port}{resource}"


class ScyllaRESTAPIClient():
    """Async Scylla REST API client"""

    def __init__(self, port: int = 10000):
        self.cli = TCPRESTClient(port)

    async def close(self):
        """Close session"""
        await self.cli.close()

    async def get_host_id(self, server_id: str):
        """Get server id (UUID)"""
        host_uuid = await self.cli.get_text("/storage_service/hostid/local", host=server_id)
        host_uuid = host_uuid.lstrip('"').rstrip('"')
        return host_uuid

    async def remove_node(self, initiator_ip: str, server_uuid: str) -> None:
        """Initiate remove node of server_uuid in initiator initiator_ip"""
        resp = await self.cli.post("/storage_service/remove_node", params={"host_id": server_uuid},
                                   host=initiator_ip)
        logger.info("remove_node status %s for %s", resp.status, server_uuid)

    async def decommission_node(self, node_ip: str) -> None:
        """Initiate remove node of server_uuid in initiator initiator_ip"""
        resp = await self.cli.post("/storage_service/decommission", host=node_ip)
        logger.debug("decommission_node status %s for %s", resp.status, node_ip)

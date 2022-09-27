#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
from abc import ABCMeta, abstractmethod
import os.path
from typing import Optional
import aiohttp


class RESTClientBase(metaclass=ABCMeta):
    """Base class for REST API operations"""

    def __init__(self, connector: aiohttp.BaseConnector = None):
        self.session: aiohttp.ClientSession = aiohttp.ClientSession(connector = connector)

    async def stop(self) -> None:
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

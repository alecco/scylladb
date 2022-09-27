#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
import os.path
import aiohttp


class UnixRESTClient():
    """An async helper for REST API operations using AF_UNIX socket"""
    session: aiohttp.ClientSession

    def __init__(self, sock_path: str):
        self.sock_name = os.path.basename(sock_path)
        self.session = aiohttp.ClientSession(connector = aiohttp.UnixConnector(path=sock_path))

    async def stop(self) -> None:
        """End session"""
        await self.session.close()

    def _resource_uri(self, resource: str) -> str:
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        return f"http+unix://{self.sock_name}{resource}"

    async def get(self, resource: str) -> aiohttp.ClientResponse:
        """Fetch remote resource or raise"""
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        resp = await self.session.get(self._resource_uri(resource))
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f'status code: {resp.status}, body text: {text}')
        return resp

    async def get_text(self, resource: str) -> str:
        """Fetch remote resource text response or raise"""
        resp = await self.get(resource)
        return await resp.text()

    async def put_json(self, resource: str, json: dict) -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.request(method='PUT', url=self._resource_uri(resource), json=json)


class TCPRESTClient():
    """An async helper for REST API operations"""
    session: aiohttp.ClientSession

    def __init__(self, port: int):
        self.session = aiohttp.ClientSession()
        self.port = port

    async def stop(self) -> None:
        """End session"""
        await self.session.close()

    def _resource_uri(self, resource: str, host: str) -> str:
        return f"http://{host}:{self.port}{resource}"

    async def get(self, resource: str, host: str) -> aiohttp.ClientResponse:
        """Fetch remote resource or raise"""
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        resp = await self.session.get(self._resource_uri(resource, host))
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f'status code: {resp.status}, body text: {text}')
        return resp

    async def get_text(self, resource: str, host: str) -> str:
        """Fetch remote resource text response or raise"""
        resp = await self.get(resource, host)
        return await resp.text()

    async def put_json(self, resource: str, json: dict, host: str) \
            -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.request(method='PUT', url=self._resource_uri(resource, host),
                                          json=json)

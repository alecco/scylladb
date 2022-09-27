#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
import asyncio
import logging
import os.path
from time import time
from typing import Dict, Optional
import aiohttp


logger = logging.getLogger(__name__)


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
            raise Exception(f"status code: {resp.status}, body text: {text}")
        return resp

    async def get_text(self, resource: str) -> str:
        """Fetch remote resource text response or raise"""
        resp = await self.get(resource)
        return await resp.text()

    async def put_json(self, resource: str, json: dict) -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.request(method="PUT", url=self._resource_uri(resource), json=json)


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
            raise Exception(f"status code: {resp.status}, body text: {text}")
        return resp

    async def get_text(self, resource: str, host: str) -> str:
        """Fetch remote resource text response or raise"""
        resp = await self.get(resource, host)
        return await resp.text()

    async def put_json(self, resource: str, json: dict, host: str) \
            -> aiohttp.ClientResponse:
        """Put JSON"""
        return await self.session.request(method="PUT", url=self._resource_uri(resource, host),
                                          json=json)

    async def post(self, resource: str, params: Dict[str, str], host: str) \
            -> aiohttp.ClientResponse:
        """Post to remote resource or raise"""
        resp = await self.session.post(self._resource_uri(resource, host), params=params)
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f"status code: {resp.status}, body text: {text}")
        return resp


class ScyllaRESTAPIClient():
    """Async Scylla REST API client"""

    def __init__(self, port: int = 10000):
        self.cli = TCPRESTClient(port)

    async def stop(self):
        """Close session"""
        await self.cli.stop()

    async def get_server_uuid(self, server_id: str):
        """Get server id (UUID)"""
        host_uuid = await self.cli.get_text("/storage_service/hostid/local", host=server_id)
        host_uuid = host_uuid.lstrip('"').rstrip('"')
        return host_uuid

    # TODO: change to UUID
    async def wait_for_host_known(self, dst_server_id: str, expect_server_id: str,
                                  timeout: float = 30.0, sleep: float = 0.05) -> bool:
        """Checks until dst_server_id knows about expect_server_id, with timeout"""
        max_time = time() + timeout
        while True:
            resp_body = await self.cli.get_text("/storage_service/host_id", dst_server_id)
            if expect_server_id in resp_body:
                logger.info("wait_for_host_known [{%s}] found, SUCCESS", expect_server_id)
                return True
            if time() > max_time:
                logger.info("wait_for_host_known [{%s}] timeout", expect_server_id)
                return False
            logger.debug("wait_for_host_known [{%s}] not found, sleeping {sleep}", expect_server_id)
            await asyncio.sleep(sleep)

    async def remove_node(self, initiator_ip: str, server_uuid: str) -> bool:
        """Initiate remove node of server_uuid in initiator initiator_ip"""
        resp = await self.cli.post("/storage_service/remove_node", params={"host_id": server_uuid},
                                   host=initiator_ip)
        logger.debug("remove_node status %s for %s", resp.status, server_uuid)
        return resp.status == 200

    async def wait_for_remove(self, initiator_ip: str, server_uuid, timeout: int = 30) -> bool:
        """Wait until server remove finishes"""
        max_time = time() + timeout
        while True:
            resp_body = await self.cli.get_text("/storage_service/removal_status", host=initiator_ip)
            if not server_uuid in resp_body:
                logger.info("wait_for_remove SUCCESS %s %s", server_uuid, resp_body)
                return True
            if time() > max_time:
                logger.error("wait_for_remove timeout reached %s", server_uuid)
                return False
            logger.info("wait_for_remove WAIT %s", server_uuid)
            await asyncio.sleep(0.05)

    async def remove_node_and_wait(self, initiator_ip: str, server_uuid: str, timeout: int = 30) \
            -> bool:
        """Initiate remove node of server_id in initiator_ip and wait to finish"""
        if not await self.remove_node(initiator_ip, server_uuid):
            logger.error("remove_node_and_wait failed to remove for %s", server_uuid)
            return False
        if not await self.wait_for_remove(initiator_ip, server_uuid, timeout):
            logger.error("remove_node_and_wait timed out for %s", server_uuid)
            return False
        logger.info("remove_node_and_wait SUCCESS for %s", server_uuid)
        return True


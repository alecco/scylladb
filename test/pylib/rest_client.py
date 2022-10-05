#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Asynchronous helper for Scylla REST API operations.
"""
from __future__ import annotations                           # Type hints as strings
from abc import ABCMeta
from collections.abc import Mapping
import logging
import os.path
from typing import Any, Optional, Union, TYPE_CHECKING, cast
from contextlib import asynccontextmanager
from aiohttp import request, BaseConnector, UnixConnector
import pytest
if TYPE_CHECKING:
    from test.pylib.manager_client import HostID, IPAddress


logger = logging.getLogger(__name__)

# TODO: support ssl and verify_ssl
class RESTClient(metaclass=ABCMeta):
    """Base class for sesion-free REST client"""
    connector: Optional[BaseConnector]
    uri_scheme: str   # e.g. http, http+unix
    default_host: str
    default_port: Optional[int]
    # pylint: disable=too-many-arguments

    async def _fetch(self, method: str, resource: str, response_type: Optional[str] = None,
                     host: Optional[str] = None, port: Optional[int] = None,
                     params: Optional[Mapping[str, str]] = None,
                     json: Optional[Mapping] = None) -> Any:
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        assert method in ["GET", "POST", "PUT", "DELETE"], f"Invalid HTTP request method {method}"
        assert response_type is None or response_type in ["text", "json"], \
                f"Invalid response type requested {response_type} (expected 'text' or 'json')"
        # Build the URI
        port = port if port else self.default_port if hasattr(self, "default_port") else None
        port_str = f":{port}" if port else ""
        assert host is not None or hasattr(self, "default_host"), "_fetch: missing host for " \
                "{method} {resource}"
        host_str = host if host is not None else self.default_host
        uri = self.uri_scheme + "://" + host_str + port_str + resource
        logging.debug(f"RESTClient fetching {method} {uri}")

        async with request(method, uri,
                           connector = self.connector if hasattr(self, "connector") else None,
                           params = params, json = json) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"status code: {resp.status}, body text: {text}, "
                                   f"resource {uri} params {params} json {json}")
            if response_type is not None:
                # Return response.text() or response.json()
                return await getattr(resp, response_type)()
        return None  # Unreached

    async def get(self, resource_uri: str, host: Optional[str] = None, port: Optional[int] = None,
                  params: Optional[Mapping[str, str]] = None) -> None:
        await self._fetch("GET", resource_uri, host = host, port = port, params = params)

    async def get_text(self, resource_uri: str, host: Optional[str] = None,
                       port: Optional[int] = None, params: Optional[Mapping[str, str]] = None
                       ) -> str:
        ret = await self._fetch("GET", resource_uri, response_type = "text", host = host,
                                port = port, params = params)
        assert isinstance(ret, str), f"get_text: expected str but got {type(ret)} {ret}"
        return ret

    async def get_json(self, resource_uri: str, host: Optional[str] = None,
                       port: Optional[int] = None, params: Optional[Mapping[str, str]] = None
                       ) -> Any:
        """Fetch URL and get JSON. Caller must check JSON content types."""
        ret = await self._fetch("GET", resource_uri, response_type = "json", host = host,
                                port = port, params = params)
        return ret

    async def post(self, resource_uri: str, host: Optional[str] = None,
                   port: Optional[int] = None, params: Optional[Mapping[str, str]] = None,
                   json: Mapping = None) -> None:
        await self._fetch("POST", resource_uri, host = host, port = port, params = params,
                          json = json)

    async def put_json(self, resource_uri: str, data: Mapping, host: Optional[str] = None,
                       port: Optional[int] = None, params: Optional[dict[str, str]] = None) -> None:
        await self._fetch("PUT", resource_uri, host = host, port = port, params = params,
                          json = data)

    async def delete(self, resource_uri: str, host: Optional[str] = None,
                     port: Optional[int] = None, params: Optional[dict[str, str]] = None,
                     json: Mapping = None) -> None:
        await self._fetch("DELETE", resource_uri, host = host, port = port, params = params,
                          json = json)


class UnixRESTClient(RESTClient):
    """An async helper for REST API operations using AF_UNIX socket"""

    def __init__(self, sock_path: str):
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        #       host parameter is ignored but set to socket name as convention
        self.uri_scheme: str = "http+unix"
        self.default_host: str = f"{os.path.basename(sock_path)}"
        self.connector = UnixConnector(path=sock_path)


class TCPRESTClient(RESTClient):
    """An async helper for REST API operations"""

    def __init__(self, port: int):
        self.uri_scheme = "http"
        self.connector = None
        self.default_port: int = port


class ScyllaRESTAPIClient():
    """Async Scylla REST API client"""

    def __init__(self, port: int = 10000):
        self.client = TCPRESTClient(port)

    async def get_host_id(self, server_id: IPAddress) -> HostID:
        """Get server id (UUID)"""
        host_uuid = await self.client.get_text("/storage_service/hostid/local", host=server_id)
        assert isinstance(host_uuid, str) and len(host_uuid) > 10, \
                f"get_host_id: invalid {host_uuid}"
        host_uuid = host_uuid.lstrip('"').rstrip('"')
        return cast("HostID", host_uuid)

    async def get_host_id_map(self, dst_server_id: str) -> list:
        """Retrieve the mapping of endpoint to host ID"""
        response = await self.client.get("/storage_service/host_id", dst_server_id)
        result = await response.json()
        assert(type(result) == list)
        return result

    async def get_down_endpoints(self, dst_server_id: str) -> list:
        """Retrieve down endpoints from gossiper's point of view """
        response = await self.client.get("/gossiper/endpoint/down/", dst_server_id)
        result = await response.json()
        assert(type(result) == list)
        return result

    async def remove_node(self, initiator_ip: IPAddress, host_id: HostID,
                          ignore_dead: list[IPAddress]) -> None:
        """Initiate remove node of host_id in initiator initiator_ip"""
        logger.info("remove_node for %s on %s", host_id, initiator_ip)
        await self.client.post("/storage_service/remove_node",
                               params = {"host_id": host_id,
                                         "ignore_nodes": ",".join(ignore_dead)},
                               host = initiator_ip)
        logger.debug("remove_node for %s finished", host_id)

    async def decommission_node(self, host_ip: str) -> None:
        """Initiate decommission node of host_ip"""
        logger.debug("decommission_node %s", host_ip)
        await self.client.post("/storage_service/decommission", host = host_ip)
        logger.debug("decommission_node %s finished", host_ip)

    async def get_gossip_generation_number(self, node_ip: str, target_ip: str) -> int:
        """Get the current generation number of `target_ip` observed by `node_ip`."""
        data = await self.client.get_json(f"/gossiper/generation_number/{target_ip}",
                                          host = node_ip)
        assert(type(data) == int)
        return data

    async def enable_injection(self, node_ip: str, injection: str, one_shot: bool) -> None:
        """Enable error injection named `injection` on `node_ip`. Depending on `one_shot`,
           the injection will be executed only once or every time the process passes the injection point.
           Note: this only has an effect in specific build modes: debug,dev,sanitize.
        """
        await self.client.post(f"/v2/error_injection/injection/{injection}",
                               host=node_ip, params={"one_shot": str(one_shot)})

    async def disable_injection(self, node_ip: str, injection: str) -> None:
        await self.client.delete(f"/v2/error_injection/injection/{injection}", host=node_ip)

    async def get_enabled_injections(self, node_ip: str) -> list[str]:
        data = await self.client.get_json("/v2/error_injection/injection", host=node_ip)
        assert(type(data) == list)
        assert(type(e) == str for e in data)
        return data


@asynccontextmanager
async def inject_error(api: ScyllaRESTAPIClient, node_ip: IPAddress, injection: str,
                       one_shot: bool):
    """Attempts to inject an error. Works only in specific build modes: debug,dev,sanitize.
       It will trigger a test to be skipped if attempting to enable an injection has no effect.
    """
    await api.enable_injection(node_ip, injection, one_shot)
    enabled = await api.get_enabled_injections(node_ip)
    logging.info(f"Error injections enabled on {node_ip}: {enabled}")
    if not enabled:
        pytest.skip("Error injection not enabled in Scylla - try compiling in dev/debug/sanitize mode")
    try:
        yield
    finally:
        logger.info(f"Disabling error injection {injection}")
        await api.disable_injection(node_ip, injection)

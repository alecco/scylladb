#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import asyncio
import pathlib
import sys
from typing import AsyncGenerator
from test.pylib.random_tables import RandomTables                        # type: ignore
import pytest
from cassandra.cluster import Session, ResponseFuture                    # type: ignore

# Add test.pylib to the search path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from test.pylib.util import unique_name                                  # type: ignore


# Change default pytest-asyncio event_loop fixture scope to session to
# allow async fixtures with scope larger than function. (e.g. keyspace fixture)
# See https://github.com/pytest-dev/pytest-asyncio/issues/68
@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def _wrap_future(f: ResponseFuture) -> asyncio.Future:
    """Wrap a cassandra Future into an asyncio.Future object.

    Args:
        f: future to wrap

    Returns:
        And asyncio.Future object which can be awaited.
    """
    loop = asyncio.get_event_loop()
    aio_future = loop.create_future()

    def on_result(result):
        loop.call_soon_threadsafe(aio_future.set_result, result)

    def on_error(exception, *_):
        loop.call_soon_threadsafe(aio_future.set_exception, exception)

    f.add_callback(on_result)
    f.add_errback(on_error)
    return aio_future


def run_async(self, *args, **kwargs) -> asyncio.Future:
    return _wrap_future(self.execute_async(*args, **kwargs))


Session.run_async = run_async


# While the raft-based schema modifications are still experimental and only
# optionally enabled some tests are expected to fail on Scylla without this
# option enabled, and pass with it enabled (and also pass on Cassandra).
# These tests should use the "fails_without_raft" fixture. When Raft mode
# becomes the default, this fixture can be removed.
@pytest.fixture(scope="session")
def check_pre_raft(cql):
    # If not running on Scylla, return false.
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        return False
    # In Scylla, we check Raft mode by inspecting the configuration via CQL.
    experimental_features = list(cql.execute("SELECT value FROM system.config WHERE name = 'experimental_features'"))[0].value
    return not '"raft"' in experimental_features


@pytest.fixture(scope="function")
def fails_without_raft(request, check_pre_raft):
    if check_pre_raft:
        request.node.add_marker(pytest.mark.xfail(reason='Test expected to fail without Raft experimental feature on'))


# "random_tables" fixture: Creates and returns a temporary RandomTables object
# used in tests to make schema changes. Tables are dropped after finished.
@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def random_tables(request, cql, keyspace) -> AsyncGenerator:
    tables = RandomTables(request.node.name, cql, keyspace)
    yield tables
    await tables.drop_all_tables()

# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
# and automatically deleted at the end. We use scope="session" so that all
# tests will reuse the same keyspace.
@pytest.fixture(scope="session")
def keyspace(cql):
    name = unique_name()
    cql.execute(f"CREATE KEYSPACE {name} WITH REPLICATION = "
                "{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
    yield name
    cql.execute(f"DROP KEYSPACE {name}")

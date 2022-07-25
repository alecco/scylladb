# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Common pytest fixtures for all subdirectories which specifically
# request these by adding "addopts = --confcutdir .." in their
# pytest.ini or lack pytest.ini altogether.
#
# Please avoid adding bloat (you can also import
# fixtures directly from test/pylib).
#
#
import asyncio
from cassandra.cluster import Cluster, ConsistencyLevel                  # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore
from cassandra.cluster import Session, ResponseFuture                    # type: ignore
from cassandra.policies import RoundRobinPolicy                          # type: ignore
from test.pylib.util import unique_name                                  # type: ignore
import pytest
from typing import List, AsyncGenerator
from test.pylib.manager_cli import ManagerCli
from test.pylib.random_tables import RandomTables                        # type: ignore


# By default, tests run against a CQL server (Scylla or Cassandra) listening
# on localhost:9042. Add the --host and --port options to allow overiding
# these defaults.
def pytest_addoption(parser):
    parser.addoption('--manager-api', action='store', required=True,
                     help='Manager unix socket path')


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


def cluster_con(hosts: List[str], port: int):
    """Create a CQL Cluster connection object according to configuration.
       It does not .connect() yet."""
    assert len(hosts) > 0, "python driver connection needs at least one host to connect to"
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeout (in seconds) for execute() commands is 10, which
        # should have been more than enough, but in some extreme cases with a
        # very slow debug build running on a very busy machine and a very slow
        # request (e.g., a DROP KEYSPACE needing to drop multiple tables)
        # 10 seconds may not be enough, so let's increase it. See issue #7838.
        request_timeout=120)
    return Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                   contact_points=hosts,
                   port=port,
                   # TODO: make the protocol version an option, to allow testing with
                   # different versions. If we drop this setting completely, it will
                   # mean pick the latest version supported by the client and the server.
                   protocol_version=4,
                   )


# Change default pytest-asyncio event_loop fixture scope to session to
# allow async fixtures with scope larger than function. (e.g. manager fixture)
# See https://github.com/pytest-dev/pytest-asyncio/issues/68
@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
@pytest.fixture(scope="session")
async def manager(event_loop, request):
    """Session fixture to set up client object for communicating with the Cluster API.
       Pass the Unix socket path where the Manager server API is listening.
       Pass a function to create driver connections.
       Test cases (functions) should not use this fixture.
    """
    manager_int = ManagerCli(request.config.getoption('manager_api'), cluster_con)
    await manager_int.start()
    await manager_int.driver_connect()
    yield manager_int
    manager_int.driver_close()   # Close after last test case

# "cql" fixture: set up client object for communicating with the CQL API.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def cql(manager):
    yield manager.cql


# A function-scoped autouse=True fixture allows us to test after every test
# that the CQL connection is still alive - and if not report the test which
# crashed Scylla and stop running any more tests.
@pytest.fixture(scope="function", autouse=True)
def cql_test_connection(cql, request):
    yield
    try:
        # We want to run a do-nothing CQL command. "use system" is the
        # closest to do-nothing I could find...
        cql.execute("use system")
    except:     # noqa: E722
        pytest.exit(f"Scylla appears to have crashed in test {request.node.parent.name}::{request.node.name}")


# While the raft-based schema modifications are still experimental and only
# optionally enabled some tests are expected to fail on Scylla without this
# option enabled, and pass with it enabled (and also pass on Cassandra).
# These tests should use the "fails_without_raft" fixture. When Raft mode
# becomes the default, this fixture can be removed.
@pytest.fixture(scope="session")
async def check_pre_raft(cql):
    # If not running on Scylla, return false.
    names = [row.table_name for row in await cql.run_async("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        return False
    # In Scylla, we check Raft mode by inspecting the configuration via CQL.
    experimental_features = list(await cql.run_async("SELECT value FROM system.config WHERE name = 'experimental_features'"))[0].value
    return not '"raft"' in experimental_features


@pytest.fixture(scope="function")
async def fails_without_raft(request, check_pre_raft):
    if check_pre_raft:
        request.node.add_marker(pytest.mark.xfail(reason='Test expected to fail without Raft experimental feature on'))


# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
# and automatically deleted at the end.
@pytest.fixture(scope="function")
async def keyspace(cql):
    name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {name} WITH REPLICATION = "
                        "{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
    yield name
    await cql.run_async(f"DROP KEYSPACE {name}")


# "random_tables" fixture: Creates and returns a temporary RandomTables object
# used in tests to make schema changes. A keyspace is created for it and it is
# dropped once the fixture is done.
@pytest.fixture(scope="function")
def random_tables(request, cql):
    tables = RandomTables(request.node.name, cql, unique_name())
    yield tables
    tables.drop_all()

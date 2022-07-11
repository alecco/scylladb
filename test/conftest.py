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
from functools import partial
from cassandra.auth import PlainTextAuthProvider                         # type: ignore
from cassandra.cluster import Cluster, ConsistencyLevel                  # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore
from cassandra.cluster import Session, ResponseFuture                    # type: ignore
from cassandra.policies import RoundRobinPolicy                          # type: ignore
from test.pylib.util import unique_name                                  # type: ignore
import pytest
import ssl
from typing import List, AsyncGenerator
from test.pylib.random_tables import RandomTables                        # type: ignore
from test.pylib.harness_cli import HarnessCli
from sys import stderr # XXX



# By default, tests run against a CQL server (Scylla or Cassandra) listening
# on localhost:9042. Add the --host and --port options to allow overiding
# these defaults.
def pytest_addoption(parser):
    parser.addoption('--api', action='store', required=True,
                     help='Harness unix socket path')
    parser.addoption('--ssl', action='store_true',
                     help='Connect to CQL via an encrypted TLSv1.2 connection')


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


def cluster_con(hosts: List[str], port: int, use_ssl: bool):
    """Create a CQL Cluster connection object according to configuration.
       It does not .connect() yet."""
    assert len(hosts) > 1, "python driver connection needs at least one host to connect to"
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
    if use_ssl:
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None
    return Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                   contact_points=hosts,
                   port=port,
                   # TODO: make the protocol version an option, to allow testing with
                   # different versions. If we drop this setting completely, it will
                   # mean pick the latest version supported by the client and the server.
                   protocol_version=4,
                   # Use the default superuser credentials, which work for both Scylla and Cassandra
                   auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
                   ssl_context=ssl_context,
                   )


@pytest.fixture(scope="session")
async def harness_internal(request):
    """Session fixture to set up client object for communicating with the Cluster API.
       Pass the Unix socket path where the Harness server API is listening.
       Pass a function to create driver connections.
       Test cases (functions) should not use this fixture.
    """
    sock_path = request.config.getoption('api')
    cluster_con_ssl = partial(cluster_con, use_ssl=request.config.getoption('ssl'))
    cli = HarnessCli(sock_path, cluster_con_ssl)
    yield cli
    print(f"XXX {request.node.name} fixture harness INTERNAL DONE", file=stderr)  # XXX


@pytest.fixture(scope="function")
async def harness(request, harness_internal):
    """Per test fixture to notify Harness client object when tests begin so it can
    perform checks for cluster state.
    """
    await harness_internal.before_test(request.node.name)
    yield harness_internal
    print(f"XXX {request.node.name} fixture harness after test", file=stderr)  # XXX
    await harness_internal.after_test(request.node.name)
    print(f"XXX {request.node.name} fixture harness after test DONE", file=stderr)  # XXX


@pytest.fixture(scope="function")
async def cql(request, harness):
    yield harness.cql

# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will have
# a "this_dc" fixture to figure out the name of the current DC, so it can be
# used in NetworkTopologyStrategy.
@pytest.fixture()
def this_dc(harness, cql):
    yield cql.execute("SELECT data_center FROM system.local").one()[0]

# While the raft-based schema modifications are still experimental and only
# optionally enabled some tests are expected to fail on Scylla without this
# option enabled, and pass with it enabled (and also pass on Cassandra).
# These tests should use the "fails_without_raft" fixture. When Raft mode
# becomes the default, this fixture can be removed.
@pytest.fixture()
async def check_pre_raft(harness, cql):
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
# and automatically deleted at the end. We use scope="session" so that all
# tests will reuse the same keyspace.
@pytest.fixture(scope="function")
async def keyspace(harness, cql,
        request,  # XXX
        this_dc):
    name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {name} WITH REPLICATION = "
                        f"{{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    yield name
    print(f"XXX {request.node.name} fixture keyspace dropping", file=stderr)  # XXX
    await cql.run_async("DROP KEYSPACE " + name)


# "random_tables" fixture: Creates and returns a temporary RandomTables object
# used in tests to make schema changes. Tables are dropped after finished.
@pytest.fixture(scope="function")
async def random_tables(request, harness, cql, keyspace) -> AsyncGenerator:
    tables = RandomTables(request.node.name, cql, keyspace)
    yield tables
    print(f"XXX {request.node.name} fixture random_tables dropping all tables", file=stderr)  # XXX
    await tables.drop_all_tables()
    print(f"XXX {request.node.name} fixture random_tables dropping all tables DONE", file=stderr)  # XXX

# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an invididual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT, ResponseFuture
from cassandra.policies import RoundRobinPolicy
import asyncio
import pathlib
import pytest
import ssl
import sys
import itertools

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))


# Default initial values
DEFAULT_DCRF = 3        # Replication Factor for this_dc


# By default, tests run against a CQL server (Scylla or Cassandra) listening
# on localhost:9042. Add the --host and --port options to allow overiding
# these defaults.
def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
                     help='CQL server host to connect to')
    parser.addoption('--port', action='store', default='9042',
                     help='CQL server port to connect to')
    parser.addoption('--ssl', action='store_true',
                     help='Connect to CQL via an encrypted TLSv1.2 connection')


@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def _wrap_future(f: ResponseFuture):
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


def run_async(self, *args, **kwargs):
    return _wrap_future(self.execute_async(*args, **kwargs))


Session.run_async = run_async


# "cql" fixture: set up client object for communicating with the CQL API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 9042, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def cql(request):
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
    if request.config.getoption('ssl'):
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                      contact_points=[request.config.getoption('host')],
                      port=request.config.getoption('port'),
                      # TODO: make the protocol version an option, to allow testing with
                      # different versions. If we drop this setting completely, it will
                      # mean pick the latest version supported by the client and the server.
                      protocol_version=4,
                      # Use the default superuser credentials, which work for both Scylla and Cassandra
                      auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
                      ssl_context=ssl_context,
                      )
    return cluster.connect()


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


# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will have
# a "this_dc" fixture to figure out the name of the current DC, so it can be
# used in NetworkTopologyStrategy.
@pytest.fixture(scope="session")
def this_dc(cql):
    yield cql.execute("SELECT data_center FROM system.local").one()[0]


class Keyspace():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql, this_dc, dc_rf):
        self.id = Keyspace.newid()
        self.name = f"ks_{self.id:04}"
        self.replication_strategy = 'NetworkTopologyStrategy'
        self.cql = cql
        self.this_dc = this_dc
        self.dc_rf = dc_rf

    async def create(self):
        await self.cql.run_async(f"CREATE KEYSPACE {self.name} WITH REPLICATION = "
                                 f"{{ 'class' : '{self.replication_strategy}', '{self.this_dc}' : {self.dc_rf} }}")

    async def drop(self):
        await self.cql.run_async(f"DROP KEYSPACE {self.name}")


# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in a test. The keyspace is created with RF=2
# and destroyed after each test (not reused).
@pytest.fixture(scope="session")
async def keyspace(request, cql, this_dc):
    marker_dcrf = request.node.get_closest_marker("dc_rf")
    dc_rf = marker_dcrf.args[0] if marker_dcrf is not None else DEFAULT_DCRF
    ks = Keyspace(cql, this_dc, dc_rf)
    await ks.create()
    yield ks
    await ks.drop()


# The "scylla_only" fixture can be used by tests for Scylla-only features,
# which do not exist on Apache Cassandra. A test using this fixture will be
# skipped if running with "run-cassandra".
@pytest.fixture(scope="session")
def scylla_only(cql):
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        pytest.skip('Scylla-only test skipped')


# "cassandra_bug" is similar to "scylla_only", except instead of skipping
# the test, it is expected to fail (xfail) on Cassandra. It should be used
# in rare cases where we consider Scylla's behavior to be the correct one,
# and Cassandra's to be the bug.
@pytest.fixture(scope="session")
def cassandra_bug(cql):
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        pytest.xfail('A known Cassandra bug')

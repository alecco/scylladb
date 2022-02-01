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
from pylib.util import random_string, unique_name

# Default initial values
DEFAULT_DCRF = 3        # Replication Factor for this_dc
DEFAULT_NTABLES = 2     # Initial tables
DEFAULT_NCOLUMNS = 4    # Columns per table


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


class Table():
    newid = itertools.count(start=1).__next__

    def __init__(self, cql, keyspace, ncolumns, pks=2, name=None):
        """Set up a new table definition from column definitions.
           If column definitions not specified pick a random number of columns with random types.
           By default there will be 4 columns with first column as Primary Key"""
        self.id = Table.newid()
        self.cql = cql
        self.keyspace = keyspace
        self.name = name if name is not None else f"t_{self.id:02}"
        self.full_name = keyspace + "." + self.name
        # TODO: assumes primary key is composed of first self.pks columns
        self.pks = pks

        assert ncolumns > pks, "Not enough value columns provided"
        self.next_clustering_id = itertools.count(start=1).__next__
        self.next_value_id = itertools.count(start=1).__next__
        # Primary key pk, clustering columns c_xx, value columns v_xx
        self.columns = ["pk"]
        self.columns += [f"c_{self.next_clustering_id():02}" for i in range(1, pks)]
        self.columns += [f"v_{self.next_value_id():02}" for i in range(ncolumns - pks)]

    async def create(self):
        await self.cql.run_async(f"CREATE TABLE {self.full_name} (" + ", ".join(f"{c} text" for c in self.columns) +
                                 ", primary key(" + ", ".join(c for c in self.columns[:self.pks]) + "))")

    async def drop(self):
        await self.cql.run_async(f"DROP TABLE {self.full_name}")


class TestTables():
    """Create tables for a test in a given keyspace"""
    def __init__(self, test_name, cql, keyspace, ntables: int, ncolumns: int):
        self.test_name = test_name
        self.cql = cql
        self.keyspace = keyspace
        self.tables = [Table(cql, keyspace, ncolumns) for _ in range(ntables)]
        self.removed_tables = []

    def new_table(self, ncolumns=None, pks=2, name=None):
        table = Table(self.cql, self.keyspace, ncolumns, pks=pks, name=name)
        self.tables.append(table)
        return table

    def __getitem__(self, pos):
        return self.tables[pos]

    def append(self, table):
        return self.tables.append(table)

    def extend(self, tables):
        return self.tables.extend(tables)

    async def create_tables(self):
        await asyncio.gather(*(t.create() for t in self.tables))

    async def drop_table(self, table):
        if type(table) is str:
            table = next(t for t in self.tables)
        else:
            assert type(table) is Table, f"Invalid table type {type(table)}"
        await table.drop()
        self.tables.remove(table)
        self.removed_tables.append(table)
        return table

    async def drop_all_tables(self):
        """Drop all current tables"""
        await asyncio.gather(*(t.drop() for t in self.tables))
        self.removed_tables.extend(tables)

    async def verify_schema(self, table=None):
        """Verify keyspace table schema"""
        if type(table) is Table:
            tables = {table.name}
            q = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}' " \
                f"AND table_name = '{table.name}'"
        elif type(table) is str:
            if table.startswith(f"{self.keyspace.name}."):
                table = table[len(self.keyspace.name) + 1:]
            tables = {table}
            q = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}' " \
                f"AND table_name = '{table}'"
        else:
            tables = set(t.name for t in self.tables)
            q = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{self.keyspace}'"

        res = {row.table_name for row in await self.cql.run_async(q)}
        assert not tables - res, f"Tables {tables - res} not present"


# "keyspace" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace.  It's automatically dropped
# at the end of the session. All tests will reuse the same keyspace.
@pytest.fixture(scope="session")
async def keyspace(request, cql, this_dc):
    name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {name} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', " +
                        f"'{this_dc}' : '{DEFAULT_DCRF}' }}")
    yield name
    await cql.run_async("DROP KEYSPACE " + name)


@pytest.fixture()
async def tables(request, cql, keyspace):
    test_name = request.node.name.removeprefix("test_")
    marker_tables = request.node.get_closest_marker("ntables")
    ntables = marker_tables.args[0] if marker_tables is not None else DEFAULT_NTABLES
    marker_columns = request.node.get_closest_marker("ncolumns")
    ncolumns = marker_tables.args[0] if marker_columns is not None else DEFAULT_NCOLUMNS
    tables = TestTables(test_name, cql, keyspace, ntables, ncolumns)
    await tables.create_tables()
    yield tables
    # Let keyspace cleanup do the drops for now
    # await tables.drop_all()


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

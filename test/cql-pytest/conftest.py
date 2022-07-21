# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an invididual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import asyncio
import sys 
import pytest

from cassandra.cluster import Cluster
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION
import ssl

from util import unique_name, new_test_table, cql_session
# Use pylib libraries
sys.path.insert(1, sys.path[0] + '/../pylib')
from harness_cli import HarnessCli

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


@pytest.fixture(scope="session")
def harness(request, event_loop):
    """Session fixture to set up client object for communicating with the Cluster API.
       Pass the Unix socket path where the Harness server API is listening.
    """
    yield HarnessCli(request.config.getoption('api'))


# We use scope="session" so that all tests will reuse the same client object.
@pytest.mark.asyncio
@pytest.fixture(scope="session")
async def cql(request, harness):
    # Use the default superuser credentials, which work for both Scylla and Cassandra
    host = (await harness.nodes())[0]
    port = await harness.cql_port()
    from sys import stderr    # XXX
    print(f"XXX host {host} port {port}", file=stderr)  # XXX
    print(f"XXX calling before_test", file=stderr)  # XXX
    await harness.before_test(request.node.name)
    with cql_session(host,
        port,
        request.config.getoption('ssl'),
        username="cassandra",
        password="cassandra"
    ) as session:
        yield session
        session.shutdown()
    print(f"XXX calling after_test", file=stderr)  # XXX
    await harness.after_test(request.node.name)

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
    except:
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

# "test_keyspace" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
# and automatically deleted at the end. We use scope="session" so that all
# tests will reuse the same keyspace.
@pytest.fixture(scope="session")
def test_keyspace(cql, this_dc):
    name = unique_name()
    from sys import stderr
    print(f"XXX CREATE KEYSPACE {name}", file=stderr)  # XXX
    cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    print(f"XXX DROP KEYSPACE {name}", file=stderr)  # XXX
    cql.execute("DROP KEYSPACE " + name)

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

# While the raft-based schema modifications are still experimental and only
# optionally enabled (the "--raft" option to test/cql-pytest/run enables it
# in Scylla), some tests are expected to fail on Scylla without this option
# enabled, and pass with it enabled (and also pass on Cassandra). These tests
# should use the "fails_without_raft" fixture. When Raft mode becomes the
# default, this fixture can be removed.
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

# Older versions of the Cassandra driver had a bug where if Scylla returns
# an empty page, the driver would immediately stop reading even if this was
# not the last page. Some tests which filter out most of the results can end
# up with some empty pages, and break on buggy versions of the driver. These
# tests should be skipped when using a buggy version of the driver. This is
# the purpose of the following fixture.
# This driver bug was fixed in Scylla driver 3.24.5 and Datastax driver
# 3.25.1, in the following commits:
# https://github.com/scylladb/python-driver/commit/6ed53d9f7004177e18d9f2ea000a7d159ff9278e,
# https://github.com/datastax/python-driver/commit/1d9077d3f4c937929acc14f45c7693e76dde39a9
@pytest.fixture(scope="function")
def driver_bug_1():
    scylla_driver = 'Scylla' in DRIVER_NAME
    driver_version = tuple(int(x) for x in DRIVER_VERSION.split('.'))
    if (scylla_driver and driver_version < (3, 24, 5) or
            not scylla_driver and driver_version <= (3, 25, 0)):
        pytest.skip("Python driver too old to run this test")

# TODO: use new_test_table and "yield from" to make shared test_table
# fixtures with some common schemas.

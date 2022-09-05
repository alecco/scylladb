#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import asyncio
import logging
import pathlib
import ssl
import sys
from typing import List
# Also pylib modules
sys.path.append(sys.path[0] + '/../pylib')
from random_tables import RandomTables       # type: ignore # pylint: disable=import-error
from util import unique_name                 # type: ignore # pylint: disable=import-error
from manager_client import ManagerClient     # type: ignore # pylint: disable=import-error
import pytest
import cassandra                                                         # type: ignore
from cassandra.cluster import Session, ResponseFuture                    # type: ignore
from cassandra.cluster import Cluster, ConsistencyLevel                  # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy, RetryPolicy    # type: ignore


# Add test.pylib to the search path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))


logger = logging.getLogger(__name__)
logger.debug("Python driver version in use: {}".format(cassandra.__version__))


def pytest_addoption(parser):
    parser.addoption('--manager-api', action='store', required=True,
                     help='Manager unix socket path')

# Change default pytest-asyncio event_loop fixture scope to session to
# allow async fixtures with scope larger than function. (e.g. manager fixture)
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
        print("\nXXX on_result scheduled result OK", file=sys.stderr) # XXX

    def on_error(exception, *_):
        print(f"\nXXX on_error scheduling exception {exception}", file=sys.stderr) # XXX
        loop.call_soon_threadsafe(aio_future.set_exception, exception)

    f.add_callback(on_result)
    f.add_errback(on_error)
    return aio_future


def run_async(self, *args, **kwargs) -> asyncio.Future:
    kwargs.setdefault("timeout", 2.0)
    return _wrap_future(self.execute_async(*args, **kwargs))


Session.run_async = run_async


class TopologyChangeRetryPolicy(RetryPolicy):
    """
    A retry policy that changes to another host after failure
    """

    def on_read_timeout(self, *args, **kwargs):
        """
        This is called when a read operation times out from the coordinator's
        perspective (i.e. a replica did not respond to the coordinator in time).
        See cassandra.policies RetryPolicy
        """
        if kwargs['retry_num'] < 3:
            print("Retrying read after timeout. Attempt #%s" % kwargs['retry_num'], file=sys.stderr)
            logger.debug("Retrying read after timeout. Attempt #%s", kwargs['retry_num'])
            return (self.RETRY, None)
        elif kwargs['retry_num'] < 6:
            logger.debug("Retrying read after timeout. Next host. Attempt #%s",
                         kwargs['retry_num'])
            return (self.RETRY_NEXT_HOST , None)
        return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        """Retry 5 times with another host, then raise"""
        if kwargs['retry_num'] < 3:
            print("Retrying write after timeout. Attempt #%s" % kwargs['retry_num'], file=sys.stderr)
            logger.debug("Retrying write after timeout. Attempt #%s", kwargs['retry_num'])
            return (self.RETRY, None)
        elif kwargs['retry_num'] < 6:
            logger.debug("Retrying write after timeout. Next host. Attempt #%s",
                         kwargs['retry_num'])
            return (self.RETRY_NEXT_HOST , None)
        return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        """Retry 5 times with another host, then raise"""
        if kwargs['retry_num'] < 3:
            print("Retrying request after UE. Attempt #%s" % kwargs['retry_num'], file=sys.stderr)
            logger.debug("Retrying request after UE. Attempt #%s", kwargs['retry_num'])
            return (self.RETRY, None)
        elif kwargs['retry_num'] < 6:
            logger.debug("Retrying request after UE. Attempt #%s", kwargs['retry_num'])
            return (self.RETRY_NEXT_HOST , None)
        return (self.RETHROW, None)


# cluster_con helper: set up client object for communicating with the CQL API.
def cluster_con(hosts: List[str], port: int, ssl: bool):
    """Create a CQL Cluster connection object according to configuration.
       It does not .connect() yet."""
    assert len(hosts) > 0, "python driver connection needs at least one host to connect to"
    # print(f"\nXXX cluster_con WhiteListRoundRobinPolicy(hosts={hosts})", file=sys.stderr)  # XXX
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(hosts=hosts),
        retry_policy=TopologyChangeRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeout (in seconds) for execute() commands is 10, which
        # should have been more than enough, but in some extreme cases with a
        # very slow debug build running on a very busy machine and a very slow
        # request (e.g., a DROP KEYSPACE needing to drop multiple tables)
        # 10 seconds may not be enough, so let's increase it. See issue #7838.
        request_timeout=5)
    if ssl:
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
                   # NOTE: No auth provider as auth keysppace has RF=1 and topology will take
                   # down nodes, causing errors. If auth is needed in the future for topology
                   # tests, they should bump up auth RF and run repair.
                   ssl_context=ssl_context,
                   # The default timeout for new connections is 5 seconds, and for
                   # requests made by the control connection is 2 seconds. These should
                   # have been more than enough, but in some extreme cases with a very
                   # slow debug build running on a very busy machine, they may not be.
                   # so let's increase them to 60 seconds. See issue #11289.
                   connect_timeout = 5,
                   control_connection_timeout = 5,
                   max_schema_agreement_wait=7,
                   )


@pytest.mark.asyncio
@pytest.fixture(scope="session")
async def manager_internal(event_loop, request):
    """Session fixture to set up client object for communicating with the Cluster API.
       Pass the Unix socket path where the Manager server API is listening.
       Pass a function to create driver connections.
       Test cases (functions) should not use this fixture.
    """
    port = int(request.config.getoption('port'))
    ssl = bool(request.config.getoption('ssl'))
    manager_int = ManagerClient(request.config.getoption('manager_api'), port, ssl, cluster_con)
    await manager_int.start()
    yield manager_int
    print(f"\nXXX fixture manager_internal closing driver", file=sys.stderr) # XXX
    manager_int.driver_close()   # Close after last test case
    print(f"\nXXX fixture manager_internal closing driver DONE", file=sys.stderr) # XXX

@pytest.fixture(scope="function")
async def manager(request, manager_internal):
    """Per test fixture to notify Manager client object when tests begin so it can
    perform checks for cluster state.
    """
    await manager_internal.before_test(request.node.name)
    yield manager_internal
    await manager_internal.after_test(request.node.name)

# "cql" fixture: set up client object for communicating with the CQL API.
# Since connection is managed by manager just return that object
@pytest.fixture(scope="function")
def cql(manager):
    yield manager.cql

# While the raft-based schema modifications are still experimental and only
# optionally enabled some tests are expected to fail on Scylla without this
# option enabled, and pass with it enabled (and also pass on Cassandra).
# These tests should use the "fails_without_raft" fixture. When Raft mode
# becomes the default, this fixture can be removed.
@pytest.fixture(scope="function")
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
@pytest.fixture(scope="function")
async def random_tables(request, cql, manager):
    tables = RandomTables(request.node.name, cql, unique_name())
    yield tables
    debug_msg = f"\nXXX fixture random_tables cleanup: restarting driver {request.node.name}"
    print(debug_msg, file=sys.stderr) # XXX
    manager.driver_close()   # Close after last test case
    await manager.driver_connect()  # Connect driver to new cluster
    debug_msg = f"\nXXX fixture random_tables cleanup: dropping keyspace {request.node.name}"
    print(debug_msg, file=sys.stderr) # XXX
    # NOTE: to avoid occasional timeouts on keyspace teardown, start stopped servers
    # XXX await manager.start_stopped()
    await tables.drop_all()

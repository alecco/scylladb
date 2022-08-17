#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import sys
import pytest

from test.pylib.util import unique_name


def pytest_addoption(parser):
    parser.addoption('--manager-api', action='store', required=True,
                     help='Manager unix socket path')
    parser.addoption('--ssl', action='store_true',
        help='Connect to CQL via an encrypted TLSv1.2 connection')
    parser.addoption('--port', action='store', default='9042',
        help='Scylla CQL port to connect to')


# "test_keyspace" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
# and automatically deleted at the end.
@pytest.fixture(scope="function")
def test_keyspace(cql):
    name = unique_name()
    cql.execute(f"CREATE KEYSPACE {name} WITH REPLICATION = "
                 "{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
    yield name
    cql.execute(f"DROP KEYSPACE {name}")


from test.topology.conftest import event_loop, manager_internal, manager, cql


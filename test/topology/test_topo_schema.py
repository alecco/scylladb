# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# The cql-pytest framework is about testing CQL functionality, so
# implementation details like sstables cannot be tested directly. However,
# we are still able to reproduce some bugs by tricks such as writing some
# data to the table and then force it to be written to the disk (nodetool
# flush) and then trying to read the data again, knowing it must come from
# the disk.
#############################################################################

import pytest


@pytest.fixture(scope="session")
def table_simple(harness):
    """Creates cluster, keyspace, and a table"""
    tables.new_table(ncols=3)
    yield tables

# Schema + topology changes
#   Common test flow:
#     Create cluster
#     Create schema
#     Run traffic
#     Run topology changes
#     In parallel run schema changes
#     Wait processes finish


# Schema + topology changes: Run in parallel topology change: add nodes

def test_new_column(table_simple):
    """Alter table with new column"""
    table = table_simple


def test_remove_column(table_simple):
    """Alter table with remove column"""
    table = table_simple


def test_XXX(table_simple):
    """Alter table with various options including cdc, cp etc"""
    table = table_simple


def test_XXX(table_simple):
    """Alter keyspace with new table, MV, UDT, GSI, LSI"""
    table = table_simple


def test_XXX(table_simple):
    """Alter keyspace with replication factor, class strategy"""
    table = table_simple


#   Two-dc setup, LOCAL_QUORUM/LOCAL_SERIAL consistency level, shut down one of the DCs,
#   test *ALL*  DML statements that access local objects work  (DML, including secondary
#   keys and views, hints). Bring the disconnected DC back, make sure DDL works.
#
#   Same as above, but instead of two DC, test a cluster of 3 nodes, shut down two nodes,
#   test all ONE/LOCAL_ONE queries continue to work. Make sure DDL doesn’t work, because we lost quorum. Bring back the previous nodes, make sure DDL works and data is upgraded correctly in both DCs.
#
#   Testing group 0 ID change
#
#   Test raft voting weights (aka being able to run without the physical majority of the nodes, because the remaining nodes have the voting majority).
#
#   Procedures from https://docs.scylladb.com/operating-scylla/procedures/cluster-management/replace-dead-node-or-more/ should continue working in case majority is preserved.




def test_alter_table_1(table_simple):
#   Alter table with new column
    table = table_simple
    # XXX blah drop leader blah


def test_drop_leader(harness):
    cluster = harness.create("drop_leader", nodes=3)
    # XXX blah drop leader blah


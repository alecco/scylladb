# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the tools hosted by scylla
#############################################################################

import contextlib
import glob
import json
from . import nodetool
import os
import pytest
import subprocess
import tempfile
import random
from . import util

# To run the Scylla tools, we need to run Scylla executable itself, so we
# need to find the path of the executable that was used to run Scylla for
# this test. We do this by trying to find a local process which is listening
# to the address and port to which our our CQL connection is connected.
# If such a process exists, we verify that it is Scylla, and return the
# executable's path. If we can't find the Scylla executable we use
# pytest.skip() to skip tests relying on this executable.
@pytest.fixture(scope="module")
def scylla_path(cql):
    pid = util.local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local Scylla process")
    # Now that we know the process id, use /proc to find the executable.
    try:
        path = os.readlink(f'/proc/{pid}/exe')
    except:
        pytest.skip("Can't find local Scylla executable")
    # Confirm that this executable is a real tool-providing Scylla by trying
    # to run it with the "--list-tools" option
    try:
        subprocess.check_output([path, '--list-tools'])
    except:
        pytest.skip("Local server isn't Scylla")
    return path

# A fixture for finding Scylla's data directory. We get it using the CQL
# interface to Scylla's configuration. Note that if the server is remote,
# the directory retrieved this way may be irrelevant, whether or not it
# exists on the local machine... However, if the same test that uses this
# fixture also uses the scylla_path fixture, the test will anyway be skipped
# if the running Scylla is not on the local machine local.
@pytest.fixture(scope="module")
def scylla_data_dir(cql):
    try:
        dir = json.loads(cql.execute("SELECT value FROM system.config WHERE name = 'data_file_directories'").one().value)[0]
        return dir
    except:
        pytest.skip("Can't find Scylla sstable directory")


def simple_no_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v int) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        x = random.randrange(0, 4)
        if x == 0:
            # partition tombstone
            cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk}")
        else:
            # live row
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES ({pk}, 0)")

        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def simple_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v int, s int STATIC, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            x = random.randrange(0, 8)
            if x == 0:
                # ttl
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, 0) USING TTL 6000")
            elif x == 1:
                # row tombstone
                cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk} AND ck = {ck}")
            elif x == 2:
                # cell tombstone
                cql.execute(f"DELETE v FROM {keyspace}.{table} WHERE pk = {pk} AND ck = {ck}")
            elif x == 3:
                # range tombstone
                l = ck * 10
                u = ck * 11
                cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk} AND ck > {l} AND ck < {u}")
            else:
                # live row
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, 0)")

        if pk == 5:
            cql.execute(f"UPDATE {keyspace}.{table} SET s = 10 WHERE pk = {pk}")
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_collection(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v map<int, text>, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            map_vals = {f"{p}: '{c}'" for p in range(0, pk) for c in range(0, ck)}
            map_str = ", ".join(map_vals)
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, {{{map_str}}})")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_udt(cql, keyspace):
    table = util.unique_name()
    create_type_schema = f"CREATE TYPE IF NOT EXISTS {keyspace}.type1 (f1 int, f2 text)"
    create_table_schema = f" CREATE TABLE {keyspace}.{table} (pk int, ck int, v type1, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(create_type_schema)
    cql.execute(create_table_schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, {{f1: 100, f2: 'asd'}})")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, "; ".join((create_type_schema, create_table_schema))


def table_with_counters(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v counter) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for c in range(0, 4):
            cql.execute(f"UPDATE {keyspace}.{table} SET v = v + 1 WHERE pk = {pk};")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


@contextlib.contextmanager
def scylla_sstable(table_factory, cql, ks, data_dir):
    table, schema = table_factory(cql, ks)

    schema_file = os.path.join(data_dir, "..", "test_tools_schema.cql")
    with open(schema_file, "w") as f:
        f.write(schema)

    sstables = glob.glob(os.path.join(data_dir, ks, table + '-*', '*-Data.db'))

    try:
        yield (schema_file, sstables)
    finally:
        cql.execute(f"DROP TABLE {ks}.{table}")
        os.unlink(schema_file)


def one_sstable(sstables):
    assert len(sstables) > 1
    return [sstables[0]]


def all_sstables(sstables):
    assert len(sstables) > 1
    return sstables


@pytest.mark.parametrize("what", ["index", "compression-info", "summary", "statistics", "scylla-metadata"])
@pytest.mark.parametrize("which_sstables", [one_sstable, all_sstables])
def test_scylla_sstable_dump_component(cql, test_keyspace, scylla_path, scylla_data_dir, what, which_sstables):
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        out = subprocess.check_output([scylla_path, "sstable", f"dump-{what}", "--schema-file", schema_file] + which_sstables(sstables))

    print(out)

    assert out
    assert json.loads(out)


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
        clustering_table_with_collection,
        clustering_table_with_udt,
        table_with_counters,
])
@pytest.mark.parametrize("merge", [True, False])
@pytest.mark.parametrize("output_format", ["text", "json"])
def test_scylla_sstable_dump_data(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory, merge, output_format):
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", output_format]
        if merge:
            args.append("--merge")
        out = subprocess.check_output(args + sstables)

    print(out)

    assert out
    if output_format == "json":
        assert json.loads(out)


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
])
def test_scylla_sstable_write(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory):
    with scylla_sstable(table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        with tempfile.TemporaryDirectory() as tmp_dir:
            dump_common_args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", "json", "--merge"]
            generation = util.unique_key_int()

            original_out = subprocess.check_output(dump_common_args + sstables)
            original_json = json.loads(original_out)["sstables"]["anonymous"]

            input_file = os.path.join(tmp_dir, 'input.json')

            with open(input_file, 'w') as f:
                json.dump(original_json, f)

            subprocess.check_call([scylla_path, "sstable", "write", "--schema-file", schema_file, "--input-file", input_file, "--output-dir", tmp_dir, "--generation", str(generation), '--logger-log-level', 'scylla-sstable=trace'])

            sstable_file = os.path.join(tmp_dir, f"me-{generation}-big-Data.db")

            actual_out = subprocess.check_output(dump_common_args + [sstable_file])
            actual_json = json.loads(actual_out)["sstables"]["anonymous"]

            assert actual_json == original_json
#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import InvalidRequest   # type: ignore
from pylib.util import unique_name
from pylib.schema_helper import get_schema
import pytest
import sys   # XXX

async def get_nodes(harness):   # XXX
    nodes = await harness.nodes()
    print(f"XXX test nodes {nodes}", file=sys.stderr)  # XXX
    return await harness.nodes()


@pytest.mark.asyncio
async def test_delete_empty_string_key(cql, harness):
    nodes = await get_nodes(harness)
    print(f"XXX test_null adding node", file=sys.stderr)  # XXX
    ret = await harness.node_add()
    print(f"XXX test_null added node {ret}", file=sys.stderr)  # XXX
    nodes = await get_nodes(harness)
    await harness.node_restart(nodes[0])
    await get_nodes(harness)
    print(f"XXX test_null removing {nodes[1]}", file=sys.stderr)  # XXX
    await harness.node_remove(nodes[1])
    print(f"XXX test_null removed", file=sys.stderr)  # XXX
    await get_nodes(harness)

    raise Exception("XXX")  # XXX force print and keep output

    tables = await get_schema("delete_empty_string_key", cql, ntables=1, ncolumns=5)
    s = "foobar"
    # An empty-string clustering *is* allowed:
    await cql.run_async(f"DELETE FROM {tables[0]} WHERE pk = '{s}' AND {tables[0].columns[1].name} = ''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        await cql.run_async(f"DELETE FROM {tables[0]} WHERE pk = '' AND {tables[0].columns[1].name} = '{s}'")
    await tables.verify_schema()

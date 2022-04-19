#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import InvalidRequest
from pylib.util import random_string
from pylib.schema_helper import get_schema
import pytest


@pytest.mark.asyncio
async def test_delete_empty_string_key(cql):
    # raise Exception("XXX")  # XXX
    tables = await get_schema("delete_empty_string_key", cql, ntables=1, ncolumns=5)
    s = random_string()
    # An empty-string clustering *is* allowed:
    await cql.run_async(f"DELETE FROM {tables[0]} WHERE pk = '{s}' AND {tables[0].columns[1].name} = ''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        await cql.run_async(f"DELETE FROM {tables[0]} WHERE pk = '' AND {tables[0].columns[1].name} = '{s}'")
    await tables.verify_schema()

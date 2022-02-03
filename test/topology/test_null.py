#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import InvalidRequest
from pylib.util import random_string
import pytest


@pytest.mark.asyncio
@pytest.mark.ntables(1)
async def test_delete_empty_string_key(cql, tables):
    s = random_string()
    # An empty-string clustering *is* allowed:
    await cql.run_async(f"DELETE FROM {tables[0].full_name} WHERE pk='{s}' AND c_01=''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        await cql.run_async(f"DELETE FROM {tables[0].full_name} WHERE pk='' AND c_01='{s}'")

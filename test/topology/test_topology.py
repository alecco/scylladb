#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""


@pytest.mark.asyncio
async def test_1(harness):
    await harness.node_add()
    await harness.mark_dirty()

@pytest.mark.asyncio
async def test_2(harness)
    await harness.node_add()

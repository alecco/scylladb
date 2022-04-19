# XXX XXX XXX debug
#
import pytest
import sys   # XXX


async def get_nodes(harness):   # XXX
    nodes = await harness.nodes()
    print(f"XXX test nodes {nodes}", file=sys.stderr)  # XXX
    return nodes


@pytest.mark.asyncio
async def test_harness_xxx(cql, harness):
    nodes = await get_nodes(harness)
    print(f"XXX test_null removing {nodes[1]}", file=sys.stderr)  # XXX
    await harness.node_remove(nodes[1])
    print("XXX test_null removed", file=sys.stderr)  # XXX
    await get_nodes(harness)
    print("XXX test_null adding node", file=sys.stderr)  # XXX
    ret = await harness.node_add()
    print(f"XXX test_null added node {ret}", file=sys.stderr)  # XXX
    nodes = await get_nodes(harness)
    await harness.node_restart(nodes[0])
    await get_nodes(harness)

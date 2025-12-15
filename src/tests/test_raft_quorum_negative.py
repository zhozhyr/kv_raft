from __future__ import annotations

import pytest

from .conftest import Cluster, rand_key, kv_put, wait_for_single_leader, wait_until


@pytest.mark.asyncio
async def test_no_quorum_write_fails(client, cluster: Cluster, compose):
    # stop two nodes
    compose(["stop", "node2"])
    compose(["stop", "node3"])

    key = rand_key("noquorum")

    # write to node1 (only node alive)
    r = await kv_put(client, cluster.node1, key, "x")

    # acceptable failures:
    # - 409 "not leader" (cannot elect)
    # - 503 "failed to commit"
    assert r.status_code in (409, 503), r.text

    # restore nodes
    compose(["start", "node2"])
    compose(["start", "node3"])

    # wait cluster stabilizes with a leader
    await wait_for_single_leader(client, cluster)

    # now a write should succeed
    leader_base, _ = await wait_for_single_leader(client, cluster)
    rr = await kv_put(client, leader_base, rand_key("after"), "ok")
    assert rr.status_code == 200, rr.text

from __future__ import annotations

import pytest

from .conftest import Cluster, rand_key, kv_get, kv_put, wait_for_single_leader, wait_until


@pytest.mark.asyncio
async def test_single_leader_exists(client, cluster: Cluster):
    leader_base, leader_state = await wait_for_single_leader(client, cluster)
    assert leader_state["role"] == "leader"
    assert leader_state["node_id"] is not None


@pytest.mark.asyncio
async def test_put_then_read_from_all_nodes(client, cluster: Cluster):
    leader_base, _ = await wait_for_single_leader(client, cluster)

    key = rand_key("foo")
    value = "bar"

    r = await kv_put(client, leader_base, key, value)
    assert r.status_code == 200, r.text

    async def _replicated_everywhere():
        for n in cluster.nodes:
            rr = await kv_get(client, n, key)
            if rr.status_code != 200:
                return False
            if rr.json().get("value") != value:
                return False
        return True

    await wait_until(_replicated_everywhere, timeout=6.0, desc="value replicated to all nodes")

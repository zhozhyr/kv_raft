from __future__ import annotations

import pytest

from .conftest import Cluster, rand_key, kv_put, wait_for_single_leader, wait_until


@pytest.mark.asyncio
async def test_no_quorum_write_fails(client, cluster: Cluster, compose):
    # stop two nodes
    compose(["stop", "node2"])
    compose(["stop", "node3"])
    compose(["stop", "node4"])
    compose(["stop", "node5"])

    key = rand_key("noquorum")

    # записываем на ноду 1 (единсвенную живую)
    r = await kv_put(client, cluster.node1, key, "x")

    assert r.status_code in (409, 503), r.text

    # оживляем ноды
    compose(["start", "node2"])
    compose(["start", "node3"])
    compose(["start", "node4"])
    compose(["start", "node5"])

    # ждем пока кластер синхронизируется с лидером
    await wait_for_single_leader(client, cluster)

    leader_base, _ = await wait_for_single_leader(client, cluster)
    rr = await kv_put(client, leader_base, rand_key("after"), "ok")
    assert rr.status_code == 200, rr.text

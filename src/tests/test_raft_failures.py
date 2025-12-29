from __future__ import annotations

import pytest

from .conftest import (
    Cluster,
    rand_key,
    kv_get_maybe,
    kv_put,
    wait_for_single_leader,
    wait_for_single_leader_tolerant,
    wait_until,
)



def _container_name_from_url(base: str) -> str:
    # our compose exposes:
    # node1 -> localhost:8001, node2 -> 8002, node3 -> 8003
    if base.endswith(":8001"):
        return "node1"
    if base.endswith(":8002"):
        return "node2"
    if base.endswith(":8003"):
        return "node3"
    if base.endswith(":8004"):
        return "node4"
    if base.endswith(":8005"):
        return "node5"
    raise ValueError(f"unknown node base: {base}")


@pytest.mark.asyncio
async def test_leader_failover_keeps_committed_data(client, cluster: Cluster, compose):
    leader_base, _ = await wait_for_single_leader(client, cluster)

    key = rand_key("failover")
    value = "v1"
    r = await kv_put(client, leader_base, key, value)
    assert r.status_code == 200, r.text

    leader_container = _container_name_from_url(leader_base)
    # stop leader
    compose(["stop", leader_container])

    try:
        # wait new leader among remaining nodes (may take couple seconds)
        async def _has_new_leader():
            try:
                lb, st = await wait_for_single_leader_tolerant(client, cluster, timeout=2.0, min_reachable=2)
                return lb != leader_base and st["role"] == "leader"
            except Exception:
                return False

        await wait_until(_has_new_leader, timeout=12.0, desc="new leader elected")

        # data must still be readable from alive nodes (may include old leader down)
        async def _read_from_any_alive():
            ok = 0
            for n in cluster.nodes:
                data = await kv_get_maybe(client, n, key)
                if data is not None and data.get("value") == value:
                    ok += 1
            # при одном остановленном узле хотим видеть значение на двух живых
            return ok >= 2

        await wait_until(_read_from_any_alive, timeout=8.0, desc="committed data available after failover")
    finally:
        # bring it back for subsequent tests
        compose(["start", leader_container])

        # allow it to rejoin
        await wait_for_single_leader(client, cluster)

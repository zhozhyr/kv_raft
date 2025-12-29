from __future__ import annotations

import asyncio
import subprocess

import pytest

from .conftest import Cluster, kv_put, kv_get, rand_key, wait_for_single_leader, wait_until


def _compose(args: list[str]) -> None:
    subprocess.run(["docker", "compose"] + args, check=True, capture_output=True, text=True)


@pytest.mark.asyncio
async def test_get_quorum_allows_lagging_follower(client, cluster: Cluster):
    # Находим лидера
    leader_base, _ = await wait_for_single_leader(client, cluster)

    # Останавливаем один follower, чтобы он отстал по логу
    _compose(["stop", "node3"])
    try:
        key = rand_key("lag")
        # Два PUT, чтобы лидер ушёл вперёд
        r1 = await kv_put(client, leader_base, key, "v1")
        assert r1.status_code == 200, r1.text
        r2 = await kv_put(client, leader_base, key, "v2")
        assert r2.status_code == 200, r2.text
    finally:
        # Возвращаем follower
        _compose(["start", "node3"])

    # Сразу после старта node3 он ещё может не догнать лог, но GET на лидере
    # должен пройти (кворум: лидер + актуальный follower).
    async def _get_ok():
        r = await kv_get(client, leader_base, key)
        return r.status_code == 200 and r.json().get("value") == "v2"

    await wait_until(_get_ok, timeout=8.0, desc="GET works with lagging follower")

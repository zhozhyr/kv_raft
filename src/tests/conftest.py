from __future__ import annotations
import asyncio
import os
import random
import string
import subprocess
import time
from dataclasses import dataclass
from typing import Callable

import httpx
import pytest


@dataclass(frozen=True)
class Cluster:
    node1: str = "http://localhost:8001"
    node2: str = "http://localhost:8002"
    node3: str = "http://localhost:8003"
    node4: str = "http://localhost:8004"
    node5: str = "http://localhost:8005"

    @property
    def nodes(self) -> list[str]:
        return [self.node1, self.node2, self.node3, self.node4, self.node5]


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def _compose_cmd() -> list[str]:
    # allow override, but default is good for most setups
    # e.g. export DOCKER_COMPOSE="docker compose"
    raw = os.environ.get("DOCKER_COMPOSE", "docker compose")
    return raw.split()


@pytest.fixture(scope="session")
def cluster() -> Cluster:
    return Cluster()


@pytest.fixture(scope="session")
def compose() -> Callable[[list[str]], subprocess.CompletedProcess]:
    base = _compose_cmd()

    def call(args: list[str]) -> subprocess.CompletedProcess:
        return _run(base + args)

    return call


@pytest.fixture(scope="session", autouse=True)
def compose_up(compose: Callable[[list[str]], subprocess.CompletedProcess]):
    # поднять кластер один раз на сессию
    compose(["up", "-d", "--build"])
    yield
    # и погасить после всех тестов
    compose(["down", "-v"])


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=2.0) as c:
        yield c


def rand_key(prefix: str = "k") -> str:
    tail = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
    return f"{prefix}_{tail}"


async def get_state(client: httpx.AsyncClient, base: str) -> dict:
    r = await client.get(f"{base}/raft/state")
    r.raise_for_status()
    return r.json()


async def wait_until(
    predicate,
    timeout: float = 8.0,
    interval: float = 0.2,
    desc: str = "condition",
):
    start = time.monotonic()
    last_exc = None
    while time.monotonic() - start < timeout:
        try:
            if await predicate():
                return
        except Exception as e:
            last_exc = e
        await asyncio.sleep(interval)
    if last_exc:
        raise AssertionError(f"Timeout waiting for {desc}. Last error: {last_exc}") from last_exc
    raise AssertionError(f"Timeout waiting for {desc}")


async def find_leader(client: httpx.AsyncClient, cluster: Cluster) -> tuple[str, dict]:
    states = []
    for n in cluster.nodes:
        st = await get_state(client, n)
        states.append((n, st))
    leaders = [(base, st) for (base, st) in states if st.get("role") == "leader"]
    if len(leaders) != 1:
        raise AssertionError(f"Expected exactly 1 leader, got {len(leaders)}: {states}")
    return leaders[0]


async def wait_for_single_leader(client: httpx.AsyncClient, cluster: Cluster, timeout: float = 10.0) -> tuple[str, dict]:
    async def _pred():
        try:
            await find_leader(client, cluster)
            return True
        except AssertionError:
            return False

    await wait_until(_pred, timeout=timeout, desc="single leader")
    return await find_leader(client, cluster)


async def kv_put(client: httpx.AsyncClient, base: str, key: str, value: str) -> httpx.Response:
    return await client.put(f"{base}/kv/{key}", json={"value": value})


async def kv_get(client: httpx.AsyncClient, base: str, key: str) -> httpx.Response:
    return await client.get(f"{base}/kv/{key}")

async def kv_get_maybe(client: httpx.AsyncClient, base: str, key: str) -> dict | None:
    """
    GET value; return parsed JSON on 200, None if node unreachable or not found.
    """
    try:
        r = await client.get(f"{base}/kv/{key}")
    except Exception:
        return None
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


async def kv_delete(client: httpx.AsyncClient, base: str, key: str) -> httpx.Response:
    return await client.delete(f"{base}/kv/{key}")


async def get_state_maybe(client: httpx.AsyncClient, base: str) -> dict | None:
    """
    Like get_state(), but returns None if node is unreachable.
    """
    try:
        r = await client.get(f"{base}/raft/state")
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


async def find_leader_tolerant(
    client: httpx.AsyncClient,
    cluster: Cluster,
    min_reachable: int = 2,
) -> tuple[str, dict]:
    """
    Find exactly one leader among reachable nodes.
    Skips nodes that are down/unreachable.
    """
    states: list[tuple[str, dict]] = []
    for n in cluster.nodes:
        st = await get_state_maybe(client, n)
        if st is not None:
            states.append((n, st))

    if len(states) < min_reachable:
        raise AssertionError(f"Not enough reachable nodes: {len(states)} < {min_reachable}")

    leaders = [(base, st) for (base, st) in states if st.get("role") == "leader"]
    if len(leaders) != 1:
        raise AssertionError(f"Expected exactly 1 leader among reachable, got {len(leaders)}. states={states}")
    return leaders[0]


async def wait_for_single_leader_tolerant(
    client: httpx.AsyncClient,
    cluster: Cluster,
    timeout: float = 10.0,
    min_reachable: int = 2,
) -> tuple[str, dict]:
    async def _pred():
        try:
            await find_leader_tolerant(client, cluster, min_reachable=min_reachable)
            return True
        except AssertionError:
            return False

    await wait_until(_pred, timeout=timeout, desc="single leader (tolerant)")
    return await find_leader_tolerant(client, cluster, min_reachable=min_reachable)

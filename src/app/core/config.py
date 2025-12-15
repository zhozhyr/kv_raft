from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


@dataclass(frozen=True)
class Settings:
    node_id: str
    host: str
    port: int
    peers: list[str]
    data_dir: Path

    election_timeout_min: float
    election_timeout_max: float
    heartbeat_interval: float
    rpc_timeout: float

    @staticmethod
    def load() -> "Settings":
        node_id = _env("NODE_ID", "node1")
        host = _env("HOST", "0.0.0.0")
        port = int(_env("PORT", "8000"))

        peers_raw = _env("PEERS", "")
        peers = [p.strip().rstrip("/") for p in peers_raw.split(",") if p.strip()]

        data_dir = Path(_env("DATA_DIR", f"./data/{node_id}")).resolve()
        data_dir.mkdir(parents=True, exist_ok=True)

        return Settings(
            node_id=node_id,
            host=host,
            port=port,
            peers=peers,
            data_dir=data_dir,
            election_timeout_min=float(_env("ELECTION_TIMEOUT_MIN", "1.5")),
            election_timeout_max=float(_env("ELECTION_TIMEOUT_MAX", "3.0")),
            heartbeat_interval=float(_env("HEARTBEAT_INTERVAL", "0.4")),
            rpc_timeout=float(_env("RPC_TIMEOUT", "1.2")),
        )

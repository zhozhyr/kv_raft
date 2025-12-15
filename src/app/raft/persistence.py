from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .types import LogEntry


def _atomic_write(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


@dataclass
class PersistentState:
    current_term: int
    voted_for: str | None
    log: list[LogEntry]
    commit_index: int
    last_applied: int
    kv: dict[str, str]


class Persistence:
    def __init__(self, data_dir: Path):
        self.state_path = data_dir / "state.json"

    def load(self) -> PersistentState:
        if not self.state_path.exists():
            return PersistentState(
                current_term=0,
                voted_for=None,
                log=[],
                commit_index=-1,
                last_applied=-1,
                kv={},
            )

        raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        log = [LogEntry.model_validate(e) for e in raw.get("log", [])]

        return PersistentState(
            current_term=int(raw.get("current_term", 0)),
            voted_for=raw.get("voted_for", None),
            log=log,
            commit_index=int(raw.get("commit_index", -1)),
            last_applied=int(raw.get("last_applied", -1)),
            kv=dict(raw.get("kv", {})),
        )

    def save(self, st: PersistentState) -> None:
        obj: dict[str, Any] = {
            "current_term": st.current_term,
            "voted_for": st.voted_for,
            "log": [e.model_dump() for e in st.log],
            "commit_index": st.commit_index,
            "last_applied": st.last_applied,
            "kv": st.kv,
        }
        _atomic_write(self.state_path, json.dumps(obj, ensure_ascii=False, indent=2))

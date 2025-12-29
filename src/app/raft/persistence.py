from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .types import LogEntry


def _atomic_write(path: Path, data: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


@dataclass
class PersistentState:
    current_term: int
    voted_for: str | None
    commit_index: int
    log: list[LogEntry]


class Persistence:
    """
    Хранение Raft состояния в двух файлах:
      - meta.json  (атомарно): term/vote/commit_index
      - wal.jsonl  (append-only): log entries, по одной JSON-строке на запись
    """

    def __init__(self, data_dir: Path):
        self.meta_path = data_dir / "meta.json"
        self.wal_path = data_dir / "wal.jsonl"

    def load(self) -> PersistentState:
        # 1) meta
        if self.meta_path.exists():
            raw = json.loads(self.meta_path.read_text(encoding="utf-8"))
            current_term = int(raw.get("current_term", 0))
            voted_for = raw.get("voted_for", None)
            commit_index = int(raw.get("commit_index", -1))
        else:
            current_term = 0
            voted_for = None
            commit_index = -1

        # 2) wal
        log: list[LogEntry] = []
        if self.wal_path.exists():
            # читаем построчно, JSONL
            with self.wal_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    log.append(LogEntry.model_validate(obj))

        return PersistentState(
            current_term=current_term,
            voted_for=voted_for,
            commit_index=commit_index,
            log=log,
        )

    def save_meta(self, *, current_term: int, voted_for: str | None, commit_index: int) -> None:
        obj: dict[str, Any] = {
            "current_term": current_term,
            "voted_for": voted_for,
            "commit_index": commit_index,
        }
        _atomic_write(self.meta_path, json.dumps(obj, ensure_ascii=False, indent=2))

    def append_log_entries(self, entries: Iterable[LogEntry]) -> None:
        # append-only: дописываем строки в конец
        if not entries:
            return
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.wal_path.open("a", encoding="utf-8") as f:
            for e in entries:
                f.write(json.dumps(e.model_dump(), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def rewrite_log(self, full_log: list[LogEntry]) -> None:
        """
        Полная пересборка WAL. Нужна, когда Raft обрезает хвост лога
        из-за конфликта (AppendEntries consistency check).
        """
        tmp = self.wal_path.with_suffix(self.wal_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for e in full_log:
                f.write(json.dumps(e.model_dump(), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(self.wal_path)

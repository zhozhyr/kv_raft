from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

from urllib.parse import urlparse

import httpx

logger = logging.getLogger("raft")

from app.core.config import Settings
from .persistence import Persistence, PersistentState
from .types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)


@dataclass
class LeaderHint:
    leader_id: str | None
    leader_url: str | None


class RaftNode:
    def __init__(self, settings: Settings):
        self.s = settings
        self.persist = Persistence(settings.data_dir)

        st = self.persist.load()
        self.current_term = st.current_term
        self.voted_for = st.voted_for
        self.log: list[LogEntry] = st.log
        self.commit_index = st.commit_index

        self.last_applied = -1
        self.kv: dict[str, str] = {}

        self.role: Role = "follower"
        self.leader_id: str | None = None
        self.leader_url: str | None = None

        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._apply_event = asyncio.Event()
        self._apply_task: asyncio.Task | None = None
        self._commit_waiters: dict[int, asyncio.Future[None]] = {}

        self._last_heartbeat = time.monotonic()
        self._election_task: asyncio.Task | None = None
        self._leader_task: asyncio.Task | None = None

        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._client = httpx.AsyncClient(timeout=self.s.rpc_timeout)


    async def close(self) -> None:
        self._stop.set()
        if self._election_task:
            self._election_task.cancel()
        if self._leader_task:
            self._leader_task.cancel()
        if self._apply_task:
            self._apply_task.cancel()
        await self._client.aclose()

    def _self_url(self) -> str:
        return f"http://{self.s.node_id}:{self.s.port}"

    def leader_hint(self) -> LeaderHint:
        return LeaderHint(self.leader_id, self.leader_url)

    async def resolve_leader_url(self) -> str | None:
        # 1) validate cached leader
        if self.leader_url:
            try:
                r = await self._client.get(f"{self.leader_url}/raft/state")
                if r.status_code == 200 and r.json().get("role") == "leader":
                    return self.leader_url
            except Exception:
                pass
            self.leader_url = None
            self.leader_id = None

        # 2) probe peers in parallel
        async def probe(peer: str):
            try:
                r = await self._client.get(f"{peer}/raft/state")
                if r.status_code == 200:
                    st = r.json()
                    if st.get("role") == "leader":
                        return peer, st.get("node_id")
            except Exception:
                return None
            return None

        results = await asyncio.gather(*(probe(p) for p in self.s.peers), return_exceptions=True)
        for res in results:
            if isinstance(res, tuple):
                peer, node_id = res
                self.leader_url = peer
                self.leader_id = node_id
                return peer

        return None

    def _persist_meta_now(self) -> None:
        self.persist.save_meta(
            current_term=self.current_term,
            voted_for=self.voted_for,
            commit_index=self.commit_index,
        )

    def _last_log_index(self) -> int:
        return len(self.log) - 1

    def _last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _election_timeout(self) -> float:
        return random.uniform(self.s.election_timeout_min, self.s.election_timeout_max)

    async def start_background_tasks(self) -> None:
        # Rebuild state machine from persisted log/commitIndex.
        if self.commit_index >= 0:
            logger.info(f"replay applied entries up to commit_index={self.commit_index} node={self.s.node_id}")
        self._apply_task = asyncio.create_task(self._apply_loop(), name="apply_loop")
        self._election_task = asyncio.create_task(self._election_loop(), name="election_loop")
        # Kick apply loop once on startup.
        self._apply_event.set()

    async def _election_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(0.05)
                async with self._lock:
                    if self.role == "leader":
                        continue
                    elapsed = time.monotonic() - self._last_heartbeat
                    timeout = getattr(self, "_cur_timeout", None)
                    if timeout is None:
                        self._cur_timeout = self._election_timeout()
                        timeout = self._cur_timeout
                    if elapsed < timeout:
                        continue
                await self._start_election()
        except asyncio.CancelledError:
            return

    async def _start_election(self) -> None:
        async with self._lock:
            self._last_heartbeat = time.monotonic()
            self._cur_timeout = self._election_timeout()

            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.s.node_id
            self._persist_meta_now()

            term = self.current_term
            self.leader_id = None
            self.leader_url = None

            last_index = self._last_log_index()
            last_term = self._last_log_term()

        votes = 1
        quorum = (len(self.s.peers) + 1) // 2 + 1

        req = RequestVoteRequest(
            term=term,
            candidate_id=self.s.node_id,
            last_log_index=last_index,
            last_log_term=last_term,
        )

        async def ask(peer: str) -> bool:
            try:
                r = await self._client.post(
                    f"{peer}/raft/request_vote",
                    json=req.model_dump(),
                    headers={"X-Sender": self._self_url()},
                )
                resp = RequestVoteResponse.model_validate(r.json())
            except Exception:
                return False

            async with self._lock:
                if resp.term > self.current_term:
                    self._become_follower(resp.term, None, None)
                    return False
                if self.role != "candidate" or self.current_term != term:
                    return False
            return resp.vote_granted

        results = await asyncio.gather(*(ask(p) for p in self.s.peers), return_exceptions=True)
        votes += sum(1 for x in results if x is True)

        async with self._lock:
            if self.role == "candidate" and self.current_term == term and votes >= quorum:
                self._become_leader()
            else:
                self.role = "follower"
                self._persist_meta_now()

    def _become_leader(self) -> None:
        self.role = "leader"
        self.leader_id = self.s.node_id
        self.leader_url = self._self_url()

        last_idx_plus_1 = len(self.log)
        self.next_index = {p: last_idx_plus_1 for p in self.s.peers}
        self.match_index = {p: -1 for p in self.s.peers}

        if self._leader_task:
            self._leader_task.cancel()
        self._leader_task = asyncio.create_task(self._leader_loop())

        self._persist_meta_now()

    def _become_follower(self, term: int, leader_id: str | None, leader_url: str | None) -> None:
        self.role = "follower"
        self.current_term = term
        self.voted_for = None
        self.leader_id = leader_id
        self.leader_url = leader_url
        self._last_heartbeat = time.monotonic()
        self._cur_timeout = self._election_timeout()

        self._persist_meta_now()

    async def _leader_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await self._send_heartbeats_and_replicate()
                await asyncio.sleep(self.s.heartbeat_interval)
        except asyncio.CancelledError:
            return

    async def _broadcast_commit_index(self) -> None:
        """
        After leader advances commit_index, immediately notify followers
        so they can apply committed entries (crucial for failover correctness).
        """
        async with self._lock:
            if self.role != "leader":
                return
            term = self.current_term
            leader_id = self.s.node_id
            leader_commit = self.commit_index

        async def notify(peer: str) -> None:
            async with self._lock:
                if self.role != "leader" or self.current_term != term:
                    return
                ni = self.next_index.get(peer, len(self.log))
                prev_idx = ni - 1
                prev_term = self.log[prev_idx].term if prev_idx >= 0 else 0

                req = AppendEntriesRequest(
                    term=term,
                    leader_id=leader_id,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=[],
                    leader_commit=leader_commit,
                )

            try:
                r = await self._client.post(f"{peer}/raft/append_entries", json=req.model_dump())
                resp = AppendEntriesResponse.model_validate(r.json())
            except Exception:
                return

            async with self._lock:
                if resp.term > self.current_term:
                    self._become_follower(resp.term, leader_id=None, leader_url=None)

        await asyncio.gather(*(notify(p) for p in self.s.peers))

    async def _send_heartbeats_and_replicate(self) -> None:
        async with self._lock:
            if self.role != "leader":
                return
            term = self.current_term
            leader_id = self.s.node_id

        async def replicate_to(peer: str) -> None:
            async with self._lock:
                if self.role != "leader" or self.current_term != term:
                    return

                ni = self.next_index.get(peer, len(self.log))
                prev_idx = ni - 1
                prev_term = self.log[prev_idx].term if prev_idx >= 0 else 0
                entries = self.log[ni:]

                req = AppendEntriesRequest(
                    term=term,
                    leader_id=leader_id,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=entries,
                    leader_commit=self.commit_index,
                )

            try:
                r = await self._client.post(f"{peer}/raft/append_entries", json=req.model_dump())
                resp = AppendEntriesResponse.model_validate(r.json())
            except Exception:
                return

            async with self._lock:
                if resp.term > self.current_term:
                    self._become_follower(resp.term, leader_id=None, leader_url=None)
                    return
                if self.role != "leader" or self.current_term != term:
                    return

                if resp.success:
                    self.match_index[peer] = req.prev_log_index + len(req.entries)
                    self.next_index[peer] = self.match_index[peer] + 1
                    return

                # fail -> просто подвинем next_index, а ретрай будет на следующем heartbeat tick
                if resp.conflict_term is not None and resp.conflict_index is not None:
                    ct = resp.conflict_term
                    last = -1
                    for i in range(len(self.log) - 1, -1, -1):
                        if self.log[i].term == ct:
                            last = i
                            break
                    self.next_index[peer] = (last + 1) if last != -1 else resp.conflict_index
                elif resp.conflict_index is not None:
                    self.next_index[peer] = resp.conflict_index
                else:
                    cur = self.next_index.get(peer, len(self.log))
                    self.next_index[peer] = max(0, cur - 1)

        await asyncio.gather(*(replicate_to(p) for p in self.s.peers))

        # try advance commit_index (majority match_index)
        advanced = False
        async with self._lock:
            if self.role != "leader":
                return

            match = [self._last_log_index()] + [self.match_index.get(p, -1) for p in self.s.peers]
            match.sort()
            quorum = (len(self.s.peers) + 1) // 2 + 1
            N = match[-quorum]

            # Only commit entries from current term (Raft safety rule)
            if N > self.commit_index and N >= 0 and self.log[N].term == self.current_term:
                old_ci = self.commit_index
                self.commit_index = N
                logger.info(f"commit_index_advanced term={self.current_term} node={self.s.node_id} old={old_ci} new={self.commit_index}")
                self._notify_commit_waiters_locked()
                self._persist_meta_now()
                advanced = True

        self._apply_event.set()

        if advanced:
            await self._broadcast_commit_index()

    async def _apply_loop(self) -> None:
        """Применяет committed-записи лога к state machine (KV).

        Разделение ответственности (Raft):
        - commitIndex меняется только модулем консенсуса;
        - lastApplied двигается только здесь;
        - state machine мутируется только здесь.

        Цикл event-driven: при изменении commitIndex выставляется _apply_event.
        """
        try:
            while not self._stop.is_set():
                await self._apply_event.wait()
                self._apply_event.clear()

                # Apply as much as we can in one go.
                while True:
                    async with self._lock:
                        if self.last_applied >= self.commit_index:
                            break
                        self.last_applied += 1
                        idx = self.last_applied
                        entry = self.log[idx]

                    # Apply outside the lock would be nice, but our state machine is
                    # an in-memory dict and tests expect simple semantics; keep it simple.
                    async with self._lock:
                        if entry.command == "put":
                            assert entry.value is not None
                            self.kv[entry.key] = entry.value
                        else:
                            self.kv.pop(entry.key, None)

                        logger.info(f"apply_index term={self.current_term} node={self.s.node_id} idx={idx}")

                        # Wake any propose() waiters.
                        fut = self._commit_waiters.pop(idx, None)
                        if fut and not fut.done():
                            fut.set_result(None)

                        # Persist *Raft state* (term/vote/log/commitIndex). lastApplied is volatile.
                        self._persist_meta_now()
        except asyncio.CancelledError:
            return

    def _notify_commit_waiters_locked(self) -> None:
        """Complete any commit waiters whose index is now committed.

        Must be called under self._lock.
        """
        ready = [i for i in self._commit_waiters.keys() if i <= self.commit_index]
        for i in ready:
            fut = self._commit_waiters.pop(i, None)
            if fut and not fut.done():
                fut.set_result(None)


    # ---------- RPC handlers ----------


    async def on_request_vote(self, req: RequestVoteRequest, sender_url: str | None) -> RequestVoteResponse:
        async with self._lock:
            if req.term < self.current_term:
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

            if req.term > self.current_term:
                self._become_follower(req.term, None, None)

            up_to_date = (
                req.last_log_term > self._last_log_term()
                or (
                    req.last_log_term == self._last_log_term()
                    and req.last_log_index >= self._last_log_index()
                )
            )

            if (self.voted_for in (None, req.candidate_id)) and up_to_date:
                self.voted_for = req.candidate_id

                self._last_heartbeat = time.monotonic()
                self._cur_timeout = self._election_timeout()

                self._persist_meta_now()
                return RequestVoteResponse(term=self.current_term, vote_granted=True)

            return RequestVoteResponse(term=self.current_term, vote_granted=False)

    async def on_append_entries(self, req: AppendEntriesRequest, sender_url: str | None) -> AppendEntriesResponse:
        async with self._lock:
            if req.term < self.current_term:
                return AppendEntriesResponse(term=self.current_term, success=False)

            if req.term > self.current_term or self.role != "follower":
                self._become_follower(req.term, req.leader_id, sender_url)
            else:
                self.leader_id = req.leader_id
                self.leader_url = sender_url

            self._last_heartbeat = time.monotonic()
            self._cur_timeout = self._election_timeout()

            # consistency check
            if req.prev_log_index >= 0:
                if req.prev_log_index >= len(self.log):
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        conflict_index=len(self.log),
                    )
                if self.log[req.prev_log_index].term != req.prev_log_term:
                    ct = self.log[req.prev_log_index].term
                    first = req.prev_log_index
                    while first > 0 and self.log[first - 1].term == ct:
                        first -= 1
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        conflict_index=first,
                        conflict_term=ct,
                    )

            truncated = False
            new_entries: list[LogEntry] = []

            idx = req.prev_log_index + 1
            for e in req.entries:
                if idx < len(self.log) and self.log[idx].term != e.term:
                    self.log = self.log[:idx]
                    truncated = True
                if idx == len(self.log):
                    self.log.append(e)
                    new_entries.append(e)
                idx += 1

            # persistence
            if truncated:
                # если резали хвост, проще и надёжнее пересобрать WAL целиком
                self.persist.rewrite_log(self.log)
            elif new_entries:
                # если только дописали - append-only
                self.persist.append_log_entries(new_entries)

            self._persist_meta_now()

            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log) - 1)

            self._persist_meta_now()

        self._apply_event.set()
        return AppendEntriesResponse(term=self.current_term, success=True)

    # ---------- Client-facing operations (leader only) ----------

    async def propose(self, entry: LogEntry) -> bool:
        """Реплицирует команду клиента через Raft и ждёт коммита большинством.

        Возвращает True только когда индекс записи стал committed (commitIndex продвинут
        на кворуме). Возвращает False при таймауте или потере лидерства.
        """
        async with self._lock:
            if self.role != "leader":
                return False

            entry = LogEntry(term=self.current_term, command=entry.command, key=entry.key, value=entry.value)
            self.log.append(entry)
            idx = len(self.log) - 1

            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._commit_waiters[idx] = fut
            self.persist.append_log_entries([entry])
            self._persist_meta_now()

        await self._send_heartbeats_and_replicate()

        timeout = max(self.s.rpc_timeout * 2.0, self.s.heartbeat_interval * 5.0)
        try:
            await asyncio.wait_for(fut, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            async with self._lock:
                self._commit_waiters.pop(idx, None)
            return False
        except asyncio.CancelledError:
            raise
        except Exception:
            async with self._lock:
                self._commit_waiters.pop(idx, None)
            return False

    async def get_value(self, key: str) -> Optional[str]:
        """
        Чтение (только на лидере).

        Перед тем как отдать значение, лидер подтверждает лидерство в текущем term
        через heartbeat-ACK от большинства (минимальный ReadIndex).
        Это защищает от "устаревших" чтений после потери лидерства.
        """
        async with self._lock:
            if self.role != "leader":
                return None
            term = self.current_term

        ok = await self._confirm_leadership(term)
        if not ok:
            async with self._lock:
                if self.role == "leader" and self.current_term == term:
                    self._become_follower(self.current_term, leader_id=None, leader_url=None)
            return None

        while True:
            async with self._lock:
                if self.last_applied >= self.commit_index:
                    break
            self._apply_event.set()
            await asyncio.sleep(0)

        async with self._lock:
            return self.kv.get(key)


    async def _confirm_leadership(self, term: int) -> bool:
        """Подтверждает, что этот узел *всё ещё лидер* в заданном term."""
        async with self._lock:
            if self.role != "leader" or self.current_term != term:
                return False
            leader_id = self.s.node_id
            leader_commit = self.commit_index
            log_snapshot = list(self.log)
            next_index_snapshot = dict(self.next_index)

        quorum = (len(self.s.peers) + 1) // 2 + 1

        async def ping(peer: str) -> bool:
            ni = next_index_snapshot.get(peer, len(log_snapshot))
            prev_idx = ni - 1
            prev_term = log_snapshot[prev_idx].term if prev_idx >= 0 else 0

            req = AppendEntriesRequest(
                term=term,
                leader_id=leader_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=[],
                leader_commit=leader_commit,
            )

            try:
                r = await self._client.post(f"{peer}/raft/append_entries", json=req.model_dump())
                resp = AppendEntriesResponse.model_validate(r.json())
            except Exception:
                return False

            if resp.term > term:
                async with self._lock:
                    if resp.term > self.current_term:
                        self._become_follower(resp.term, leader_id=None, leader_url=None)
                return False
            return resp.success

        results = await asyncio.gather(*(ping(p) for p in self.s.peers))
        acks = 1 + sum(1 for x in results if x)
        return acks >= quorum

    def _infer_peer_url_by_id(self, peer_id: str | None) -> str | None:
        if not peer_id:
            return None

        for u in self.s.peers:
            try:
                if urlparse(u).hostname == peer_id:
                    return u
            except Exception:
                continue
        return None

    async def debug_state(self) -> dict:
        async with self._lock:
            leader_url = self.leader_url

            if leader_url is None and self.leader_id is not None:
                leader_url = self._infer_peer_url_by_id(self.leader_id)

            peers = list(self.s.peers)

            base = {
                "node_id": self.s.node_id,
                "role": self.role,
                "term": self.current_term,
                "voted_for": self.voted_for,
                "leader_id": self.leader_id,
                "leader_url": leader_url,
                "log_len": len(self.log),
                "last_log_index": self._last_log_index(),
                "last_log_term": self._last_log_term(),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "peers": peers,
            }

            if self.role == "leader":
                base["next_index"] = dict(self.next_index)
                base["match_index"] = dict(self.match_index)

        base["active_peers"] = await self._detect_active_peers(peers)
        return base

    async def _detect_active_peers(self, peers: list[str]) -> list[str]:
        """
        Возвращает peers, которые сейчас отвечают на /health.

        Используется только для наблюдаемости в /raft/state.
        """
        if not peers:
            return []
        timeout_s = min(0.3, max(0.05, self.s.rpc_timeout * 0.25))

        async def _probe(client: httpx.AsyncClient, url: str) -> bool:
            try:
                r = await client.get(f"{url.rstrip('/')}/health")
                return r.status_code == 200
            except Exception:
                return False

        async with httpx.AsyncClient(timeout=timeout_s) as client:
            results = await asyncio.gather(*[_probe(client, p) for p in peers], return_exceptions=True)

        active: list[str] = []
        for peer, ok in zip(peers, results):
            if ok is True:
                active.append(peer)
        return active


    async def debug_log(self, frm: int = 0, to: int | None = None) -> list[dict]:
        """Возвращает срез лога для отладки (/raft/log)."""
        async with self._lock:
            if frm < 0:
                frm = 0
            if to is None or to > len(self.log):
                to = len(self.log)
                out = []
            for i in range(frm, to):
                e = self.log[i]
                out.append({"index": i, "term": e.term, "command": e.command, "key": e.key, "value": e.value})
            return out

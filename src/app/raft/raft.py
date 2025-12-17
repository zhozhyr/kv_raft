from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Optional

import httpx

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
        self.last_applied = st.last_applied
        self.kv: dict[str, str] = st.kv

        self.role: Role = "follower"
        self.leader_id: str | None = None
        self.leader_url: str | None = None

        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()

        self._last_heartbeat = time.monotonic()
        self._election_task: asyncio.Task | None = None
        self._leader_task: asyncio.Task | None = None

        # leader-only replication state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._client = httpx.AsyncClient(timeout=self.s.rpc_timeout)

    async def close(self) -> None:
        self._stop.set()
        if self._election_task:
            self._election_task.cancel()
        if self._leader_task:
            self._leader_task.cancel()
        await self._client.aclose()

    def leader_hint(self) -> LeaderHint:
        return LeaderHint(self.leader_id, self.leader_url)

    async def resolve_leader_url(self) -> str | None:
        """Best-effort leader discovery for client request forwarding.

        If this node is not a leader, we may still want to accept client write
        requests and transparently forward them to the current leader.

        Strategy:
        1) Use the last known leader URL (from heartbeats/AppendEntries), if any.
        2) Otherwise, probe peers' /raft/state and pick the one that reports
           itself as leader.

        Returns leader base URL like "http://node2:8000" or None if unknown.
        """

        # Fast path: we already know.
        if self.leader_url:
            return self.leader_url

        # Probe peers. This is best-effort and should not block forever.
        for peer in self.s.peers:
            try:
                r = await self._client.get(f"{peer}/raft/state")
                if r.status_code != 200:
                    continue
                st = r.json()
                if st.get("role") == "leader":
                    # Cache for future forwards.
                    self.leader_url = peer
                    self.leader_id = st.get("node_id")
                    return peer
            except Exception:
                continue

        return None

    def _persist_now(self) -> None:
        st = PersistentState(
            current_term=self.current_term,
            voted_for=self.voted_for,
            log=self.log,
            commit_index=self.commit_index,
            last_applied=self.last_applied,
            kv=self.kv,
        )
        self.persist.save(st)

    def _last_log_index(self) -> int:
        return len(self.log) - 1

    def _last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _election_timeout(self) -> float:
        return random.uniform(self.s.election_timeout_min, self.s.election_timeout_max)

    async def start_background_tasks(self) -> None:
        self._election_task = asyncio.create_task(self._election_loop(), name="election_loop")

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
            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.s.node_id
            self._persist_now()

            term = self.current_term
            self.leader_id = None
            self.leader_url = None
            self._cur_timeout = self._election_timeout()
            self._last_heartbeat = time.monotonic()

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
                r = await self._client.post(f"{peer}/raft/request_vote", json=req.model_dump())
                resp = RequestVoteResponse.model_validate(r.json())
            except Exception:
                return False

            async with self._lock:
                if resp.term > self.current_term:
                    self._become_follower(resp.term, leader_id=None, leader_url=None)
                    return False
                if self.role != "candidate" or self.current_term != term:
                    return False
            return resp.vote_granted

        results = await asyncio.gather(*(ask(p) for p in self.s.peers), return_exceptions=True)
        votes += sum(1 for x in results if x is True)

        async with self._lock:
            if self.role != "candidate" or self.current_term != term:
                return
            if votes >= quorum:
                self._become_leader()
            else:
                self.role = "follower"
                self._persist_now()

    def _become_follower(self, new_term: int, leader_id: str | None, leader_url: str | None) -> None:
        self.role = "follower"
        self.current_term = new_term
        self.voted_for = None
        self.leader_id = leader_id
        self.leader_url = leader_url
        self._last_heartbeat = time.monotonic()
        self._cur_timeout = self._election_timeout()
        self._persist_now()

    def _become_leader(self) -> None:
        self.role = "leader"
        self.leader_id = self.s.node_id
        self.leader_url = None

        last_idx_plus_1 = len(self.log)
        self.next_index = {p: last_idx_plus_1 for p in self.s.peers}
        self.match_index = {p: -1 for p in self.s.peers}

        if self._leader_task:
            self._leader_task.cancel()
        self._leader_task = asyncio.create_task(self._leader_loop(), name="leader_loop")
        self._persist_now()

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
            while True:
                async with self._lock:
                    if self.role != "leader" or self.current_term != term:
                        return

                    ni = self.next_index.get(peer, len(self.log))
                    prev_idx = ni - 1
                    prev_term = self.log[prev_idx].term if prev_idx >= 0 else 0
                    entries = self.log[ni:]

                    # IMPORTANT: leader_commit must be read fresh each attempt
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
                    else:
                        cur = self.next_index.get(peer, len(self.log))
                        self.next_index[peer] = max(0, cur - 1)
                        # retry

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
                self.commit_index = N
                self._persist_now()
                advanced = True

        await self._apply_committed()

        # CRUCIAL: notify followers commit index immediately
        if advanced:
            await self._broadcast_commit_index()

    async def _apply_committed(self) -> None:
        async with self._lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied]
                if entry.command == "put":
                    assert entry.value is not None
                    self.kv[entry.key] = entry.value
                else:
                    self.kv.pop(entry.key, None)
            self._persist_now()

    # ---------- RPC handlers ----------

    async def on_request_vote(self, req: RequestVoteRequest, sender_url: str | None) -> RequestVoteResponse:
        async with self._lock:
            if req.term < self.current_term:
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

            if req.term > self.current_term:
                self._become_follower(req.term, leader_id=None, leader_url=None)

            up_to_date = (req.last_log_term > self._last_log_term()) or (
                req.last_log_term == self._last_log_term() and req.last_log_index >= self._last_log_index()
            )
            can_vote = (self.voted_for is None) or (self.voted_for == req.candidate_id)

            if can_vote and up_to_date:
                self.voted_for = req.candidate_id
                self._last_heartbeat = time.monotonic()
                self._cur_timeout = self._election_timeout()
                self._persist_now()
                return RequestVoteResponse(term=self.current_term, vote_granted=True)

            return RequestVoteResponse(term=self.current_term, vote_granted=False)

    async def on_append_entries(self, req: AppendEntriesRequest, sender_url: str | None) -> AppendEntriesResponse:
        async with self._lock:
            if req.term < self.current_term:
                return AppendEntriesResponse(term=self.current_term, success=False)

            if req.term > self.current_term or self.role != "follower":
                self._become_follower(req.term, leader_id=req.leader_id, leader_url=sender_url)
            else:
                self.leader_id = req.leader_id
                self.leader_url = sender_url

            self._last_heartbeat = time.monotonic()
            self._cur_timeout = self._election_timeout()

            # consistency check
            if req.prev_log_index >= 0:
                if req.prev_log_index >= len(self.log):
                    return AppendEntriesResponse(term=self.current_term, success=False, conflict_index=len(self.log))
                if self.log[req.prev_log_index].term != req.prev_log_term:
                    self.log = self.log[: req.prev_log_index]
                    self._persist_now()
                    return AppendEntriesResponse(term=self.current_term, success=False, conflict_index=req.prev_log_index)

            # append new entries, overwriting conflicts
            idx = req.prev_log_index + 1
            for e in req.entries:
                if idx < len(self.log):
                    if self.log[idx].term != e.term:
                        self.log = self.log[:idx]
                        self.log.append(e)
                else:
                    self.log.append(e)
                idx += 1

            # update commit index
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log) - 1)

            self._persist_now()

        await self._apply_committed()
        return AppendEntriesResponse(term=self.current_term, success=True)

    # ---------- Client-facing operations (leader only) ----------

    async def propose(self, entry: LogEntry) -> bool:
        async with self._lock:
            if self.role != "leader":
                return False
            entry = LogEntry(term=self.current_term, command=entry.command, key=entry.key, value=entry.value)
            self.log.append(entry)
            self._persist_now()

        # replicate immediately
        await self._send_heartbeats_and_replicate()

        async with self._lock:
            return self.commit_index >= (len(self.log) - 1)

    async def get_value(self, key: str) -> Optional[str]:
        async with self._lock:
            return self.kv.get(key)

    async def debug_state(self) -> dict:
        async with self._lock:
            return {
                "node_id": self.s.node_id,
                "role": self.role,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "leader_url": self.leader_url,
                "log_len": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "peers": self.s.peers,
            }

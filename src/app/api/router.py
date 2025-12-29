from __future__ import annotations

import time
import logging

import httpx
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse

from app.raft.raft import RaftNode
from app.raft.types import (
    AppendEntriesRequest,
    PutValue,
    RedirectHint,
    RequestVoteRequest,
    LogEntry,
)


logger = logging.getLogger("api.forward")


def build_router(node: RaftNode) -> APIRouter:
    r = APIRouter()
    @r.get("/health")
    async def health():
        return {"ok": True, "node_id": node.s.node_id}

    async def _forward_to_leader(method: str, path: str, json: dict | None = None):
        leader_url = await node.resolve_leader_url()
        hint = node.leader_hint()

        leader_id = hint.leader_id
        leader_url_hint = hint.leader_url or leader_url

        if not leader_url:
            logger.warning(
                "forward_failed_no_leader method=%s path=%s hint_leader_id=%s hint_leader_url=%s",
                 method, path, leader_id, leader_url_hint,
            )
            raise HTTPException(
                status_code=409,
                detail=RedirectHint(
                    leader_id=leader_id,
                    leader_url=leader_url_hint,
                    message="лидер неизвестен; попробуйте ещё раз чуть позже",
                ).model_dump(),
            )

        url = f"{leader_url}{path}"

        logger.info(
            "forward_to_leader leader_id=%s leader_url=%s method=%s path=%s",
             leader_id, leader_url, method, path,
        )

        t0 = time.perf_counter()
        try:
            forward_timeout = max(node.s.rpc_timeout * 2.5, node.s.heartbeat_interval * 20.0)
            async with httpx.AsyncClient(timeout=forward_timeout) as client:
                resp = await client.request(method, url, json=json)

        except httpx.HTTPError as e:
            dt_ms = (time.perf_counter() - t0) * 1000
            logger.warning(
                "forward_failed_http leader_id=%s leader_url=%s method=%s path=%s dt_ms=%.1f err=%r",
                leader_id, leader_url, method, path, dt_ms, e,
            )
            raise HTTPException(status_code=503, detail=f"не удалось достучаться до лидера: {e}")

        dt_ms = (time.perf_counter() - t0) * 1000

        # Пробрасываем ответ лидера как есть.
        try:
            payload = resp.json()
        except Exception:
            payload = {"detail": resp.text}

        logger.info(
            "forward_done leader_id=%s method=%s path=%s status=%s dt_ms=%.1f",
            leader_id, method, path, resp.status_code, dt_ms,
        )

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload.get("detail", payload))
        return payload

    @r.get("/kv/{key}")
    async def kv_get(key: str):
        if node.role != "leader":
            return await _forward_to_leader("GET", f"/kv/{key}")
        v = await node.get_value(key)
        if v is None:
            raise HTTPException(status_code=404, detail="ключ не найден")
        return {"key": key, "value": v}

    @r.put("/kv/{key}")
    async def kv_put(key: str, body: PutValue):
        if node.role != "leader":
            return await _forward_to_leader("PUT", f"/kv/{key}", json={"value": body.value})
        ok = await node.propose(LogEntry(term=node.current_term, command="put", key=key, value=body.value))
        if not ok:
            raise HTTPException(status_code=503, detail="не удалось закоммитить (нет кворума?)")
        return {"ok": True}

    @r.delete("/kv/{key}")
    async def kv_delete(key: str):
        if node.role != "leader":
            return await _forward_to_leader("DELETE", f"/kv/{key}")
        ok = await node.propose(LogEntry(term=node.current_term, command="delete", key=key, value=None))
        if not ok:
            raise HTTPException(status_code=503, detail="не удалось закоммитить (нет кворума?)")
        return {"ok": True}

    @r.post("/raft/request_vote")
    async def raft_request_vote(req: RequestVoteRequest, x_sender: str | None = Header(default=None)):
        resp = await node.on_request_vote(req, sender_url=x_sender)
        return JSONResponse(resp.model_dump())

    @r.post("/raft/append_entries")
    async def raft_append_entries(req: AppendEntriesRequest, x_sender: str | None = Header(default=None)):
        resp = await node.on_append_entries(req, sender_url=x_sender)
        return JSONResponse(resp.model_dump())

    @r.get("/raft/log")
    async def raft_log(from_: int = 0, to: int | None = None):
        return await node.debug_log(frm=from_, to=to)

    @r.get("/raft/state")
    async def raft_state():
        return await node.debug_state()


    @r.get("/metrics")
    async def metrics():
        st = await node.debug_state()
        lines = [
            f'raft_term{{node="{st["node_id"]}"}} {st["term"]}',
            f'raft_commit_index{{node="{st["node_id"]}"}} {st["commit_index"]}',
            f'raft_last_applied{{node="{st["node_id"]}"}} {st["last_applied"]}',
            f'raft_log_len{{node="{st["node_id"]}"}} {st["log_len"]}',
        ]
        return "\n".join(lines) + "\n"

    return r

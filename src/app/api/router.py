from __future__ import annotations

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


def build_router(node: RaftNode) -> APIRouter:
    r = APIRouter()

    async def _forward_to_leader(method: str, path: str, json: dict | None = None):
        """Forward a client request to the current leader.

        We keep this logic at the API layer so the rest of the RAFT code remains
        focused on consensus.
        """

        leader_url = await node.resolve_leader_url()
        if not leader_url:
            hint = node.leader_hint()
            raise HTTPException(
                status_code=409,
                detail=RedirectHint(
                    leader_id=hint.leader_id,
                    leader_url=hint.leader_url,
                    message="leader is unknown; retry in a moment",
                ).model_dump(),
            )

        url = f"{leader_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=node.s.rpc_timeout) as client:
                resp = await client.request(method, url, json=json)
        except httpx.HTTPError as e:
            raise HTTPException(status_code=503, detail=f"failed to reach leader: {e}")

        # Pass-through leader response.
        try:
            payload = resp.json()
        except Exception:
            payload = {"detail": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload.get("detail", payload))
        return payload

    @r.get("/kv/{key}")
    async def kv_get(key: str):
        v = await node.get_value(key)
        if v is None:
            raise HTTPException(status_code=404, detail="key not found")
        return {"key": key, "value": v}

    @r.put("/kv/{key}")
    async def kv_put(key: str, body: PutValue):
        if node.role != "leader":
            # Transparent redirect: accept writes on any node and forward.
            return await _forward_to_leader("PUT", f"/kv/{key}", json={"value": body.value})
        ok = await node.propose(LogEntry(term=node.current_term, command="put", key=key, value=body.value))
        if not ok:
            raise HTTPException(status_code=503, detail="failed to commit (no quorum?)")
        return {"ok": True}

    @r.delete("/kv/{key}")
    async def kv_delete(key: str):
        if node.role != "leader":
            return await _forward_to_leader("DELETE", f"/kv/{key}")
        ok = await node.propose(LogEntry(term=node.current_term, command="delete", key=key, value=None))
        if not ok:
            raise HTTPException(status_code=503, detail="failed to commit (no quorum?)")
        return {"ok": True}

    @r.post("/raft/request_vote")
    async def raft_request_vote(req: RequestVoteRequest, x_sender: str | None = Header(default=None)):
        resp = await node.on_request_vote(req, sender_url=x_sender)
        return JSONResponse(resp.model_dump())

    @r.post("/raft/append_entries")
    async def raft_append_entries(req: AppendEntriesRequest, x_sender: str | None = Header(default=None)):
        resp = await node.on_append_entries(req, sender_url=x_sender)
        return JSONResponse(resp.model_dump())

    @r.get("/raft/state")
    async def raft_state():
        return await node.debug_state()

    return r

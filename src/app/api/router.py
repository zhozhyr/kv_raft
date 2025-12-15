from __future__ import annotations

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

    @r.get("/kv/{key}")
    async def kv_get(key: str):
        v = await node.get_value(key)
        if v is None:
            raise HTTPException(status_code=404, detail="key not found")
        return {"key": key, "value": v}

    @r.put("/kv/{key}")
    async def kv_put(key: str, body: PutValue):
        if node.role != "leader":
            hint = node.leader_hint()
            raise HTTPException(
                status_code=409,
                detail=RedirectHint(
                    leader_id=hint.leader_id,
                    leader_url=hint.leader_url,
                    message="not a leader; send writes to leader",
                ).model_dump(),
            )
        ok = await node.propose(LogEntry(term=node.current_term, command="put", key=key, value=body.value))
        if not ok:
            raise HTTPException(status_code=503, detail="failed to commit (no quorum?)")
        return {"ok": True}

    @r.delete("/kv/{key}")
    async def kv_delete(key: str):
        if node.role != "leader":
            hint = node.leader_hint()
            raise HTTPException(
                status_code=409,
                detail=RedirectHint(
                    leader_id=hint.leader_id,
                    leader_url=hint.leader_url,
                    message="not a leader; send writes to leader",
                ).model_dump(),
            )
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

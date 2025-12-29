from __future__ import annotations

import logging

from fastapi import FastAPI

from app.core.config import Settings
from app.raft.raft import RaftNode
from app.api.router import build_router


def create_app() -> FastAPI:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    s = Settings.load()
    node = RaftNode(s)

    app = FastAPI(title="kv-raft", version="0.1.0")
    app.state.raft = node
    app.include_router(build_router(node))

    @app.on_event("startup")
    async def _startup():
        await node.start_background_tasks()

    @app.on_event("shutdown")
    async def _shutdown():
        await node.close()

    return app


app = create_app()

from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field


Role = Literal["follower", "candidate", "leader"]


class LogEntry(BaseModel):
    term: int
    command: Literal["put", "delete"]
    key: str
    value: Optional[str] = None


class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class RequestVoteResponse(BaseModel):
    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = Field(default_factory=list)
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    conflict_index: int | None = None


class PutValue(BaseModel):
    value: str


class RedirectHint(BaseModel):
    leader_id: str | None
    leader_url: str | None
    message: str

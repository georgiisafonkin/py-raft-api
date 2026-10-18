from pydantic import BaseModel, Field

from raft_api.commands import LogEntry


class RequestVoteArgs(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class RequestVoteReply(BaseModel):
    term: int
    vote_granted: bool


class AppendEntriesArgs(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = Field(default_factory=list)
    leader_commit: int


class AppendEntriesReply(BaseModel):
    term: int
    success: bool

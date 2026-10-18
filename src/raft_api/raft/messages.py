from dataclasses import dataclass

from raft_api.raft.rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


@dataclass(frozen=True, slots=True)
class ElectionTimeout:
    pass


@dataclass(frozen=True, slots=True)
class HeartbeatTick:
    pass


@dataclass(frozen=True, slots=True)
class RequestVoteRpc:
    args: RequestVoteArgs


@dataclass(frozen=True, slots=True)
class AppendEntriesRpc:
    args: AppendEntriesArgs


@dataclass(frozen=True, slots=True)
class VoteReplyMsg:
    peer_id: str
    term: int
    reply: RequestVoteReply


@dataclass(frozen=True, slots=True)
class AppendReplyMsg:
    peer_id: str
    term: int
    reply: AppendEntriesReply


@dataclass(frozen=True, slots=True)
class StatusQuery:
    pass


Message = (
    ElectionTimeout
    | HeartbeatTick
    | RequestVoteRpc
    | AppendEntriesRpc
    | VoteReplyMsg
    | AppendReplyMsg
    | StatusQuery
)

from typing import Protocol

from raft_api.raft.rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class RaftTransport(Protocol):
    async def request_vote(self, peer_id: str, args: RequestVoteArgs) -> RequestVoteReply: ...

    async def append_entries(self, peer_id: str, args: AppendEntriesArgs) -> AppendEntriesReply: ...

    async def aclose(self) -> None: ...

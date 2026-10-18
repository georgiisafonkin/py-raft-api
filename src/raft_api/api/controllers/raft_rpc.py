from litestar import Controller, Request, post

from raft_api.raft.messages import AppendEntriesRpc, RequestVoteRpc
from raft_api.raft.node import RaftNode
from raft_api.raft.rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class RaftRpcController(Controller):
    path = "/raft"
    tags = ["internal"]

    @post("/request-vote", status_code=200)
    async def request_vote(self, request: Request, data: RequestVoteArgs) -> RequestVoteReply:
        node: RaftNode = request.app.state.node
        reply: RequestVoteReply = await node.submit(RequestVoteRpc(data))
        return reply

    @post("/append-entries", status_code=200)
    async def append_entries(
        self,
        request: Request,
        data: AppendEntriesArgs,
    ) -> AppendEntriesReply:
        node: RaftNode = request.app.state.node
        reply: AppendEntriesReply = await node.submit(AppendEntriesRpc(data))
        return reply

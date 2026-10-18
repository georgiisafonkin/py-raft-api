import asyncio

from raft_api.raft.messages import AppendEntriesRpc, RequestVoteRpc
from raft_api.raft.node import RaftNode
from raft_api.raft.rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class MemoryNetwork:
    """In-process links between nodes, with directed partitions."""

    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.blocked: set[tuple[str, str]] = set()
        self._resume: asyncio.Event | None = None

    def pause(self) -> None:
        self._resume = asyncio.Event()

    def resume(self) -> None:
        if self._resume is not None:
            self._resume.set()
            self._resume = None

    def register(self, node_id: str, node: RaftNode) -> None:
        self.nodes[node_id] = node

    def disconnect(self, left: str, right: str) -> None:
        self.blocked.add((left, right))
        self.blocked.add((right, left))


class MemoryTransport:
    def __init__(self, network: MemoryNetwork, node_id: str) -> None:
        self._network = network
        self._node_id = node_id

    async def request_vote(self, peer_id: str, args: RequestVoteArgs) -> RequestVoteReply:
        await self._wait_until_open()
        node = self._peer(peer_id)
        reply = await node.submit(RequestVoteRpc(args))
        if not isinstance(reply, RequestVoteReply):
            raise TypeError(f"unexpected vote reply from {peer_id}")
        return reply

    async def append_entries(self, peer_id: str, args: AppendEntriesArgs) -> AppendEntriesReply:
        await self._wait_until_open()
        node = self._peer(peer_id)
        reply = await node.submit(AppendEntriesRpc(args))
        if not isinstance(reply, AppendEntriesReply):
            raise TypeError(f"unexpected append reply from {peer_id}")
        return reply

    async def aclose(self) -> None:
        return None

    async def _wait_until_open(self) -> None:
        resume = self._network._resume
        if resume is not None:
            await resume.wait()

    def _peer(self, peer_id: str) -> RaftNode:
        if (self._node_id, peer_id) in self._network.blocked:
            raise ConnectionError(f"partition between {self._node_id} and {peer_id}")
        node = self._network.nodes.get(peer_id)
        if node is None:
            raise ConnectionError(f"node {peer_id} is not registered")
        return node

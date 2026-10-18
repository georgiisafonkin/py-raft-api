import asyncio
import logging
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

from raft_api.clock import Clock
from raft_api.config import Timeouts
from raft_api.raft.log import Log
from raft_api.raft.messages import (
    AppendEntriesRpc,
    AppendReplyMsg,
    ElectionTimeout,
    HeartbeatTick,
    Message,
    RequestVoteRpc,
    StatusQuery,
    VoteReplyMsg,
)
from raft_api.raft.rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)
from raft_api.storage.base import PersistentStore
from raft_api.transport.base import RaftTransport

logger = logging.getLogger(__name__)


class Role(StrEnum):
    follower = "follower"
    candidate = "candidate"
    leader = "leader"


class NodeStatus(BaseModel):
    node_id: str
    role: Role
    term: int
    leader_id: str | None
    commit_index: int
    last_applied: int
    last_log_index: int
    peers: list[str]


class RaftNode:
    """Single-threaded Raft actor. Callers only enqueue messages."""

    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        *,
        clock: Clock,
        store: PersistentStore,
        transport: RaftTransport,
        timeouts: Timeouts,
    ) -> None:
        self.node_id = node_id
        self.peer_ids = list(peer_ids)
        self._clock = clock
        self._store = store
        self._transport = transport
        self._timeouts = timeouts
        self.role = Role.follower
        self.current_term = 0
        self.voted_for: str | None = None
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0
        self._log = Log()
        self._votes: set[str] = set()
        self._inbox: asyncio.Queue[tuple[Message, asyncio.Future[Any]]] = asyncio.Queue()
        self._run_task: asyncio.Task[None] | None = None
        self._election_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopped = False

    @property
    def quorum(self) -> int:
        return (1 + len(self.peer_ids)) // 2 + 1

    async def start(self) -> None:
        state = await self._store.load()
        self.current_term = state.current_term
        self.voted_for = state.voted_for
        self.commit_index = state.commit_index
        self._log = Log(state.log)
        self._run_task = asyncio.create_task(self._run(), name=f"raft-{self.node_id}")
        self._arm_election()

    async def stop(self) -> None:
        self._stopped = True
        self._cancel(self._election_task)
        self._cancel(self._heartbeat_task)
        if self._run_task is not None:
            self._run_task.cancel()
            await asyncio.gather(self._run_task, return_exceptions=True)
            self._run_task = None

    async def submit(self, message: Message) -> Any:
        if self._stopped:
            raise RuntimeError(f"node {self.node_id} is stopped")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        await self._inbox.put((message, future))
        return await future

    async def _run(self) -> None:
        while True:
            message, future = await self._inbox.get()
            try:
                result = await self._dispatch(message)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
                logger.exception(
                    "node %s failed to handle %s",
                    self.node_id,
                    type(message).__name__,
                )
            else:
                if not future.done():
                    future.set_result(result)

    async def _dispatch(self, message: Message) -> Any:
        match message:
            case ElectionTimeout():
                await self._on_election_timeout()
                return None
            case HeartbeatTick():
                self._on_heartbeat()
                return None
            case RequestVoteRpc(args):
                return await self._on_request_vote(args)
            case AppendEntriesRpc(args):
                return await self._on_append_entries(args)
            case VoteReplyMsg():
                await self._on_vote_reply(message)
                return None
            case AppendReplyMsg():
                await self._on_append_reply(message)
                return None
            case StatusQuery():
                return self._status()

    def _status(self) -> NodeStatus:
        return NodeStatus(
            node_id=self.node_id,
            role=self.role,
            term=self.current_term,
            leader_id=self.leader_id,
            commit_index=self.commit_index,
            last_applied=self.last_applied,
            last_log_index=self._log.last_index(),
            peers=sorted(self.peer_ids),
        )

    async def _on_election_timeout(self) -> None:
        if self.role == Role.leader:
            return
        self.role = Role.candidate
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._votes = {self.node_id}
        await self._store.save_hard_state(self.current_term, self.voted_for)
        args = RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self._log.last_index(),
            last_log_term=self._log.last_term(),
        )
        term = self.current_term
        for peer_id in self.peer_ids:
            asyncio.create_task(self._send_vote(peer_id, term, args), name=f"vote-{peer_id}")

    async def _on_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        if args.term > self.current_term:
            await self._become_follower(args.term, leader_id=None)
        if args.term < self.current_term:
            return RequestVoteReply(term=self.current_term, vote_granted=False)
        if not self._log_is_up_to_date(args.last_log_index, args.last_log_term):
            return RequestVoteReply(term=self.current_term, vote_granted=False)
        if self.voted_for is not None and self.voted_for != args.candidate_id:
            return RequestVoteReply(term=self.current_term, vote_granted=False)
        self.voted_for = args.candidate_id
        await self._store.save_hard_state(self.current_term, self.voted_for)
        self._arm_election()
        return RequestVoteReply(term=self.current_term, vote_granted=True)

    async def _on_vote_reply(self, message: VoteReplyMsg) -> None:
        if message.reply.term > self.current_term:
            await self._become_follower(message.reply.term, leader_id=None)
            return
        if self.role != Role.candidate or message.term != self.current_term:
            return
        if not message.reply.vote_granted:
            return
        self._votes.add(message.peer_id)
        if len(self._votes) >= self.quorum:
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = Role.leader
        self.leader_id = self.node_id
        self._cancel(self._election_task)
        self._election_task = None
        self._arm_heartbeat()
        self._send_heartbeats()

    async def _on_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        if args.term > self.current_term:
            await self._become_follower(args.term, leader_id=args.leader_id)
        if args.term < self.current_term:
            return AppendEntriesReply(term=self.current_term, success=False)
        self.role = Role.follower
        self.leader_id = args.leader_id
        self._cancel(self._heartbeat_task)
        self._heartbeat_task = None
        self._arm_election()
        if not self._prev_matches(args.prev_log_index, args.prev_log_term):
            return AppendEntriesReply(term=self.current_term, success=False)
        if args.entries:
            # Log replication is stage 2. Heartbeats carry an empty entry list.
            return AppendEntriesReply(term=self.current_term, success=False)
        return AppendEntriesReply(term=self.current_term, success=True)

    async def _on_append_reply(self, message: AppendReplyMsg) -> None:
        if message.reply.term > self.current_term:
            await self._become_follower(message.reply.term, leader_id=None)

    def _on_heartbeat(self) -> None:
        if self.role == Role.leader:
            self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        args = AppendEntriesArgs(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=self._log.last_index(),
            prev_log_term=self._log.last_term(),
            entries=[],
            leader_commit=self.commit_index,
        )
        term = self.current_term
        for peer_id in self.peer_ids:
            asyncio.create_task(
                self._send_append(peer_id, term, args),
                name=f"heartbeat-{self.node_id}-{peer_id}",
            )

    async def _become_follower(self, term: int, leader_id: str | None) -> None:
        self.current_term = term
        self.role = Role.follower
        self.voted_for = None
        self.leader_id = leader_id
        self._votes = set()
        await self._store.save_hard_state(self.current_term, self.voted_for)
        self._cancel(self._heartbeat_task)
        self._heartbeat_task = None
        self._arm_election()

    def _log_is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        own_term = self._log.last_term()
        if last_log_term != own_term:
            return last_log_term > own_term
        return last_log_index >= self._log.last_index()

    def _prev_matches(self, prev_log_index: int, prev_log_term: int) -> bool:
        if prev_log_index == 0:
            return prev_log_term == 0
        return False

    async def _send_vote(self, peer_id: str, term: int, args: RequestVoteArgs) -> None:
        try:
            reply = await self._transport.request_vote(peer_id, args)
        except Exception:
            logger.debug("request-vote to %s failed", peer_id, exc_info=True)
            return
        if self._stopped:
            return
        await self.submit(VoteReplyMsg(peer_id=peer_id, term=term, reply=reply))

    async def _send_append(self, peer_id: str, term: int, args: AppendEntriesArgs) -> None:
        try:
            reply = await self._transport.append_entries(peer_id, args)
        except Exception:
            logger.debug("append-entries to %s failed", peer_id, exc_info=True)
            return
        if self._stopped:
            return
        await self.submit(AppendReplyMsg(peer_id=peer_id, term=term, reply=reply))

    def _arm_election(self) -> None:
        if self._stopped:
            return
        self._cancel(self._election_task)
        self._election_task = asyncio.create_task(
            self._election_loop(),
            name=f"election-{self.node_id}",
        )

    def _arm_heartbeat(self) -> None:
        if self._stopped:
            return
        self._cancel(self._heartbeat_task)
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"heartbeat-{self.node_id}",
        )

    async def _election_loop(self) -> None:
        try:
            while not self._stopped:
                await self._clock.sleep(self._clock.election_timeout())
                await self.submit(ElectionTimeout())
        except asyncio.CancelledError:
            return

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stopped:
                await self._clock.sleep(self._timeouts.heartbeat_seconds)
                await self.submit(HeartbeatTick())
        except asyncio.CancelledError:
            return

    @staticmethod
    def _cancel(task: asyncio.Task[None] | None) -> None:
        if task is not None and not task.done():
            task.cancel()

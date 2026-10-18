from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from litestar import Litestar
from litestar.openapi import OpenAPIConfig

from raft_api.api.controllers.raft_rpc import RaftRpcController
from raft_api.api.controllers.status import StatusController
from raft_api.clock import AsyncioClock, Clock
from raft_api.config import NodeConfig
from raft_api.raft.node import RaftNode
from raft_api.storage.base import PersistentStore
from raft_api.storage.memory import MemoryStore
from raft_api.transport.base import RaftTransport
from raft_api.transport.http import HttpTransport


def create_app(
    config: NodeConfig,
    *,
    clock: Clock | None = None,
    store: PersistentStore | None = None,
    transport: RaftTransport | None = None,
) -> Litestar:
    @asynccontextmanager
    async def lifespan(app: Litestar) -> AsyncIterator[None]:
        node_clock = clock or AsyncioClock(config.timeouts)
        node_store = store or MemoryStore()
        node_transport = transport or HttpTransport(
            config.peers,
            config.timeouts.rpc_timeout_seconds,
        )
        node = RaftNode(
            config.node_id,
            list(config.peers),
            clock=node_clock,
            store=node_store,
            transport=node_transport,
            timeouts=config.timeouts,
        )
        app.state.settings = config
        app.state.node = node
        await node.start()
        try:
            yield
        finally:
            await node.stop()
            await node_transport.aclose()

    return Litestar(
        route_handlers=[StatusController, RaftRpcController],
        lifespan=[lifespan],
        openapi_config=OpenAPIConfig(title="Raft API", version="0.1.0"),
    )

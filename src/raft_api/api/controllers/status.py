from litestar import Controller, Request, get

from raft_api.api.schemas import HealthResponse
from raft_api.raft.messages import StatusQuery
from raft_api.raft.node import NodeStatus, RaftNode


class StatusController(Controller):
    path = "/v1"
    tags = ["status"]

    @get("/health")
    async def health(self, request: Request) -> HealthResponse:
        return HealthResponse(status="ok", node_id=request.app.state.settings.node_id)

    @get("/status")
    async def status(self, request: Request) -> NodeStatus:
        node: RaftNode = request.app.state.node
        status: NodeStatus = await node.submit(StatusQuery())
        return status

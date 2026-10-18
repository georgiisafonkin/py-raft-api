import httpx

from raft_api.raft.rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class HttpTransport:
    def __init__(
        self,
        peers: dict[str, str],
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._peers = {peer_id: url.rstrip("/") for peer_id, url in peers.items()}
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)

    async def request_vote(self, peer_id: str, args: RequestVoteArgs) -> RequestVoteReply:
        response = await self._client.post(
            f"{self._url(peer_id)}/raft/request-vote",
            json=args.model_dump(),
        )
        response.raise_for_status()
        return RequestVoteReply.model_validate(response.json())

    async def append_entries(self, peer_id: str, args: AppendEntriesArgs) -> AppendEntriesReply:
        response = await self._client.post(
            f"{self._url(peer_id)}/raft/append-entries",
            json=args.model_dump(),
        )
        response.raise_for_status()
        return AppendEntriesReply.model_validate(response.json())

    async def aclose(self) -> None:
        await self._client.aclose()

    def _url(self, peer_id: str) -> str:
        try:
            return self._peers[peer_id]
        except KeyError as exc:
            raise ConnectionError(f"unknown peer {peer_id}") from exc

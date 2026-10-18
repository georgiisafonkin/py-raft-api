from typing import Protocol

from pydantic import BaseModel, Field

from raft_api.commands import LogEntry


class PersistedState(BaseModel):
    current_term: int = 0
    voted_for: str | None = None
    commit_index: int = 0
    log: list[LogEntry] = Field(default_factory=list)


class PersistentStore(Protocol):
    async def load(self) -> PersistedState: ...

    async def save_hard_state(self, term: int, voted_for: str | None) -> None: ...

    async def save_commit_index(self, commit_index: int) -> None: ...

    async def append(self, entries: list[LogEntry]) -> None: ...

    async def truncate_from(self, index: int) -> None: ...

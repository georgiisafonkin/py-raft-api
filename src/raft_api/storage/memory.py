from raft_api.commands import LogEntry
from raft_api.storage.base import PersistedState


class MemoryStore:
    def __init__(self, state: PersistedState | None = None) -> None:
        self._state = state.model_copy(deep=True) if state is not None else PersistedState()

    async def load(self) -> PersistedState:
        return self._state.model_copy(deep=True)

    async def save_hard_state(self, term: int, voted_for: str | None) -> None:
        self._state.current_term = term
        self._state.voted_for = voted_for

    async def save_commit_index(self, commit_index: int) -> None:
        self._state.commit_index = commit_index

    async def append(self, entries: list[LogEntry]) -> None:
        self._state.log.extend(entry.model_copy(deep=True) for entry in entries)

    async def truncate_from(self, index: int) -> None:
        self._state.log = [entry for entry in self._state.log if entry.index < index]

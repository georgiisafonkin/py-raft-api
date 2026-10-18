from typing import Protocol

from raft_api.commands import ApplyResult, Command


class StateMachine(Protocol):
    def apply(self, index: int, command: Command) -> ApplyResult: ...

    def get(self, key: str) -> bytes | None: ...

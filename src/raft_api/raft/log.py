from raft_api.commands import LogEntry, NoopCommand


class Log:
    """In-memory Raft log. Index 0 is a sentinel so prevLogIndex=0 is a normal case."""

    def __init__(self, entries: list[LogEntry] | None = None) -> None:
        self._entries: list[LogEntry] = [LogEntry(index=0, term=0, command=NoopCommand())]
        for entry in entries or []:
            self.append(entry)

    def last_index(self) -> int:
        return self._entries[-1].index

    def last_term(self) -> int:
        return self._entries[-1].term

    def append(self, entry: LogEntry) -> None:
        expected = self.last_index() + 1
        if entry.index != expected:
            raise ValueError(f"expected log index {expected}, got {entry.index}")
        self._entries.append(entry)

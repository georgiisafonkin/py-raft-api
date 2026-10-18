from typing import Annotated, Literal

from pydantic import BaseModel, Field


class NoopCommand(BaseModel):
    type: Literal["noop"] = "noop"
    client_id: str = ""
    request_id: str = ""


class PutCommand(BaseModel):
    type: Literal["put"] = "put"
    client_id: str
    request_id: str
    key: str
    value: str


class DeleteCommand(BaseModel):
    type: Literal["delete"] = "delete"
    client_id: str
    request_id: str
    key: str


class CasCommand(BaseModel):
    type: Literal["cas"] = "cas"
    client_id: str
    request_id: str
    key: str
    expected: str | None
    desired: str | None


class LockAcquireCommand(BaseModel):
    type: Literal["lock_acquire"] = "lock_acquire"
    client_id: str
    request_id: str
    name: str


class LockReleaseCommand(BaseModel):
    type: Literal["lock_release"] = "lock_release"
    client_id: str
    request_id: str
    name: str
    fence: int


Command = Annotated[
    NoopCommand | PutCommand | DeleteCommand | CasCommand | LockAcquireCommand | LockReleaseCommand,
    Field(discriminator="type"),
]


class LogEntry(BaseModel):
    index: int
    term: int
    command: Command


class ApplyResult(BaseModel):
    index: int
    success: bool = True
    value: str | None = None

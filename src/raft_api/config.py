from pathlib import Path
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PeerConfig(BaseModel):
    url: str


class Timeouts(BaseModel):
    heartbeat_seconds: float = 0.05
    election_timeout_min_seconds: float = 0.3
    election_timeout_max_seconds: float = 0.5
    rpc_timeout_seconds: float = 0.2


class ClusterConfig(BaseModel):
    nodes: dict[str, PeerConfig]
    timeouts: Timeouts = Field(default_factory=Timeouts)


class NodeConfig(BaseModel):
    node_id: str
    url: str
    peers: dict[str, str]
    timeouts: Timeouts


class TimeoutOverrides(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RAFT_", extra="ignore")

    heartbeat_seconds: float | None = None
    election_timeout_min_seconds: float | None = None
    election_timeout_max_seconds: float | None = None
    rpc_timeout_seconds: float | None = None


def load_cluster_config(path: Path) -> ClusterConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"cluster config must be a mapping: {path}")
    config = ClusterConfig.model_validate(raw)
    overrides = TimeoutOverrides()
    data = config.timeouts.model_dump()
    for key, value in overrides.model_dump().items():
        if value is not None:
            data[key] = value
    config.timeouts = Timeouts.model_validate(data)
    low = config.timeouts.election_timeout_min_seconds
    high = config.timeouts.election_timeout_max_seconds
    if low > high:
        raise ValueError("election_timeout_min_seconds must be <= election_timeout_max_seconds")
    if config.timeouts.heartbeat_seconds >= low:
        raise ValueError("heartbeat_seconds must be smaller than the election timeout")
    return config


def load_node_config(path: Path, node_id: str) -> NodeConfig:
    cluster = load_cluster_config(path)
    if node_id not in cluster.nodes:
        known = ", ".join(sorted(cluster.nodes))
        raise ValueError(f"unknown node {node_id!r}; known nodes: {known}")
    peers = {peer_id: peer.url for peer_id, peer in cluster.nodes.items() if peer_id != node_id}
    return NodeConfig(
        node_id=node_id,
        url=cluster.nodes[node_id].url,
        peers=peers,
        timeouts=cluster.timeouts,
    )


def listen_address(url: str) -> tuple[str, int]:
    parsed = urlparse(url)
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"node url must include host and port: {url}")
    return parsed.hostname, parsed.port

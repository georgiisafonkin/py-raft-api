import argparse
from pathlib import Path

import uvicorn

from raft_api.api.app import create_app
from raft_api.config import listen_address, load_node_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one Raft node")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--config", type=Path, default=Path("configs/cluster.yaml"))
    args = parser.parse_args()
    config = load_node_config(args.config, args.node_id)
    host, port = listen_address(config.url)
    uvicorn.run(create_app(config), host=host, port=port)


if __name__ == "__main__":
    main()

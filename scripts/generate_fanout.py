from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def build_fanout(shard_count: int = 5) -> dict[str, Any]:
    if not 1 <= shard_count <= 50:
        raise ValueError("shard_count must be between 1 and 50")

    branches = []
    for shard in range(shard_count):
        branches.append(
            [
                {
                    "name": "aca_start",
                    "taskReferenceName": f"start_{shard}",
                    "type": "SIMPLE",
                    "inputParameters": {
                        "correlation_id": f"${{workflow.input.correlation_id}}-{shard}"
                    },
                },
                {
                    "name": "aca_wait",
                    "taskReferenceName": f"wait_{shard}",
                    "type": "SIMPLE",
                    "inputParameters": {
                        "execution_name": f"${{start_{shard}.output.execution_name}}",
                        "correlation_id": f"${{start_{shard}.output.correlation_id}}",
                    },
                },
            ]
        )

    description_count = "five" if shard_count == 5 else str(shard_count)
    return {
        "name": "aca_five_way_fanout" if shard_count == 5 else f"aca_{shard_count}_way_fanout",
        "description": f"Start {description_count} ACA Job executions in parallel and join their terminal results",
        "version": 1,
        "schemaVersion": 2,
        "restartable": True,
        "inputParameters": ["correlation_id"],
        "tasks": [
            {
                "name": "fork",
                "taskReferenceName": "fork",
                "type": "FORK_JOIN",
                "forkTasks": branches,
            },
            {
                "name": "join",
                "taskReferenceName": "join",
                "type": "JOIN",
                "joinOn": [f"wait_{shard}" for shard in range(shard_count)],
            },
        ],
        "outputParameters": {
            f"shard_{shard}": f"${{wait_{shard}.output}}" for shard in range(shard_count)
        },
        "timeoutPolicy": "TIME_OUT_WF",
        "timeoutSeconds": 2100,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a native Conductor ACA fan-out workflow")
    parser.add_argument("--shards", type=int, default=5, choices=range(1, 51))
    parser.add_argument("--output", type=Path, default=Path("workflows/fanout.json"))
    args = parser.parse_args()
    args.output.write_text(json.dumps(build_fanout(args.shards), indent=2) + "\n")


if __name__ == "__main__":
    main()

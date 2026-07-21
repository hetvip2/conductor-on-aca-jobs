from __future__ import annotations

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.orkes_clients import ConductorClients

import conductor_aca_jobs.workers  # noqa: F401


class ArmStubHandler(BaseHTTPRequestHandler):
    executions: dict[str, dict[str, Any]] = {}
    starts: list[str] = []
    stops: list[str] = []

    def do_GET(self) -> None:
        if "/executions/" in self.path:
            name = self.path.split("/executions/", 1)[1].split("?", 1)[0]
            execution = self.executions[name]
            execution["requests"] += 1
            if execution["mode"] == "transient" and execution["requests"] == 1:
                self._json(429, {})
                return
            execution["polls"] += 1
            if execution["mode"] == "failure" and execution["polls"] > 1:
                status = "Failed"
            elif execution["mode"] == "timeout":
                status = "Running"
            elif execution["mode"] == "restart" and execution["polls"] < 3:
                status = "Running"
            else:
                status = "Succeeded" if execution["polls"] > 1 else "Running"
            self._json(200, {"properties": {"status": status}})
            return
        self._json(
            200,
            {
                "properties": {
                    "template": {
                        "containers": [
                            {
                                "name": "worker",
                                "image": "offline.test/worker:1",
                                "resources": {"cpu": 0.5, "memory": "1Gi"},
                                "env": [],
                            }
                        ]
                    }
                }
            },
        )

    def do_POST(self) -> None:
        if "/executions/" in self.path and "/stop?" in self.path:
            name = self.path.split("/executions/", 1)[1].split("/stop", 1)[0]
            self.stops.append(name)
            self._json(200, {})
            return
        if "/start?" in self.path:
            name = f"offline-{uuid.uuid4()}"
            correlation = self.headers.get("x-ms-client-request-id", "")
            mode = next(
                (
                    candidate
                    for candidate in ("failure", "transient", "timeout", "restart")
                    if candidate in correlation
                ),
                "success",
            )
            self.executions[name] = {
                "correlation": correlation,
                "mode": mode,
                "polls": 0,
                "requests": 0,
            }
            self.starts.append(name)
            self._json(200, {"name": name})
            return
        self._json(200, {})

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_arm_stub() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), ArmStubHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    os.environ.update(
        {
            "ACA_ARM_ENDPOINT": f"http://127.0.0.1:{server.server_port}",
            "ACA_STATIC_TOKEN": "local-smoke-only",
            "AZURE_SUBSCRIPTION_ID": "offline-subscription",
            "ACA_RESOURCE_GROUP": "offline-group",
            "ACA_JOB_NAME": "offline-job",
        }
    )
    return server


def await_workflow(client: Any, workflow_id: str, timeout_seconds: int = 120) -> Any:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        workflow = client.get_workflow(workflow_id, include_tasks=True)
        status = str(workflow.status).split(".")[-1]
        if status in {"COMPLETED", "FAILED", "TIMED_OUT", "TERMINATED", "PAUSED"}:
            return workflow
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for Conductor workflow '{workflow_id}'")


def await_condition(predicate: Any, description: str, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {description}")


def configure_smoke_task_definitions(base_url: str) -> None:
    with httpx.Client(timeout=10) as client:
        for name in ("aca_start", "aca_wait"):
            definition: dict[str, Any] | None = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                response = client.get(f"{base_url}/metadata/taskdefs/{name}")
                if response.status_code == 200:
                    definition = response.json()
                    break
                time.sleep(0.25)
            if definition is None:
                raise TimeoutError(f"Timed out waiting for task definition '{name}'")
            definition.update({"retryCount": 0, "retryDelaySeconds": 0})
            response = client.put(f"{base_url}/metadata/taskdefs", json=definition)
            response.raise_for_status()


def smoke_workflows() -> list[dict[str, Any]]:
    failure = {
        "name": "aca_failure_suppression",
        "version": 1,
        "schemaVersion": 2,
        "tasks": [
            {
                "name": "aca_start",
                "taskReferenceName": "start",
                "type": "SIMPLE",
                "inputParameters": {"correlation_id": "${workflow.input.correlation_id}"},
            },
            {
                "name": "aca_wait",
                "taskReferenceName": "wait",
                "type": "SIMPLE",
                "inputParameters": {
                    "execution_name": "${start.output.execution_name}",
                    "correlation_id": "${start.output.correlation_id}",
                    "poll_interval_seconds": 1,
                },
            },
            {
                "name": "aca_start",
                "taskReferenceName": "must_not_run",
                "type": "SIMPLE",
                "inputParameters": {
                    "correlation_id": "${workflow.input.correlation_id}-downstream"
                },
            },
        ],
        "timeoutPolicy": "TIME_OUT_WF",
        "timeoutSeconds": 120,
    }
    timeout = {
        "name": "aca_timeout_cancel",
        "version": 1,
        "schemaVersion": 2,
        "tasks": [
            {
                "name": "aca_start",
                "taskReferenceName": "start",
                "type": "SIMPLE",
                "inputParameters": {"correlation_id": "${workflow.input.correlation_id}"},
            },
            {
                "name": "aca_wait",
                "taskReferenceName": "wait",
                "type": "SIMPLE",
                "inputParameters": {
                    "execution_name": "${start.output.execution_name}",
                    "correlation_id": "${start.output.correlation_id}",
                    "poll_interval_seconds": 1,
                    "max_polls": 1,
                },
            },
        ],
        "timeoutPolicy": "TIME_OUT_WF",
        "timeoutSeconds": 120,
    }
    return [failure, timeout]


def main() -> None:
    base_url = os.getenv("CONDUCTOR_SERVER_URL", "http://localhost:8080/api")
    root = Path(__file__).parents[1]
    arm_stub = start_arm_stub()
    with httpx.Client(timeout=30) as client:
        definitions = [
            json.loads((root / "workflows" / "single.json").read_text()),
            json.loads((root / "workflows" / "fanout.json").read_text()),
            *smoke_workflows(),
        ]
        response = client.put(f"{base_url}/metadata/workflow", json=definitions)
        response.raise_for_status()

    try:
        configuration = Configuration(server_api_url=base_url)
        clients = ConductorClients(configuration=configuration)
        workflow_client = clients.get_workflow_client()
        correlation = f"local-{uuid.uuid4()}"
        with TaskHandler(configuration=configuration, scan_for_annotated_workers=True) as handler:
            handler.start_processes()
            configure_smoke_task_definitions(base_url)
            single_id = workflow_client.start_workflow_by_name(
                name="aca_single",
                version=1,
                input={"correlation_id": correlation, "args": []},
                correlationId=correlation,
            )
            single = await_workflow(workflow_client, single_id)
            fanout_id = workflow_client.start_workflow_by_name(
                name="aca_five_way_fanout",
                version=1,
                input={"correlation_id": f"{correlation}-fanout"},
                correlationId=f"{correlation}-fanout",
            )
            fanout = await_workflow(workflow_client, fanout_id)
            transient_id = workflow_client.start_workflow_by_name(
                name="aca_single",
                version=1,
                input={"correlation_id": f"{correlation}-transient", "args": []},
                correlationId=f"{correlation}-transient",
            )
            transient = await_workflow(workflow_client, transient_id)
            failure_starts = len(ArmStubHandler.starts)
            failure_id = workflow_client.start_workflow_by_name(
                name="aca_failure_suppression",
                version=1,
                input={"correlation_id": f"{correlation}-failure"},
                correlationId=f"{correlation}-failure",
            )
            failure = await_workflow(workflow_client, failure_id)
            failure_start_count = len(ArmStubHandler.starts) - failure_starts
            timeout_id = workflow_client.start_workflow_by_name(
                name="aca_timeout_cancel",
                version=1,
                input={"correlation_id": f"{correlation}-timeout"},
                correlationId=f"{correlation}-timeout",
            )
            timeout = await_workflow(workflow_client, timeout_id)
            restart_id = workflow_client.start_workflow_by_name(
                name="aca_single",
                version=1,
                input={"correlation_id": f"{correlation}-restart", "args": []},
                correlationId=f"{correlation}-restart",
            )
            await_condition(
                lambda: any(
                    item["mode"] == "restart" and item["polls"] >= 1
                    for item in ArmStubHandler.executions.values()
                ),
                "the first worker host to durably poll the restart execution",
            )
        with TaskHandler(
            configuration=configuration, scan_for_annotated_workers=True
        ) as restarted_handler:
            restarted_handler.start_processes()
            restarted = await_workflow(workflow_client, restart_id)
    finally:
        arm_stub.shutdown()
    actual = {
        "single": str(single.status),
        "fanout": str(fanout.status),
        "transient": str(transient.status),
        "failure": str(failure.status),
        "timeout": str(timeout.status),
        "restart": str(restarted.status),
    }
    expected = {
        "single": "COMPLETED",
        "fanout": "COMPLETED",
        "transient": "COMPLETED",
        "failure": "FAILED",
        "timeout": "FAILED",
        "restart": "COMPLETED",
    }
    if actual != expected:
        raise RuntimeError(
            f"Conductor smoke statuses did not match: actual={actual}, expected={expected}"
        )
    if failure_start_count != 1:
        raise RuntimeError(
            f"Failure downstream was not suppressed; observed {failure_start_count} starts"
        )
    if not ArmStubHandler.stops:
        raise RuntimeError("Timeout scenario did not cancel its ACA execution")
    restart_executions = [
        item for item in ArmStubHandler.executions.values() if item["mode"] == "restart"
    ]
    if len(restart_executions) != 1:
        raise RuntimeError(f"Worker restart created {len(restart_executions)} ACA executions")
    print(
        "PASS single=COMPLETED fanout=COMPLETED transientRetry=COMPLETED "
        "failure=FAILED downstream=Suppressed timeout=FAILED cancellation=Observed "
        "workerRestart=COMPLETED restartExecutions=1"
    )


if __name__ == "__main__":
    main()

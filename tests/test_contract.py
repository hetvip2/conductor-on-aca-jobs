from __future__ import annotations

import json
import inspect
from pathlib import Path
from typing import Any

import pytest
from azure.core.credentials import AccessToken
from conductor.client.automator.utils import convert_from_dict_or_list

from conductor_aca_jobs.client import AcaJobsClient, AcaJobsError, JobRef, Response
from conductor_aca_jobs.workers import aca_start, aca_wait, build_client


ROOT = Path(__file__).parents[1]


class Credential:
    def __init__(self) -> None:
        self.calls = 0

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        self.calls += 1
        return AccessToken(f"token-{self.calls}", 4_000_000_000)


class Transport:
    def __init__(self, *responses: Response) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def request(self, method: str, url: str, **kwargs: Any) -> Response:
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_retry_refresh_status_and_redaction() -> None:
    delays: list[float] = []

    async def sleep(value: float) -> None:
        delays.append(value)

    credential = Credential()
    transport = Transport(
        Response(401, {}, {}),
        Response(429, {"Retry-After": "2"}, {}),
        Response(200, {}, {"properties": {"status": "Succeeded"}}),
    )
    client = AcaJobsClient(JobRef("sub", "rg", "job"), credential, transport, sleep=sleep)
    assert await client.status("same-execution", "workflow-id") == "Succeeded"
    assert credential.calls == 2
    assert delays == [2]

    denied = AcaJobsClient(
        JobRef("sub", "rg", "job"),
        Credential(),
        Transport(Response(403, {}, {"secret": "hidden"})),
    )
    with pytest.raises(AcaJobsError, match="HTTP 403") as error:
        await denied.start("workflow-id")
    assert "hidden" not in str(error.value)


def test_workflows_preserve_identity_and_have_five_branches() -> None:
    single = json.loads((ROOT / "workflows" / "single.json").read_text())
    assert (
        single["tasks"][1]["inputParameters"]["execution_name"] == "${start.output.execution_name}"
    )
    fanout = json.loads((ROOT / "workflows" / "fanout.json").read_text())
    branches = fanout["tasks"][0]["forkTasks"]
    assert len(branches) == 5
    assert fanout["tasks"][1]["joinOn"] == [f"wait_{index}" for index in range(5)]


def test_worker_annotations_are_supported_by_conductor_converter() -> None:
    start_parameters = inspect.signature(aca_start).parameters
    assert convert_from_dict_or_list(start_parameters["args"].annotation, ["--shard", "0"]) == [
        "--shard",
        "0",
    ]
    for worker in (aca_start, aca_wait):
        for parameter in inspect.signature(worker).parameters.values():
            assert parameter.annotation is not inspect.Parameter.empty


def test_static_token_is_restricted_to_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACA_ARM_ENDPOINT", "https://management.azure.com")
    monkeypatch.setenv("ACA_STATIC_TOKEN", "fake")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("ACA_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("ACA_JOB_NAME", "job")
    with pytest.raises(ValueError, match="loopback"):
        build_client()

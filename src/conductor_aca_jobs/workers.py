import os
from typing import Any, List
from urllib.parse import urlsplit, urlunsplit

import httpx
from azure.core.credentials import AccessToken
from azure.identity import DefaultAzureCredential
from conductor.client.context.task_context import (  # type: ignore[import-untyped]
    TaskInProgress,
    get_task_context,
)
from conductor.client.worker.worker_task import worker_task  # type: ignore[import-untyped]

from conductor_aca_jobs.client import AcaJobsClient, AcaJobsError, JobRef, Overrides, Response


class HttpxTransport:
    async def request(self, method: str, url: str, **kwargs: Any) -> Response:
        endpoint = os.getenv("ACA_ARM_ENDPOINT")
        if endpoint:
            source = urlsplit(url)
            target = urlsplit(endpoint)
            url = urlunsplit((target.scheme, target.netloc, source.path, source.query, ""))
        async with httpx.AsyncClient() as client:
            response = await client.request(method, url, **kwargs)
        try:
            body = response.json()
        except ValueError:
            body = None
        return Response(response.status_code, response.headers, body)


class StaticCredential:
    def __init__(self, token: str) -> None:
        self.token = token

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        return AccessToken(self.token, 4_000_000_000)


def build_client() -> AcaJobsClient:
    endpoint = os.getenv("ACA_ARM_ENDPOINT")
    static_token = os.getenv("ACA_STATIC_TOKEN")
    if static_token and (
        not endpoint or urlsplit(endpoint).hostname not in {"127.0.0.1", "::1", "localhost"}
    ):
        raise ValueError("ACA_STATIC_TOKEN is allowed only with a loopback ACA_ARM_ENDPOINT")
    credential = StaticCredential(static_token) if static_token else DefaultAzureCredential()
    return AcaJobsClient(
        JobRef(
            os.environ["AZURE_SUBSCRIPTION_ID"],
            os.environ["ACA_RESOURCE_GROUP"],
            os.environ["ACA_JOB_NAME"],
        ),
        credential,
        HttpxTransport(),
    )


@worker_task(task_definition_name="aca_start", register_task_def=True)
async def aca_start(
    correlation_id: str,
    container_name: str = "worker",
    args: List[str] = None,  # type: ignore[assignment]
    cpu: float = None,  # type: ignore[assignment]
    memory: str = None,  # type: ignore[assignment]
) -> dict[str, str]:
    execution_name = await build_client().start(
        correlation_id,
        Overrides(container_name, args=args, cpu=cpu, memory=memory),
    )
    return {"execution_name": execution_name, "correlation_id": correlation_id}


@worker_task(task_definition_name="aca_wait", register_task_def=True)
async def aca_wait(
    execution_name: str,
    correlation_id: str,
    poll_interval_seconds: int = 5,
    max_polls: int = 360,
):
    context = get_task_context()
    if context.get_poll_count() >= max_polls:
        await build_client().cancel(execution_name, correlation_id)
        raise AcaJobsError(f"Timed out waiting for ACA Job execution '{execution_name}'")
    status = await build_client().status(execution_name, correlation_id)
    if status == "Succeeded":
        return {
            "execution_name": execution_name,
            "correlation_id": correlation_id,
            "status": status,
        }
    if status in {"Failed", "Canceled"}:
        raise AcaJobsError(f"ACA Job execution '{execution_name}' finished with status '{status}'")
    return TaskInProgress(
        callback_after_seconds=poll_interval_seconds,
        output={
            "execution_name": execution_name,
            "correlation_id": correlation_id,
            "status": status,
        },
    )

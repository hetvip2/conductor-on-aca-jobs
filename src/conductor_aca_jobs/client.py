from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Protocol
from urllib.parse import quote

from azure.core.credentials import TokenCredential

API_VERSION = "2024-03-01"
USER_AGENT = "conductor-aca-jobs/0.1.0"
RETRYABLE = {429, 500, 502, 503, 504}
ACTIVE = {"Pending", "Processing", "Running"}
FAILED = {"Failed", "Canceled"}


class AcaJobsError(RuntimeError):
    pass


@dataclass(frozen=True)
class JobRef:
    subscription_id: str
    resource_group: str
    job_name: str

    @classmethod
    def from_resource_id(cls, value: str) -> JobRef:
        match = re.fullmatch(
            r"/subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/Microsoft\.App/jobs/([^/]+)",
            value,
            re.IGNORECASE,
        )
        if not match:
            raise ValueError("job_resource_id is not a valid Microsoft.App/jobs resource ID")
        return cls(*match.groups())

    @property
    def url(self) -> str:
        subscription, group, job = (
            quote(value, safe="")
            for value in (self.subscription_id, self.resource_group, self.job_name)
        )
        return (
            f"https://management.azure.com/subscriptions/{subscription}/resourceGroups/{group}"
            f"/providers/Microsoft.App/jobs/{job}"
        )


@dataclass(frozen=True)
class Overrides:
    container_name: str
    command: list[str] | None = None
    args: list[str] | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    secret_env: Mapping[str, str] = field(default_factory=dict)
    cpu: float | None = None
    memory: str | None = None


@dataclass(frozen=True)
class Response:
    status_code: int
    headers: Mapping[str, str]
    body: Any = None


class Transport(Protocol):
    async def request(self, method: str, url: str, **kwargs: Any) -> Response: ...


class AcaJobsClient:
    def __init__(
        self,
        job: JobRef,
        credential: TokenCredential,
        transport: Transport,
        *,
        max_retries: int = 6,
        request_timeout: float = 30,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.job = job
        self.credential = credential
        self.transport = transport
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.sleep = sleep

    async def start(self, correlation_id: str, overrides: Overrides | None = None) -> str:
        body = await self._merge(overrides) if overrides else None
        response = await self._request("POST", "start", correlation_id, body)
        payload = _body(response)
        name = payload.get("name")
        if isinstance(name, str) and name:
            return name
        location = _header(response.headers, "location") or ""
        match = re.search(r"/executions/([^/?]+)", location)
        if match:
            return match.group(1)
        raise AcaJobsError("ACA start response did not include an execution name")

    async def status(self, execution_name: str, correlation_id: str) -> str:
        response = await self._request(
            "GET", f"executions/{quote(execution_name, safe='')}", correlation_id
        )
        properties = _body(response).get("properties")
        if not isinstance(properties, dict) or not isinstance(properties.get("status"), str):
            raise AcaJobsError("ACA execution response did not include properties.status")
        status = properties["status"]
        if status not in ACTIVE | FAILED | {"Succeeded"}:
            raise AcaJobsError(
                f"ACA Job execution '{execution_name}' returned unknown status '{status}'"
            )
        return status

    async def cancel(self, execution_name: str, correlation_id: str) -> None:
        await self._request(
            "POST", f"executions/{quote(execution_name, safe='')}/stop", correlation_id
        )

    async def _merge(self, overrides: Overrides) -> Mapping[str, Any]:
        definition = _body(await self._request("GET", "", "resolve-overrides"))
        properties = definition.get("properties")
        containers = (
            properties.get("template", {}).get("containers", [])
            if isinstance(properties, dict)
            else []
        )
        container = next(
            (
                dict(item)
                for item in containers
                if isinstance(item, dict) and item.get("name") == overrides.container_name
            ),
            None,
        )
        if container is None:
            raise AcaJobsError(
                f"ACA Job definition has no container named '{overrides.container_name}'"
            )
        for key, value in (
            ("command", overrides.command),
            ("args", overrides.args),
            ("cpu", overrides.cpu),
            ("memory", overrides.memory),
        ):
            if value is not None:
                container[key] = value
        environment = {
            item["name"]: dict(item)
            for item in container.get("env", [])
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        }
        environment.update(
            {name: {"name": name, "value": value} for name, value in overrides.env.items()}
        )
        environment.update(
            {
                name: {"name": name, "secretRef": value}
                for name, value in overrides.secret_env.items()
            }
        )
        container["env"] = list(environment.values())
        return {"containers": [container]}

    async def _request(
        self,
        method: str,
        path: str,
        correlation_id: str,
        body: Mapping[str, Any] | None = None,
    ) -> Response:
        url = f"{self.job.url}/{path}" if path else self.job.url
        url = f"{url}?api-version={API_VERSION}"
        token = self.credential.get_token("https://management.azure.com/.default").token
        refreshed = False
        for attempt in range(self.max_retries + 1):
            response = await self.transport.request(
                method,
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "User-Agent": USER_AGENT,
                    "x-ms-client-request-id": correlation_id,
                },
                json=body,
                timeout=self.request_timeout,
            )
            if response.status_code == 401 and not refreshed:
                token = self.credential.get_token("https://management.azure.com/.default").token
                refreshed = True
                continue
            if response.status_code in RETRYABLE and attempt < self.max_retries:
                retry_after = _header(response.headers, "retry-after")
                try:
                    delay = min(float(retry_after), 60) if retry_after else min(2**attempt, 60)
                except ValueError:
                    delay = min(2**attempt, 60)
                await self.sleep(delay)
                continue
            if response.status_code >= 300:
                raise AcaJobsError(f"ARM request failed with HTTP {response.status_code}")
            return response
        raise AcaJobsError("ARM request exhausted its retry budget")


def _body(response: Response) -> dict[str, Any]:
    if not isinstance(response.body, dict):
        raise AcaJobsError("ARM returned malformed or non-object JSON")
    return response.body


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)

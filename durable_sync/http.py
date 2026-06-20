"""Shared httpx retry/backoff for REST sources & destinations.

The destinations and the GitHub source all talk HTTP and all need the same
manners under rate limiting (honor `Retry-After`, otherwise exponential backoff).
That logic had drifted — Asana honored `Retry-After`, GitHub had no backoff at
all — so it lives here once. NOT used by the Notion destination: the MCP
transport surfaces failures as `isError` *results* rather than HTTP statuses, so
it keeps its own small retry loop in `NotionDestination.call`.

Runs inside Temporal activities (source fetch / destination session), never in a
workflow, so wall-clock `asyncio.sleep` is fine. Sleeps are capped so a long
rate-limit window becomes an activity retry (bounded by the activity timeout)
rather than a single multi-minute blocking sleep.
"""
from __future__ import annotations

import asyncio

import httpx

_MAX_ATTEMPTS = 6
_BASE_DELAY_SECONDS = 1.0
_MAX_DELAY_SECONDS = 60.0


def _should_retry(resp: httpx.Response, retry_statuses: tuple[int, ...]) -> bool:
    if resp.status_code in retry_statuses:
        return True
    # GitHub signals both its primary and secondary rate limits with 403 plus
    # either a Retry-After or an exhausted X-RateLimit-Remaining. A plain 403
    # (genuine permission failure) has neither and is NOT retried — it surfaces
    # so `is_auth_error` can pause the workflow.
    if resp.status_code == 403 and (
        resp.headers.get("Retry-After")
        or resp.headers.get("X-RateLimit-Remaining") == "0"
    ):
        return True
    return False


def _retry_delay(resp: httpx.Response, attempt: int, base: float) -> float:
    retry_after = resp.headers.get("Retry-After")
    if retry_after and retry_after.isdigit():
        return min(float(retry_after), _MAX_DELAY_SECONDS)
    return min(base * (2 ** attempt), _MAX_DELAY_SECONDS)


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    json: object | None = None,
    max_attempts: int = _MAX_ATTEMPTS,
    base_delay: float = _BASE_DELAY_SECONDS,
    retry_statuses: tuple[int, ...] = (429,),
) -> httpx.Response:
    """Issue an httpx request, retrying rate-limited/transient responses with
    backoff that honors `Retry-After`. Returns the final `Response` (this helper
    never raises on HTTP status — the caller decides how to treat 4xx/5xx, since
    e.g. GitHub treats 404 as "skip" and Asana raises). Network errors propagate
    to Temporal, which retries the whole activity."""
    resp = await client.request(method, url, headers=headers, params=params, json=json)
    for attempt in range(max_attempts - 1):
        if not _should_retry(resp, retry_statuses):
            return resp
        await asyncio.sleep(_retry_delay(resp, attempt, base_delay))
        resp = await client.request(method, url, headers=headers, params=params, json=json)
    return resp

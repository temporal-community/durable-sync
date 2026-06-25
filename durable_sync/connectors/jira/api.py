"""Jira Cloud API helpers — pure async HTTP + small pure transforms. No Temporal,
no config globals: every call takes its `base` + `headers`, so it's reusable from
the Source's fetch loop, the Destination's session, AND an app's enrich hook.

Auth is HTTP Basic with an account email + API token (id.atlassian.com ->
Security -> API tokens) — self-serve, no admin, no OAuth. Base URL is the site,
e.g. https://your-site.atlassian.net.

Verify paths/params against current Jira Cloud docs as they evolve:
  search   https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/
  issues   https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/
  props    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-properties/
ADF (description) format: https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/
"""
from __future__ import annotations

import base64
import datetime as dt
import logging
from typing import Any

import httpx

from durable_sync.core import DestinationHTTPError
from durable_sync.http import request_with_retry

SEARCH_PATH = "/rest/api/3/search/jql"          # token-paginated (replaced startAt /search)
ISSUE_PATH = "/rest/api/3/issue"
PAGE_LIMIT = 100
log = logging.getLogger("durable_sync.connectors.jira")


def build_headers(email: str | None, token: str | None) -> dict[str, str]:
    """HTTP Basic header for `email:token`, plus JSON accept/content-type."""
    raw = f"{email or ''}:{token or ''}".encode()
    return {
        "Authorization": "Basic " + base64.b64encode(raw).decode(),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


async def _request(
    client: httpx.AsyncClient, method: str, url: str, *, json: Any = None
) -> dict[str, Any]:
    """One Jira call through the shared backoff; raise DestinationHTTPError on
    >=400 so the status_code (not the body text) drives auth classification."""
    r = await request_with_retry(client, method, url, json=json)
    if r.status_code >= 400:
        raise DestinationHTTPError(
            r.status_code, f"Jira {method} {url} -> {r.status_code}: {r.text[:600]}"
        )
    return r.json() if r.content else {}


# --- read side (used by the Source AND by Destination.query_existing_ids) -----


async def search_page(
    client: httpx.AsyncClient, base: str, headers: dict, jql: str, *,
    next_token: str | None = None, fields: list[str] | None = None,
    properties: list[str] | None = None, page_limit: int = PAGE_LIMIT,
) -> tuple[list[dict[str, Any]], str | None]:
    """ONE page of issues matching `jql` + the next page token (None on the last
    page). `fields` selects issue fields; `properties` requests entity properties
    inline (so a destination reads its idempotency stamp without an N+1)."""
    payload: dict[str, Any] = {"jql": jql, "maxResults": page_limit}
    if next_token:
        payload["nextPageToken"] = next_token
    if fields is not None:
        payload["fields"] = fields
    if properties:
        payload["properties"] = properties
    # client carries base_url + auth headers (set by the caller's AsyncClient)
    data = await _request(client, "POST", SEARCH_PATH, json=payload)
    return data.get("issues", []), data.get("nextPageToken")


async def get_issue(
    client: httpx.AsyncClient, base: str, headers: dict, id_or_key: str, *,
    fields: list[str] | None = None, properties: list[str] | None = None,
) -> dict[str, Any] | None:
    """One issue by id or key, or None if it's gone (404). Used for targeted
    refreshes (only_items)."""
    if not id_or_key:
        return None
    params = []
    if fields is not None:
        params.append("fields=" + ",".join(fields))
    if properties:
        params.append("properties=" + ",".join(properties))
    qs = ("?" + "&".join(params)) if params else ""
    r = await request_with_retry(client, "GET", f"{ISSUE_PATH}/{id_or_key}{qs}")
    if r.status_code == 404:
        log.warning("Jira issue not found, skipping: %s", id_or_key)
        return None
    if r.status_code >= 400:
        raise DestinationHTTPError(
            r.status_code, f"Jira GET {ISSUE_PATH}/{id_or_key} -> {r.status_code}: {r.text[:600]}"
        )
    return r.json()


# --- write side (used by JiraDestination) ------------------------------------


async def create_issue(client: httpx.AsyncClient, base: str, headers: dict, fields: dict) -> str:
    """Create an issue; return its immutable numeric id (string)."""
    data = await _request(client, "POST", ISSUE_PATH, json={"fields": fields})
    return str(data.get("id") or "")


async def update_issue(
    client: httpx.AsyncClient, base: str, headers: dict, issue_id: str, fields: dict
) -> None:
    """Update an existing issue in place (PUT /issue/{id})."""
    await _request(client, "PUT", f"{ISSUE_PATH}/{issue_id}", json={"fields": fields})


async def set_issue_property(
    client: httpx.AsyncClient, base: str, headers: dict, issue_id: str, key: str, value: Any
) -> None:
    """Stash arbitrary JSON on an issue (PUT /issue/{id}/properties/{key}) — the
    idempotency handle, invisible to users and queryable inline via search."""
    await _request(client, "PUT", f"{ISSUE_PATH}/{issue_id}/properties/{key}", json=value)


# --- pure transforms (unit-tested, no IO) ------------------------------------


def parse_dt(value: str | None) -> dt.datetime | str | None:
    """Jira timestamp ("2026-06-19T12:00:00.000-0700") -> aware datetime (a neutral
    Record type). Falls back to the raw string if it can't be parsed; None -> None."""
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return value


def adf_to_text(adf: Any) -> str | None:
    """Flatten an Atlassian Document Format doc to plain text for Record.body.
    Tolerant: a plain string (older API) passes through, None -> None. Paragraphs
    and headings are separated by blank lines; list items by single newlines."""
    if adf is None:
        return None
    if isinstance(adf, str):
        return adf

    parts: list[str] = []

    def walk(node: Any) -> None:
        if not isinstance(node, dict):
            return
        ntype = node.get("type")
        if ntype == "text":
            parts.append(node.get("text", ""))
            return
        if ntype == "hardBreak":
            parts.append("\n")
            return
        for child in node.get("content", []) or []:
            walk(child)
        if ntype in ("paragraph", "heading"):
            parts.append("\n\n")
        elif ntype == "listItem":
            parts.append("\n")

    walk(adf)
    return "".join(parts).strip() or None


def text_to_adf(text: str | None) -> dict[str, Any]:
    """Plain text -> minimal ADF doc (one paragraph per non-empty line). Always
    returns a valid doc, even for empty input (a single empty paragraph)."""
    lines = [ln for ln in (text or "").split("\n") if ln.strip()]
    content = [
        {"type": "paragraph", "content": [{"type": "text", "text": ln}]} for ln in lines
    ] or [{"type": "paragraph"}]
    return {"type": "doc", "version": 1, "content": content}

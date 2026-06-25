"""JiraSource — issues from a JQL query (or a project), with a source-side
enrichment hook.

Config is injected (no module globals), so the same code serves any site/query.
The base fetch produces a neutral Record per issue. If you pass an `enrich` hook,
the source ALSO hands it a `JiraIssueContext` (the raw issue + the live client) so
your app can layer on domain logic — e.g. resolve assignees against your own
directory, pull comments — WITHOUT the source baking that policy in.

Auth: HTTP Basic with an account email + API token, read from the env vars named
by `JiraConfig` (no OAuth — same self-serve shape as Asana's PAT). Requires the
`jira` extra:  pip install "durable-sync[jira]"

GOTCHA: `primary_key` is the issue **id** (immutable), NOT the key (`ENG-123`) —
a key changes if the issue is moved between projects, which would break idempotent
upsert. The key is kept as a human-facing property + in the URL only.
"""
from __future__ import annotations

import inspect
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Union

import httpx
from temporalio import activity

from durable_sync.core import Record, SourceSpec
from durable_sync.connectors.jira import api

log = logging.getLogger("durable_sync.connectors.jira")

# enrich(record, ctx) -> Record (sync) or Awaitable[Record] (async); both ok.
EnrichHook = Callable[[Record, "JiraIssueContext"], Union[Record, Awaitable[Record]]]

# Issue fields the source maps; requested explicitly so payloads stay lean.
_FIELDS = [
    "summary", "status", "issuetype", "assignee", "reporter",
    "priority", "labels", "created", "updated", "description", "project",
]


@dataclass
class JiraConfig:
    """Everything Jira-specific a deployment supplies.

    Scope is one or more JQL queries; `projects` is sugar that builds a
    `project = "<KEY>" ORDER BY created ASC` query per key. Provide either (or both).
    """
    queries: list[tuple[str, str]] = field(default_factory=list)  # (key_slug, jql)
    projects: list[str] = field(default_factory=list)             # project keys -> JQL sugar
    base_url_env: str = "JIRA_BASE_URL"      # e.g. https://your-site.atlassian.net
    email_env: str = "JIRA_EMAIL"
    token_env: str = "JIRA_API_TOKEN"
    interval_minutes: int = 30
    title_property: str = "Summary"
    item_type: str = "Issue"                 # value written to the neutral "Type" column


@dataclass
class JiraIssueContext:
    """Handed to the enrich hook: the raw issue, plus the live client + base/headers
    so enrich can make extra calls (comments, custom fields) without re-auth."""
    raw_issue: dict
    client: httpx.AsyncClient
    base: str
    headers: dict[str, str]


def _heartbeat(detail: str) -> None:
    """Heartbeat inside a Temporal activity; no-op otherwise, so the Source stays
    runnable/testable standalone."""
    if activity.in_activity():
        activity.heartbeat(detail)


class JiraSource:
    name = "jira"

    def __init__(self, config: JiraConfig | None = None, *, enrich: EnrichHook | None = None):
        self._config = config or JiraConfig()
        self._enrich = enrich

    def specs(self) -> list[SourceSpec]:
        cfg = self._config
        specs: list[SourceSpec] = []
        for slug, jql in cfg.queries:
            specs.append(SourceSpec(
                key=f"jql:{slug}", interval_minutes=cfg.interval_minutes,
                params={"jql": jql},
            ))
        for proj in cfg.projects:
            specs.append(SourceSpec(
                key=f"project:{proj}", interval_minutes=cfg.interval_minutes,
                params={"jql": f'project = "{proj}" ORDER BY created ASC'},
            ))
        return specs

    async def fetch_page(
        self, spec: SourceSpec, only_items: list[str] | None, cursor: str | None
    ) -> tuple[list[Record], str | None]:
        """ONE page of issues + the next cursor (None on the last page). The cursor
        is Jira's `nextPageToken`, so the spine bounds history for a large query. A
        targeted (`only_items`) refresh queries by id and returns a single page."""
        cfg = self._config
        base = os.environ.get(cfg.base_url_env, "").rstrip("/")
        headers = api.build_headers(os.environ.get(cfg.email_env), os.environ.get(cfg.token_env))

        async with httpx.AsyncClient(base_url=base, headers=headers, timeout=30) as client:
            if only_items:
                ids = ", ".join(f'"{i}"' for i in only_items)
                jql = f"id in ({ids})"
                issues, next_token = await api.search_page(
                    client, base, headers, jql, fields=_FIELDS)
                next_cursor = None
            else:
                issues, next_token = await api.search_page(
                    client, base, headers, spec.params["jql"],
                    next_token=cursor, fields=_FIELDS)
                next_cursor = next_token

            out: list[Record] = []
            for issue in issues:
                record = self._to_record(issue, base)
                if self._enrich is not None:
                    ctx = JiraIssueContext(raw_issue=issue, client=client, base=base, headers=headers)
                    result = self._enrich(record, ctx)
                    record = await result if inspect.isawaitable(result) else result
                out.append(record)
                _heartbeat(record.primary_key)

        log.info("Fetched %d Jira issues for %s (cursor=%s -> %s)",
                 len(out), spec.key, cursor, next_cursor)
        return out, next_cursor

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        """Whole query as one list — drains fetch_page (standalone/non-Temporal)."""
        records: list[Record] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.fetch_page(spec, only_items, cursor)
            records.extend(page)
            if cursor is None:
                return records

    def _to_record(self, issue: dict, base: str) -> Record:
        """Map one Jira issue to a neutral Record. Pure (no IO)."""
        cfg = self._config
        fields = issue.get("fields") or {}
        key = issue.get("key") or ""

        def name_of(obj: Any) -> str:
            return (obj or {}).get("displayName") or (obj or {}).get("name") or ""

        props: dict[str, Any] = {
            cfg.title_property: fields.get("summary") or "(no summary)",
            "Type": cfg.item_type,
            "Source": "Jira",
            "Issue Key": key,
            "Issue Type": (fields.get("issuetype") or {}).get("name") or "",
            "Status": (fields.get("status") or {}).get("name") or "",
            "Priority": (fields.get("priority") or {}).get("name") or "",
            "Assignee": name_of(fields.get("assignee")),
            "Reporter": name_of(fields.get("reporter")),
            "Labels": list(fields.get("labels") or []),
            "URL": f"{base}/browse/{key}" if key and base else None,
            "Created": api.parse_dt(fields.get("created")),
            "Updated": api.parse_dt(fields.get("updated")),
        }
        return Record(
            primary_key=str(issue["id"]),          # IMMUTABLE id, never the key
            properties={k: v for k, v in props.items() if v is not None},
            body=api.adf_to_text(fields.get("description")),
        )

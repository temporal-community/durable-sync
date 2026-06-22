"""GitHubSource — the reference Source, with a source-side enrichment hook.

Config is injected (no module globals), so the same code serves any orgs/repos.
The base fetch produces a raw Record per repo. If you pass an `enrich` hook, the
source ALSO hands it a `RepoContext` (the raw repo + readme + language bytes +
authors + employee members + the live HTTP client) so your app can layer on
domain enrichment WITHOUT importing the source's internals.
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
from durable_sync.connectors.github import api

log = logging.getLogger("durable_sync.connectors.github")

# enrich(record, ctx) -> Record (sync) or Awaitable[Record] (async); both ok.
EnrichHook = Callable[[Record, "RepoContext"], Union[Record, Awaitable[Record]]]


@dataclass
class GitHubConfig:
    """Everything GitHub-specific a deployment supplies.

    sources: list of ("org", "name") and/or ("repos", ["owner/repo", ...]).
      org sources are gated by inclusion_topics (unless empty / discovery_mode);
      named repos are included by virtue of being named.
    """
    sources: list[tuple[str, Any]]
    # Include only org repos carrying ANY of these GitHub topics (OR-gate). Empty
    # = no topic gate (include every non-archived repo). Accepts any iterable of
    # topic strings; named-repo sources are always included regardless.
    inclusion_topics: set[str] = field(default_factory=set)
    discovery_mode: bool = False          # org sweep ignores topic + skips README
    # Orgs whose member logins are surfaced to the enrich hook as RepoContext.members
    # (e.g. to distinguish insiders from outside contributors). The source attaches
    # the set; YOUR hook decides what membership means.
    member_orgs: list[str] = field(default_factory=list)
    title_property: str = "Name"
    interval_minutes: int = 30
    per_page: int = api.PER_PAGE
    token_env: str = "GITHUB_TOKEN"
    contributor_limit: int = 5


@dataclass
class RepoContext:
    """Handed to the enrich hook: everything already fetched for one repo, plus
    the live client + headers so enrich can make extra calls (e.g. download a
    tarball for static analysis) without re-authenticating or re-fetching."""
    raw_repo: dict
    readme: str | None
    language_bytes: dict[str, int]
    authors: list[str]
    members: set[str]
    client: httpx.AsyncClient
    headers: dict[str, str]


def _heartbeat(detail: str) -> None:
    """Heartbeat inside a Temporal activity; no-op otherwise, so the Source stays
    runnable/testable standalone."""
    if activity.in_activity():
        activity.heartbeat(detail)


class GitHubSource:
    name = "github"

    def __init__(self, config: GitHubConfig, *, enrich: EnrichHook | None = None):
        self._config = config
        self._enrich = enrich

    # --- Source protocol ---------------------------------------------------

    def specs(self) -> list[SourceSpec]:
        cfg = self._config
        specs: list[SourceSpec] = []
        for kind, value in cfg.sources:
            if kind == "org":
                specs.append(SourceSpec(
                    key=f"org:{value}",
                    interval_minutes=cfg.interval_minutes,
                    params={"kind": "org", "org": str(value)},
                ))
            else:  # "repos"
                specs.append(SourceSpec(
                    key="repos:named",
                    interval_minutes=cfg.interval_minutes,
                    params={"kind": "repos", "repos": list(value)},
                ))
        return specs

    async def fetch_page(
        self, spec: SourceSpec, only_items: list[str] | None, cursor: str | None
    ) -> tuple[list[Record], str | None]:
        """ONE page of records + the next cursor (None on the last page). For an org
        sweep the cursor is the GitHub page number, so the spine bounds history even
        for a huge org. The named-repos / targeted (`only_items`) paths are small and
        bounded, so they return everything as a single page (next_cursor=None)."""
        cfg = self._config
        kind = spec.params.get("kind")
        headers = api.build_headers(os.environ.get(cfg.token_env))

        async with httpx.AsyncClient(timeout=30) as client:
            members = await self._members(client, headers)
            repos, next_cursor = await self._select_repos_page(
                client, headers, spec, kind, only_items, cursor)
            records = await self._records_for_repos(client, headers, repos, members)

        log.info("Fetched %d records for %s (cursor=%s -> %s)", len(records), spec.key, cursor, next_cursor)
        return records, next_cursor

    async def fetch(
        self, spec: SourceSpec, only_items: list[str] | None = None
    ) -> list[Record]:
        """Whole unit as one list — drains fetch_page. Convenience for standalone /
        non-Temporal callers; the spine drives fetch_page page-by-page instead."""
        records: list[Record] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.fetch_page(spec, only_items, cursor)
            records.extend(page)
            if cursor is None:
                return records

    # --- internals ---------------------------------------------------------

    async def _members(self, client, headers) -> set[str]:
        """Org member logins for the enrich hook — only when a hook can use them.
        Re-fetched per page in the paged path (activities are stateless); members
        change rarely and member_orgs is opt-in, so the extra calls are acceptable."""
        members: set[str] = set()
        if self._enrich and self._config.member_orgs:
            for org in self._config.member_orgs:
                members |= await api.fetch_org_members(client, org, headers)
        return members

    async def _records_for_repos(self, client, headers, repos, members) -> list[Record]:
        cfg = self._config
        out: list[Record] = []
        seen: set[str] = set()
        for repo in repos:
            rid = str(repo["id"])
            if rid in seen:  # de-dupe within the page (cross-page dups resolve to
                continue     # updates in the idempotent upsert, so per-page is enough)
            seen.add(rid)
            # Discovery skips READMEs (hundreds of calls).
            readme = None if cfg.discovery_mode else await api.fetch_readme(
                client, repo["full_name"], headers)
            lang_bytes = await api.fetch_languages(client, repo["full_name"], headers)
            authors = await api.fetch_contributors(
                client, repo["full_name"], headers, limit=cfg.contributor_limit)

            record = self._base_record(repo, readme, lang_bytes, authors)
            if self._enrich is not None:
                ctx = RepoContext(
                    raw_repo=repo, readme=readme, language_bytes=lang_bytes,
                    authors=authors, members=members, client=client, headers=headers,
                )
                result = self._enrich(record, ctx)
                record = await result if inspect.isawaitable(result) else result
            out.append(record)
            _heartbeat(repo["full_name"])
        return out

    async def _select_repos_page(
        self, client, headers, spec, kind, only_items, cursor
    ) -> tuple[list[dict], str | None]:
        if only_items:  # targeted refresh — bounded, one page (gate org repos)
            return await self._repos_by_name(client, headers, only_items, gate=(kind == "org")), None
        if kind == "org":
            page = int(cursor) if cursor else 1
            batch, has_more = await api.fetch_org_repos_page(
                client, spec.params.get("org", ""), headers, page=page, per_page=self._config.per_page)
            gated = [r for r in batch if self._passes_gate(r)]
            return gated, (str(page + 1) if has_more else None)
        # named repos — included by virtue of being named, bounded, one page
        return await self._repos_by_name(client, headers, spec.params.get("repos", []), gate=False), None

    async def _repos_by_name(self, client, headers, names, *, gate: bool) -> list[dict]:
        repos: list[dict] = []
        for full in names:
            repo = await api.get_repo(client, full, headers)
            if repo is None:
                continue
            if gate and not self._passes_gate(repo):
                continue
            repos.append(repo)
        return repos

    def _passes_gate(self, repo: dict) -> bool:
        if repo.get("archived"):
            return False
        if self._config.discovery_mode or not self._config.inclusion_topics:
            return True
        topics = {t.lower() for t in (repo.get("topics") or [])}
        return any(t.lower() in topics for t in self._config.inclusion_topics)

    def _base_record(
        self, repo: dict, readme: str | None, lang_bytes: dict[str, int], authors: list[str]
    ) -> Record:
        languages = api.raw_languages(lang_bytes)
        spdx = (repo.get("license") or {}).get("spdx_id")
        props = {
            self._config.title_property: repo["name"],
            "Repo ID": str(repo["id"]),
            "Repo URL": repo["html_url"],
            "Description": repo.get("description") or "",
            "Languages": ", ".join(languages),
            "Topics (raw)": ", ".join(repo.get("topics") or []),
            "Authors": ", ".join(authors),
            "Stars": int(repo.get("stargazers_count") or 0),
            "Forks": int(repo.get("forks_count") or 0),
            "Open issues": int(repo.get("open_issues_count") or 0),
            "Is fork": bool(repo.get("fork")),
            # NOASSERTION = no recognized license -> blank (itself a signal)
            "License": spdx if spdx and spdx != "NOASSERTION" else None,
            "Created": api.iso_date(repo.get("created_at")),
            "Last updated": api.iso_date(repo.get("pushed_at") or repo.get("created_at")),
        }
        return Record(primary_key=str(repo["id"]), properties=props, body=readme)

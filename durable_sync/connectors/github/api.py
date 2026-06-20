"""GitHub REST helpers — pure HTTP + small pure transforms. No Temporal, no
config globals: every call takes its `headers`. Reusable from the Source's
fetch loop AND from an app's enrich hook (which gets the live client via
RepoContext).
"""
from __future__ import annotations

import logging

import httpx

from durable_sync.http import request_with_retry

GITHUB_API = "https://api.github.com"
PER_PAGE = 100
log = logging.getLogger("durable_sync.connectors.github")


def build_headers(token: str | None, *, user_agent: str = "durable-sync") -> dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": user_agent,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


# --- pure transforms -------------------------------------------------------

def raw_languages(byte_counts: dict[str, int]) -> list[str]:
    """All GitHub-reported languages, most-bytes-first."""
    return sorted(byte_counts, key=lambda lang: -byte_counts[lang])


def classify(topics: list[str], mapping: dict[str, str]) -> list[str]:
    """Map topics through a {topic_lower: label} dict, de-duped, order-preserving.
    The mapping itself is app vocab — the library just applies it."""
    out: list[str] = []
    for t in topics:
        label = mapping.get(t.lower())
        if label and label not in out:
            out.append(label)
    return out


def is_member(handle: str, members: set[str]) -> bool:
    """Whether a contributor handle belongs to the org-member set (insider-or-not).
    Neutral primitive: an app's enrich hook picks its own labels
    (e.g. Employee/Community, Staff/External) from this boolean."""
    return handle in members


def iso_date(s: str | None) -> str | None:
    """ISO date (YYYY-MM-DD); the destination handles date_properties specially."""
    return s[:10] if s else None


# --- HTTP fetchers ---------------------------------------------------------
# All go through request_with_retry, which backs off on 429 + GitHub's
# rate-limited 403 (honoring Retry-After). The enrichment fetchers below tolerate
# a failed call by returning empty, but LOG it first — a silently empty languages
# list (because we got rate-limited) reads as "this repo has no languages", which
# is a data-quality landmine on a large org sweep.

async def get_repo(client: httpx.AsyncClient, full_name: str, headers: dict) -> dict | None:
    r = await request_with_retry(client, "GET", f"{GITHUB_API}/repos/{full_name}", headers=headers)
    if r.status_code == 404:
        log.warning("Repo not found, skipping: %s", full_name)
        return None
    r.raise_for_status()
    return r.json()


async def fetch_org_repos_page(
    client: httpx.AsyncClient, org: str, headers: dict, *, page: int, per_page: int = PER_PAGE
) -> tuple[list[dict], bool]:
    """ONE page of an org's public repos. Returns (batch, has_more). The page number
    is the pagination cursor the spine threads through `GitHubSource.fetch_page`, so
    the fetch result never passes through workflow history as one oversized payload.
    Caller applies inclusion gating. Ordered by full_name (stable across pages)."""
    r = await request_with_retry(
        client, "GET", f"{GITHUB_API}/orgs/{org}/repos",
        headers=headers,
        params={"per_page": per_page, "page": page, "type": "public", "sort": "full_name"},
    )
    r.raise_for_status()
    batch = r.json()
    return batch, len(batch) == per_page


async def fetch_org_repos(
    client: httpx.AsyncClient, org: str, headers: dict, *, per_page: int = PER_PAGE
) -> list[dict]:
    """All public repos in an org — drains fetch_org_repos_page. For non-Temporal
    callers (an enrich hook, a script); the spine uses the paged form directly."""
    repos: list[dict] = []
    page = 1
    while True:
        batch, has_more = await fetch_org_repos_page(client, org, headers, page=page, per_page=per_page)
        repos.extend(batch)
        if not has_more:
            return repos
        page += 1


async def fetch_readme(client: httpx.AsyncClient, full_name: str, headers: dict) -> str | None:
    h = dict(headers, Accept="application/vnd.github.raw")
    r = await request_with_retry(client, "GET", f"{GITHUB_API}/repos/{full_name}/readme", headers=h)
    if r.status_code == 200:
        return r.text
    if r.status_code != 404:  # 404 = no README (normal); anything else is a real failure
        log.warning("README fetch for %s failed: HTTP %s", full_name, r.status_code)
    return None


async def fetch_languages(client: httpx.AsyncClient, full_name: str, headers: dict) -> dict[str, int]:
    r = await request_with_retry(client, "GET", f"{GITHUB_API}/repos/{full_name}/languages", headers=headers)
    if r.status_code == 200:
        return r.json()
    log.warning("Languages fetch for %s failed: HTTP %s — record will list none", full_name, r.status_code)
    return {}


async def fetch_contributors(
    client: httpx.AsyncClient, full_name: str, headers: dict, *, limit: int = 5
) -> list[str]:
    """Top contributor handles, most-commits-first, bots filtered out."""
    r = await request_with_retry(
        client, "GET", f"{GITHUB_API}/repos/{full_name}/contributors",
        headers=headers, params={"per_page": 25},
    )
    if r.status_code != 200:
        log.warning("Contributors fetch for %s failed: HTTP %s", full_name, r.status_code)
        return []
    data = r.json()
    if not isinstance(data, list):
        return []
    out: list[str] = []
    for c in data:
        login = c.get("login")
        if login and not login.endswith("[bot]"):
            out.append(login)
        if len(out) >= limit:
            break
    return out


async def fetch_org_members(client: httpx.AsyncClient, org: str, headers: dict) -> set[str]:
    """Member logins for an org (needs read:org to see private members)."""
    members: set[str] = set()
    page = 1
    while True:
        r = await request_with_retry(
            client, "GET", f"{GITHUB_API}/orgs/{org}/members",
            headers=headers, params={"per_page": 100, "page": page},
        )
        if r.status_code != 200:
            log.warning("Org members fetch for %s failed: HTTP %s", org, r.status_code)
            break
        batch = r.json()
        if not isinstance(batch, list):
            break
        members.update(m["login"] for m in batch if m.get("login"))
        if len(batch) < 100:
            break
        page += 1
    return members

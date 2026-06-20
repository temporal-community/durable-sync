"""GitHub REST helpers — pure HTTP + small pure transforms. No Temporal, no
config globals: every call takes its `headers`. Reusable from the Source's
fetch loop AND from an app's enrich hook (which gets the live client via
RepoContext).
"""
from __future__ import annotations

import logging

import httpx

GITHUB_API = "https://api.github.com"
PER_PAGE = 100
log = logging.getLogger("durable_sync.sources.github")


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


def author_type(handle: str, members: set[str]) -> str:
    """Employee/Community from org membership (insider-or-not, not identity)."""
    return "Employee" if handle in members else "Community"


def iso_date(s: str | None) -> str | None:
    """ISO date (YYYY-MM-DD); the destination handles date_properties specially."""
    return s[:10] if s else None


# --- HTTP fetchers ---------------------------------------------------------

async def get_repo(client: httpx.AsyncClient, full_name: str, headers: dict) -> dict | None:
    r = await client.get(f"{GITHUB_API}/repos/{full_name}", headers=headers)
    if r.status_code == 404:
        log.warning("Repo not found, skipping: %s", full_name)
        return None
    r.raise_for_status()
    return r.json()


async def fetch_org_repos(
    client: httpx.AsyncClient, org: str, headers: dict, *, per_page: int = PER_PAGE
) -> list[dict]:
    """All public repos in an org (paginated). Caller applies inclusion gating."""
    repos: list[dict] = []
    page = 1
    while True:
        r = await client.get(
            f"{GITHUB_API}/orgs/{org}/repos",
            headers=headers,
            params={"per_page": per_page, "page": page, "type": "public", "sort": "full_name"},
        )
        r.raise_for_status()
        batch = r.json()
        repos.extend(batch)
        if len(batch) < per_page:
            return repos
        page += 1


async def fetch_readme(client: httpx.AsyncClient, full_name: str, headers: dict) -> str | None:
    h = dict(headers, Accept="application/vnd.github.raw")
    r = await client.get(f"{GITHUB_API}/repos/{full_name}/readme", headers=h)
    return r.text if r.status_code == 200 else None


async def fetch_languages(client: httpx.AsyncClient, full_name: str, headers: dict) -> dict[str, int]:
    r = await client.get(f"{GITHUB_API}/repos/{full_name}/languages", headers=headers)
    return r.json() if r.status_code == 200 else {}


async def fetch_contributors(
    client: httpx.AsyncClient, full_name: str, headers: dict, *, limit: int = 5
) -> list[str]:
    """Top contributor handles, most-commits-first, bots filtered out."""
    r = await client.get(
        f"{GITHUB_API}/repos/{full_name}/contributors",
        headers=headers, params={"per_page": 25},
    )
    if r.status_code != 200 or not isinstance(r.json(), list):
        return []
    out: list[str] = []
    for c in r.json():
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
        r = await client.get(
            f"{GITHUB_API}/orgs/{org}/members",
            headers=headers, params={"per_page": 100, "page": page},
        )
        if r.status_code != 200 or not isinstance(r.json(), list):
            break
        batch = r.json()
        members.update(m["login"] for m in batch if m.get("login"))
        if len(batch) < 100:
            break
        page += 1
    return members

"""Contentful API helpers — pure async HTTP + pure transforms. No Temporal, no
config globals; everything Contentful-specific (space, tokens, locale) rides on a
`ContentfulSpace` passed in.

Two access modes, chosen by which token is set, both yielding the SAME flattened
`(entry, authors)` shape so the normalizer is identical regardless of mode:

- **CDA (preferred):** the read-only Delivery API. Needs just a delivery token, no
  admin. Returns only *published* entries, flattened to the default locale, with
  linked author entries resolved inline under `includes.Entry`.
- **CMA (fallback, and the only way to see in-process drafts):** the Management
  API + a self-serve PAT. Returns ALL entries (incl. drafts) with per-locale field
  maps and NO link resolution — so we flatten locales, mark publish state, and
  resolve authors against a one-time `person` index. The PAT is write-capable;
  prefer CDA when you can.

Docs:
https://www.contentful.com/developers/docs/references/content-delivery-api/
https://www.contentful.com/developers/docs/references/content-management-api/
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from durable_sync.http import request_with_retry

CDA_BASE_URL = "https://cdn.contentful.com"
CMA_BASE_URL = "https://api.contentful.com"
PAGE_LIMIT = 100
log = logging.getLogger("durable_sync.connectors.contentful")

# --- Content-model seam -----------------------------------------------------
# These field names depend on the Contentful content model, so they live in one
# place. `person` typically exposes `name` but NO email, so author matching falls
# back to NAME. `authorOverwriteText` (read in source.py) covers community authors
# that have no `person` entry.
_AUTHOR_LINK_FIELDS = ("authors", "author", "presenters", "createdBy")
_AUTHOR_NAME_FIELDS = ("name", "fullName", "displayName")
_AUTHOR_EMAIL_FIELDS = ("email", "emailAddress")


@dataclass(frozen=True)
class ContentfulSpace:
    """Connection facts for one space/environment. `delivery_token` selects CDA
    (preferred); else `cma_token` selects the CMA fallback."""
    space_id: str
    environment: str = "master"
    default_locale: str = "en-US"
    delivery_token: str = ""
    cma_token: str = ""


def _entries_url(base: str, space: ContentfulSpace) -> str:
    return f"{base}/spaces/{space.space_id}/environments/{space.environment}/entries"


async def iter_entries(
    client: httpx.AsyncClient, space: ContentfulSpace, content_type: str, after_iso: str
) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """(entry, authors) for one content type, updated on/after `after_iso`.
    `authors` is a (possibly empty) list of {name, email}. Routes to the CMA
    fallback only when no CDA token is configured."""
    if space.delivery_token:
        return await _iter_cda(client, space, content_type, after_iso)
    if space.cma_token:
        return await _iter_cma(client, space, content_type, after_iso)
    raise RuntimeError(
        "Contentful: set a delivery token (preferred) or a CMA token "
        "(ContentfulConfig.delivery_token_env / cma_token_env)."
    )


# --- CDA (Delivery API) -----------------------------------------------------

async def _iter_cda(client, space, content_type, after_iso):
    out: list[tuple[dict, list[dict]]] = []
    skip = 0
    while True:
        params: dict[str, Any] = {
            "content_type": content_type,
            # Window on UPDATED-at, not created-at: catches entries recently
            # published (publishing bumps updatedAt, even for old entries) AND
            # recently-edited drafts. Overlap is idempotent-safe.
            "sys.updatedAt[gte]": after_iso,
            "order": "-sys.updatedAt",
            "limit": PAGE_LIMIT,
            "skip": skip,
            "include": 1,  # pull linked author entries into `includes`
        }
        data = await _get(client, CDA_BASE_URL, space, space.delivery_token, params)
        items = data.get("items", [])
        author_index = _index_includes(data)
        for entry in items:
            entry["_published"] = True  # CDA only ever returns published entries
            out.append((entry, _resolve_authors(entry, author_index)))
        skip += len(items)
        if not items or skip >= data.get("total", 0):
            return out


def _index_includes(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """{sys.id: entry} for linked entries returned (already flat) in includes."""
    includes = data.get("includes", {}).get("Entry", [])
    return {e.get("sys", {}).get("id"): e for e in includes if e.get("sys", {}).get("id")}


# --- CMA (Management API) fallback ------------------------------------------

async def _iter_cma(client, space, content_type, after_iso):
    """Like _iter_cda, but the CMA returns drafts + per-locale fields + no link
    resolution — so flatten locales, mark publish state, and resolve via a person
    index. We keep drafts: in-process items are still worth indexing."""
    person_index = await _load_person_index_cma(client, space)
    out: list[tuple[dict, list[dict]]] = []
    skip = 0
    while True:
        params: dict[str, Any] = {
            "content_type": content_type,
            "sys.updatedAt[gte]": after_iso,
            "order": "-sys.updatedAt",
            "limit": PAGE_LIMIT,
            "skip": skip,
        }
        data = await _get(client, CMA_BASE_URL, space, space.cma_token, params)
        items = data.get("items", [])
        for raw in items:
            entry = _flatten_entry(raw, space.default_locale)
            entry["_published"] = _is_published(raw)
            out.append((entry, _resolve_authors(entry, person_index)))
        skip += len(items)
        if not items or skip >= data.get("total", 0):
            return out


async def _load_person_index_cma(client, space) -> dict[str, dict[str, Any]]:
    """{person id: {"fields": <flattened>}} for every person (CMA has no includes)."""
    index: dict[str, dict[str, Any]] = {}
    skip = 0
    while True:
        data = await _get(client, CMA_BASE_URL, space, space.cma_token,
                          {"content_type": "person", "limit": PAGE_LIMIT, "skip": skip})
        items = data.get("items", [])
        for raw in items:
            pid = raw.get("sys", {}).get("id")
            if pid:
                index[pid] = _flatten_entry(raw, space.default_locale)
        skip += len(items)
        if not items or skip >= data.get("total", 0):
            return index


def _is_published(raw: dict[str, Any]) -> bool:
    """A CMA entry is currently published iff it has a publishedVersion. Pure."""
    return raw.get("sys", {}).get("publishedVersion") is not None


def _flatten_entry(raw: dict[str, Any], locale: str) -> dict[str, Any]:
    """Collapse CMA per-locale field maps ({"en-US": v}) to the default locale,
    leaving the same flat shape the CDA returns. Pure (no IO)."""
    fields = {k: _pick_locale(v, locale) for k, v in raw.get("fields", {}).items()}
    return {"sys": raw.get("sys", {}), "fields": fields}


def _pick_locale(value: Any, locale: str) -> Any:
    """Pick `locale` from a CMA per-locale field map; fall back to the first locale
    present. Non-dict values pass through. Pure."""
    if isinstance(value, dict):
        if locale in value:
            return value[locale]
        for v in value.values():  # single non-default locale
            return v
        return None
    return value


# --- Shared -----------------------------------------------------------------

async def _get(client, base: str, space: ContentfulSpace, token: str, params: dict[str, Any]) -> dict[str, Any]:
    r = await request_with_retry(
        client, "GET", _entries_url(base, space),
        headers={"Authorization": f"Bearer {token}"}, params=params,
    )
    r.raise_for_status()
    return r.json()


def _resolve_authors(entry: dict[str, Any], author_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Resolve an entry's author link(s) to [{name, email}, ...]. Pure (dict only).

    Handles a single link or an array of links. Unresolvable links are skipped.
    `person` has no email, so email is typically "" and matching falls back to
    name. Expects a FLAT entry (CDA native; CMA via _flatten_entry)."""
    raw = next((entry.get("fields", {}).get(f) for f in _AUTHOR_LINK_FIELDS
                if entry.get("fields", {}).get(f)), None)
    links = raw if isinstance(raw, list) else [raw]

    authors: list[dict[str, Any]] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        person = author_index.get(link.get("sys", {}).get("id"), {})
        pf = person.get("fields", {})
        name = next((pf[f] for f in _AUTHOR_NAME_FIELDS if pf.get(f)), "")
        email = next((pf[f] for f in _AUTHOR_EMAIL_FIELDS if pf.get(f)), "")
        if name or email:
            authors.append({"name": name, "email": email})
    return authors


# --- write side: CMA only (the Delivery API is read-only) -------------------
# Verify against the CMA docs — writes are versioned (optimistic locking) and
# locale-wrapped: https://www.contentful.com/developers/docs/references/content-management-api/

def cma_entries_url(space: ContentfulSpace) -> str:
    return f"{CMA_BASE_URL}/spaces/{space.space_id}/environments/{space.environment}/entries"


async def _cma(client: httpx.AsyncClient, method: str, url: str, *, headers=None, json=None) -> dict[str, Any]:
    """A CMA call; raise with status text so is_auth_error classifies a 401.
    The client carries the Bearer + content-type headers (set in connect)."""
    r = await request_with_retry(client, method, url, headers=headers, json=json)
    if r.status_code >= 400:
        raise RuntimeError(f"Contentful {method} {url.rsplit('/', 1)[-1]} -> {r.status_code}: {r.text[:600]}")
    return r.json() if r.content else {}


async def create_entry(
    client: httpx.AsyncClient, space: ContentfulSpace, content_type: str, fields: dict[str, Any]
) -> tuple[str, int]:
    """Create an entry of `content_type`. Returns (entry id, version)."""
    data = await _cma(client, "POST", cma_entries_url(space),
                      headers={"X-Contentful-Content-Type": content_type}, json={"fields": fields})
    sys = data.get("sys", {})
    return sys.get("id", ""), sys.get("version", 1)


async def entry_version(client: httpx.AsyncClient, space: ContentfulSpace, entry_id: str) -> int:
    """Current sys.version of an entry (needed for the optimistic-locking header)."""
    data = await _cma(client, "GET", f"{cma_entries_url(space)}/{entry_id}")
    return data.get("sys", {}).get("version", 0)


async def update_entry(
    client: httpx.AsyncClient, space: ContentfulSpace, entry_id: str, fields: dict[str, Any], *, version: int
) -> int:
    """PUT an entry's fields with the version header (optimistic lock). Returns the
    new version."""
    data = await _cma(client, "PUT", f"{cma_entries_url(space)}/{entry_id}",
                      headers={"X-Contentful-Version": str(version)}, json={"fields": fields})
    return data.get("sys", {}).get("version", version + 1)


async def publish_entry(client: httpx.AsyncClient, space: ContentfulSpace, entry_id: str, *, version: int) -> None:
    """Publish an entry at `version` (else it stays a draft, CMA-only-visible)."""
    await _cma(client, "PUT", f"{cma_entries_url(space)}/{entry_id}/published",
               headers={"X-Contentful-Version": str(version)})

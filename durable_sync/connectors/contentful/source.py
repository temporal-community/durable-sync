"""ContentfulSource — entries of chosen content types -> Records, with an
enrichment hook.

Contentful is usually shared across teams, so a Source here is scoped by CONTENT
TYPE: `content_types` maps each content-type id you care about to the "Type" label
it should carry (e.g. {"blogPost": "Blog"}). One entity workflow per content type.
Whether a *shared* type's entries are kept (e.g. only when an author matches your
own directory) is app policy — do it in your `enrich`/transform hook, which gets
the resolved authors via `ContentfulEntryContext`.

Auth: prefer a read-only Delivery (CDA) token; a self-serve Management (CMA) PAT
is the fallback (and the only mode that sees drafts). Each is read from the env
var named in `ContentfulConfig`. Requires the `contentful` extra.
"""
from __future__ import annotations

import inspect
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Union

import httpx
from temporalio import activity

from durable_sync.core import Record, SourceSpec
from durable_sync.connectors import content
from durable_sync.connectors.contentful import api
from durable_sync.connectors.contentful.api import ContentfulSpace

log = logging.getLogger("durable_sync.connectors.contentful")

EnrichHook = Callable[[Record, "ContentfulEntryContext"], Union[Record, Awaitable[Record]]]


@dataclass
class ContentfulConfig:
    """Everything Contentful-specific a deployment supplies. `content_types` maps
    content-type id -> the "Type" label its entries carry (and is the allowlist of
    what gets fetched). `url_prefixes` maps content-type id -> a public URL prefix
    the entry slug is appended to."""
    space_id: str
    content_types: dict[str, str]                                   # {ct_id: type_label}
    url_prefixes: dict[str, str] = field(default_factory=dict)      # {ct_id: url_prefix}
    environment: str = "master"
    default_locale: str = "en-US"
    delivery_token_env: str = "CONTENTFUL_DELIVERY_TOKEN"           # CDA, preferred
    cma_token_env: str = "CONTENTFUL_CMA_TOKEN"                     # CMA PAT, fallback
    lookback_days: int = 21
    interval_minutes: int = 360
    title_property: str = "Name"


@dataclass
class ContentfulEntryContext:
    """Handed to the enrich hook: the raw (flattened) entry, its resolved authors
    ({name, email}), the content type, and the live client."""
    raw_entry: dict
    authors: list[dict]
    content_type: str
    client: httpx.AsyncClient


def _heartbeat(detail: str) -> None:
    if activity.in_activity():
        activity.heartbeat(detail)


class ContentfulSource:
    name = "contentful"

    def __init__(self, config: ContentfulConfig, *, enrich: EnrichHook | None = None):
        self._config = config
        self._enrich = enrich

    def specs(self) -> list[SourceSpec]:
        # One spec (=> one entity workflow) per content type, so each type syncs +
        # retries independently.
        cfg = self._config
        return [
            SourceSpec(key=f"type:{ct_id}", interval_minutes=cfg.interval_minutes,
                       params={"content_type": ct_id, "item_type": label})
            for ct_id, label in cfg.content_types.items()
        ]

    def _space(self) -> ContentfulSpace:
        cfg = self._config
        return ContentfulSpace(
            space_id=cfg.space_id,
            environment=cfg.environment,
            default_locale=cfg.default_locale,
            delivery_token=os.environ.get(cfg.delivery_token_env, ""),
            cma_token=os.environ.get(cfg.cma_token_env, ""),
        )

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        cfg = self._config
        content_type = spec.params["content_type"]
        item_type = spec.params.get("item_type") or cfg.content_types.get(content_type, content_type)
        space = self._space()
        after_iso = (datetime.now(timezone.utc) - timedelta(days=cfg.lookback_days)).isoformat()
        targeted = set(only_items or [])

        async with httpx.AsyncClient(timeout=30) as client:
            pairs = await api.iter_entries(client, space, content_type, after_iso)
            out: list[Record] = []
            for entry, authors in pairs:
                source_id = entry.get("sys", {}).get("id", "")
                if targeted and source_id not in targeted:
                    continue
                if not _has_title(entry):
                    continue  # empty-shell draft, no title yet -> not a real item
                record = self._to_record(entry, item_type, authors)
                if self._enrich is not None:
                    ctx = ContentfulEntryContext(raw_entry=entry, authors=authors,
                                                 content_type=content_type, client=client)
                    result = self._enrich(record, ctx)
                    record = await result if inspect.isawaitable(result) else result
                out.append(record)
                _heartbeat(source_id)

        log.info("Fetched %d Contentful %s entries for %s", len(out), content_type, spec.key)
        return out

    def _to_record(self, entry: dict, item_type: str, authors: list[dict]) -> Record:
        """Map one Contentful entry (+ resolved authors) to a neutral Record. Pure."""
        cfg = self._config
        sys = entry.get("sys", {})
        fields = entry.get("fields", {})

        source_id = sys.get("id", "")
        name = fields.get("title") or fields.get("name") or "(untitled entry)"
        # Date = explicit publish-date field if present, else createdAt.
        item_date = fields.get("publishDate") or fields.get("date") or sys.get("createdAt")
        status = "Published" if entry.get("_published", True) else "Draft"

        slug = fields.get("slug")
        ct_id = sys.get("contentType", {}).get("sys", {}).get("id", "")
        prefix = cfg.url_prefixes.get(ct_id) if slug else None
        url = f"{prefix}{slug}" if prefix else None

        host_names = [a["name"] for a in authors if a.get("name")]
        # authorOverwriteText (a community author with no `person`) wins for the
        # human-readable label; resolved names still drive any author matching.
        author = fields.get("authorOverwriteText") or ", ".join(host_names)
        tags = [t for t in (fields.get("tags") or []) if isinstance(t, str)]

        return content.content_record(
            primary_key=source_id,
            title_property=cfg.title_property,
            title=str(name),
            item_type=item_type,
            source="Contentful",
            url=url,
            date=item_date,
            status=status,
            author=str(author),
            authors=host_names,
            extra={"Tags": tags},
        )


def _has_title(entry: dict[str, Any]) -> bool:
    """True if the entry has a real title/name (titled drafts count; blank ones
    don't). Keeps us from writing '(untitled entry)' placeholder rows. Pure."""
    fields = entry.get("fields", {})
    return bool(fields.get("title") or fields.get("name"))

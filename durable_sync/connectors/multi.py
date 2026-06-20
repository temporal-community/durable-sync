"""MultiSource — fan several Sources into one, so a single worker + a single
bootstrap host them all.

A Source can already emit many units of work via `specs()` (GitHub does: org +
named-repos). MultiSource generalizes that across *different* Sources: each inner
spec keeps becoming its own entity workflow, but now one `run_worker(...)` /
`start_sources(...)` covers the whole bundle instead of one process per source.

This is the ergonomic shape of the old "connectors registry" — but it composes
the independent, individually-usable Source classes rather than being the only way
to use them. Use a single source directly when you want just one
(`YouTubeSource()`); wrap them when you want the bundle on one worker:

    SOURCE = MultiSource(LumaSource(), YouTubeSource(), ContentfulSource(cfg))
    asyncio.run(run_worker(SOURCE, DESTINATION))

Assumes one destination + one task queue for the bundle (true for the common
"many feeds -> one catalog" case). If two sources need *different* destinations,
run separate workers instead — that's what the independent classes are for.
"""
from __future__ import annotations

from dataclasses import replace

from durable_sync.core import Record, Source, SourceSpec

_SEP = ":"


class MultiSource:
    name = "multi"

    def __init__(self, *sources: Source):
        if not sources:
            raise ValueError("MultiSource needs at least one source")
        names = [s.name for s in sources]
        # Specs are dispatched by a `<source-name>:<inner-key>` prefix, so names
        # must be unique and free of the separator or routing would be ambiguous.
        if len(set(names)) != len(names):
            raise ValueError(f"MultiSource sources must have unique names, got {names}")
        bad = [n for n in names if _SEP in n]
        if bad:
            raise ValueError(f"MultiSource source names must not contain {_SEP!r}: {bad}")
        self._by_name: dict[str, Source] = {s.name: s for s in sources}

    def specs(self) -> list[SourceSpec]:
        """Every inner source's specs, with each key namespaced by source name so
        workflow ids stay unique across the bundle (e.g. `luma:events`)."""
        return [
            replace(spec, key=f"{name}{_SEP}{spec.key}")
            for name, source in self._by_name.items()
            for spec in source.specs()
        ]

    def _route(self, spec: SourceSpec) -> tuple[Source, SourceSpec]:
        """The owning source + the spec with its inner (un-namespaced) key restored."""
        name, _, inner_key = spec.key.partition(_SEP)
        source = self._by_name.get(name)
        if source is None:
            raise ValueError(f"MultiSource: no source named {name!r} for spec key {spec.key!r}")
        return source, replace(spec, key=inner_key)

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        """Route to the owning source by the key prefix, restoring the inner key."""
        source, inner = self._route(spec)
        return await source.fetch(inner, only_items)

    async def fetch_page(
        self, spec: SourceSpec, only_items: list[str] | None, cursor: str | None
    ) -> tuple[list[Record], str | None]:
        """Route paging to the owning source. If that source paginates, the bundle
        does too; if it only implements fetch(), it returns as a single page — so a
        mixed bundle of paged and whole-list sources all work on one worker."""
        source, inner = self._route(spec)
        inner_fetch_page = getattr(source, "fetch_page", None)
        if callable(inner_fetch_page):
            return await inner_fetch_page(inner, only_items, cursor)
        return await source.fetch(inner, only_items), None

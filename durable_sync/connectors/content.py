"""Shared property vocabulary for content-style sources (events, videos, CMS
entries, …).

These sources all map an external item onto the SAME neutral columns, so the
names live here ONCE instead of being hand-typed in each source's `_to_record` —
where they would silently drift (the same failure mode we hit with `is_auth_error`,
now fixed by one shared matcher). A destination or transform can import the `P_*`
constants to address the same columns without re-typing the strings.

Opt-in: `GitHubSource` deliberately does NOT use this — its columns (Stars, Forks,
License, …) are repo-specific. A source uses `content_record` only when the shared
content shape genuinely fits; per-source logic (URL building, status rules, author
resolution) still lives in that source — that's real variation, not duplication.
"""
from __future__ import annotations

from typing import Any

from durable_sync.core import Record

# Canonical neutral property names every content-style source emits.
P_TYPE = "Type"
P_SOURCE = "Source"
P_SOURCE_ID = "Source ID"
P_URL = "URL"
P_DATE = "Date"
P_STATUS = "Status"
P_AUTHOR = "Author"
P_AUTHORS = "Authors"

_MAX_TEXT = 2000


def content_record(
    *,
    primary_key: str,
    title_property: str,
    title: str,
    item_type: str,
    source: str,
    url: str | None = None,
    date: str | None = None,
    status: str = "Published",
    author: str = "",
    authors: list[str] | None = None,
    extra: dict[str, Any] | None = None,
) -> Record:
    """Build a Record with the shared content columns (+ any source-specific
    `extra`). `primary_key` is also written as the Source ID column. `title`/
    `author` are length-capped here so each source doesn't repeat that."""
    props: dict[str, Any] = {
        title_property: (title or "")[:_MAX_TEXT],
        P_TYPE: item_type,
        P_SOURCE: source,
        P_SOURCE_ID: primary_key,
        P_URL: url,
        P_DATE: date,
        P_STATUS: status,
        P_AUTHOR: (author or "")[:_MAX_TEXT],
        P_AUTHORS: authors or [],
    }
    if extra:
        props.update(extra)
    return Record(primary_key=primary_key, properties=props)

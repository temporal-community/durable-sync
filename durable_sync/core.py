"""Generic, source/destination-agnostic spine. No I/O here — this module is
imported into the Temporal workflow sandbox, so it must stay side-effect-free.

The whole library reduces to two seams:

  * a Source produces `Record`s (fetch + map your data),
  * a Destination upserts them idempotently.

Everything painful — durable orchestration, idempotent upsert, OAuth refresh,
pagination, rate-limit backoff, error handling — lives in the spine and is
inherited for free. To add a source you implement `Source`; to add a
destination, `Destination`. Reference implementations: GitHub (source), Notion
and Asana (destinations).
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Any, AsyncContextManager, Protocol, runtime_checkable


@dataclass
class Record:
    """One row to upsert, in DESTINATION-AGNOSTIC form. `properties` values are
    NEUTRAL Python types — the Destination owns wire-encoding, so a Source author
    never learns a destination's quirks (Notion's multi-select JSON, Asana's
    custom fields, etc.):

        str            -> text / url / select / title
        bool           -> checkbox
        int | float    -> number
        list[str]      -> multi-select
        datetime.date  -> date          datetime.datetime -> datetime
        None           -> property omitted

    `primary_key` is the IMMUTABLE idempotency key (e.g. a repo id), never a
    name/URL — this is what makes at-least-once retries safe. `body` is optional
    long-form content (e.g. a README / task notes), written on create.
    """
    primary_key: str
    properties: dict[str, Any]
    body: str | None = None


@dataclass
class SourceSpec:
    """One unit of work for a Source, handed to its per-source entity workflow.
    `key` is a stable id used to derive the workflow id. `params` is opaque,
    source-defined config (e.g. {"kind": "org", "org": "temporal-community"})."""
    key: str
    interval_minutes: int = 30
    params: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Source(Protocol):
    """Implement this for your data source. GitHubSource is the reference impl."""
    name: str

    def specs(self) -> list[SourceSpec]:
        """One SourceSpec per independent unit (each gets its own workflow)."""
        ...

    async def fetch(
        self, spec: SourceSpec, only_items: list[str] | None = None
    ) -> list[Record]:
        """Fetch (optionally just `only_items`) and map to Records. All
        source-specific I/O and field-mapping happens here. Returns the WHOLE unit
        in one list — simplest, and fine up to ~hundreds of records.

        For a source that can return many thousands, implement `fetch_page` (below)
        instead: the spine drives it page-by-page so neither the fetch result nor
        the upsert ever passes through Temporal history as one oversized payload."""
        ...

    # OPTIONAL (checked via getattr by the spine — like the Destination's aux hooks),
    # but PREFERRED: every shipped connector (GitHub/Luma/YouTube/Contentful)
    # implements it, with fetch() as a thin drain over it. The spine calls it
    # repeatedly, threading your cursor, and upserts each page before asking for the
    # next — bounding history regardless of total size. When present it's used in
    # preference to fetch(). A genuinely tiny source can skip it and just implement
    # fetch() (the spine treats that as one page).
    #
    #   async def fetch_page(
    #       self, spec: SourceSpec, only_items: list[str] | None, cursor: str | None
    #   ) -> tuple[list[Record], str | None]:
    #       """Return (records_for_this_page, next_cursor). next_cursor is None on
    #       the last page. `cursor` is None on the first call. Opaque to the spine —
    #       use whatever your API's pagination token is (offset, page no, cursor)."""


class DestinationSession(Protocol):
    """An open connection to the destination for one sync pass."""

    async def query_existing_ids(self) -> dict[str, str]:
        """{ primary_key -> destination-internal id } for rows already present."""
        ...

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        """Insert a new row. `synced_at` is the sync-pass timestamp (a real
        datetime — the destination formats it however its schema needs).
        Returns True if written, False if SKIPPED (e.g. a destination-side enrich
        hook dropped the record as out-of-scope)."""
        ...

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        """Refresh an existing row, leaving `create_only` properties untouched so
        human edits to those seeds survive. `synced_at` as in create(). Returns
        True if written, False if skipped."""
        ...


class Destination(Protocol):
    """Implement this for your destination. NotionDestination / AsanaDestination
    are the reference impls (MCP+OAuth and REST+PAT respectively — the protocol is
    intentionally neither transport- nor auth-shaped)."""
    name: str

    # True once the destination has the config it needs to write (e.g. a target
    # id). The spine refuses to sync an unconfigured destination.
    configured: bool

    # Properties written only on CREATE — enrichment seeds a human refines, never
    # overwritten on update. The mechanism is generic; each Source supplies which
    # fields. Honored by update().
    create_only_properties: set[str]

    def connect(self) -> AsyncContextManager[DestinationSession]: ...

    # OPTIONAL hooks (checked via getattr by the worker — don't define if unused):
    #   def aux_workflows(self) -> list: ...   extra Temporal workflows to register
    #   def aux_activities(self) -> list: ...  extra activities to register
    # e.g. the Notion destination registers its token-owner auth workflow here.

    @property
    def config_hint(self) -> str:
        """Human-readable hint naming what to set when `configured` is False
        (e.g. an env var). Keeps destination-specific config names out of the
        generic spine's error messages."""
        ...

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """True if `err` is an auth failure only a human can fix (so the workflow
        pauses instead of hammering). Destination-specific. OPTIONAL: destinations
        with no interactive auth (e.g. a local DB) should just `return False`.
        Most HTTP destinations can delegate to `auth_error_in_chain` below."""
        ...


class DestinationHTTPError(RuntimeError):
    """An HTTP error from a destination, carrying the numeric `status_code`
    SEPARATELY from the message. `auth_error_in_chain` keys auth-classification on
    this code rather than scanning the (up-to-600-char) response body — where a
    stray standalone "403" in an error payload would spuriously pause the workflow.
    Destinations should raise this (not a bare RuntimeError) for HTTP failures."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


# Default auth-failure signatures shared by HTTP destinations. The numeric code is
# taken from a DestinationHTTPError.status_code when present; otherwise (a plain
# exception) we fall back to a WORD-BOUNDARY text match so a bare "401"/"403"
# inside a UUID or request-id can't false-positive — the bug that once paused a
# workflow on a Notion validation_error whose id contained "401e".
_AUTH_TEXT_NEEDLES = ("unauthorized", "forbidden", "invalid_token", "invalid_grant")
_AUTH_STATUS_CODES = (401, 403)
_AUTH_CODE_RE = re.compile(r"\b(401|403)\b")


def auth_error_in_chain(err: BaseException, *, extra_needles: tuple[str, ...] = ()) -> bool:
    """Shared `is_auth_error` implementation: walk `err`'s cause/context chain and
    any ExceptionGroup, returning True if any link looks like a human-fixable auth
    failure (401/403, unauthorized, forbidden, invalid_token/grant). A destination
    passes `extra_needles` for service-specific phrasings (e.g. Asana's "not
    authorized"). Pure/deterministic — no I/O — so it's safe to import widely.

    Classification order per link: (1) a DestinationHTTPError's exact status_code;
    (2) a text needle; (3) ONLY when the link carries no status_code, a
    word-boundary 401/403 in the message. (3) is skipped for status-carrying errors
    so a 422/500 whose body mentions "403" isn't misread as auth.

    This lives in the spine so every destination shares ONE correct matcher
    instead of re-deriving the chain walk + code check (which is exactly where
    Notion and Asana had drifted apart)."""
    needles = _AUTH_TEXT_NEEDLES + tuple(n.lower() for n in extra_needles)
    seen: set[int] = set()
    stack: list[BaseException] = [err]
    while stack:
        cur = stack.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        status = getattr(cur, "status_code", None)
        msg = str(cur).lower()
        if status in _AUTH_STATUS_CODES:
            return True
        if any(n in msg for n in needles):
            return True
        if status is None and _AUTH_CODE_RE.search(msg):
            return True
        if isinstance(cur, BaseExceptionGroup):
            stack.extend(cur.exceptions)
        for nxt in (cur.__cause__, cur.__context__):
            if nxt is not None:
                stack.append(nxt)
    return False

"""Neutral schema model + inference — Layer 1 of destination schema generation.

The reusable, destination-AGNOSTIC heart: given a sample of `Record`s, infer a
neutral `Schema` (columns described by neutral `Kind` + `Role`). A destination then
materializes that neutral schema into its own wire vocabulary via the optional
`Destination.ensure_schema(schema)` hook (Layer 2) — Notion to a `CREATE TABLE` DDL,
Contentful to content-type fields, etc. Keeping inference here, not in a connector,
is the same library-owns-mechanism / destination-owns-vocabulary split as the rest
of the spine.

PURE and import-light (mirrors `core.py`): no I/O, no `datetime.now`, no randomness,
deterministic for a given input — so it's trivially unit-testable and safe to import
anywhere. The neutral type map matches `core.Record`'s contract:

    str            -> TEXT          (SELECT only via an explicit override — two
    bool           -> CHECKBOX       strings are indistinguishable from free text,
    int | float    -> NUMBER         so inference never guesses SELECT)
    list[str]      -> MULTI_SELECT
    datetime.date  -> DATE          datetime.datetime -> DATE (has_time=True)
    None           -> column skipped (can't infer a type from nothing) unless the
                      name is the title/key/synced or carries an override
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Iterator

from durable_sync.core import Record


class Kind(str, Enum):
    """Neutral column kinds. Subclass of `str` so values compare/serialize plainly."""
    TEXT = "text"
    NUMBER = "number"
    CHECKBOX = "checkbox"
    MULTI_SELECT = "multi_select"
    SELECT = "select"          # override-only; inference never produces this
    DATE = "date"


class Role(str, Enum):
    """A column's role in the sync. TITLE/KEY/SYNCED are the three the spine cares
    about; everything else is NORMAL. (There's no BODY role — `Record.body` is
    long-form content the destination writes to page-content/notes, not a column.)"""
    TITLE = "title"
    KEY = "key"
    SYNCED = "synced"
    NORMAL = "normal"


@dataclass(frozen=True)
class Column:
    name: str
    kind: Kind
    role: Role = Role.NORMAL
    # Only meaningful for DATE: a datetime carries time-of-day, a date does not.
    has_time: bool = False


@dataclass(frozen=True)
class Schema:
    """An ordered, neutral description of a destination table. Immutable so an
    inferred schema is safe to pass around / compare in tests. `name` is the
    human-facing table name (Notion database title, Contentful content-type name,
    …); None lets the destination pick a default."""
    columns: tuple[Column, ...]
    name: str | None = None

    def __iter__(self) -> Iterator[Column]:
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)

    @property
    def title(self) -> Column | None:
        return next((c for c in self.columns if c.role is Role.TITLE), None)

    def by_name(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


def _coerce_kind(value: Any) -> Kind:
    """Accept a `Kind`, or a string naming one (case-insensitive, by enum NAME like
    'MULTI_SELECT' or value like 'multi_select')."""
    if isinstance(value, Kind):
        return value
    s = str(value).strip().lower()
    try:
        return Kind(s)                      # by value: "multi_select"
    except ValueError:
        try:
            return Kind[s.upper()]          # by name: "MULTI_SELECT"
        except KeyError:
            raise ValueError(f"unknown column kind: {value!r}") from None


def _kind_of(value: Any) -> tuple[Kind, bool]:
    """Map one neutral Python value to (Kind, has_time). bool BEFORE int — bool
    subclasses int, so the order matters."""
    if isinstance(value, bool):
        return Kind.CHECKBOX, False
    if isinstance(value, (int, float)):
        return Kind.NUMBER, False
    if isinstance(value, dt.datetime):      # before date — datetime subclasses date
        return Kind.DATE, True
    if isinstance(value, dt.date):
        return Kind.DATE, False
    if isinstance(value, (list, tuple)):
        return Kind.MULTI_SELECT, False
    return Kind.TEXT, False


def infer_schema(
    records: Iterable[Record],
    *,
    title: str,
    key: str | None = None,
    synced: str | None = None,
    overrides: dict[str, Any] | None = None,
    name: str | None = None,
) -> Schema:
    """Infer a neutral `Schema` from a sample of `Record`s.

    `title` is the (always-emitted) title column; `key` the idempotency-key column;
    `synced` an optional sync-heartbeat DATE column the destination stamps at write
    time (so it's emitted even though it never appears in `properties`). `overrides`
    maps a column name to a forced `Kind` (or a string naming one) — the only way to
    get a `SELECT`, and the way to pin a column inference would otherwise guess.
    `name` is the human-facing table name carried onto the resulting `Schema`.

    Deterministic: column order is title, key, then other columns in first-seen
    order, then synced last (matching the hand-written DDL in smoke_notion.py).
    """
    overrides = {k: _coerce_kind(v) for k, v in (overrides or {}).items()}

    # Collect per-name observed kinds, preserving first-seen order across records.
    observed: dict[str, set[tuple[Kind, bool]]] = {}
    for rec in records:
        for prop_name, value in rec.properties.items():
            seen = observed.setdefault(prop_name, set())
            if value is not None:
                seen.add(_kind_of(value))

    def resolve(name: str) -> tuple[Kind, bool] | None:
        """The (kind, has_time) for a name, honoring overrides and conflicts.
        None means 'no type known' (all-None / never observed, no override)."""
        if name in overrides:
            return overrides[name], False
        kinds = observed.get(name) or set()
        if not kinds:
            return None                     # nothing to infer from
        if len(kinds) == 1:
            return next(iter(kinds))
        # Mixed types across records -> TEXT is the safe superset (all stringify).
        return Kind.TEXT, False

    columns: list[Column] = []
    placed: set[str] = set()

    def add(name: str, role: Role, *, default: tuple[Kind, bool] | None = None) -> None:
        if name in placed:
            return
        resolved = resolve(name)
        if resolved is None:
            if default is None:
                return                      # skip: can't infer and no fallback
            resolved = default
        kind, has_time = resolved
        columns.append(Column(name=name, kind=kind, role=role, has_time=has_time))
        placed.add(name)

    # 1) title — always present (every destination table needs one); TEXT default.
    add(title, Role.TITLE, default=(Kind.TEXT, False))
    # 2) key — emitted if named; TEXT default (it stores the primary_key string).
    if key:
        add(key, Role.KEY, default=(Kind.TEXT, False))
    # 3) every other observed (or override-only) column, in first-seen order.
    for prop_name in [*observed, *overrides]:
        if prop_name == synced:
            continue                        # synced is appended last, below
        add(prop_name, Role.NORMAL)
    # 4) synced — trailing DATE the destination stamps at write time.
    if synced:
        add(synced, Role.SYNCED, default=(Kind.DATE, False))

    return Schema(columns=tuple(columns), name=name)

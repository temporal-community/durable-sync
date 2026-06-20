# Contributing to durable-sync

durable-sync is a small spine with a few well-defined seams. Almost everything you'd
add is one of four things: a **source**, a **destination**, an **auth mechanism**, or a
**transformation**. This guide shows how to add each, with the real signatures and the
gotchas we learned the hard way.

## The mental model (read this first)

```
  Source.fetch(spec) ─► [Record, …] ─► (transform) ─► Destination upserts (idempotent, keyed on primary_key)
```

- A **`Record`** is the neutral interchange unit: `{primary_key, properties, body}`.
  `properties` values are *plain Python* — `str`, `bool`, `int/float`, `list[str]`,
  `datetime.date`/`datetime`, or `None` (omit). The **destination** owns all wire-encoding,
  so a source author never learns a destination's quirks.
- **`primary_key`** is the immutable idempotency key (an upstream id — never a name/URL).
- The whole thing runs on Temporal: one durable entity workflow per source unit, idempotent
  upsert, retries, OAuth refresh — all inherited. You write the seam, not the plumbing.

Dev setup:

```bash
pip install -e ".[all,dev]"     # editable, with every integration extra
python -m pytest                # unit tests (no network)
# live smokes need a dev server: temporal server start-dev
```

---

## Add a Source

Implement the `Source` protocol (`durable_sync/core.py`). Reference: `durable_sync/sources/github`.

```python
from durable_sync.core import Record, SourceSpec

class MySource:
    name = "my-source"

    def specs(self) -> list[SourceSpec]:
        # One SourceSpec per independent unit → each gets its own durable workflow.
        return [SourceSpec(key="things", interval_minutes=30, params={"kind": "things"})]

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        rows = await call_my_api(spec.params)          # all source I/O lives here
        return [
            Record(
                primary_key=str(row["id"]),            # IMMUTABLE — never a name/URL
                properties={"Name": row["title"], "Stars": row["likes"]},  # neutral values
                body=row.get("notes"),                 # optional long-form content
            )
            for row in rows
        ]
```

- Keep config **injected**, not hardcoded — see `GitHubConfig` (orgs/topic/maps are passed in,
  not baked into the source).
- Long fetch? Heartbeat with `activity.heartbeat(...)` guarded by `activity.in_activity()` so
  the source stays runnable standalone (see `sources/github/source._heartbeat`).
- Need enrichment that uses *source internals* (a README, a tarball, org members)? Expose a
  **source-side enrich hook** that hands the app a typed context — see `RepoContext` and
  `GitHubSource(config, enrich=…)`. Don't make the app reach into your private fetchers.

Wire it in your app's `pipeline.py`: `SOURCE = MySource()`.

---

## Add a Destination

Implement `Destination` + `DestinationSession` (`durable_sync/core.py`). References:
`destinations/notion` (MCP + workflow-owned OAuth) and `destinations/asana` (REST + PAT) — two
deliberately different transports/auth, proving the protocol is neither.

```python
from contextlib import asynccontextmanager
import datetime as dt
from durable_sync.core import Record

class MyDestination:
    name = "my-dest"
    create_only_properties: set[str] = set()   # props written once, never overwritten on update

    def __init__(self, target_id: str):
        self.target_id = target_id

    @property
    def configured(self) -> bool:        # the spine refuses to sync if False
        return bool(self.target_id)

    @property
    def config_hint(self) -> str:        # what to set when not configured (no library-specific names in the spine)
        return "MY_TARGET_ID unset"

    @asynccontextmanager
    async def connect(self):
        async with open_client(self.target_id) as client:
            yield _MySession(client, self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        return False                      # return True only for human-fixable auth failures (see gotchas)


class _MySession:
    def __init__(self, client, dest):
        self._client, self._d = client, dest

    async def query_existing_ids(self) -> dict[str, str]:
        # { primary_key -> your-internal-row-id } for rows already present.
        ...

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        ...                               # encode record.properties → your wire format
        return True                       # return False to signal "skipped" (see session_enrich)

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        # Skip keys in self._d.create_only_properties so human edits survive.
        ...
        return True
```

Key contracts:
- **Idempotency is yours to honor:** `query_existing_ids` → update-or-create on `primary_key`.
  It must be stable (if you paginate, order by the key, or concurrent edits cause dupes).
- **`create`/`update` return `bool`** — `True` if written, `False` if skipped. The spine tallies
  `{created, updated, skipped}`.
- Where does the idempotency key live? Your call: Notion stores it in a property column; Asana
  uses the task's `external.gid`. Both fine.
- **Optional `session_enrich` hook** — for enrichment/filtering that must *read the destination*
  mid-write (e.g. resolve a relation). `async (session, record) -> Record | None`; return `None`
  to drop. See `NotionDestination(session_enrich=…)`.
- **Optional `aux_workflows()` / `aux_activities()`** — extra Temporal workflows/activities your
  destination needs registered (e.g. an auth workflow). The worker auto-registers them. Omit if
  you don't need any (Asana doesn't).

---

## Add an auth mechanism

Auth is per *destination*, and how much it needs depends on the credential:

- **Self-serve static token (PAT/API key)?** Don't add anything — read the env var and send the
  header *inline* in your destination. It's ~2 lines (see `AsanaDestination`). No package.
- **OAuth-as-an-individual (no admin token)?** Reuse the toolkit in `durable_sync/auth/oauth/` —
  the pure-HTTP flow (discovery/PKCE/DCR/refresh) **plus** `OAuthTokenWorkflow`, which *owns* the
  rotating refresh token and serves access tokens via query (so the secret never enters event
  history). Bind it like `destinations/notion` does: a thin `oauth.py` (your endpoints), expose
  the workflow via `aux_workflows`/`aux_activities`, and default your `token_provider` to query it.
- **A genuinely new mechanism with shared machinery?** Add `durable_sync/auth/<mechanism>/`,
  mirroring `auth/oauth/`. (We deliberately don't have an `auth/pat/` — a PAT has nothing to share.)

> ⚠️ The `auth/oauth/__init__.py` is intentionally **import-free** — see gotchas.

---

## Add a transformation

There are **three** transform seams; pick by what context the transform needs:

| Seam | Where | Signature | Use when… |
|------|-------|-----------|-----------|
| **Generic transform** | `make_activities(…, transform=)` / `run_worker(…, transform=)` | `Record -> Record \| None` | the transform needs only the Record (rename/derive/redact/**filter** — `None` drops it). Source- and destination-agnostic. |
| **Source-side enrich** | a source's own hook (e.g. `GitHubSource(enrich=)`) | `(Record, <SourceContext>) -> Record` | enrichment needs source internals (readme, tarball, members). |
| **Destination-side `session_enrich`** | a destination's hook (e.g. `NotionDestination(session_enrich=)`) | `(session, Record) -> Record \| None` | enrichment needs to *read the destination* (resolve a relation), or filter against it. |

All three may be sync or async. Returning `None` (where supported) drops the record — that's how
you implement filtering. Keep the *mechanism* generic; keep the *policy* (prompts, maps, taxonomies)
in your app.

---

## Testing

- **Unit-test the pure parts** with no network (e.g. `tests/test_asana_encode.py` tests the
  Record→wire encoding directly). This is what you'll thank yourself for.
- **`MemoryDestination`** (`tests/memory_destination.py`) is a full-protocol, network-free
  destination — use it to exercise the whole spine offline (`tests/smoke_spine.py`).
- **Live smokes** (`tests/smoke_github/notion/asana.py`) run against the real service — keep them
  runnable-by-hand, gated on a token, never in the default `pytest` run.
- A new destination should pass the spine end-to-end via the `MemoryDestination` pattern, and ideally
  ship a unit test for its encoding.

---

## Conventions & gotchas (hard-won — please don't relearn these)

- **Workflow sandbox is strict.** Workflow modules and anything they import at module load must be
  deterministic and free of heavy deps. In particular: **keep package `__init__.py` files
  import-free** if the package contains a workflow — an eager re-export there once pulled `requests`
  into the sandbox and broke workflow validation. Import in submodules; import activities inside
  `with workflow.unsafe.imports_passed_through():`.
- **Sync activities need the thread pool.** A `requests`-based (blocking) activity must run under the
  worker's `activity_executor` — `run_worker` provides one. Prefer async activities (httpx) where you
  can.
- **Signal handlers must never raise.** A throwing handler *poisons the workflow task* (it re-fails
  forever). Keep them trivial (flip a flag), and tolerate stray payloads (`def resume(self, *_)`).
- **`is_auth_error` must be precise.** Match status codes with word boundaries — a bare `"401" in msg`
  false-positives on UUIDs/request-ids like `…-401e-…` and will wrongly pause a workflow.
- **Determinism:** no clock/IO/randomness in workflows; use `workflow.now()`. All side effects live in
  activities.
- **Records pass through workflow history** — fine at tens/hundreds; batch if a source grows into many
  thousands.
- **Never auto-delete.** Sync only ever creates/updates rows it fetched; rows it didn't fetch are left
  untouched (so hand-added metadata and out-of-scope rows survive).

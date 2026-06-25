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

## What belongs in durable-sync (the library vs. app boundary)

This question recurs, so here's the doctrine. durable-sync owns **durable, idempotent
sync mechanism**. Your app owns **domain policy**. The dividing test is *judgment*:

> If producing the answer needs your data or your rules, it's **policy → app**.
> If it's a mechanism that's the same for everyone, it's **library**.

- **Matching fuzzy identity is always the app's.** "Is GitHub `webchick` the same person
  as Luma `angie@…` as Contentful `Angie Byron`?" needs your roster and your matching rules
  (email-then-name, the "Angie vs Angela" problem). The library **never matches** — it has no
  basis to. Resolve it in an `enrich` / `session_enrich` hook (see [[Add a transformation]]).
- **Remembering a link the sync itself minted is the library's** — that's just idempotency.
  When a write creates a row, the sync knows *with certainty* which `primary_key` it came from,
  so it records that. This is `query_existing_ids` / `external.gid` / the Notion key column today.
  No judgment involved.
- **The escape hatch, stated explicitly:** a hook may *write* its resolved answer into any store;
  the library must never *compute* that answer. Mechanism carries policy's output; it never
  produces it. (So `primary_key` is an immutable upstream id, **never a name/URL** — names are
  exactly the fuzzy identity the library refuses to reason about.)
- **Promotion rule for shared infrastructure:** durable-sync exists because *two* apps shared
  logic and we extracted it. Hold new shared machinery (e.g. a cross-system entity/correspondence
  store) in the **app** until a **second** consumer proves it's common — then promote it with a
  real second use case to shape it, not a guess. One consumer is not yet a library feature.

When unsure, default to the app. It's cheap to promote later and expensive to walk back a
mechanism that baked in one app's policy.

---

## Add a Source

Implement the `Source` protocol (`durable_sync/core.py`). Reference: `durable_sync/connectors/github`.

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
  the source stays runnable standalone (see `connectors/github/source._heartbeat`).
- Need enrichment that uses *source internals* (a README, a tarball, org members)? Expose a
  **source-side enrich hook** that hands the app a typed context — see `RepoContext` and
  `GitHubSource(config, enrich=…)`. Don't make the app reach into your private fetchers.

Wire it in your app's `pipeline.py`: `SOURCE = MySource()`. To make it
discoverable by name (and movable between packages without breaking wiring),
register an entry point — see **Register a connector for discovery** below.

---

## Add a Destination

Implement `Destination` + `DestinationSession` (`durable_sync/core.py`). References:
`connectors/notion` (MCP + workflow-owned OAuth) and `connectors/asana` (REST + PAT) — two
deliberately different transports/auth, proving the protocol is neither. `connectors/jira` is a
second example of the Asana shape (REST + self-serve token, `field_map`, FK-on-the-record
idempotency) — and, like `connectors/luma`, ships a Source *and* a Destination in one connector.

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
        # HTTP destinations: delegate to the shared matcher — it walks the cause
        # chain + ExceptionGroups and matches 401/403 with WORD BOUNDARIES (see
        # gotchas). Pass extra_needles=(...) for service-specific phrasings.
        from durable_sync.core import auth_error_in_chain
        return auth_error_in_chain(err)   # no interactive auth (e.g. a local DB)? just `return False`


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
- **Optional `ensure_schema(schema)` hook** — schema generation (below). Omit it if your
  destination has no creatable schema or a fixed one (Asana's native fields).

---

## Generate a destination schema (instead of hand-authoring one)

Rather than hand-building a destination table (a Notion DB, a Contentful content type) and a
`SCHEMA.md` to document it, you can infer one from a sample of the source's `Record`s. Two layers,
split along the usual library-owns-mechanism / destination-owns-vocabulary line:

1. **Generic inference (the spine, pure):** `durable_sync.schema.infer_schema(records, *, title,
   key=, synced=, overrides=, name=) -> Schema` maps neutral `Record` values to a neutral `Schema`
   (`Column`s with a `Kind` — `TEXT/NUMBER/CHECKBOX/MULTI_SELECT/SELECT/DATE` — and a `Role` —
   `TITLE/KEY/SYNCED/NORMAL`). `str→TEXT, bool→CHECKBOX, int|float→NUMBER, list→MULTI_SELECT,
   date(time)→DATE`. `SELECT` is **override-only** (two strings are indistinguishable from free
   text). All-`None`/mixed columns are skipped or fall back to `TEXT`. Deterministic + unit-tested.

2. **Per-destination materialization (the optional hook):**

   ```python
   async def ensure_schema(self, schema: durable_sync.schema.Schema) -> str | None:
       """CREATE-ONLY: if already configured, return None and leave the live schema alone.
       Otherwise create the table from `schema` and return its new id."""
   ```

   `NotionDestination.ensure_schema` is the reference: it renders the neutral schema to a
   `CREATE TABLE (...)` DDL (`connectors/notion/ddl.py::schema_to_ddl`, pure + tested), calls
   `notion-create-database`, and sets its `data_source_id`. **Create-only by convention** — never
   drop/alter an existing schema (see "Never auto-delete"); the write path already tolerates drift
   by dropping unknown columns.

Drive it with the generic CLI (mirrors the OAuth bootstrap — fetches a sample, infers, materializes):

```bash
PYTHONPATH=. python -m durable_sync.bootstrap_schema \
    --source myapp.pipeline:SOURCE --destination myapp.pipeline:DESTINATION \
    --name "GitHub Repos" --override State=SELECT
```

`--title/--key/--synced` default to the destination's own `title_property`/`key_property`/
`synced_property`, so usually you only pass `--name` and any `--override NAME=KIND`.

---

## Add an auth mechanism

Auth is per *destination*, and how much it needs depends on the credential:

- **Self-serve static token (PAT/API key)?** Don't add anything — read the env var and send the
  header *inline* in your destination. It's ~2 lines (see `AsanaDestination`). No package.
- **OAuth-as-an-individual (no admin token)?** Reuse the toolkit in `durable_sync/auth/oauth/` —
  the pure-HTTP flow (discovery/PKCE/DCR/refresh) **plus** `OAuthTokenWorkflow`, which *owns* the
  rotating refresh token and serves access tokens via query (so the secret never enters event
  history). Bind it like `connectors/notion` does: a thin `oauth.py` (your endpoints), expose
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

## Register a connector for discovery

Implementing the protocol is enough to *run* a connector; registering an **entry
point** makes it discoverable by name and movable between packages (core ↔
`durable-sync-contrib` ↔ your own repo) without changing any app wiring. Two
groups — a provider that is both a source and a destination registers in both:

```toml
[project.entry-points."durable_sync.sources"]
my-source = "my_pkg.source:MySource"

[project.entry-points."durable_sync.destinations"]
my-dest = "my_pkg.destination:MyDestination"
```

After `pip install -e .`, confirm with `python -m durable_sync.registry` (lists
connectors grouped by providing package). An app then wires by name instead of
import path:

```python
from durable_sync.registry import load_source, load_destination
source = load_source("my-source")()
destination = load_destination("my-dest")(...config...)
```

The entry point resolves the **class**; the app supplies config and constructs
it. See **`CONTRACT.md`** for the versioned import surface you may depend on and
the core / contrib / not-available curation policy.

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
- **`is_auth_error` must be precise — don't hand-roll it.** Delegate to `core.auth_error_in_chain`,
  and raise HTTP failures as `core.DestinationHTTPError(status_code, msg)` so classification keys on
  the numeric status, **not** on the response body. (The matcher only word-boundary-scans the *text*
  for a code when no `status_code` is present — a fallback; otherwise a 422/500 whose body contains a
  standalone `403` would wrongly pause the workflow. A bare `"401" in msg` also false-positives on
  UUIDs/request-ids like `…-401e-…`.) Both reference destinations re-rolled this and drifted — Asana
  lost the word-boundary check *and* the `403` case — which is why it's now one shared matcher.
- **HTTP fetches should back off, not hammer.** Route REST calls (source or destination) through
  `durable_sync.http.request_with_retry`, which honors `Retry-After` and backs off on `429` / GitHub's
  rate-limited `403`. And don't let an enrichment call fail *silently* — a swallowed error that returns
  `[]`/`{}` looks like real-but-empty data downstream; `log.warning` first (see `connectors/github/api.py`).
  (The Notion destination is the exception — MCP surfaces failures as `isError` results, not HTTP
  statuses, so it keeps its own small retry loop.)
- **Determinism:** no clock/IO/randomness in workflows; use `workflow.now()`. All side effects live in
  activities.
- **Schema generation is create-only and lives in two layers.** Inference (`durable_sync.schema`) is
  generic and pure; materialization is the per-destination optional `ensure_schema(schema)` hook.
  Never drop/alter an existing schema (see "Never auto-delete") — a configured destination must
  no-op and return `None`. Don't put inference in a connector or make the hook required.
- **Records pass through workflow history** — the spine paginates + chunks (≤`_SYNC_CHUNK_SIZE` per
  upsert) so payloads stay under Temporal's 2MB limit. A small source can just implement `fetch()`
  (one page); a large one should implement the optional `fetch_page(spec, only_items, cursor) ->
  (records, next_cursor)` so the **fetch** result is bounded too, not just the upsert.
- **Evolving the long-lived entity workflows is a redeploy hazard.** `SourceSyncWorkflow` /
  `OAuthTokenWorkflow` run for days between continue-as-new, so a deploy spans live histories. Any
  change to the *shape/order* of commands in a run loop (add/remove/reorder an activity call, change
  `_SYNC_CHUNK_SIZE`) will cause non-determinism errors on replay. Either gate the change behind
  `workflow.patched("...")`, or opt into Worker Versioning by setting `DURABLE_SYNC_BUILD_ID` (the
  worker then pins workflows to their build and new code only affects new/continued runs). Local/dev
  runs leave it unset and stay unversioned.
- **Never auto-delete.** Sync only ever creates/updates rows it fetched; rows it didn't fetch are left
  untouched (so hand-added metadata and out-of-scope rows survive).

## Scaling (and when to reach past the built-in paging)

The spine already bounds Temporal history for large sources: a source implements
`fetch_page(spec, only_items, cursor) -> (records, next_cursor)` and the workflow drives it
page-by-page, upserting each page (chunked to ≤`_SYNC_CHUNK_SIZE`) before fetching the next — so no
single payload approaches the 2MB limit. All four shipped connectors do this; `fetch()` is just a
drain over `fetch_page` for standalone use. **This is the right tool up to ~hundreds of thousands of
records per unit.**

It has two deliberate limits, both inherent to processing one unit inside one workflow run:

1. **Serial.** Pages are fetched and synced one after another — wall-clock scales with the page count.
   There's no cross-page parallelism.
2. **One run per sweep.** A full sweep happens within a single `_run_once`; continue-as-new only
   fires *between* runs. A sweep with a very large number of pages accumulates that many activity
   events in one history (bounded by Temporal's ~50k-event limit) before it rolls.

When a real source needs **parallelism** (sync time matters) or crosses into the **millions** (event-count
pressure), graduate to a batch framework rather than growing this loop:

> **[batch-orchestra](https://github.com/drewhoskins/batch-orchestra)** — a Temporal library for
> high-scale batch processing: pipelined pagination (enqueue the next cursor *while* processing the
> current page → tens-to-hundreds of pages in flight) plus continue-as-new *during* the batch, so it
> stays under the event limit at millions of items.

The integration seam is already in place: our `fetch_page(cursor) -> (records, next_cursor)` maps
almost 1:1 onto batch-orchestra's page/cursor model, so adopting it is a localized change to
`_run_once` (swap the serial page-loop for a batch-orchestra run driven by the same `fetch_page`) — not
a connector rewrite. Don't add the dependency pre-emptively; reach for it when a source actually hits
that tier.

## Releasing (to PyPI)

Releases publish automatically via GitHub Actions + PyPI **trusted publishing** (OIDC — no API tokens
stored anywhere). `.github/workflows/publish.yml` builds, `twine check`s, and uploads whenever a
GitHub Release is published.

**One-time PyPI setup** (because `durable-sync` doesn't exist on PyPI yet, use a *pending* publisher):
PyPI → *Account settings → Publishing → Add a pending publisher* —
- PyPI project name: `durable-sync`
- Owner: `temporal-community`  ·  Repository: `durable-sync`
- Workflow filename: `publish.yml`  ·  Environment name: `pypi`

(Optionally create a GitHub Environment named `pypi` with required reviewers, for a manual approval
gate before each publish.)

**To cut a release:**
1. Bump `version` in `pyproject.toml` (PyPI versions are immutable — never reuse one).
2. Commit, then create a GitHub Release whose tag matches (e.g. `v0.1.0`).
3. The workflow builds and publishes; watch it under the repo's Actions tab.

The very first publish creates the project and "claims" the trusted publisher; the pending publisher
becomes a normal one automatically.

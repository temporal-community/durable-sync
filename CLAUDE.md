# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A **library**, not an app. It provides a durable source→destination sync *spine* on
Temporal; a consuming app wires a concrete `Source` + `Destination` together (the
"`pipeline.py`" referenced in CONTRIBUTING.md is the app's, not in this repo). GitHub→Notion
is the reference wiring. Read **CONTRIBUTING.md** first — it is the authoritative guide for
adding a source / destination / auth mechanism / transformation, with real signatures and the
hard-won gotchas. This file covers the big-picture architecture and commands.

## Commands

```bash
pip install -e ".[all,dev]"          # editable install with every integration extra
python -m pytest                      # unit tests — no network (this is the default CI run)
python -m pytest tests/test_asana_encode.py        # single test file
python -m pytest tests/test_asana_encode.py -k name # single test by name

# Live smokes hit real services / a real Temporal server — run by hand, never in pytest.
# All entrypoints need PYTHONPATH=. and a dev server: `temporal server start-dev`
PYTHONPATH=. python tests/smoke_spine.py            # full spine, network-free MemoryDestination
PYTHONPATH=. python tests/smoke_github.py           # gated on a real token
python -m durable_sync.codec                        # generate a base64 AES-256 key for DURABLE_SYNC_ENC_KEY
```

OAuth-over-MCP is a two-step, run-once setup per provider (no admin token — authorizes as an
individual via discovery/PKCE/DCR, then a workflow owns the rotating refresh token). Same flow for
Notion and Contentful (the binding just pins the MCP base URL + creds file + workflow id):
```bash
PYTHONPATH=. python -m durable_sync.connectors.notion.bootstrap      # interactive -> saves creds
PYTHONPATH=. python -m durable_sync.connectors.notion.start          # hands refresh token to OAuthTokenWorkflow
# Contentful (e.g. SSO-blocked spaces): same two steps, plus `prove`/`describe` to list MCP tools
PYTHONPATH=. python -m durable_sync.connectors.contentful.bootstrap
PYTHONPATH=. python -m durable_sync.connectors.contentful.start
```

Drive/inspect a running entity workflow by id:
```bash
temporal workflow signal --workflow-id "durable-sync:<spec.key>" --name sync_now --input '[]'
temporal workflow query  --workflow-id "durable-sync:<spec.key>" --type status
```

Config is all env vars (see `config.py`): `TEMPORAL_ADDRESS/NAMESPACE/API_KEY` (set the API key
for Temporal Cloud → TLS), `DURABLE_SYNC_TASK_QUEUE`, `DURABLE_SYNC_ENC_KEY`. Integration-specific
config (which orgs, which Notion DB) lives in the wired Source/Destination, never in `config.py`.

## Architecture

The whole library reduces to two seams an app implements; everything painful is inherited:

```
Source.fetch(spec) ─► [Record, …] ─► (transform) ─► Destination upserts (idempotent, keyed on primary_key)
```

- **`core.py`** — the entire contract: `Record` (neutral interchange unit — `properties` are plain
  Python values, the destination owns all wire-encoding), `SourceSpec`, and the `Source` /
  `Destination` / `DestinationSession` protocols. **Side-effect-free and import-light** because it
  is loaded into the Temporal workflow sandbox.
- **`activities.py`** — `make_activities(source, destination, transform=)` is a **factory**: a
  library can't `from pipeline import SOURCE` the way an app would, so the app calls this once with
  its wired seams. Produces two activities registered under stable string names (`FETCH_SOURCE`,
  `SYNC_RECORDS`). `sync_records` is the idempotent upsert: `query_existing_ids` → update-or-create
  per `primary_key`, tallying `{created, updated, skipped}`.
- **`workflows/sync.py`** — `SourceSyncWorkflow`, one long-lived **entity workflow per source unit**.
  It is its own durable interruptible timer (sleeps `interval_minutes`, wakes early on a `sync_now`
  signal), answers a `status` query, and uses continue-as-new to bound history. **There is no
  Temporal Schedule — the loop itself is the periodicity.** It calls activities *by name*, so it
  never imports their closures and stays sandbox-clean.
- **`worker.py`** — `run_worker(SOURCE, DESTINATION)` assembles a Worker hosting the workflow +
  activities, plus any `aux_workflows()` / `aux_activities()` the destination exposes (checked via
  `getattr`). Provides a thread-pool `activity_executor` so **sync activities** (e.g. Notion's
  `requests`-based OAuth refresh) can run.
- **`bootstrap.py`** — `start_sources(SOURCE)` starts one workflow per `spec` with id
  `durable-sync:<spec.key>` using `USE_EXISTING`, so it's idempotent and doubles as a reconcile.
- **`schema.py` + `bootstrap_schema.py`** — optional schema generation. `schema.infer_schema(...)`
  maps a sample of `Record`s to a neutral `Schema` (generic + pure); a destination materializes it
  via the optional `ensure_schema(schema)` hook (Notion → `CREATE TABLE` via `connectors/notion/
  ddl.py`; others omit it). `bootstrap_schema` is the generic CLI front door. **Create-only** —
  inference is generic, materialization is per-destination, and an existing schema is never touched.
- **`transport/`** — transport mechanisms, orthogonal to `auth/`: `transport/mcp.py` is the generic
  MCP session/`call`/tool-listing over streamable-HTTP (Notion + Contentful both ride it — its
  second consumer is why it was promoted out of `connectors/notion`); REST connectors use `http.py`.
  A connector composes a transport + an auth mechanism. (`http.py` is the other transport; it can
  move under `transport/` for symmetry — trivial follow-up.)
- **`http.py`** — shared httpx retry/backoff (`request_with_retry`) for REST connectors:
  honors `Retry-After`, backs off on `429` and GitHub's rate-limited `403`. Runs in activities, so
  wall-clock sleeps are fine; sleeps are capped so a long rate-limit window becomes an activity retry.
- **`temporal_client.py` + `codec.py`** — `connect()` is the single place a client is opened, with
  the opt-in AES-GCM payload codec wired into the data_converter (must be consistent across worker,
  starters, and token accessor or one client reads ciphertext it can't decode).

### Two load-bearing patterns

1. **Auth failure pauses the workflow instead of hammering.** When `destination.is_auth_error(e)` is
   true, `sync_records` re-raises as a non-retryable `ApplicationError(type="AuthError")`. The
   workflow's `_is_auth_failure` walks the cause chain, sets `paused=True`, and stops the timer loop.
   A human re-authorizes, then sends the `resume` signal to catch up. (`ConfigError` is the other
   non-retryable type; everything else stays retryable/transient.)
2. **A workflow owns the rotating OAuth refresh token** (`auth/oauth/` + `connectors/notion`). The
   refresh token lives in `OAuthTokenWorkflow` state and serves fresh access tokens via query — so
   refreshes serialize (no rotation race), survive restarts, and the secret never enters event
   history. This is why the encryption codec exists (it encrypts the token in history at rest).

## Conventions that will bite you (full list in CONTRIBUTING.md "gotchas")

- **Keep `__init__.py` import-free** in any package containing a workflow — an eager re-export once
  pulled `requests` into the sandbox and broke workflow validation. `auth/oauth/__init__.py` is
  intentionally empty for this reason. Import in submodules; use `with workflow.unsafe.imports_passed_through():`.
- **Signal handlers must never raise** — a throwing handler poisons the workflow task forever. Keep
  them flag-flips only, and let no-arg signals absorb stray payloads (`def resume(self, *_)`).
- **`is_auth_error` — delegate to `core.auth_error_in_chain`**, don't hand-roll it. It matches
  `401/403` with word boundaries (a bare `"401" in msg` false-positives on UUIDs/request-ids and
  wrongly pauses the workflow) and walks the cause chain + ExceptionGroups. Pass `extra_needles=`
  for service-specific phrasings.
- **HTTP calls go through `durable_sync.http.request_with_retry`** (REST connectors) — it
  honors `Retry-After` and backs off on `429`/rate-limited `403`. Notion is the exception (MCP
  surfaces errors as `isError` results, so it has its own retry loop in `NotionDestination.call`).
- **Determinism in workflows**: `workflow.now()` not `datetime.now()`; no IO/randomness; all side
  effects in activities.
- **Never auto-delete.** Sync only creates/updates rows it fetched; rows it didn't fetch are left
  untouched, so hand-added metadata and out-of-scope rows survive.
- **Records pass through workflow history**, so the spine paginates fetch + chunks the upsert
  (`_SYNC_CHUNK_SIZE`) to stay under Temporal's 2MB payload limit. Large sources implement the optional
  `Source.fetch_page(...)` to bound the fetch too; small ones just implement `fetch()`.
- **Long-lived workflows + redeploys:** changing a run-loop's command shape breaks replay of in-flight
  histories. Guard with `workflow.patched(...)` or opt into Worker Versioning via `DURABLE_SYNC_BUILD_ID`.

## Sources

`connectors/github` is the reference, but `luma`, `youtube`, and `contentful` follow the same shape:
an injected config dataclass (secrets read from an env var named in the config, never hardcoded), an
`api.py` of pure async-httpx fetchers + pure transforms, a `source.py` mapping to neutral `Record`s,
and an optional source-side `enrich` hook handing the app a typed context (raw entry + live client)
so app policy — e.g. matching authors/hosts to a roster — stays out of the source. All REST fetchers
go through `http.request_with_retry`. Contentful has two auth modes (CDA delivery token preferred;
CMA PAT fallback, the only mode that sees drafts), selected by which token env var is set.

`connectors/spotify` is a source too, but differs in two ways worth knowing. (1) Its primary_key is
the track's **ISRC**, not the Spotify id — the ISRC is the cross-service identity a destination
(e.g. Apple Music) can resolve, so a Spotify track id would be useless for dedupe. Tracks with no
ISRC are dropped + logged. (2) Auth is workflow-owned OAuth (the Notion/Contentful `OAuthTokenWorkflow`
pattern, reused unchanged) rather than an env-var API key — but Spotify has **no DCR/discovery**, so
its `oauth.py` pins fixed endpoints + the `user-library-read` scope, and the source gets its access
token from a `token_provider` (default: query the auth workflow) instead of reading a token env var.

`connectors/jira` is **both** a source and a destination (issues), so it's the second proof — after
Luma — that the two seams compose in one connector. The source maps a JQL query (one `SourceSpec` per
query; a project key is sugar that builds one) to neutral `Record`s, paginating on Jira's
`nextPageToken`. Two Jira gotchas live here: (1) `primary_key` is the issue **id**, never the key
(`ENG-123`) — a key *changes* when an issue moves projects, which would break idempotent upsert; the
key is kept only as a property + in the URL. (2) the description is **ADF** (Atlassian Document
Format), so `api.py` carries pure `adf_to_text`/`text_to_adf` transforms. Auth is self-serve HTTP
Basic (`JIRA_EMAIL` + `JIRA_API_TOKEN`, base `JIRA_BASE_URL`) read inline — no OAuth, like Asana. Its
columns are issue-specific, so like GitHub it opts out of `content.py`. See "Two write paths" below for
the destination's idempotency trick.

The content-style sources (luma/youtube/contentful/spotify) share one neutral column vocabulary via
`connectors/content.py` (`content_record(...)` + `P_*` constants) so the names live in one place, not
copy-pasted per source (GitHub opts out — its columns are repo-specific). `connectors/multi.py`'s
`MultiSource(*sources)` fans several sources onto one worker/bootstrap by namespacing each inner
spec key as `<source-name>:<key>` and routing `fetch` back by that prefix — use it for a bundle on
one task queue; use a single source directly otherwise.

## Two write paths for Contentful

`ContentfulDestination` (REST CMA, clean JSON) needs a CMA token. When that's blocked (e.g. the org
SSO-gates static tokens), `ContentfulMcpDestination` writes over the MCP server with a workflow-owned
OAuth token instead — same auth toolkit as Notion. Its responses are agent-oriented pseudo-XML, so
`connectors/contentful/mcp.py` scrapes only the two scalars writes need (entry id from the sys URN,
`version` for the optimistic-lock update) rather than parsing the document; the field-encoding
(`encode.py`) is shared with the REST destination. `publish` is optional and tolerant — Contentful's
MCP *app installation* has its own per-tool permission layer (separate from OAuth scopes), so a
forbidden `publish_entry` leaves a draft + warning, never fails the row. MCP *reads* are deliberately
not built (multi-entry XML is fragile) — use the REST source when you have CMA access.

## Jira destination idempotency (the Asana shape, native variant)

`JiraDestination` follows `AsanaDestination`'s shape — fixed issue schema + custom fields by id
(`customfield_NNNNN`) via a destination-owned `field_map`; unmapped properties are dropped — but
stashes the source `primary_key` in a native issue **entity property** (default key `durable-sync`)
rather than Asana's `external.gid`. `query_existing_ids` scopes a JQL search to the project and reads
that property **inline** (`properties=[...]` on `/search/jql` returns the value on each issue), so it
recovers `primary_key → issue id` with no N+1 — and create stamps the property in a second call right
after the insert (failing loudly if that stamp doesn't land, so a retry can't silently duplicate).
**Gotcha (confirmed live):** an entity property set via the REST API is *not JQL-indexed* unless a
Forge/Connect app registers it, so you cannot *filter* on it (`issue.property[...] IS NOT EMPTY`
matches nothing) — you can only *read it back inline*. Hence the scope-by-project-then-read pattern,
exactly like Asana reading `external.gid` off every task in the project. It needs no auth workflow, so
it defines no `aux_workflows`/`aux_activities`.

## Testing a source / destination

A destination should pass the spine end-to-end via the `MemoryDestination` pattern
(`tests/memory_destination.py` is a full-protocol, network-free destination; `tests/smoke_spine.py`
exercises the whole spine offline) and ship a unit test for its Record→wire encoding (see
`tests/test_asana_encode.py`). A source should unit-test its pure `_to_record` normalizer with no
network (see `tests/test_{luma,youtube,contentful,jira}_normalize.py`). A connector that is both
(Jira) ships both: `tests/test_jira_normalize.py` (issue→Record + ADF flatten) and
`tests/test_jira_encode.py` (Record→issue fields + ADF + idempotency-on-create).

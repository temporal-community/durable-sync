# Changelog

All notable changes to this project are documented here. The **connector
contract** (the import surface out-of-repo connectors depend on, see
`CONTRACT.md`) is versioned by `durable_sync.core.CONTRACT_VERSION`; any
backward-incompatible change to it is called out below with a migration note.

The format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.3.0] - 2026-06-25

The "connector platform" release: connectors are now discovered by name and can
live in core, in `durable-sync-contrib`, or in your own package.

### Added
- **Connector discovery via entry points** (`durable_sync.registry`). Connectors
  register under the `durable_sync.sources` / `durable_sync.destinations` entry-
  point groups; apps resolve them **by name** (`load_source`/`load_destination`)
  instead of import path, so a connector can move between `durable-sync` and an
  out-of-repo `durable-sync-contrib` package without changing app wiring.
  `python -m durable_sync.registry` lists installed connectors grouped by package.
- **`CONTRACT.md`** + **`CONNECTORS.md`** + `durable_sync.core.CONTRACT_VERSION` (= 1):
  document and version the public connector contract, and the core / contrib /
  not-available (incl. private/in-house) curation model.
- **`build_authorize_url(..., extra_params=...)`** — lets a connector add provider-
  specific authorize params. Load-bearing case: Spotify's `show_dialog=true`, which
  forces the consent screen so re-authorizing to ADD a scope isn't silently served
  the old cached grant.

### Changed
- **Extracted the off-domain connectors to
  [`durable-sync-contrib`](https://github.com/temporal-community/durable-sync-contrib)**
  to keep the core repo focused on the martech/devrel stack. **Spotify** and
  **ListenBrainz** now ship there; install `durable-sync-contrib` and they are
  discovered by name exactly as before. Removed the `spotify` extra and
  `config.SPOTIFY_AUTH_WORKFLOW_ID` from core (the contrib connector reads its own
  `DURABLE_SYNC_SPOTIFY_AUTH_WORKFLOW_ID`).
- **Error observability:** the `status` query's `last_error` now reports the
  **root cause** (e.g. `Spotify PUT /me/tracks -> 403: Forbidden`) instead of the
  generic `Activity task failed`. The workflow flattens the full `__cause__` chain
  plus `ExceptionGroup` leaves, and `sync_records` unwraps solo `ExceptionGroup`s
  (from `async with httpx.AsyncClient()` / anyio task groups) so the real
  `DestinationHTTPError` survives into history. Failures are self-diagnosing from a
  single `status` query.

### Docs
- Documented operational gotchas surfaced building a second route (CONTRIBUTING.md +
  CLAUDE.md): **one task queue per route** (shared queues cross-wire `sync_records`),
  **workflow-owned-OAuth connectors must register the token workflow as `aux`** to be
  self-contained, **re-auth-to-add-a-scope must force consent**, and **scope
  `is_auth_error` to the credential a human can actually re-authorize** (a secondary
  service's 401/403 shouldn't pause the workflow).

## [0.2.0]
- Jira source + destination connector.
- Auto-generated destination schemas from source records.

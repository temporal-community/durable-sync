# Changelog

All notable changes to this project are documented here. The **connector
contract** (the import surface out-of-repo connectors depend on, see
`CONTRACT.md`) is versioned by `durable_sync.core.CONTRACT_VERSION`; any
backward-incompatible change to it is called out below with a migration note.

The format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- **Connector discovery via entry points** (`durable_sync.registry`). Connectors
  register under the `durable_sync.sources` / `durable_sync.destinations` entry-
  point groups; apps resolve them **by name** (`load_source`/`load_destination`)
  instead of import path, so a connector can move between `durable-sync` and an
  out-of-repo `durable-sync-contrib` package without changing app wiring.
  `python -m durable_sync.registry` lists installed connectors grouped by package.
- **`CONTRACT.md`** + `durable_sync.core.CONTRACT_VERSION` (= 1): documents and
  versions the public connector contract, and the core / contrib / not-available
  curation policy.

## [0.2.0]
- Jira source + destination connector.
- Auto-generated destination schemas from source records.

"""Temporal workflows. `SourceSyncWorkflow` is the generic per-source entity
workflow; destinations may add their own (e.g. the Notion auth workflow).

Intentionally import-free: a package that contains a workflow must not eagerly
re-export from its submodules, or that import runs inside the Temporal workflow
sandbox during validation and can drag a non-deterministic dependency (e.g.
`requests`) in — the exact failure mode documented in CONTRIBUTING.md. Import
from `durable_sync.workflows.sync` directly at call sites (bootstrap/worker do).
"""

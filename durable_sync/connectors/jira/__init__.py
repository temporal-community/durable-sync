"""Jira connector: source (issues via JQL) + destination (create/update issues).

Re-exports are safe here — no workflow lives in this package, so nothing pulls a
heavy/non-deterministic import into the Temporal sandbox (cf. github/__init__.py).

Requires the `jira` extra:  pip install "durable-sync[jira]"
"""
from __future__ import annotations

from durable_sync.connectors.jira.source import (
    JiraConfig,
    JiraIssueContext,
    JiraSource,
)
from durable_sync.connectors.jira.destination import JiraDestination

__all__ = [
    "JiraSource",
    "JiraConfig",
    "JiraIssueContext",
    "JiraDestination",
]

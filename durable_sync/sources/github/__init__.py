"""GitHub source: orgs + named repos -> Records.

The reference Source. Ships the GitHub *mechanism* (HTTP fetchers + generic
helpers); the *policy/vocab* (which topics mean what, language->SDK maps,
static analysis) belongs in your app's `enrich` hook — see RepoContext.

Requires the `github` extra:  pip install "durable-sync[github]"
"""
from __future__ import annotations

from durable_sync.sources.github.api import (
    author_type,
    build_headers,
    classify,
    fetch_org_members,
    raw_languages,
)
from durable_sync.sources.github.source import (
    GitHubConfig,
    GitHubSource,
    RepoContext,
)

__all__ = [
    "GitHubSource",
    "GitHubConfig",
    "RepoContext",
    "author_type",
    "classify",
    "raw_languages",
    "fetch_org_members",
    "build_headers",
]

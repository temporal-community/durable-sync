"""Live smoke test of the GitHub source + enrich hook. Needs GITHUB_TOKEN.

    GITHUB_TOKEN=... PYTHONPATH=. python tests/smoke_github.py
"""
from __future__ import annotations

import asyncio

from durable_sync.sources.github import (
    GitHubConfig,
    GitHubSource,
    RepoContext,
    classify,
    is_member,
)

# Toy app vocab (in the real app this is the Temporal pattern/SDK maps).
TOPIC_PATTERNS = {"wordle": "Game", "temporal": "Temporal", "python": "Python"}


def enrich(record, ctx: RepoContext):
    """Source-side enrichment (option A): layer domain fields using the context,
    WITHOUT importing the source's internals. Note the Employee/Community labels
    live HERE, in app code — the library only hands over the neutral `is_member`
    boolean + the member set."""
    topics = ctx.raw_repo.get("topics") or []
    record.properties["Patterns"] = classify(topics, TOPIC_PATTERNS)
    kinds = {("Employee" if is_member(a, ctx.members) else "Community") for a in ctx.authors} or {"Community"}
    record.properties["Owner type"] = (
        "Employee" if kinds == {"Employee"}
        else "Community" if kinds == {"Community"} else "Mixed"
    )
    record.properties["README chars"] = len(ctx.readme or "")
    return record


async def main() -> None:
    cfg = GitHubConfig(
        sources=[("repos", ["temporal-community/durable-wordle"])],
        member_orgs=["temporal-community"],
    )
    src = GitHubSource(cfg, enrich=enrich)
    [spec] = src.specs()
    print("spec:", spec)
    records = await src.fetch(spec)
    assert records, "no records fetched"
    for r in records:
        print("\nprimary_key:", r.primary_key)
        for k, v in r.properties.items():
            print(f"  {k}: {v!r}")
        print("  body (README) chars:", len(r.body or ""))
    print("\nGITHUB SOURCE SMOKE PASS ✅")


if __name__ == "__main__":
    asyncio.run(main())

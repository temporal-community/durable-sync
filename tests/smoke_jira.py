"""Live smoke test of the Jira source + destination round-trip. Needs a Jira Cloud
site, an account email, an API token, and a scratch project key you can write to.

    JIRA_BASE_URL=https://your-site.atlassian.net \
    JIRA_EMAIL=you@example.com \
    JIRA_API_TOKEN=... \
    JIRA_SMOKE_PROJECT=ENG \
    PYTHONPATH=. python tests/smoke_jira.py

Run by hand only — NEVER in pytest (it hits the real API and creates an issue).
It exercises the interesting paths: create stamps the entity property, a second
sync recognizes it via query_existing_ids (no duplicate), and update edits in
place. Cleanup is manual (the issue is left so you can eyeball it).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
import uuid

from durable_sync.core import Record
from durable_sync.env import load_env
from durable_sync.connectors.jira import JiraConfig, JiraSource, JiraDestination


async def main() -> None:
    load_env()  # pull JIRA_* from a local .env, like the other smokes
    missing = [k for k in ("JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "JIRA_SMOKE_PROJECT")
               if not os.environ.get(k)]
    assert not missing, f"set these in .env or the environment: {', '.join(missing)}"
    project = os.environ["JIRA_SMOKE_PROJECT"]
    now = dt.datetime.now(dt.timezone.utc)

    # --- source: pull a page of issues from the project --------------------
    src = JiraSource(JiraConfig(projects=[project]))
    [spec] = src.specs()
    print("spec:", spec.key, "->", spec.params["jql"])
    issues = await src.fetch(spec)
    print(f"fetched {len(issues)} issue(s)")
    if issues:
        sample = issues[0]
        print("  sample:", sample.primary_key, sample.properties.get("Issue Key"),
              "|", sample.properties.get("Summary"))

    # --- destination: create -> read-back -> update ------------------------
    pk = f"smoke-{uuid.uuid4().hex[:8]}"
    dest = JiraDestination(project, field_map={"Labels": "labels"})
    assert dest.configured, dest.config_hint

    rec = Record(
        primary_key=pk,
        properties={"Summary": f"durable-sync smoke {pk}", "Labels": ["durable_sync_smoke"]},
        body="Created by tests/smoke_jira.py — safe to delete.",
    )
    async with dest.connect() as s:
        before = await s.query_existing_ids()
        assert pk not in before, "fresh primary_key already present?!"
        await s.create(rec, now)
        print("created issue for", pk)

        after = await s.query_existing_ids()
        issue_id = after.get(pk)
        assert issue_id, "create did not stamp the entity property (not found on re-query)"
        print("  query_existing_ids recovered it ->", issue_id)

        rec.properties["Summary"] = f"durable-sync smoke {pk} (updated)"
        await s.update(issue_id, rec, now)
        print("  updated in place; no duplicate created ✅")

    print("\nJIRA SMOKE OK ✅  (delete the scratch issue manually)")


if __name__ == "__main__":
    asyncio.run(main())

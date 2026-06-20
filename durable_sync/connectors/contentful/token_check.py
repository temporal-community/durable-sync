"""Does the MCP-minted OAuth token also work against the plain CMA REST API?

The Contentful MCP returns LLM-oriented XML, awkward to parse for a sync pipeline.
But if the OAuth token we mint for the MCP server ALSO authenticates the CMA REST
API, we can skip the XML entirely: reuse the existing (clean-JSON) REST
ContentfulSource/Destination with a durable, workflow-owned OAuth token — no-admin
auth AND clean JSON. This probe answers that decisively.

    PYTHONPATH=. python -m durable_sync.connectors.contentful.token_check
"""
from __future__ import annotations

import httpx

from durable_sync.env import load_env
from durable_sync.connectors.contentful import oauth, store


def main() -> None:
    load_env()
    creds = store.load()
    if not creds:
        raise SystemExit("No credentials — run connectors.contentful.bootstrap first.")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    if tokens.get("refresh_token"):
        creds["refresh_token"] = tokens["refresh_token"]
        store.save(creds)
    token = tokens["access_token"]

    for url in ("https://api.contentful.com/users/me", "https://api.contentful.com/spaces"):
        r = httpx.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        print(f"GET {url} -> {r.status_code}")
        print(f"   {r.text[:300].strip()}")
        print()

    print("Verdict:")
    print("  /spaces 200 (lists spaces)  -> MCP-OAuth token works on CMA REST: reuse the REST connector.")
    print("  401 / 403 / empty           -> token is MCP-scoped only: we parse MCP tool output instead.")


if __name__ == "__main__":
    main()

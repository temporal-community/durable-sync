"""Transport mechanisms — how a connector moves bytes/calls, independent of auth.

Two today:
  * `mcp` — Model Context Protocol over streamable-HTTP (Notion, Contentful), the
    no-admin path: OAuth-as-an-individual unlocks a token the static-token API
    can't (admin-issued, or SSO-blocked).
  * `http` — shared httpx retry/backoff for REST connectors (GitHub, Luma, Asana, …).

Transport is orthogonal to auth (durable_sync.auth): you pick a transport AND an
auth mechanism and compose them in a connector. Import-free package (a connector
may contain workflow code).
"""

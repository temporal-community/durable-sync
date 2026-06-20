"""Authentication mechanisms for destinations.

A cross-cutting toolkit (not a connector), organized by mechanism. Today there's
one: `oauth/` — the OAuth-as-a-workflow toolkit (token-owner workflow + flow +
store) for no-admin providers. Mechanisms that need NO shared code (e.g. a
self-serve PAT, which is a one-liner) live inline in their connector, so they get
no package here until there's something to share.
"""

"""Authentication mechanisms for destinations.

Organized by mechanism, mirroring sources/ and destinations/. Today there's one:
`oauth/` — the OAuth-as-a-workflow toolkit (token-owner workflow + flow + store)
for no-admin providers. Mechanisms that need NO shared code (e.g. a self-serve
PAT, which is a one-liner) live inline in their destination, so they get no
package here until there's something to share.
"""

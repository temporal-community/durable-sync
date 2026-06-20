"""Generic OAuth-as-a-workflow toolkit (provider-agnostic).

For destinations whose API offers no admin-free static token: authorize as an
individual via OAuth 2.1 + PKCE + dynamic client registration, then let a Temporal
workflow OWN the rotating refresh token — refreshing on a timer and serving fresh
access tokens via query (so the secret never enters event history). Standards-based
(RFC 8414 discovery, RFC 7591 dynamic registration, PKCE).

Import from the SUBMODULES, not this package. This __init__ deliberately imports
nothing: `flow` pulls in `requests`, and the Temporal workflow sandbox forbids
that — so it must only ever be loaded via the workflow's pass-through import, never
eagerly here (an eager re-export here breaks `OAuthTokenWorkflow` sandbox validation).

    from durable_sync.auth.oauth.workflow import OAuthTokenWorkflow, AuthParams
    from durable_sync.auth.oauth.refresh  import refresh_oauth_token
    from durable_sync.auth.oauth.token    import current_access_token
    from durable_sync.auth.oauth import flow, store
"""

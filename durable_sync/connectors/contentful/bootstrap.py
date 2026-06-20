"""One-time interactive OAuth bootstrap for Contentful's MCP server. Run once, as
yourself, in a browser:

    PYTHONPATH=. python -m durable_sync.connectors.contentful.bootstrap

No CMA token, no org admin, no SSO workaround: this self-registers an OAuth client
(RFC 7591) and authorizes as *you* through your org's SSO — so it gets the access
a static personal token can't. Saves the refresh token + client_id locally (see
store.py) so the headless path (prove.py, then OAuthTokenWorkflow) can mint access
tokens unattended.

(Near-identical to the Notion bootstrap — once we wire a second provider for real,
this OAuth-CLI flow is worth generalizing into auth/oauth/.)
"""
from __future__ import annotations

import http.server
import threading
import urllib.parse
import webbrowser

from durable_sync.connectors.contentful import oauth, store

_PORT = 8788
REDIRECT_URI = f"http://localhost:{_PORT}/callback"


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    result: dict[str, str] = {}

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return
        params = urllib.parse.parse_qs(parsed.query)
        _CallbackHandler.result = {k: v[0] for k, v in params.items()}
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h2>durable-sync: Contentful authorized.</h2>"
            b"You can close this tab and return to the terminal.</body></html>"
        )

    def log_message(self, *args: object) -> None:  # silence default logging
        pass


def _wait_for_callback() -> dict[str, str]:
    server = http.server.HTTPServer(("localhost", _PORT), _CallbackHandler)
    thread = threading.Thread(target=server.handle_request)  # one request, then stop
    thread.start()
    thread.join()
    server.server_close()
    return _CallbackHandler.result


def main() -> None:
    print("Discovering Contentful MCP OAuth endpoints...")
    endpoints = oauth.discover()

    print("Registering an OAuth client (dynamic, no admin needed)...")
    client = oauth.register_client(endpoints["registration_endpoint"], REDIRECT_URI)
    client_id = client["client_id"]

    verifier, challenge = oauth.gen_pkce()
    state = oauth.new_state()
    url = oauth.build_authorize_url(
        endpoints["authorization_endpoint"], client_id, REDIRECT_URI, challenge, state
    )

    print(f"\nOpening your browser to authorize as yourself (via SSO):\n  {url}\n")
    webbrowser.open(url)
    print(f"Waiting for the redirect to {REDIRECT_URI} ...")
    cb = _wait_for_callback()

    if cb.get("state") != state:
        raise SystemExit("State mismatch — aborting (possible CSRF).")
    if "code" not in cb:
        raise SystemExit(f"No authorization code in callback: {cb}")

    print("Exchanging authorization code for tokens...")
    tokens = oauth.exchange_code(
        endpoints["token_endpoint"], client_id, cb["code"], REDIRECT_URI, verifier
    )

    store.save({
        "client_id": client_id,
        "token_endpoint": endpoints["token_endpoint"],
        "refresh_token": tokens["refresh_token"],
    })
    print(
        f"\nSaved credentials to {store.path()}.\n"
        f"Next: list the Contentful MCP tools (and prove headless minting) with\n"
        f"  PYTHONPATH=. python -m durable_sync.connectors.contentful.prove"
    )


if __name__ == "__main__":
    main()

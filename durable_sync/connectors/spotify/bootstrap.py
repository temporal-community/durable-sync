"""One-time interactive OAuth bootstrap for Spotify. Run once, as yourself:

    SPOTIFY_CLIENT_ID=xxxx PYTHONPATH=. python -m durable_sync.connectors.spotify.bootstrap

Unlike Notion/Contentful there is NO dynamic client registration: first create an
app in the Spotify dashboard (https://developer.spotify.com/dashboard), add the
redirect URI printed below to it, and export its Client ID as SPOTIFY_CLIENT_ID.
This then runs Authorization Code + PKCE (public client, no secret) for the
`user-library-read` scope and saves the resulting refresh token + client_id
locally (see store.py) so `start` can hand it to OAuthTokenWorkflow.
"""
from __future__ import annotations

import http.server
import os
import threading
import urllib.parse
import webbrowser

from durable_sync.connectors.spotify import oauth, store

_PORT = 8790
REDIRECT_URI = f"http://127.0.0.1:{_PORT}/callback"


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
            b"<html><body><h2>durable-sync: Spotify authorized.</h2>"
            b"You can close this tab and return to the terminal.</body></html>"
        )

    def log_message(self, *args: object) -> None:  # silence default logging
        pass


def _wait_for_callback() -> dict[str, str]:
    server = http.server.HTTPServer(("127.0.0.1", _PORT), _CallbackHandler)
    thread = threading.Thread(target=server.handle_request)  # one request, then stop
    thread.start()
    thread.join()
    server.server_close()
    return _CallbackHandler.result


def main() -> None:
    client_id = os.environ.get(oauth.CLIENT_ID_ENV)
    if not client_id:
        raise SystemExit(
            f"Set {oauth.CLIENT_ID_ENV} to your Spotify app's Client ID first.\n"
            f"Create an app at https://developer.spotify.com/dashboard and add the\n"
            f"redirect URI:  {REDIRECT_URI}"
        )

    verifier, challenge = oauth.gen_pkce()
    state = oauth.new_state()
    url = oauth.build_authorize_url(
        oauth.AUTHORIZE_ENDPOINT, client_id, REDIRECT_URI, challenge, state,
        scope=oauth.DEFAULT_SCOPE,
    )

    print(f"\nOpening your browser to authorize as yourself:\n  {url}\n")
    webbrowser.open(url)
    print(f"Waiting for the redirect to {REDIRECT_URI} ...")
    cb = _wait_for_callback()

    if cb.get("state") != state:
        raise SystemExit("State mismatch — aborting (possible CSRF).")
    if "code" not in cb:
        raise SystemExit(f"No authorization code in callback: {cb}")

    print("Exchanging authorization code for tokens...")
    tokens = oauth.exchange_code(
        oauth.TOKEN_ENDPOINT, client_id, cb["code"], REDIRECT_URI, verifier
    )

    store.save({
        "client_id": client_id,
        "token_endpoint": oauth.TOKEN_ENDPOINT,
        "refresh_token": tokens["refresh_token"],
    })
    print(
        f"\nSaved credentials to {store.path()}.\n"
        f"Next: hand the refresh token to the auth workflow with\n"
        f"  PYTHONPATH=. python -m durable_sync.connectors.spotify.start"
    )


if __name__ == "__main__":
    main()

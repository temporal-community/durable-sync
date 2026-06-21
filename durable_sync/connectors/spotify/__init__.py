"""Spotify connector. Kept import-free (re-exports would risk pulling the
bootstrap's `requests`/`webbrowser` deps into a workflow sandbox); import the
concrete class from the submodule:

    from durable_sync.connectors.spotify.source import SpotifySource
"""

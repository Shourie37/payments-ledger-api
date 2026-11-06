"""Mint a demo bearer token: python -m ledger.token --sub demo-client."""

from __future__ import annotations

import argparse
import os
import sys

from .auth import InMemoryWindowStore, SlidingWindowCounter, TokenAuthenticator
from .observability import Metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sub", default="demo-client")
    parser.add_argument("--ttl", type=int, default=300)
    args = parser.parse_args()
    secret = os.environ.get("LEDGER_AUTH_SECRET")
    if not secret:
        sys.exit("set LEDGER_AUTH_SECRET to the same value the server uses")
    window = int(os.environ.get("LEDGER_REPLAY_WINDOW_SECONDS", "300"))
    auth = TokenAuthenticator(secret.encode(), SlidingWindowCounter(InMemoryWindowStore(), window, 1), Metrics())
    print(auth.issue(args.sub, min(args.ttl, window)))


if __name__ == "__main__":
    main()

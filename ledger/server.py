"""Run the API with the standard library WSGI server and an outbox worker thread."""

from __future__ import annotations

import logging
import socketserver
import threading
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from .app import App
from .config import Settings
from .observability import configure_logging, log_event
from .outbox import HttpSender, OutboxWorker, log_sender

log = logging.getLogger("ledger.server")


class ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        # The app writes its own structured access log.
        pass


def main() -> None:
    configure_logging()
    settings = Settings.from_env()
    app = App(settings)
    sender = HttpSender(settings.webhook_url, settings.webhook_secret) if settings.webhook_url else log_sender
    worker = OutboxWorker(
        app.db, sender, app.metrics, settings.outbox_max_attempts, settings.outbox_base_delay, settings.outbox_max_delay
    )
    threading.Thread(target=worker.run_forever, name="outbox-worker", daemon=True).start()
    server = make_server(settings.host, settings.port, app, server_class=ThreadingWSGIServer, handler_class=QuietHandler)
    log_event(log, "listening", host=settings.host, port=settings.port, replay_fail_mode=settings.replay_fail_mode)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        server.server_close()


if __name__ == "__main__":
    main()

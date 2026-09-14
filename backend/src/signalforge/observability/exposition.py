"""Lifecycle for a process-local Prometheus HTTP exposition server."""

from dataclasses import dataclass
from http.server import HTTPServer
from threading import Thread

from prometheus_client import CollectorRegistry, start_http_server


@dataclass(slots=True)
class MetricsHttpServer:
    """Own the official Prometheus server and its serving thread."""

    server: HTTPServer
    thread: Thread

    @classmethod
    def start(
        cls, registry: CollectorRegistry, *, host: str, port: int
    ) -> "MetricsHttpServer":
        server, thread = start_http_server(port, addr=host, registry=registry)
        return cls(server, thread)

    def stop(self) -> None:
        """Stop serving and release the socket, even if shutdown raises."""
        try:
            self.server.shutdown()
        finally:
            try:
                self.server.server_close()
            finally:
                self.thread.join()

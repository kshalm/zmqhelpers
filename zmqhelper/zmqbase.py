import zmq
import time
import threading
import signal
import os
import requests
import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from loguru import logger
from typing import Callable
import redis
import sys

class ZMQServiceBase:
    """
    Base class for ZeroMQ-based microservices with built-in HTTP metrics/health, optional Loki logging,
    and optional Redis-based service registration for discovery.

    Subclasses must implement handle_request(msg: str) -> str.
    Optionally, additional REST routes and metrics can be added.
    If redis_host is provided, the service will register itself under Redis key "services:{service_name}" 
    with value {"ip": ip, "port": rep_port} every 5 seconds, with a TTL of 10 seconds.
    """
    def __init__(
        self,
        rep_port: int = 6000,
        http_port: int = 8080,
        service_name: str = "zmq_service",
        n_workers: int = 1,
        loki_host: str = None,
        loki_port: int = 3100,
        redis_host: str = None,
        redis_port: int = 6379,
        retention: str = "90 days",
        rotation: str = "30 days"
    ):
        """
        Initialize the ZMQ service base.

        Args:
            rep_port: Port on which the ZMQ ROUTER socket listens for incoming requests.
            http_port: Port for the built-in HTTP server (metrics and health).
            service_name: Name used for logging, metrics labels, and service registry.
            n_workers: Number of worker threads to spawn behind the ROUTER–DEALER proxy.
            loki_host: Optional Loki host for pushing logs. If None, Loki sink is disabled.
            loki_port: Port of the Loki HTTP API (default 3100).
            redis_host: Optional Redis host for service registration. If None, registration is disabled.
            redis_port: Port of the Redis service (default 6379).
        """
        self.rep_port = rep_port
        self.http_port = http_port
        self.service_name = service_name
        self.n_workers = n_workers
        self.loki_host = loki_host
        self.loki_port = loki_port
        self.redis_host = redis_host
        self.redis_port = redis_port

        # Additional custom HTTP routes: path -> handler
        self._extra_routes: dict[str, Callable[[BaseHTTPRequestHandler], None]] = {}

        # Core metrics with thread-safe access
        self.metrics = {
            "request_total": 0,
            "error_count": 0,
            "status": 1,
            "start_time": time.time()
        }
        self._metrics_lock = threading.Lock()

        # Setup logging sinks (file, optional Loki)
        self.rotation=rotation
        self.retention=retention
        self.log_name = f"{self.service_name}.log"
        self._setup_logger()
        self.logger = logger  # expose logger to subclasses

        # Graceful shutdown on termination signals
        signal.signal(signal.SIGTERM, lambda s, f: self._on_critical_error("SIGTERM received"))
        signal.signal(signal.SIGINT,  lambda s, f: self._on_critical_error("SIGINT received"))

        # Shared ZeroMQ context
        self.ctx = zmq.Context.instance()

        # Redis service registration
        if self.redis_host:
            logger.info(f"Redis registration enabled for {self.service_name} at {self.redis_host}:{self.redis_port}")
            self._setup_redis_registration()

    # def _setup_logger(self):
    #     """
    #     Configure Loguru sinks:
    #      - Rotating text file in ./logs/<service>.log
    #      - Optional Loki HTTP sink if loki_host is provided
    #     """
    #     os.makedirs("logs", exist_ok=True)
    #     log_path = os.path.join("logs", self.log_name)
    #     print(f"Log file path: {log_path}")

    #     # Remove any existing handlers
    #     logger.remove()
    #     # Add rotating text log file
    #     logger.add(log_path, rotation=self.rotation, retention=self.retention, enqueue=True, encoding="utf-8")

    #     # If Loki endpoint provided, add HTTP sink
    #     if self.loki_host:
    #         loki_url = f"http://{self.loki_host}:{self.loki_port}/loki/api/v1/push"
    #         def loki_sink(message):
    #             # Build and POST a single-entry Loki push payload
    #             record = message.record
    #             ts = int(record["time"].timestamp() * 1e9)
    #             payload = {
    #                 "streams": [{
    #                     "stream": {
    #                         "service": self.service_name,
    #                         "level": record["level"].name
    #                     },
    #                     "values": [[str(ts), record["message"]]]
    #                 }]
    #             }
    #             try:
    #                 requests.post(loki_url, json=payload, timeout=1)
    #             except Exception:
    #                 logger.error(f"Failed to send log to Loki at {loki_url}")
    #         # Register the Loki sink
    #         logger.add(loki_sink, enqueue=True)

    #     logger.info(f"Logger initialized for {self.service_name}")
    
    def _setup_logger(self):
        """
        Configure Loguru sinks:
        - Always write a rotating text file to ./logs/<service>.log
        - If loki_host is provided, also push each record to Loki
        """
        os.makedirs("logs", exist_ok=True)
        log_path = os.path.join("logs", self.log_name)
        print(f"Log file path: {log_path}")

        # 1) Remove any existing handlers so we start clean
        logger.remove()

        # 2) Always add the file sink
        logger.add(
            log_path,
            rotation=self.rotation,
            retention=self.retention,
            enqueue=True,
            encoding="utf-8"
        )

        # 3) Then, if Loki is configured, add the HTTP sink on top
        if self.loki_host:
            loki_url = f"http://{self.loki_host}:{self.loki_port}/loki/api/v1/push"

            def loki_sink(message):
                record = message.record
                ts = int(record["time"].timestamp() * 1e9)
                payload = {
                    "streams": [{
                        "stream": {
                            "service": self.service_name,
                            "level": record["level"].name
                        },
                        "values": [[str(ts), record["message"]]]
                    }]
                }
                try:
                    requests.post(loki_url, json=payload, timeout=1)
                except Exception:
                    # Note: This still logs back to the file sink
                    logger.error(f"Failed to send log to Loki at {loki_url}")

            logger.add(loki_sink, enqueue=True)

        # 4) (Optional) If you still want console output, re-add it:
        logger.add(sys.stdout, level="INFO", enqueue=True)

        logger.info(f"Logger initialized for {self.service_name}")

    def _setup_redis_registration(self):
        """
        Initialize Redis client and start background thread for service registration.
        """
        # Determine local IP address for service discovery
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect(("8.8.8.8", 80))
            ip_address = sock.getsockname()[0]
        except OSError:
            ip_address = "127.0.0.1"
        finally:
            sock.close()
        self._service_ip = ip_address

        # Create Redis client
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port)
        # Start registration loop
        thread = threading.Thread(target=self._redis_registration_loop, daemon=True)
        thread.start()
        self.logger.info(f"Started Redis registration for {self.service_name} at {self._service_ip}:{self.rep_port}")

    def _redis_registration_loop(self):
        """
        Periodically register the service IP and port in Redis with a TTL for keep-alive.
        """
        key = f"services:{self.service_name}"
        while True:
            # Prepare registration payload
            value = json.dumps({"ip": self._service_ip, "port": self.rep_port})
            try:
                # Set key with expiry of 10 seconds; catch only Redis-related errors
                self.redis_client.set(key, value, ex=10)
            except redis.RedisError as e:
                self.logger.error(f"Failed to register in Redis: {e}")
            time.sleep(5)

    def start(self):
        """
        Start the HTTP server in a background thread and the ZMQ broker+workers in the main thread.
        Catches only top-level broker proxy errors as critical.
        """
        # Launch HTTP server for /metrics and /healthz
        threading.Thread(target=self._run_http_server, daemon=True).start()
        self.logger.info(f"Started HTTP metrics on port {self.http_port}")

        # Setup ROUTER (clients) -> DEALER (workers) proxy
        url_client = f"tcp://*:{self.rep_port}"
        url_worker = "inproc://workers"

        clients = self.ctx.socket(zmq.ROUTER)
        clients.bind(url_client)
        self.logger.info(f"ZMQ ROUTER bound to {url_client}")

        workers = self.ctx.socket(zmq.DEALER)
        workers.bind(url_worker)
        self.logger.info("ZMQ DEALER bound to inproc://workers")

        # Spawn worker threads
        for i in range(self.n_workers):
            t = threading.Thread(target=self._worker_loop, args=(url_worker,), daemon=True)
            t.start()
            self.logger.info(f"Started worker {i}")

        # Run the proxy; handle only critical exceptions
        try:
            self.logger.info("Starting broker proxy... press Ctrl+C to exit")
            zmq.proxy(clients, workers)
        except Exception as e:
            self._on_critical_error(f"Broker proxy failure: {e}")
        finally:
            clients.close()
            workers.close()
            self.ctx.term()

    def _worker_loop(self, url_worker: str):
        """
        Worker loop: REP socket that receives a string, updates metrics,
        calls handle_request(), and sends back the reply.
        """
        socket = self.ctx.socket(zmq.REP)
        socket.connect(url_worker)
        while True:
            message = socket.recv_string()
            # Increment request counter
            with self._metrics_lock:
                self.metrics["request_total"] += 1
            # Delegate to subclass business logic
            reply = self.handle_request(message)
            socket.send_string(reply)

    def add_route(self, path: str, fn: Callable[[BaseHTTPRequestHandler], None]):
        """
        Register an extra HTTP GET route.

        Args:
            path: URL path (e.g. "/custom").
            fn:   Function taking a BaseHTTPRequestHandler to handle the request.
        """
        self._extra_routes[path] = fn

    def _run_http_server(self):
        """
        Internal: runs an HTTPServer serving /healthz, /metrics, and any extra routes.
        Overrides log_message to suppress default console output.
        """
        class Handler(BaseHTTPRequestHandler):
            def do_GET(inner_self):
                # Handle extra registered routes first
                if inner_self.path in self._extra_routes:
                    return self._extra_routes[inner_self.path](inner_self)
                if inner_self.path == "/healthz":
                    inner_self.send_response(200)
                    inner_self.end_headers()
                    inner_self.wfile.write(b"ok")
                elif inner_self.path == "/metrics":
                    inner_self.send_response(200)
                    inner_self.send_header("Content-Type", "text/plain")
                    inner_self.end_headers()
                    with self._metrics_lock:
                        m = self.metrics.copy()
                    uptime = int(time.time() - m["start_time"])
                    out = [
                        f"request_total {m['request_total']}",
                        f"error_count {m['error_count']}",
                        f"service_status {m['status']}",
                        f"uptime_seconds {uptime}"
                    ]
                    inner_self.wfile.write("\n".join(out).encode())
                else:
                    inner_self.send_response(404)
                    inner_self.end_headers()
                    self.logger.warning(f"Unknown endpoint: {inner_self.path}")

            def log_message(self, format, *args):
                return  # no-op prevents default stderr logs

        server = HTTPServer(("", self.http_port), Handler)
        server.serve_forever()

    def handle_request(self, msg: str) -> str:
        """
        Override in subclass to process requests.
        """
        raise NotImplementedError

    def _on_critical_error(self, err: str):
        """
        Log a critical error, set status=0, pause to flush logs, and exit.
        """
        self.logger.error(f"Critical failure: {err}")
        with self._metrics_lock:
            self.metrics["status"] = 0
        time.sleep(1)  # Allow log to flush
        os._exit(1)
        

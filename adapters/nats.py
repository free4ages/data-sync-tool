import json
import asyncio
import re
import logging
import signal
from nats.aio.client import Client as NATSClient
from .base import Adapter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def safe_subject(name: str) -> str:
    if not IDENT_RE.match(name):
        raise ValueError(f"Invalid NATS subject: {name}")
    return name

class NatsAdapter(Adapter):
    """Adapter acting as both source (fetch) and sink (publish) with graceful shutdown and resume."""
    def __init__(self, config: dict, role: str):
        super().__init__(config, role)
        self.config = config
        self.subject = safe_subject(config.get('subject', ''))
        self.queue = config.get('queue')
        self.max_msgs = config.get('max_msgs', 100)
        self.per_msg_timeout = config.get('per_msg_timeout', 1.0)
        self.total_timeout = config.get('total_timeout', 10.0)
        self._shutdown = False
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        self.nc = None

    def _handle_shutdown(self, signum, frame):
        self._shutdown = True
        logger.info(f"NatsAdapter received shutdown signal {signum}")

    def connect(self):
        opts = {}
        if 'servers' in self.config:
            opts['servers'] = self.config['servers']
        if 'token' in self.config:
            opts['token'] = self.config['token']
        loop = asyncio.get_event_loop()
        self.nc = loop.run_until_complete(NATSClient.connect(**opts))
        logger.info(f"NatsAdapter connected on subject '{self.subject}'")

    def insert_or_update(self, table: str, row: dict):
        if self._shutdown:
            logger.warning("Shutdown in progress; skipping publish.")
            return
        data = json.dumps(row).encode('utf-8')
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.nc.publish(self.subject, data))
        logger.info(f"Published to NATS subject '{self.subject}'")

    def fetch(self) -> list:
        """Fetch up to max_msgs, respecting timeouts and shutdown."""
        loop = asyncio.get_event_loop()
        sub = loop.run_until_complete(self.nc.subscribe(self.subject, queue=self.queue))
        messages = []
        start = loop.time()
        try:
            for _ in range(self.max_msgs):
                if self._shutdown:
                    logger.info("Shutdown flag set; stopping NATS fetch.")
                    break
                elapsed = loop.time() - start
                if elapsed >= self.total_timeout:
                    logger.info("Total timeout reached; stopping NATS fetch.")
                    break
                try:
                    remaining = min(self.per_msg_timeout, self.total_timeout - elapsed)
                    msg = loop.run_until_complete(
                        self.nc.next_msg(timeout=remaining, sub=sub)
                    )
                    payload = msg.data.decode('utf-8')
                    messages.append(json.loads(payload))
                except asyncio.TimeoutError:
                    logger.info("Per-message timeout; stopping fetch loop.")
                    break
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
        finally:
            loop.run_until_complete(self.nc.unsubscribe(sub))
        logger.info(f"Fetched {len(messages)} messages from '{self.subject}'")
        return messages

    def execute(self, *args, **kwargs):
        pass

    def close(self):
        if self.nc:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.nc.drain())
            loop.run_until_complete(self.nc.close())
            logger.info("NatsAdapter closed")
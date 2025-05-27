logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class WebhookAdapter(Adapter):
    """Sink adapter posting rows via HTTP with retry logic and graceful shutdown."""
    def __init__(self, config: dict, role: str):
        super().__init__(config, role)
        self.url = config.get('url')
        self.headers = config.get('headers', {})
        self.timeout = config.get('timeout', 5)
        self._shutdown = False
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        self._shutdown = True
        logger.info(f"WebhookAdapter received shutdown signal {signum}")

    def connect(self):
        # nothing to persist
        pass

    @retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
    def insert_or_update(self, table: str, row: dict):
        if self._shutdown:
            logger.warning("Shutdown in progress; skipping webhook call.")
            return
        resp = requests.post(self.url, json=row, headers=self.headers, timeout=self.timeout)
        resp.raise_for_status()
        logger.info(f"WebhookAdapter: posted to {self.url}")

    def fetch(self, *args, **kwargs):
        raise NotImplementedError("WebhookAdapter does not support fetch")

    def execute(self, *args, **kwargs):
        pass

    def close(self):
        logger.info("WebhookAdapter closed")
from typing import List, Union
from adapters.http_external import HttpExternalAdapter
from adapters.redis_external import RedisExternalAdapter
from core.config import AdapterConfig, DatastoreConfig, SinkConfig, SourceConfig, StateConfig
from utils.utils_fn import load_class_from_path



EXTERNAL_ADAPTERS = {
    'redis': RedisExternalAdapter,
    'http': HttpExternalAdapter,
}


def get_adapter(datastore_name: str, datastores: List[DatastoreConfig], config: AdapterConfig, role: str):
    peer_cfg = next((p for p in datastores if p.name.lower() == datastore_name.lower()), None)
    if not peer_cfg:
        raise ValueError(f"DB peer '{datastore_name}' not found")
    if config.adapter:
        cls = load_class_from_path(config.adapter)
    elif peer_cfg.type=="postgres":
        from adapters.postgres import PostgresAdapter
        cls = PostgresAdapter
    elif peer_cfg.type=="mysql":
        from adapters.mysql import MySQLAdapter
        cls = MySQLAdapter
    elif peer_cfg.type == "clickhouse":
        from adapters.clickhouse import ClickHouseAdapter
        cls = ClickHouseAdapter
    elif peer_cfg.type == "webhook":
        from adapters.webhook import WebhookAdapter
        cls = WebhookAdapter
    
    if not cls:
        raise ValueError(f"Unsupported adapter type: {peer_cfg.type}")
    return cls(peer_cfg, config, role)

def get_external_adapter(name: str, externalstores: list):
    cfg = next((e for e in externalstores if e.name == name), None)
    if not cfg:
        raise ValueError(f"External store '{name}' not found")
    cls = EXTERNAL_ADAPTERS.get(cfg.type)
    if not cls:
        raise ValueError(f"Unsupported external adapter type: {cfg.type}")
    return cls(cfg, 'external')

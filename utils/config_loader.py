import os
import yaml
from utils.dot_dict import DotDict

Config = DotDict

def load_config(path: str) -> Config:
    """
    Load YAML config and override DB peer settings from environment variables.
    """
    with open(path, 'r') as f:
        cfg = yaml.safe_load(f)

    # Override DB peer settings
    for peer in cfg.get('datastores', []):
        prefix = peer['name'].upper()
        for key, env_key in [
            ('host', 'HOST'), ('port', 'PORT'), ('username', 'USERNAME'),
            ('password', 'PASSWORD'), ('database', 'DATABASE'),
            ('base_url', 'BASE_URL'), ('url', 'URL'),
            ('servers', 'SERVERS'), ('token', 'TOKEN'), ('subject', 'SUBJECT'),
            ('queue', 'QUEUE'), ('max_msgs', 'MAX_MSGS'),
            ('per_msg_timeout', 'PER_MSG_TIMEOUT'), ('total_timeout', 'TOTAL_TIMEOUT')
        ]:
            val = os.getenv(f"{prefix}_{env_key}")
            if val is not None:
                if key == 'port': peer[key] = int(val)
                elif key in ('servers',): peer[key] = val.split(',')
                elif key in ('max_msgs',): peer[key] = int(val)
                elif key in ('per_msg_timeout','total_timeout'): peer[key] = float(val)
                else: peer[key] = val
    return Config(cfg)

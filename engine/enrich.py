import redis
import requests
from jinja2 import Template
from typing import List, Dict, Any

def apply_enrichment(row: Dict[str, Any], enrichment_conf: List[Dict], redis_conf: Dict) -> Dict[str, Any]:
    rc = redis_conf or {}
    redis_client = redis.Redis(host=rc.get('host','localhost'), port=rc.get('port',6379), db=rc.get('db',0))
    for rule in enrichment_conf:
        if rule['type']=='redis':
            key = Template(rule['key_template']).render(**row)
            val = redis_client.get(key)
            row[rule['output_field']] = val.decode() if val else None
        elif rule['type']=='http':
            url = Template(rule['url']).render(**row)
            params = {k: Template(v).render(**row) for k,v in rule.get('params',{}).items()}
            resp = requests.get(url, params=params, timeout=5)
            resp.raise_for_status()
            row[rule['output_field']] = resp.json()
    return row

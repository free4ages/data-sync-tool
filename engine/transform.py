import jinja2
from glom import glom
from typing import Dict, Any
from dateutil import parser

def render_template(template_str: str, context: dict) -> str:
    return jinja2.Template(template_str).render(**context)


def apply_transform(row: dict, transform_conf: dict) -> dict:
    out = {}
    for key, rule in transform_conf.items():
        if 'template' in rule:
            out[key] = render_template(rule['template'], row)
        else:
            val = glom(row, rule['column'])
            if rule.get('type') == 'datetime':
                out[key] = parser.isoparse(val)
            elif rule.get('type'):
                out[key] = rule['type'](val)
            else:
                out[key] = val
    return out

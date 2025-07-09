from collections import OrderedDict
from typing import List, Optional, Literal, Union, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
import yaml
import datetime
from typing import List, Dict, Optional, Union, Callable, Any, Annotated
from pydantic import BaseModel, model_validator, ConfigDict
import jinja2
import ast

from utils.utils_fn import generate_alias, generate_column_from_alias

MD5_SUM_HASH = "md5sum_hash"
HASH_MD5_HASH = "hash_md5_hash"

def parse_lambda_from_string(expr: str):
    """
    Safely parse and evaluate a Lambda expression string, allowing datetime usage.
    """
    expr = expr.strip()

    try:
        node = ast.parse(expr, mode='eval')
    except SyntaxError as e:
        raise ValueError(f"Invalid syntax: {e}")

    if not isinstance(node.body, ast.Lambda):
        raise ValueError("Only lambda expressions are allowed")

    # Disallow unsafe AST nodes
    forbidden = (
        ast.Import, ast.ImportFrom, ast.Global, ast.Nonlocal, ast.With,
        ast.Try, ast.FunctionDef, ast.ClassDef, ast.Delete, ast.Assign
        # ast.Exec  # only for Python 2, but safe to include
    )

    for subnode in ast.walk(node):
        if isinstance(subnode, forbidden):
            raise ValueError(f"Unsafe operation detected: {type(subnode).__name__}")

    # Allow only datetime module in a safe global scope
    safe_globals = {
        "__builtins__": {},      # No built-in functions
        "datetime": datetime     # Explicitly allowed
    }

    return eval(expr, safe_globals)

def parse_dynamic_field(value: Any) -> Any:
    if isinstance(value, str):
        if value.startswith("TMPL(") and value.endswith(")"):
            tmpl_str = value[len("TMPL("):-1]
            return jinja2.Template(tmpl_str).render
        elif value.strip().startswith("lambda"):
            return parse_lambda_from_string(value)
    return value

class Block:
    def __init__(self, start: Union[datetime,int,str], end: Union[datetime,int,str], level: int, num_rows: int, hash: str):
        self.start = start
        self.end = end
        self.level = level
        self.num_rows = num_rows
        self.hash = hash

class DynamicModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode='before')
    @classmethod
    def parse_dynamic_all(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(values, dict):
            return values
        parsed: Dict[str, Any] = {}
        for k, v in values.items():
            if isinstance(v, str) and (v.startswith("TMPL(") or v.strip().startswith("lambda")):
                parsed[k] = parse_dynamic_field(v)
            elif isinstance(v, list):
                new_list = [parse_dynamic_field(item) if isinstance(item, str) and (item.startswith("TMPL(") or item.strip().startswith("lambda")) else item for item in v]
                parsed[k] = new_list
            else:
                parsed[k] = v
        return parsed

# ---------- Unified Database Model ----------
class DatabaseConfig(DynamicModel):
    name: str
    type: Literal['postgres', 'mysql', 'clickhouse']
    host: str
    port: int
    username: str
    password: str
    database: str

# ---------- Webhook & NATS Models ----------
class WebhookConfig(DynamicModel):
    name: str
    type: Literal['webhook']
    base_url: str

class NatsConfig(DynamicModel):
    name: str
    type: Literal['nats']
    servers: List[str]
    token: str
    subject: Optional[str] = None
    queue: Optional[str] = None
    max_msgs: Optional[int] = None
    per_msg_timeout: Optional[float] = None
    total_timeout: Optional[float] = None

DatastoreConfig = Union[DatabaseConfig, WebhookConfig, NatsConfig]

# ---------- External Store Models ----------
class RedisConfig(DynamicModel):
    name: str
    type: Literal['redis']
    host: str
    port: int
    db: int

class HTTPConfig(DynamicModel):
    name: str
    type: Literal['http']
    url: str

ExternalStoreConfig = Annotated[Union[RedisConfig, HTTPConfig], Field(discriminator='type')]

# ---------- Pipeline Nested Models ----------
class JoinConfig(DynamicModel):
    table: str
    alias: Optional[str] = None
    type: Literal['inner', 'left', 'right', 'full']
    on: str

class FilterConfig(DynamicModel):
    column: str
    operator: Literal['=', '!=', '<', '<=', '>', '>=']
    value: Any

class FieldConfig(DynamicModel):
    column: str
    dtype: Optional[Literal['int', 'float', 'datetime', 'str', 'date']]=None
    alias: Optional[str] = None
    source_column: Optional[Union[str,Callable]] = None
    source: Optional[str] = None
    track: bool = True

    @model_validator(mode='before')
    @classmethod
    def populate_alias(self, v):
        if isinstance(v, dict):
            if not v.get("alias"):
                v["alias"] = generate_alias(v["column"])
        return v

class MergeStrategyConfig(DynamicModel):
    strategy: Literal['delete_insert', 'upsert', 'collapse']
    allow_delete: bool = False

class TableConfig(DynamicModel):
    table: str
    alias: Optional[str] = None
    dbschema: Optional[str] = None


class StoreMeta(BaseModel):
    partition_column: Optional[str] = None
    hash_column: Optional[str] = None
    order_column: Optional[str] = None
    unique_columns: Optional[List[str]] = None

class SourceConfig(DynamicModel):
    datastore: str
    table: TableConfig
    adapter: Optional[str] = None
    joins: Optional[List[JoinConfig]] = None
    filters: Optional[List[FilterConfig]] = None
    fields: Optional[List[FieldConfig]] = None
    meta_columns: Optional[StoreMeta] = None

    @property
    def table_fields(self):
        return self.fields

    @model_validator(mode='after')
    def populate_fields_from_sink(self, info) -> 'SourceConfig':
        # Extract context passed during validation
        ctx = getattr(info, 'context', {}) or {}

        # Initialize field map
        field_map = OrderedDict({f.alias: f for f in self.fields} if self.fields else {})

        # Add sink-derived fields if available
        sink = ctx.get('sink')
        if sink and sink.fields:
            for fld in sink.fields:
                scol = fld.source_column or fld.column
                if isinstance(scol, str) and not fld.source:
                    scol = generate_alias(scol)
                    if scol not in field_map:
                        alias = scol
                        field_map[alias] = FieldConfig(column=generate_column_from_alias(scol), alias=alias)

        self.fields = [v for k,v in field_map.items()]
        return self

class SinkConfig(DynamicModel):
    datastore: str
    table: TableConfig
    batch_size: int
    adapter: Optional[str] = None
    merge_strategy: Optional[MergeStrategyConfig] = None
    filters: Optional[List[FilterConfig]] = []
    fields: Optional[List[FieldConfig]] = []
    meta_columns : Optional[StoreMeta] = None

    @property
    def table_fields(self):
        return [x for x in self.fields if not x.source]

class StateConfig(DynamicModel):
    datastore: str
    table: TableConfig
    adapter: Optional[str] = None
    filters: Optional[List[FilterConfig]] = []
    fields: Optional[List[FieldConfig]] = []
    meta_columns : Optional[StoreMeta] = None

AdapterConfig = Union[SourceConfig, SinkConfig, StateConfig]


class ReconciliationConfig(DynamicModel):
    name: Optional[str] = 'default'
    strategy: Literal["md5sum_hash", "hash_md5_hash"]
    partition_column_type: Literal['int', 'float', 'datetime', 'date', 'uuid', 'str']
    start: Optional[Union[str, Callable]] = None
    end: Optional[Union[str, Callable]] = None
    initial_partition_interval: int
    max_block_size: int = 1000
    partition_multiplier: int = 1
    source_meta_columns: Optional[StoreMeta] = None
    sink_meta_columns: Optional[StoreMeta] = None
    sourcestate_meta_columns: Optional[StoreMeta] = None
    sinkstate_meta_columns: Optional[StoreMeta] = None
    
    # @model_validator(mode='after')
    # def set_state_fields(self) -> 'ReconciliationConfig':
    #     if not self.source_state_meta_columns:
    #         self.source_state_meta_columns = self.source_meta_columns
    #     if not self.sink_state_meta_columns:
    #         self.sink_state_meta_columns = self.sink_meta_columns
    #     return self


class EnrichmentConfig(DynamicModel):
    externalstore: Optional[str] = None
    name: Optional[str] = None
    type: Optional[Literal['redis', 'http']] = None
    key_template: Optional[str] = None
    path: Optional[Union[str,Callable]] = None
    output: Optional[Union[str,Callable]] = None
    url: Optional[Union[str, Callable]] = None

class PipelineConfig(BaseModel):
    name: str
    source: SourceConfig
    sink: SinkConfig
    sourcestate: Optional[Union[StateConfig, SourceConfig]] = None
    sinkstate: Optional[Union[StateConfig, SinkConfig]] = None
    reconciliation: List[ReconciliationConfig] = []
    enrichment: List[EnrichmentConfig] = []


    @model_validator(mode='after')
    def provide_sink_to_source(self) -> 'PipelineConfig':
        ctx = {'sink': self.sink}
        src_data = self.source.model_dump()
        validated_source = SourceConfig.model_validate(src_data, context=ctx)
        self.source = validated_source
        if not self.sourcestate:
            self.sourcestate=self.source
        if not self.sinkstate:
            self.sinkstate = self.sink
        return self

# ---------- Top-Level Config Model ----------
class Config(BaseModel):
    datastores: List[DatastoreConfig]
    externalstores: List[ExternalStoreConfig]
    pipelines: List[PipelineConfig]

    @classmethod
    def load(cls, path: str) -> 'Config':
        data = yaml.safe_load(open(path)) or {}
        # parse datastores
        dss = []
        for d in data.get('datastores', []):
            t = d.get('type')
            if t in ('postgres', 'mysql', 'clickhouse'):
                dss.append(DatabaseConfig.model_validate(d))
            elif t == 'webhook':
                dss.append(WebhookConfig.model_validate(d))
            elif t == 'nats':
                dss.append(NatsConfig.model_validate(d))
        # parse externalstores
        ess = []
        for e in data.get('externalstores', []):
            if e.get('type') == 'redis':
                ess.append(RedisConfig.model_validate(e))
            elif e.get('type') == 'http':
                ess.append(HTTPConfig.model_validate(e))
        # parse pipelines
        pipes = [PipelineConfig.model_validate(p) for p in data.get('pipelines', [])]
        return cls(datastores=dss, externalstores=ess, pipelines=pipes)

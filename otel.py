```python
# File: engine/pipeline.py
from utils.db_factory import get_adapter
from engine.query_builder import build_checksum_query, build_fetch_query, build_all_fetch_query
from engine.diff import find_diff_timegroups, find_all_updates
from engine.transform import apply_transform, apply_enrichment
from glom import glom
import operator
from datetime import datetime

# OpenTelemetry imports
from opentelemetry import trace, metrics
from opentelemetry.trace import Tracer
from opentelemetry.metrics import Meter

# Mapping of supported operators for filtering
OPS = {
    '=': operator.eq,
    '==': operator.eq,
    '!=': operator.ne,
    '<>': operator.ne,
    '<': operator.lt,
    '<=': operator.le,
    '>': operator.gt,
    '>=': operator.ge,
}

# Initialize tracer and meter
tracer: Tracer = trace.get_tracer(__name__)
meter: Meter = metrics.get_meter(__name__)

# Define metrics
inserted_counter = meter.create_counter(
    name="pipeline_records_inserted",
    description="Number of records inserted by the pipeline",
    unit="1"
)

duration_histogram = meter.create_histogram(
    name="pipeline_run_duration_ms",
    description="Duration of pipeline run in milliseconds",
    unit="ms"
)


def run_pipeline(pipeline_conf, datastores_conf):
    start_time = datetime.utcnow()
    with tracer.start_as_current_span(
        "run_pipeline",
        attributes={"pipeline.name": pipeline_conf.get("name", "")}
    ) as root_span:
        # Initialize adapters
        src = get_adapter(
            next(p for p in datastores_conf if p['name'] == pipeline_conf['source']['datastore']),
            'source'
        )
        src.connect()
        snk = get_adapter(
            next(p for p in datastores_conf if p['name'] == pipeline_conf['sink']['datastore']),
            'sink'
        )
        snk.connect()
        stt = get_adapter(
            next(p for p in datastores_conf if p['name'] == pipeline_conf['reconciliation']['statedb']['datastore']),
            'state'
        )
        stt.connect()

        _validate_schema(pipeline_conf, src, snk, stt)

        # Fetch rows based on reconciliation strategy
        method = pipeline_conf['reconciliation']['method']
        with tracer.start_as_current_span("fetch_rows", attributes={"method": method}) as fetch_span:
            if method == 'hash_check':
                q_src = build_checksum_query(pipeline_conf)
                q_st = build_checksum_query(pipeline_conf)
                src_rows = src.fetch(q_src)
                st_rows = stt.fetch(q_st)
                groups = find_diff_timegroups(src_rows, st_rows)
                rows = []
                for tg in groups:
                    rows += src.fetch(build_fetch_query(pipeline_conf, tg))
            elif method == 'updated_at':
                rows = find_all_updates(src, pipeline_conf)
            else:
                raise ValueError(f"Unknown reconciliation method {method}")

        # Apply source-side filters
        source_filters = pipeline_conf['source'].get('filters', [])
        if source_filters:
            with tracer.start_as_current_span("apply_filters"):
                def _passes_all(msg):
                    for f in source_filters:
                        try:
                            val = glom(msg, f['column'])
                        except Exception:
                            return False
                        op_func = OPS.get(f['operator'])
                        if not op_func or not op_func(val, f['value']):
                            return False
                    return True
                rows = [r for r in rows if _passes_all(r)]

        inserted_count = 0
        # Transform, enrich, and sink each row
        with tracer.start_as_current_span("process_and_sink_rows"):
            for r in rows:
                rec = apply_transform(r, pipeline_conf['transform'])
                rec = apply_enrichment(
                    rec,
                    pipeline_conf.get('enrichment', []),
                    pipeline_conf.get('redis', {})
                )
                snk.insert_or_update(
                    table=f"{pipeline_conf['sink']['schema']}.{pipeline_conf['sink']['table']}",
                    row=rec
                )
                inserted_count += 1

                # Store state
                hash_val = r.get(pipeline_conf['reconciliation']['hash_algo'])
                stt.insert_or_update(
                    table=f"{pipeline_conf['reconciliation']['statedb']['schema']}.
                          {pipeline_conf['reconciliation']['statedb']['table']}",
                    row={
                        **{k: rec[k] for k in pipeline_conf['reconciliation']['unique_key'].split(',')},
                        pipeline_conf['reconciliation']['hash_column']: rec[pipeline_conf['reconciliation']['hash_column']],
                        pipeline_conf['reconciliation']['sync_column']: r[pipeline_conf['reconciliation']['sync_column']],
                    }
                )

        # Close all connections
        for adapter in (src, snk, stt):
            adapter.close()

        # Record metrics
        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000
        duration_histogram.record(duration_ms, attributes={"pipeline.name": pipeline_conf.get("name", "")})
        inserted_counter.add(inserted_count, attributes={"pipeline.name": pipeline_conf.get("name", "")})

        root_span.set_attribute("records.processed", len(rows))
        root_span.set_attribute("records.inserted", inserted_count)

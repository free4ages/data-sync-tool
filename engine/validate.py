def _validate_schema(pipeline_conf, src_adapter, snk_adapter, stt_adapter=None):
    """
    Validates that all tables and columns specified in the pipeline exist on each adapter.
    Uses limit=0 fetchs to detect missing tables/columns.
    """
    # 1. Validate source transform columns
    for key, rule in pipeline_conf['transform'].items():
        col = rule.get('column')
        if col:
            q = Query(select=[Field(expr=col, alias=key, type='column')], limit=0)
            try:
                src_adapter.fetch(q)
            except Exception as e:
                raise ValueError(f"Source missing column '{col}': {e}")

    # 2. Validate join tables existence (try selecting 0 rows)
    for join in pipeline_conf['source'].get('joins', []):
        tbl = f"{pipeline_conf['source']['schema']}.{join['table']}"
        q = Query(select=[Field(expr='1', alias=None, type='column')], limit=0)
        try:
            src_adapter.fetch(q)
        except Exception as e:
            raise ValueError(f"Source missing join table '{tbl}': {e}")

    # 3. Validate sink columns (post-transform aliases)
    for key in pipeline_conf['transform'].keys():
        q = Query(select=[Field(expr=key, alias=None, type='column')], limit=0)
        try:
            snk_adapter.fetch(q)
        except Exception as e:
            raise ValueError(f"Sink missing column '{key}': {e}")

    # 4. Validate state table for hash_check
    rec = pipeline_conf['reconciliation']
    if rec['method'] == 'hash_check' and stt_adapter:
        table = f"{rec['statedb']['schema']}.{rec['statedb']['table']}"
        # sync_column
        q_sync = Query(select=[Field(expr=rec['sync_column'], alias=None, type='column')], limit=0)
        try:
            stt_adapter.fetch(q_sync)
        except Exception as e:
            raise ValueError(f"State DB missing sync_column '{rec['sync_column']}' on {table}: {e}")
        # hash_column
        q_hash = Query(select=[Field(expr=rec['statedb']['hash_column'], alias=None, type='column')], limit=0)
        try:
            stt_adapter.fetch(q_hash)
        except Exception as e:
            raise ValueError(f"State DB missing hash_column '{rec['statedb']['hash_column']}' on {table}: {e}")

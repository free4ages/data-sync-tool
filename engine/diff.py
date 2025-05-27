from core.query import Query

def find_diff_timegroups(src_rows: list, st_rows: list) -> list:
    state = {r['timegroup']:r for r in st_rows}
    return [r['timegroup'] for r in src_rows if r.get('group_hash')!=state.get(r['timegroup'],{}).get('group_hash')]

def find_all_updates(src_adapter, pipeline_conf: dict) -> list:
    q = build_all_fetch_query(pipeline_conf)
    return src_adapter.fetch(q)
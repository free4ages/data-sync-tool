from core.query import Query, Field
from typing import Dict


def build_checksum_query(pipeline: Dict) -> Query:
    rec, src = pipeline['reconciliation'], pipeline['source']
    # fields: count, hash
    select = [Field(expr='COUNT(1)', alias='count', type='count')]
    select.append(Field(expr='', alias='group_hash', type='hash', hash_fields=[r['column'] for r in pipeline['transform'].values() if 'column' in r]))
    return Query(
        select=select,
        filters=[f"{rec['sync_column']} BETWEEN '{rec['from']}' AND '{rec.get('till','now()')}'"] + src.get('filters', []),
        group_by=[rec['sync_column']],
    )

def build_fetch_query(pipeline: Dict, timegroup: str) -> Query:
    rec, src, tr = pipeline['reconciliation'], pipeline['source'], pipeline['transform']
    select = [Field(expr=v['column'], alias=k, type='column') for k,v in tr.items() if 'column' in v]
    return Query(
        select=select,
        filters=[f"{rec['sync_column']} = '{timegroup}'"] + src.get('filters', []),
    )

def build_all_fetch_query(pipeline: Dict) -> Query:
    rec, src, tr = pipeline['reconciliation'], pipeline['source'], pipeline['transform']
    select = [Field(expr=v['column'], alias=k, type='column') for k,v in tr.items() if 'column' in v]
    return Query(
        select=select,
        filters=[f"{rec['sync_column']} BETWEEN '{rec['from']}' AND '{rec.get('till','now()')}'"] + src.get('filters', []),
    )
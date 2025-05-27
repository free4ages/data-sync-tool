from clickhouse_driver import Client
from .base import Adapter
from core.query import Query, Field

# class ClickHouseAdapter(Adapter):
#     def connect(self):
#         c = self.config
#         self.client = Client(host=c['host'], port=c['port'], user=c['username'], password=c['password'], database=c['database'])

#     def _build_sql(self, query: Query) -> str:
#         select_sql = []
#         for f in query.select:
#             if f.type == 'hash':
#                 select_sql.append(f"reinterpretAsUInt32(substr(CityHash64({','.join(f.hash_fields)}),1,4)) AS {f.alias}")
#             else:
#                 select_sql.append(f"{f.expr} AS {f.alias}" if f.alias else f.expr)
#         sql = f"SELECT {', '.join(select_sql)} FROM {self.config['schema']}.{self.config['table']}"
#         if query.filters: sql += ' WHERE ' + ' AND '.join(query.filters)
#         if query.group_by: sql += ' GROUP BY ' + ', '.join(query.group_by)
#         if query.order_by: sql += ' ORDER BY ' + ', '.join(query.order_by)
#         if query.limit: sql += f' LIMIT {query.limit}'
#         return sql + ';'

#     def fetch(self, query: Query) -> list:
#         sql = self._build_sql(query)
#         data, cols = self.client.execute(sql, with_column_types=True)
#         names = [c[0] for c in cols]
#         return [dict(zip(names,row)) for row in data]

#     def execute(self, sql: str, params=None):
#         self.client.execute(sql, params or ())

#     def insert_or_update(self, table: str, row: dict):
#         placeholders = ','.join(['%s']*len(row))
#         sql = f"INSERT INTO {table} ({','.join(row.keys())}) VALUES ({placeholders})"
#         self.execute(sql, list(row.values()))

#     def close(self): pass


class ClickHouseAdapter(Adapter):
    def connect(self):
        c = self.config
        self.client = Client(
            host=c['host'], port=c['port'], user=c['username'], password=c['password'], database=c['database']
        )

    def _build_group_name_expr(self, field: Field) -> str:
        part_type, interval, factor, uuid_len = field.hash_fields
        factors = []
        cur = int(interval)
        while cur >= 1:
            factors.append(cur)
            cur = cur // int(factor)
            if cur == 0:
                factors.append(1)
                break
        segments = []
        base = field.expr
        for idx, fct in enumerate(factors):
            if idx == 0:
                expr = f"intDiv({base}, {fct})"
            else:
                prev = factors[idx-1]
                expr = f"intDiv({base} % {prev}, {fct})"
            segments.append(f"lpad(CAST({expr} AS String), 2, '0')")
        return " || '-' || ".join(segments)

    def _rewrite_query(self, query: Query) -> Query:
        new_sel=[]
        for f in query.select:
            if f.type=='hash':
                expr = f"reinterpretAsUInt32(substr(CityHash64({','.join(f.hash_fields)}),1,4))"
                new_sel.append(Field(expr=expr, alias=f.alias, type='column'))
            else:
                new_sel.append(f)
        query.select=new_sel
        new_gb=[]
        for g in query.group_by:
            if g.type=='group_name':
                expr=self._build_group_name_expr(g)
                new_gb.append(Field(expr=expr, alias=None, type='column'))
            else:
                new_gb.append(g)
        query.group_by=new_gb
        query.select.extend(new_gb)
        return query

    def _build_sql(self, query: Query) -> (str, dict):
        q=self._rewrite_query(query)
        return SqlBuilder.build(q)

    def fetch(self, query: Query) -> list:
        sql, params = self._build_sql(query)
        data, cols = self.client.execute(sql, params, with_column_types=True)
        names = [c[0] for c in cols]
        return [dict(zip(names,row)) for row in data]

    def execute(self, sql: str, params=None):
        self.client.execute(sql, params or {})

    def insert_or_update(self, table: str, row: dict):
        placeholders = ','.join(['%s']*len(row))
        stmt = f"INSERT INTO {table} ({','.join(row.keys())}) VALUES ({placeholders})"
        self.client.execute(stmt, list(row.values()))

    def close(self):
        pass

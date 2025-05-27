import pymysql
from .base import Adapter
from core.query import Query, Field

# class MySQLAdapter(Adapter):
#     def connect(self):
#         cfg = self.config
#         self.conn = pymysql.connect(
#             host=cfg['host'], port=cfg['port'], user=cfg['username'],
#             password=cfg['password'], db=cfg['database'], cursorclass=pymysql.cursors.DictCursor
#         )
#         self.cursor = self.conn.cursor()

#     def _build_sql(self, query: Query) -> str:
#         select_sql = []
#         for f in query.select:
#             if f.type == 'hash':
#                 concat = f"CONCAT({','.join([f'COALESCE({h},"")' for h in f.hash_fields])})"
#                 select_sql.append(f"CONV(SUBSTR(MD5({concat}),1,8),16,10) AS {f.alias}")
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
#         self.cursor.execute(sql)
#         return self.cursor.fetchall()

#     def execute(self, sql: str, params=None):
#         self.cursor.execute(sql, params or ())
#         self.conn.commit()

#     def insert_or_update(self, table: str, row: dict):
#         cols, vals = zip(*row.items())
#         ph = ','.join(['%s']*len(cols))
#         up = ','.join([f"{c}=VALUES({c})" for c in cols])
#         sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {up};"
#         self.execute(sql, vals)

#     def close(self):
#         self.cursor.close(); self.conn.close()

class MySQLAdapter(Adapter):
    def connect(self):
        cfg = self.config
        self.conn = pymysql.connect(
            host=cfg['host'], port=cfg['port'], user=cfg['username'],
            password=cfg['password'], db=cfg['database'], cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.conn.cursor()

    def _build_group_name_expr(self, field: Field) -> str:
        part_type, interval, factor, uuid_len = field.hash_fields
        # similar logic as Postgres but MySQL syntax
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
                expr = f"FLOOR({base} / {fct})"
            else:
                prev = factors[idx-1]
                expr = f"FLOOR(({base} % {prev}) / {fct})"
            segments.append(f"LPAD({expr},2,'0')")
        return " CONCAT('-', ") . join(segments) . ""

    def _rewrite_query(self, query: Query) -> Query:
        # handle hash fields
        new_sel = []
        for f in query.select:
            if f.type == 'hash':
                concat = ','.join(f.hash_fields)
                expr = f"CONV(SUBSTR(MD5(CONCAT({concat})),1,8),16,10)"
                new_sel.append(Field(expr=expr, alias=f.alias, type='column'))
            else:
                new_sel.append(f)
        query.select = new_sel
        # handle group_name
        new_gb = []
        for g in query.group_by:
            if g.type=='group_name':
                expr = self._build_group_name_expr(g)
                new_gb.append(Field(expr=expr, alias=None, type='column'))
            else:
                new_gb.append(g)
        query.group_by=new_gb
        query.select.extend(new_gb)
        return query

    def _build_sql(self, query: Query) -> (str, list):
        q = self._rewrite_query(query)
        return SqlBuilder.build(q)

    def fetch(self, query: Query) -> list:
        sql, params = self._build_sql(query)
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def execute(self, sql: str, params=None):
        self.cursor.execute(sql, params or [])
        self.conn.commit()

    def insert_or_update(self, table: str, row: dict):
        cols, vals = zip(*row.items())
        placeholders = ','.join(['%s']*len(cols))
        updates = ','.join(f"{c}=VALUES({c})" for c in cols)
        stmt = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates};"
        self.execute(stmt, vals)

    def close(self):
        self.cursor.close()
        self.conn.close()
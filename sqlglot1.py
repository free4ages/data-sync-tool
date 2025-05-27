import json
import sqlglot
from sqlglot import exp, parse_one
from typing import Any

from dataclasses import dataclass, field
from typing import List, Optional, Any, Union

@dataclass
class CTE:
    """
    Common Table Expression definition: named subquery to be used in the main query.
    """
    name: str                # e.g. "recent_events"
    query: Union[str, 'Query']  # raw SQL or nested Query

@dataclass
class Table:
    table: str              # e.g. "orders"
    primary: bool           # true if this is the main/base table
    alias: Optional[str]    # e.g. "o"
    type: Optional[str] = None     # 'inner', 'left', 'right', etc. (None for primary table)
    on: Optional[str] = None      # e.g. "users.id = o.user_id" (None for primary table)

@dataclass
class OrderBy:
    column: str             # e.g. "created_at"
    direction: str          # 'ASC' or 'DESC'

@dataclass
class Filter:
    column: str             # e.g. "users.active"
    operator: str           # e.g. "=", ">", "IN", "BETWEEN"
    value: Any              # single value, list/tuple for IN/ BETWEEN

@dataclass
class Field:
    expr: str               # column name, aggregate expression, or conditional expression
    alias: Optional[str]    # optional alias for the field
    type: str               # 'column', 'count', 'sum', 'case', 'if', 'hash', etc.
    hash_fields: List[str] = field(default_factory=list)

@dataclass
class Query:
    """
    Full SQL query representation, including optional CTEs.
    """
    ctes: List[CTE] = field(default_factory=list)
    select: List[Field] = field(default_factory=list)
    tables: List[Table] = field(default_factory=list)
    filters: List[Filter] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: List[OrderBy] = field(default_factory=list)
    limit: Optional[int] = None


def build_expression(field: Field) -> exp.Expression:
    if field.type == 'column':
        return exp.column(field.expr).as_(field.alias) if field.alias else exp.column(field.expr)
    elif field.type in {'sum', 'count', 'avg', 'min', 'max'}:
        func = getattr(exp, field.type.capitalize())(this=exp.column(field.expr))
        return func.as_(field.alias) if field.alias else func
    elif field.type in {'case', 'if'}:
        expr = parse_one(field.expr)
        return expr.as_(field.alias) if field.alias else exp.raw(field.expr)
    elif field.type == 'hash':
        hash_expr = exp.func("md5", exp.func("concat_ws", exp.Literal.string(":"), *[exp.column(f) for f in field.hash_fields]))
        return hash_expr.as_(field.alias) if field.alias else hash_expr
    else:
        return exp.raw(field.expr).as_(field.alias) if field.alias else exp.raw(field.expr)


def build_condition(f: Filter) -> exp.Expression:
    col = exp.column(f.column)
    op = f.operator.upper()

    if op == "IN":
        return exp.In(this=col, expressions=[
            exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v)
            for v in f.value
        ])
    elif op == "NOT":
        return exp.Not(this=col)
    elif op == "BETWEEN":
        return exp.Between(
            this=col,
            low=exp.Literal.string(f.value[0]) if isinstance(f.value[0], str) else exp.Literal.number(f.value[0]),
            high=exp.Literal.string(f.value[1]) if isinstance(f.value[1], str) else exp.Literal.number(f.value[1])
        )
    
    # Map of operator to sqlglot expression class
    operator_map = {
        "=": exp.EQ,
        "!=": exp.NEQ,
        "<>": exp.NEQ,
        ">": exp.GT,
        ">=": exp.GTE,
        "<": exp.LT,
        "<=": exp.LTE,
    }

    if op not in operator_map:
        raise ValueError(f"Unsupported operator: {op}")

    right = exp.Literal.string(f.value) if isinstance(f.value, str) else exp.Literal.number(f.value)
    return operator_map[op](this=col, expression=right)


def build_query(q: Query) -> str:
    # Handle CTEs (Common Table Expressions)
    cte_exprs = []
    for cte in q.ctes:
        if isinstance(cte.query, str):
            inner_query = sqlglot.parse_one(cte.query)
        else:
            inner_query = sqlglot.parse_one(build_query(cte.query))
        cte_exprs.append(exp.CTE(this=inner_query, alias=exp.to_identifier(cte.name)))

    from_clause = None
    joins = []
    for table in q.tables:
        table_expr = exp.Table(this=exp.to_identifier(table.table), alias=exp.to_identifier(table.alias) if table.alias else None)
        if table.primary:
            from_clause = table_expr
        else:
            join = exp.Join(
                this=exp.Table(
                    this=exp.to_identifier(table.table),
                    alias=exp.to_identifier(table.alias) if table.alias else None
                ) ,
                on=sqlglot.parse_one(table.on),
                join_type=table.type.upper()
            )
            # join = exp.Join(
            #     this=table_expr,
            #     on=exp.raw(table.on),
            #     join_type=(table.type or "INNER").upper()
            # )
            joins.append(join)
    select_exprs = [build_expression(f) for f in q.select]
    query = exp.select(*select_exprs)
    if from_clause:
        query = query.from_(from_clause)
    for j in joins:
        query = query.join(j)

    if q.filters:
        query = query.where(exp.and_(*[build_condition(f) for f in q.filters]))
    if q.group_by:
        query = query.group_by(*[exp.column(col) for col in q.group_by])
    if q.order_by:
        query = query.order_by(*[
            exp.Ordered(this=exp.column(o.column), desc=(o.direction.upper() == "DESC"))
            for o in q.order_by
        ])

    if q.limit is not None:
        query = query.limit(q.limit)
    if cte_exprs:
        query = exp.With(this=query, expressions=cte_exprs)
    
    return query

def dict_to_query(d: dict) -> Query:
    def parse_cte(cte_data):
        if isinstance(cte_data['query'], dict):
            cte_data['query'] = dict_to_query(cte_data['query'])
        return CTE(**cte_data)

    return Query(
        ctes=[parse_cte(c) for c in d.get("ctes", [])],
        select=[Field(**f) for f in d.get("select", [])],
        tables=[Table(**t) for t in d.get("tables", [])],
        filters=[Filter(**f) for f in d.get("filters", [])],
        group_by=d.get("group_by", []),
        order_by=[OrderBy(**o) for o in d.get("order_by", [])],
        limit=d.get("limit")
    )
if __name__=="__main__":
    json_string= """
    {
    "name": "ingest_filtered_user_events",
        "config": {
            "select": [
                { "expr": "u.id",         "alias": "user_id",     "type": "column" },
                { "expr": "u.name",       "alias": "user_name",   "type": "column" },
                { "expr": "SUM(o.amount)","alias": "total_amount", "type": "sum" },
                { "expr": "CASE WHEN u.age >= 21 THEN 'adult' ELSE 'minor' END", "alias": "age_group", "type": "case" }
            ],
            "tables": [
                { "table": "users",    "primary": true,  "alias": "u" },
                { "table": "orders",   "primary": false, "alias": "o", "type": "left",  "on": "u.id = o.user_id" },
                { "table": "payments", "primary": false, "alias": "p", "type": "inner", "on": "u.id = p.user_id" }
            ],
            "filters": [
            { "column": "u.status",       "operator": "=",       "value": "active" },
            { "column": "o.amount", "operator": ">=",      "value": 21 }
            ],
            "group_by": ["u.id", "u.name"],
            "order_by": [
            { "column": "created_at", "direction": "DESC" }
            ],
            "limit": 50
        }
    }
    """

    query_config = json.loads(json_string)
    query= dict_to_query(query_config["config"])
    # print(build_condition(query.filters[0]).sql())
    query = build_query(query)
    print(query.sql())
    columns_with_tables = set()
    for col in query.find_all(exp.Column):
        table = col.table or "<unknown>"
        column = col.name
        columns_with_tables.add((table, column))

    # Print the results
    for table, column in sorted(columns_with_tables):
        print(f"{table}.{column}")

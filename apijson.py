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
    type: Optional[str]     # 'inner', 'left', 'right', etc. (None for primary table)
    on: Optional[str]       # e.g. "users.id = o.user_id" (None for primary table)

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

"""
Sample pipeline JSON configuration including CTEs, tables, aggregates, and conditional fields:

{
  "name": "ingest_filtered_user_events",
  "config": {
    "ctes": [
      {
        "name": "recent_events",
        "query": "SELECT * FROM events WHERE created_at > '2025-04-01'"
      },
      {
        "name": "recent_events",
        "query": {
          "select": [
            { "expr": "e.event_date",   "alias": "event_date",   "type": "column" },
          ],
          "tables": [
            { "table": "events",    "primary": true,  "alias": "e" },
          ]
        }
      }
    ],
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
      { "column": "status",       "operator": "=",       "value": "active" },
      { "column": "metadata.age", "operator": ">=",      "value": 21 }
    ],
    "group_by": ["u.id", "u.name"],
    "order_by": [
      { "column": "created_at", "direction": "DESC" }
    ],
    "limit": 50
  }
}
"""

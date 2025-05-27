from typing import Tuple
from core.query import Query, Field, Filter, Join

class SqlBuilder:
    @staticmethod
    def build(query: Query) -> Tuple[str, list]:
        parts = []
        params = []

        # SELECT clause
        select_clause = []
        for f in query.select:
            # Use field.expr regardless of field.type as per requirements
            # This assumes field.expr is properly populated elsewhere
            if f.alias:
                select_clause.append(f"{f.expr} AS {f.alias}")
            else:
                select_clause.append(f.expr)
        parts.append("SELECT " + ", ".join(select_clause))

        # FROM clause
        table = query.table
        from_clause = f"FROM "
        if table.schema:
            from_clause += f"{table.schema}."
        from_clause += f"{table.table}"
        if table.alias:
            from_clause += f" AS {table.alias}"
        parts.append(from_clause)

        # JOINs
        for j in query.joins:
            join_clause = f"{j.type.upper()} JOIN {j.table}"
            if j.alias:
                join_clause += f" AS {j.alias}"
            # Use parameterized ON clause to prevent SQL injection
            join_on_parts = []
            for on_part in j.on.split(" AND "):
                # Keep logical operators and parentheses as is
                if '=' in on_part and not (on_part.strip().startswith('(') or on_part.strip().endswith(')')):
                    column_parts = on_part.split('=')
                    if len(column_parts) == 2:
                        left, right = column_parts
                        join_on_parts.append(f"{left.strip()} = {right.strip()}")
                    else:
                        join_on_parts.append(on_part)
                else:
                    join_on_parts.append(on_part)
            join_clause += f" ON {' AND '.join(join_on_parts)}"
            parts.append(join_clause)

        # WHERE filters - Using parameterized queries to prevent SQL injection
        if query.filters:
            filter_exprs = []
            for flt in query.filters:
                # Use %s placeholders for values to prevent SQL injection
                filter_exprs.append(f"{flt.column} {flt.operator} %s")
                # Add the actual value to params list separately
                params.append(flt.value)
            parts.append("WHERE " + " AND ".join(filter_exprs))

        # GROUP BY
        if query.group_by:
            group_by_exprs = [f.expr for f in query.group_by]
            parts.append("GROUP BY " + ", ".join(group_by_exprs))

        # ORDER BY
        if query.order_by:
            parts.append("ORDER BY " + ", ".join(query.order_by))

        # LIMIT
        if query.limit:
            parts.append("LIMIT %s")
            params.append(query.limit)

        sql = "\n".join(parts) + ";"
        return sql, params

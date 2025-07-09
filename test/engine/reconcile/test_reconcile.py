# import unittest
# from unittest.mock import MagicMock, patch
# import duckdb
# from datetime import datetime
# import hashlib
# import json

# from adapters.base import Adapter
# from core.config import (
#     HASH_MD5_HASH, MD5_SUM_HASH, 
#     StoreMeta, ReconciliationConfig, 
#     SourceConfig, SinkConfig, StateConfig, 
#     FieldConfig, TableConfig
# )
# from core.query import Query, Field, Filter, Table, Join
# from engine.reconcile import build_blocks, calculate_blocks, build_block_hash_query


# class DuckDBAdapter(Adapter):
#     """A DuckDB adapter for testing purposes."""
    
#     def __init__(self, store_config=None, adapter_config=None, role=None):
#         super().__init__(store_config, adapter_config, role)
#         self.conn = None
#         self.queries = []
        
#     def connect(self):
#         """Connect to an in-memory DuckDB database."""
#         self.conn = duckdb.connect(database=':memory:')
        
#     def fetch(self, query: Query, name: str = None) -> list:
#         """Execute a query and return all results."""
#         if name:
#             self.queries.append((name, query))
            
#         # Convert the Query object to SQL
#         sql = self._query_to_sql(query)
        
#         # Execute the query
#         result = self.conn.execute(sql).fetchall()
        
#         # Convert to list of dicts
#         columns = [desc[0] for desc in self.conn.description()]
#         return [dict(zip(columns, row)) for row in result]
    
#     def fetch_one(self, query: Query, name: str = None) -> dict:
#         """Execute a query and return the first result."""
#         results = self.fetch(query, name)
#         if results:
#             return results[0]
#         return {}
    
#     def execute(self, sql: str, params=None):
#         """Execute a SQL statement."""
#         if params:
#             self.conn.execute(sql, params)
#         else:
#             self.conn.execute(sql)
    
#     def insert_or_update(self, table: str, row: dict):
#         """Insert or update a row in a table."""
#         columns = ', '.join(row.keys())
#         placeholders = ', '.join(['?' for _ in row.keys()])
#         values = list(row.values())
        
#         sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
#         self.conn.execute(sql, values)
    
#     def close(self):
#         """Close the connection."""
#         if self.conn:
#             self.conn.close()
#             self.conn = None
    
#     def _build_blockhash_expr(self, field: Field) -> str:
#         """Build a SQL expression for blockhash field type."""
#         metadata = field.metadata
#         if not metadata:
#             return field.expr
            
#         partition_column = metadata.partition_column
#         hash_column = metadata.hash_column or partition_column
#         fields = metadata.fields or []
#         strategy = metadata.strategy
        
#         if strategy == MD5_SUM_HASH:
#             # For MD5_SUM_HASH, we hash the partition column and hash column
#             if hash_column:
#                 return f"MD5(CAST({partition_column} AS VARCHAR) || CAST({hash_column} AS VARCHAR))"
#             else:
#                 return f"MD5(CAST({partition_column} AS VARCHAR))"
#         elif strategy == HASH_MD5_HASH:
#             # For HASH_MD5_HASH, we hash all fields
#             if fields:
#                 field_exprs = [f"COALESCE(CAST({f.column} AS VARCHAR), '')" for f in fields]
#                 return f"MD5({' || '.join(field_exprs)})"
#             else:
#                 return f"MD5(CAST({partition_column} AS VARCHAR))"
#         else:
#             return field.expr
    
#     def _build_blockname_expr(self, field: Field) -> str:
#         """Build a SQL expression for blockname field type."""
#         metadata = field.metadata
#         if not metadata:
#             return field.expr
            
#         level = metadata.level
#         intervals = metadata.intervals or [100, 10, 1]
#         partition_column = metadata.partition_column
#         partition_column_type = metadata.partition_column_type
        
#         if partition_column_type == "int":
#             # For integer partition columns, we divide by the interval
#             interval = intervals[min(level-1, len(intervals)-1)]
#             return f"CAST(FLOOR({partition_column} / {interval}) AS VARCHAR)"
#         elif partition_column_type == "datetime":
#             # For datetime partition columns, we extract epoch and divide by interval
#             interval = intervals[min(level-1, len(intervals)-1)]
#             return f"CAST(FLOOR(EXTRACT(EPOCH FROM {partition_column}) / {interval}) AS VARCHAR)"
#         else:
#             # For other types, just return the expression
#             return field.expr
    
#     def _rewrite_query(self, query: Query) -> Query:
#         """Rewrite the query to handle special field types."""
#         # Create a copy of the query to avoid modifying the original
#         rewritten_select = []
        
#         for field in query.select:
#             if field.type == "blockhash":
#                 expr = self._build_blockhash_expr(field)
#                 rewritten_select.append(Field(expr=expr, alias=field.alias, type="column"))
#             elif field.type == "blockname":
#                 expr = self._build_blockname_expr(field)
#                 rewritten_select.append(Field(expr=expr, alias=field.alias, type="column"))
#             else:
#                 rewritten_select.append(field)
        
#         # Create a new query with the rewritten select fields
#         rewritten_query = Query(
#             select=rewritten_select,
#             table=query.table,
#             joins=query.joins,
#             filters=query.filters,
#             group_by=query.group_by,
#             order_by=query.order_by,
#             limit=query.limit
#         )
        
#         return rewritten_query
    
#     def _query_to_sql(self, query: Query) -> str:
#         """Convert a Query object to SQL."""
#         # Rewrite the query to handle special field types
#         query = self._rewrite_query(query)
        
#         # Build SELECT clause
#         select_parts = []
#         for field in query.select:
#             if field.alias:
#                 select_parts.append(f"{field.expr} AS {field.alias}")
#             else:
#                 select_parts.append(field.expr)
        
#         select_clause = ", ".join(select_parts)
        
#         # Build FROM clause
#         from_clause = query.table.table
#         if query.table.schema:
#             from_clause = f"{query.table.schema}.{from_clause}"
#         if query.table.alias:
#             from_clause = f"{from_clause} AS {query.table.alias}"
        
#         # Build JOIN clause
#         join_clause = ""
#         if query.joins:
#             for join in query.joins:
#                 join_type = join.type.upper()
#                 join_table = join.table
#                 if join.alias:
#                     join_table = f"{join_table} AS {join.alias}"
#                 join_clause += f" {join_type} JOIN {join_table} ON {join.on}"
        
#         # Build WHERE clause
#         where_clause = ""
#         if query.filters:
#             conditions = []
#             for filter_item in query.filters:
#                 if isinstance(filter_item.value, str) and not filter_item.value.isdigit():
#                     conditions.append(f"{filter_item.column} {filter_item.operator} '{filter_item.value}'")
#                 else:
#                     conditions.append(f"{filter_item.column} {filter_item.operator} {filter_item.value}")
#             where_clause = " WHERE " + " AND ".join(conditions)
        
#         # Build GROUP BY clause
#         group_by_clause = ""
#         if query.group_by:
#             group_parts = [field.expr for field in query.group_by]
#             group_by_clause = " GROUP BY " + ", ".join(group_parts)
        
#         # Build ORDER BY clause
#         order_by_clause = ""
#         if query.order_by:
#             order_by_clause = " ORDER BY " + ", ".join(query.order_by)
        
#         # Build LIMIT clause
#         limit_clause = ""
#         if query.limit:
#             limit_clause = f" LIMIT {query.limit}"
        
#         # Combine all clauses
#         sql = f"SELECT {select_clause} FROM {from_clause}{join_clause}{where_clause}{group_by_clause}{order_by_clause}{limit_clause}"
        
#         return sql


# class XTestReconcile(unittest.TestCase):
    
#     def setUp(self):
#         """Set up the test environment."""
#         # Create source and sink adapters
#         self.source = DuckDBAdapter()
#         self.source.connect()
        
#         self.sink = DuckDBAdapter()
#         self.sink.connect()
        
#         self.sourcestate = DuckDBAdapter()
#         self.sourcestate.connect()
        
#         self.sinkstate = DuckDBAdapter()
#         self.sinkstate.connect()
        
#         # Create tables in source and sink
#         self.source.execute("""
#             CREATE TABLE source_table (
#                 id INTEGER PRIMARY KEY,
#                 timestamp TIMESTAMP,
#                 data VARCHAR,
#                 updated_at TIMESTAMP
#             )
#         """)
        
#         self.sink.execute("""
#             CREATE TABLE sink_table (
#                 id INTEGER PRIMARY KEY,
#                 timestamp TIMESTAMP,
#                 data VARCHAR,
#                 updated_at TIMESTAMP
#             )
#         """)
        
#         # Create state tables
#         self.sourcestate.execute("""
#             CREATE TABLE source_state (
#                 id INTEGER PRIMARY KEY,
#                 timestamp TIMESTAMP,
#                 data VARCHAR,
#                 updated_at TIMESTAMP
#             )
#         """)
        
#         self.sinkstate.execute("""
#             CREATE TABLE sink_state (
#                 id INTEGER PRIMARY KEY,
#                 timestamp TIMESTAMP,
#                 data VARCHAR,
#                 updated_at TIMESTAMP
#             )
#         """)
        
#         # Set up configuration
#         self.source_config = SourceConfig(
#             datastore="source_db",
#             batch_size=10,
#             table=TableConfig(table="source_table", dbschema=None, alias="src"),
#             fields=[
#                 FieldConfig(column="id"),
#                 FieldConfig(column="timestamp", dtype="datetime"),
#                 FieldConfig(column="data"),
#                 FieldConfig(column="updated_at", dtype="datetime")
#             ]
#         )
        
#         self.sink_config = SinkConfig(
#             datastore="sink_db",
#             batch_size=10,
#             table=TableConfig(table="sink_table", dbschema=None, alias="snk"),
#             fields=[
#                 FieldConfig(column="id"),
#                 FieldConfig(column="timestamp", dtype="datetime"),
#                 FieldConfig(column="data"),
#                 FieldConfig(column="updated_at", dtype="datetime")
#             ]
#         )
        
#         self.sourcestate_config = StateConfig(
#             datastore="source_state_db",
#             table=TableConfig(table="source_state", dbschema=None, alias="src_state"),
#             fields=[
#                 FieldConfig(column="id"),
#                 FieldConfig(column="timestamp", dtype="datetime"),
#                 FieldConfig(column="data"),
#                 FieldConfig(column="updated_at", dtype="datetime")
#             ]
#         )
        
#         self.sinkstate_config = StateConfig(
#             datastore="sink_state_db",
#             table=TableConfig(table="sink_state", dbschema=None, alias="snk_state"),
#             fields=[
#                 FieldConfig(column="id"),
#                 FieldConfig(column="timestamp", dtype="datetime"),
#                 FieldConfig(column="data"),
#                 FieldConfig(column="updated_at", dtype="datetime")
#             ]
#         )
        
#         # Set up reconciliation config
#         self.r_config = ReconciliationConfig(
#             strategy=MD5_SUM_HASH,
#             partition_column_type="int",
#             initial_partition_interval=100,
#             source_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column="data",
#                 order_column="id"
#             ),
#             sink_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column="data",
#                 order_column="id"
#             ),
#             source_state_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column="data",
#                 order_column="id"
#             ),
#             sink_state_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column="data",
#                 order_column="id"
#             )
#         )
        
#         # Set up mock pipeline
#         self.pipeline = MagicMock()
#         self.pipeline.source = self.source
#         self.pipeline.sink = self.sink
#         self.pipeline.sourcestate = self.sourcestate
#         self.pipeline.sinkstate = self.sinkstate
        
#         # Assign configs to adapters
#         self.source.adapter_config = self.source_config
#         self.sink.adapter_config = self.sink_config
#         self.sourcestate.adapter_config = self.sourcestate_config
#         self.sinkstate.adapter_config = self.sinkstate_config
        
#     def tearDown(self):
#         """Clean up after the test."""
#         self.source.close()
#         self.sink.close()
#         self.sourcestate.close()
#         self.sinkstate.close()
    
#     def _populate_test_data(self):
#         """Populate test data for source and sink."""
#         # Source data
#         source_data = [
#             (1, datetime(2023, 1, 1), "data1", datetime(2023, 1, 1)),
#             (2, datetime(2023, 1, 2), "data2", datetime(2023, 1, 2)),
#             (3, datetime(2023, 1, 3), "data3", datetime(2023, 1, 3)),
#             (4, datetime(2023, 1, 4), "data4", datetime(2023, 1, 4)),
#             (5, datetime(2023, 1, 5), "data5", datetime(2023, 1, 5)),
#             # Missing in sink
#             (6, datetime(2023, 1, 6), "data6", datetime(2023, 1, 6)),
#             (7, datetime(2023, 1, 7), "data7", datetime(2023, 1, 7)),
#             # Different in sink
#             (8, datetime(2023, 1, 8), "data8_source", datetime(2023, 1, 8)),
#             (9, datetime(2023, 1, 9), "data9_source", datetime(2023, 1, 9)),
#             (10, datetime(2023, 1, 10), "data10", datetime(2023, 1, 10)),
#         ]
        
#         # Sink data
#         sink_data = [
#             (1, datetime(2023, 1, 1), "data1", datetime(2023, 1, 1)),
#             (2, datetime(2023, 1, 2), "data2", datetime(2023, 1, 2)),
#             (3, datetime(2023, 1, 3), "data3", datetime(2023, 1, 3)),
#             (4, datetime(2023, 1, 4), "data4", datetime(2023, 1, 4)),
#             (5, datetime(2023, 1, 5), "data5", datetime(2023, 1, 5)),
#             # Missing from source
#             (11, datetime(2023, 1, 11), "data11", datetime(2023, 1, 11)),
#             (12, datetime(2023, 1, 12), "data12", datetime(2023, 1, 12)),
#             # Different in source
#             (8, datetime(2023, 1, 8), "data8_sink", datetime(2023, 1, 8)),
#             (9, datetime(2023, 1, 9), "data9_sink", datetime(2023, 1, 9)),
#             (10, datetime(2023, 1, 10), "data10", datetime(2023, 1, 10)),
#         ]
        
#         # Insert data into source
#         for row in source_data:
#             self.source.execute(
#                 "INSERT INTO source_table (id, timestamp, data, updated_at) VALUES (?, ?, ?, ?)",
#                 row
#             )
#             self.sourcestate.execute(
#                 "INSERT INTO source_state (id, timestamp, data, updated_at) VALUES (?, ?, ?, ?)",
#                 row
#             )
        
#         # Insert data into sink
#         for row in sink_data:
#             self.sink.execute(
#                 "INSERT INTO sink_table (id, timestamp, data, updated_at) VALUES (?, ?, ?, ?)",
#                 row
#             )
#             self.sinkstate.execute(
#                 "INSERT INTO sink_state (id, timestamp, data, updated_at) VALUES (?, ?, ?, ?)",
#                 row
#             )
    
#     def test_id_based_reconciliation(self):
#         """Test reconciliation based on ID."""
#         self._populate_test_data()
        
#         # Run reconciliation
#         blocks, statuses = build_blocks(
#             self.sourcestate,
#             self.sinkstate,
#             start=1,
#             end=12,
#             r_config=self.r_config,
#             source_max_block_size=5,
#             sink_max_block_size=5,
#             intervals=[100, 10, 1]
#         )
        
#         # Check results
#         self.assertEqual(len(blocks), len(statuses))
        
#         # Count status types
#         status_counts = {'N': 0, 'M': 0, 'A': 0, 'D': 0}
#         for status in statuses:
#             status_counts[status] += 1
        
#         # We expect:
#         # - 5 matching records (N)
#         # - 2 mismatched records (M)
#         # - 2 records in source but not sink (A)
#         # - 2 records in sink but not source (D)
#         self.assertEqual(status_counts['N'], 6)  # 1-5 and 10 match
#         self.assertEqual(status_counts['M'], 2)  # 8-9 are different
#         self.assertEqual(status_counts['A'], 2)  # 6-7 only in source
#         self.assertEqual(status_counts['D'], 2)  # 11-12 only in sink
    
#     def test_datetime_based_reconciliation(self):
#         """Test reconciliation based on datetime."""
#         self._populate_test_data()
        
#         # Create a datetime-based reconciliation config
#         datetime_r_config = ReconciliationConfig(
#             strategy=MD5_SUM_HASH,
#             partition_column_type="datetime",
#             initial_partition_interval=86400,  # 1 day in seconds
#             source_meta_columns=StoreMeta(
#                 partition_column="timestamp",
#                 hash_column="data",
#                 order_column="timestamp"
#             ),
#             sink_meta_columns=StoreMeta(
#                 partition_column="timestamp",
#                 hash_column="data",
#                 order_column="timestamp"
#             ),
#             source_state_meta_columns=StoreMeta(
#                 partition_column="timestamp",
#                 hash_column="data",
#                 order_column="timestamp"
#             ),
#             sink_state_meta_columns=StoreMeta(
#                 partition_column="timestamp",
#                 hash_column="data",
#                 order_column="timestamp"
#             )
#         )
        
#         # Run reconciliation
#         blocks, statuses = build_blocks(
#             self.sourcestate,
#             self.sinkstate,
#             start=datetime(2023, 1, 1),
#             end=datetime(2023, 1, 12),
#             r_config=datetime_r_config,
#             source_max_block_size=5,
#             sink_max_block_size=5,
#             intervals=[86400, 3600, 60]  # day, hour, minute
#         )
        
#         # Check results
#         self.assertEqual(len(blocks), len(statuses))
        
#         # Count status types
#         status_counts = {'N': 0, 'M': 0, 'A': 0, 'D': 0}
#         for status in statuses:
#             status_counts[status] += 1
        
#         # We should have some of each status type
#         self.assertGreater(status_counts['N'], 0)
#         self.assertGreater(status_counts['M'], 0)
#         self.assertGreater(status_counts['A'], 0)
#         self.assertGreater(status_counts['D'], 0)
    
#     def test_hash_md5_hash_strategy(self):
#         """Test reconciliation using the HASH_MD5_HASH strategy."""
#         self._populate_test_data()
        
#         # Create a config with HASH_MD5_HASH strategy
#         hash_md5_r_config = ReconciliationConfig(
#             strategy=HASH_MD5_HASH,
#             partition_column_type="int",
#             initial_partition_interval=100,
#             source_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column=None,  # No hash column, will use all fields
#                 order_column="id"
#             ),
#             sink_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column=None,  # No hash column, will use all fields
#                 order_column="id"
#             ),
#             source_state_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column=None,
#                 order_column="id"
#             ),
#             sink_state_meta_columns=StoreMeta(
#                 partition_column="id",
#                 hash_column=None,
#                 order_column="id"
#             )
#         )
        
#         # Run reconciliation
#         blocks, statuses = build_blocks(
#             self.sourcestate,
#             self.sinkstate,
#             start=1,
#             end=12,
#             r_config=hash_md5_r_config,
#             source_max_block_size=5,
#             sink_max_block_size=5,
#             intervals=[100, 10, 1]
#         )
        
#         # Check results
#         self.assertEqual(len(blocks), len(statuses))
        
#         # Count status types
#         status_counts = {'N': 0, 'M': 0, 'A': 0, 'D': 0}
#         for status in statuses:
#             status_counts[status] += 1
        
#         # We should have some of each status type
#         self.assertGreater(status_counts['N'], 0)
#         self.assertGreater(status_counts['M'], 0)
#         self.assertGreater(status_counts['A'], 0)
#         self.assertGreater(status_counts['D'], 0)
    
#     def test_calculate_blocks_with_mismatches(self):
#         """Test the calculate_blocks function with mismatched data."""
#         self._populate_test_data()
        
#         # Run calculate_blocks
#         blocks, statuses = calculate_blocks(
#             self.sourcestate,
#             self.sinkstate,
#             start=1,
#             end=12,
#             level=1,
#             r_config=self.r_config,
#             intervals=[100, 10, 1],
#             max_block_size=5,
#             max_level=3
#         )
        
#         # Check results
#         self.assertEqual(len(blocks), len(statuses))
        
#         # Count status types
#         status_counts = {'N': 0, 'M': 0, 'A': 0, 'D': 0}
#         for status in statuses:
#             status_counts[status] += 1
        
#         # We should have some of each status type
#         self.assertGreater(status_counts['N'], 0)
#         self.assertGreater(status_counts['M'], 0)
#         self.assertGreater(status_counts['A'], 0)
#         self.assertGreater(status_counts['D'], 0)
    
#     def test_build_block_hash_query_for_id_based_sync(self):
#         """Test building a block hash query for ID-based syncing."""
#         query = build_block_hash_query(
#             start=1,
#             end=10,
#             level=1,
#             intervals=[100, 10, 1],
#             config=self.source_config,
#             r_config=self.r_config,
#             target="source"
#         )
        
#         # Check query structure
#         self.assertEqual(len(query.select), 3)
#         self.assertEqual(query.select[0].alias, "row_count")
#         self.assertEqual(query.select[1].alias, "blockhash")
#         self.assertEqual(query.select[2].alias, "blockname")
        
#         # Check filters
#         self.assertEqual(len(query.filters), 2)
#         self.assertEqual(query.filters[0].column, "id")
#         self.assertEqual(query.filters[0].operator, ">=")
#         self.assertEqual(query.filters[0].value, 1)
#         self.assertEqual(query.filters[1].column, "id")
#         self.assertEqual(query.filters[1].operator, "<")
#         self.assertEqual(query.filters[1].value, 10)
    
#     @patch('engine.reconcile.build_blocks')
#     @patch('engine.reconcile.get_data_range')
#     def test_reconcile_function(self, mock_get_data_range, mock_build_blocks):
#         """Test the reconcile function."""
#         self._populate_test_data()
        
#         # Mock the get_data_range function to return a fixed range
#         mock_get_data_range.return_value = (1, 12)
        
#         # Mock the build_blocks function to return some blocks and statuses
#         mock_blocks = [MagicMock(), MagicMock()]
#         mock_statuses = ['N', 'M']
#         mock_build_blocks.return_value = (mock_blocks, mock_statuses)
        
#         # Call the reconcile function
#         result_blocks, result_statuses = reconcile(
#             pipeline=self.pipeline,
#             r_config=self.r_config,
#             initial_partition_interval=100,
#             source_max_block_size=5,
#             sink_max_block_size=5,
#             interval_reduction_factor=10,
#             start=1,
#             end=12,
#             force_update=False
#         )
        
#         # Check that get_data_range was called with the right arguments
#         mock_get_data_range.assert_called_once_with(
#             self.sourcestate, 
#             self.sinkstate, 
#             self.r_config, 
#             start=1, 
#             end=12
#         )
        
#         # Check that build_blocks was called with the right arguments
#         mock_build_blocks.assert_called_once()
#         args, kwargs = mock_build_blocks.call_args
#         self.assertEqual(args[0], self.sourcestate)
#         self.assertEqual(args[1], self.sinkstate)
#         self.assertEqual(args[2], 1)
#         self.assertEqual(args[3], 12)
#         self.assertEqual(args[4], self.r_config)
#         self.assertEqual(kwargs['source_max_block_size'], 5)
#         self.assertEqual(kwargs['sink_max_block_size'], 5)
#         self.assertIn('intervals', kwargs)
#         self.assertEqual(kwargs['force_update'], False)
        
#         # Check that the function returns the blocks and statuses from build_blocks
#         self.assertEqual(result_blocks, mock_blocks)
#         self.assertEqual(result_statuses, mock_statuses)


# if __name__ == '__main__':
#     unittest.main()

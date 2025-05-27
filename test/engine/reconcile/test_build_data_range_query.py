import unittest
from unittest.mock import MagicMock
from engine.reconcile import build_data_range_query, get_data_range
from core.config import MD5_SUM_HASH, PartitionFieldConfig, ReconciliationConfig, SourceConfig, SinkConfig, StateConfig, FieldConfig, FilterConfig, JoinConfig, TableConfig
from core.query import Filter, Field, Table, Join
from datetime import datetime

class TestBuildDataRangeQuery(unittest.TestCase):
    def setUp(self):
        self.r_config = ReconciliationConfig(
            strategy=MD5_SUM_HASH,
            partition_column_type="datetime",
            initial_partition_interval=25*100,
            source_pfield=PartitionFieldConfig(partition_column="timestamp"),
            sink_pfield=PartitionFieldConfig(partition_column="timestamp")
        )
        self.src_config = SourceConfig(
            datastore="db1",
            table=TableConfig(table="source_table", dbschema="source_schema", alias="source_alias"),
            joins=[JoinConfig(table="join_table", alias="join_alias", on="source_alias.id = join_alias.source_id", type="inner")],
            filters=[FilterConfig(column="status", operator="=", value="active")],
            batch_size=100
        )
        self.sink_config = SinkConfig(
            datastore="db1",
            table=TableConfig(table="sink_table", dbschema="sink_schema", alias="sink_alias"),
            joins=[JoinConfig(table="join_table", alias="join_alias", on="sink_alias.id = join_alias.sink_id", type="inner")],
            filters=[FilterConfig(column="status", operator="=", value="active")],
            batch_size=100
        )

    def test_basic_functionality(self):
        query = build_data_range_query(self.r_config, self.src_config)
        self.assertEqual(query.select, [
            Field(expr="min(timestamp)", alias='start', type='column'),
            Field(expr="max(timestamp)", alias='end', type='column')
        ])
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(query.joins, [
            Join(table="join_table", alias="join_alias", on="source_alias.id = join_alias.source_id", type="inner")
        ])
        self.assertEqual(query.filters, [
            Filter(column="status", operator="=", value="active")
        ])

    def test_no_joins_or_filters(self):
        src_config = SourceConfig(
            datastore="db1",
            batch_size=100,
            table=TableConfig(table="source_table", dbschema="source_schema", alias="source_alias"),
            joins=[],
            filters=[]
        )
        query = build_data_range_query(self.r_config, src_config)
        self.assertEqual(query.select, [
            Field(expr="min(timestamp)", alias='start', type='column'),
            Field(expr="max(timestamp)", alias='end', type='column')
        ])
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(query.joins, [])
        self.assertEqual(query.filters, [])

    def test_with_joins(self):
        query = build_data_range_query(self.r_config, self.src_config)
        self.assertEqual(query.joins, [
            Join(table="join_table", alias="join_alias", on="source_alias.id = join_alias.source_id", type="inner")
        ])

    def test_with_filters(self):
        query = build_data_range_query(self.r_config, self.src_config)
        self.assertEqual(query.filters, [
            Filter(column="status", operator="=", value="active")
        ])

    def test_edge_case_empty_config(self):
        src_config = SourceConfig(
            datastore="db1",
            batch_size=100,
            table=TableConfig(table="source_table", dbschema="source_schema", alias="source_alias")
        )
        sink_config = SinkConfig(
            datastore="db1",
            batch_size=100,
            table=TableConfig(table="sink_table", dbschema="sink_schema", alias="sink_alias")
        )
        query = build_data_range_query(self.r_config, src_config, sink_config)
        self.assertEqual(query.select, [
            Field(expr="min(timestamp)", alias='start', type='column'),
            Field(expr="max(timestamp)", alias='end', type='column')
        ])
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(query.joins, [])
        self.assertEqual(query.filters, [])
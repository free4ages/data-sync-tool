import unittest
from datetime import datetime, timedelta
from engine.reconcile import build_block_hash_query
from core.config import HASH_MD5_HASH, FieldConfig, ReconciliationConfig, SourceConfig, SinkConfig, PartitionFieldConfig, TableConfig

class TestBuildBlockHashQuery(unittest.TestCase):

    def setUp(self):
        self.source_config = SourceConfig(
            datastore="source_datastore",
            batch_size=1000,
            table=TableConfig(table="source_table", dbschema="source_schema", alias="source_alias"),
            hash_column="source_hash_column",
            fields=[FieldConfig(column="field1"), FieldConfig(column="field2")]
        )
        self.sink_config = SinkConfig(
            datastore="sink_datastore",
            batch_size=1000,
            table=TableConfig(table="sink_table", dbschema="sink_schema", alias="sink_alias"),
            hash_column="sink_hash_column",
            fields=[FieldConfig(column="field1"), FieldConfig(column="field2")]
        )
        self.reconciliation_config = ReconciliationConfig(
            source_pfield=PartitionFieldConfig(
                partition_column="partition_column",
                hash_column="source_hash_column",
                order_column="order_column"
            ),
            sink_pfield=PartitionFieldConfig(
                partition_column="partition_column",
                hash_column="sink_hash_column",
                order_column="order_column"
            ),
            partition_column_type="datetime",
            strategy=HASH_MD5_HASH,
            initial_partition_interval=365*24*60*60
        )

    def test_build_block_hash_query_datetime_source(self):
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        level = 1
        interval_factor = 10
        interval_reduction_factor = 2

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=[86400],
            config=self.source_config,
            r_config=self.reconciliation_config,
            target="source"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, "2023-01-01 00:00:00")
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, "2023-01-02 00:00:00")
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_datetime_sink(self):
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        level = 1
        intervals = [86400]
        interval_reduction_factor = 2

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.sink_config,
            r_config=self.reconciliation_config,
            target="sink"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "sink_table")
        self.assertEqual(query.table.schema, "sink_schema")
        self.assertEqual(query.table.alias, "sink_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, "2023-01-01 00:00:00")
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, "2023-01-02 00:00:00")
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_int_source(self):
        start = 13
        end = 57
        level = 1
        intervals = [50]

        self.reconciliation_config.partition_column_type = "int"

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.source_config,
            r_config=self.reconciliation_config,
            target="source"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, 13)
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, 57)
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_int_sink(self):
        start = 13
        end = 57
        level = 1
        intervals = [50]

        self.reconciliation_config.partition_column_type = "int"

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.sink_config,
            r_config=self.reconciliation_config,
            target="sink"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "sink_table")
        self.assertEqual(query.table.schema, "sink_schema")
        self.assertEqual(query.table.alias, "sink_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, 13)
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, 57)
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_str_source(self):
        start = "A"
        end = "Z"
        level = 1
        intervals = [10]

        self.reconciliation_config.partition_column_type = "str"

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.source_config,
            r_config=self.reconciliation_config,
            target="source"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, "A")
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, "Z")
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_str_sink(self):
        start = "A"
        end = "Z"
        level = 1
        intervals = [10]

        self.reconciliation_config.partition_column_type = "str"

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.sink_config,
            r_config=self.reconciliation_config,
            target="sink"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "sink_table")
        self.assertEqual(query.table.schema, "sink_schema")
        self.assertEqual(query.table.alias, "sink_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, "A")
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, "Z")
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_no_hash_column_source(self):
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        level = 1
        intervals = [10]

        self.reconciliation_config.source_pfield.hash_column = None

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.source_config,
            r_config=self.reconciliation_config,
            target="source"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "source_table")
        self.assertEqual(query.table.schema, "source_schema")
        self.assertEqual(query.table.alias, "source_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, "2023-01-01 00:00:00")
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, "2023-01-02 00:00:00")
        self.assertEqual(query.group_by[0].expr, "blockname")

    def test_build_block_hash_query_no_hash_column_sink(self):
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        level = 1
        intervals = [86400]

        self.reconciliation_config.sink_pfield.hash_column = None

        query = build_block_hash_query(
            start=start,
            end=end,
            level=level,
            intervals=intervals,
            config=self.sink_config,
            r_config=self.reconciliation_config,
            target="sink"
        )

        self.assertEqual(query.select[0].expr, "COUNT(1)")
        self.assertEqual(query.select[0].alias, "row_count")
        self.assertEqual(query.select[1].expr, "partition_column")
        self.assertEqual(query.select[1].alias, "blockhash")
        self.assertEqual(query.select[2].expr, "blockname")
        self.assertEqual(query.select[2].alias, "blockname")
        self.assertEqual(query.table.table, "sink_table")
        self.assertEqual(query.table.schema, "sink_schema")
        self.assertEqual(query.table.alias, "sink_alias")
        self.assertEqual(len(query.filters), 2)
        self.assertEqual(query.filters[0].column, "partition_column")
        self.assertEqual(query.filters[0].operator, ">=")
        self.assertEqual(query.filters[0].value, "2023-01-01 00:00:00")
        self.assertEqual(query.filters[1].column, "partition_column")
        self.assertEqual(query.filters[1].operator, "<")
        self.assertEqual(query.filters[1].value, "2023-01-02 00:00:00")
        self.assertEqual(query.group_by[0].expr, "blockname")

if __name__ == '__main__':
    unittest.main()

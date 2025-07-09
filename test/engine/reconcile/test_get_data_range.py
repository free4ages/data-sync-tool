import unittest
from unittest.mock import MagicMock
from engine.reconcile import build_data_range_query, get_data_range
from core.config import MD5_SUM_HASH, StoreMeta, ReconciliationConfig, SourceConfig, SinkConfig, StateConfig, FieldConfig, FilterConfig, JoinConfig, TableConfig
from core.query import Filter, Field, Table, Join
from datetime import datetime

class TestGetDataRange(unittest.TestCase):
    def setUp(self):
        self.r_config = ReconciliationConfig(
            strategy=MD5_SUM_HASH,
            partition_column_type="datetime",
            initial_partition_interval=25*100,
            # source_meta_columns=StoreMeta(partition_column="timestamp"),
            # sink_meta_columns=StoreMeta(partition_column="timestamp")
        )
        self.src_config = SourceConfig(
            datastore="db1",
            table=TableConfig(table="source_table", dbschema="source_schema", alias="source_alias"),
            meta_columns=StoreMeta(partition_column="timestamp")
        )
        self.sink_config = SinkConfig(
            datastore="db1",
            table=TableConfig(table="sink_table", dbschema="sink_schema", alias="sink_alias"),
            batch_size=100,
            meta_columns=StoreMeta(partition_column="timestamp")
        )
        self.source_state = MagicMock()
        self.sink_state = MagicMock()

    def test_basic_functionality(self):
        self.source_state.fetch_one.return_value = {
            "start": datetime(2023, 1, 1, 0, 0, 0),
            "end": datetime(2023, 1, 1, 22,0,0)
        }
        self.sink_state.fetch_one.return_value = {
            "start": None,
            "end": None
        }

        start, end = get_data_range(self.source_state, self.sink_state, self.r_config)

        self.assertEqual(start, datetime(2023, 1, 1, 0, 0, 0))
        self.assertEqual(end, datetime(2023, 1, 1, 22, 0, 1))
        self.source_state.fetch_one.assert_called_once()
        self.sink_state.fetch_one.assert_called_once()

    def test_edge_case_empty_state(self):
        self.source_state.fetch_one.return_value = {
            "start": None,
            "end": None
        }
        self.sink_state.fetch_one.return_value = {
            "start": None,
            "end": None
        }

        start, end = get_data_range(self.source_state, self.sink_state, self.r_config)

        self.assertIsNone(start)
        self.assertIsNone(end)
        self.source_state.fetch_one.assert_called_once()

    def test_user_provided_start_end(self):
        self.r_config.start = datetime(2023, 1, 15, 0, 0, 0)
        self.r_config.end = datetime(2023, 1, 16, 0, 0, 0)
        self.source_state.fetch_one.return_value = {
            "start": datetime(2023, 1, 1, 0, 0, 0),
            "end": datetime(2023, 1, 31, 23, 59, 59)
        }

        start, end = get_data_range(self.source_state, self.sink_state, self.r_config)

        self.assertEqual(start, datetime(2023, 1, 15, 0, 0, 0))
        self.assertEqual(end, datetime(2023, 1, 16, 0, 0, 0))
        # self.source_state.fetch_one.assert_called_once()
        self.assertFalse(self.source_state.fetch_one.called)

    def test_user_provided_start_only(self):
        self.r_config.start = datetime(2023, 1, 15, 0, 0, 0)
        self.source_state.fetch_one.return_value = {
            "start": datetime(2023, 1, 1, 0, 0, 0),
            "end": datetime(2023, 1, 31, 23, 59, 59)
        }
        self.sink_state.fetch_one.return_value = {
            "start": datetime(2023, 1, 1, 0, 0, 0),
            "end": datetime(2023, 1, 31, 23, 59, 59)
        }

        start, end = get_data_range(self.source_state, self.sink_state, self.r_config)

        self.assertEqual(start, datetime(2023, 1, 15, 0, 0, 0))
        self.assertEqual(end, datetime(2023, 2, 1, 0,0,0))
        self.source_state.fetch_one.assert_called_once()

    def test_user_provided_end_only(self):
        self.r_config.end = datetime(2023, 1, 16, 0, 0, 0)
        self.source_state.fetch_one.return_value = {
            "start": datetime(2023, 1, 1, 0, 0, 0),
            "end": datetime(2023, 1, 31, 23, 59, 59)
        }
        self.sink_state.fetch_one.return_value = {
            "start": datetime(2023, 1, 1, 0, 0, 0),
            "end": datetime(2023, 1, 31, 23, 59, 59)
        }

        start, end = get_data_range(self.source_state, self.sink_state, self.r_config)

        self.assertEqual(start, datetime(2023, 1, 1, 0, 0, 0))
        self.assertEqual(end, datetime(2023, 1, 16, 0, 0, 0))
        self.source_state.fetch_one.assert_called_once()

if __name__ == '__main__':
    unittest.main()

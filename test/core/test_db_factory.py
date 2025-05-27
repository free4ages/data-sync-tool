import unittest
from core.db_factory import get_adapter
from core.config import DatabaseConfig, DatastoreConfig, SourceConfig, WebhookConfig, TableConfig
from adapters.postgres import PostgresAdapter
from adapters.base import Adapter

class TestGetAdapter(unittest.TestCase):
    def setUp(self):
        self.datastores = [
            DatabaseConfig(name="postgres", type="postgres", host="localhost", port=5432, username="user", password="pass", database="db"),
            DatabaseConfig(name="mysql", type="mysql", host="localhost", port=3306, username="user", password="pass", database="db"),
            DatabaseConfig(name="clickhouse", type="clickhouse", host="localhost", port=9000, username="user", password="pass", database="db"),
            WebhookConfig(name="webhook", type="webhook", base_url="http://example.com/webhook"),
            # NatsConfig(name="nats", type="nats", servers=["nats://localhost:4222"], token="token"),
        ]

    def test_valid_adapter(self):
        table_config = TableConfig(table="test_table")
        config = SourceConfig(datastore="postgres", batch_size=100, table=table_config, adapter=None)
        adapter = get_adapter("postgres", self.datastores, config, "source")
        self.assertIsInstance(adapter, PostgresAdapter)

    def test_custom_adapter_path(self):
        from adapters.custom import CustomAdapter
        table_config = TableConfig(table="test_table")
        config = SourceConfig(datastore="postgres", batch_size=100, table=table_config, adapter="adapters.custom.CustomAdapter")
        adapter = get_adapter("postgres", self.datastores, config, "source")
        self.assertIsInstance(adapter, CustomAdapter)


    def test_datastore_not_found(self):
        table_config = TableConfig(table="test_table")
        config = SourceConfig(datastore="nonexistent", batch_size=100, table=table_config, adapter=None)
        with self.assertRaises(ValueError) as context:
            get_adapter("nonexistent", self.datastores, config, "source")
        self.assertEqual(str(context.exception), "DB peer 'nonexistent' not found")

    def test_empty_datastores_list(self):
        table_config = TableConfig(table="test_table")
        config = SourceConfig(datastore="postgres", batch_size=100, table=table_config, adapter=None)
        with self.assertRaises(ValueError) as context:
            get_adapter("postgres", [], config, "source")
        self.assertEqual(str(context.exception), "DB peer 'postgres' not found")

    def test_case_sensitivity(self):
        table_config = TableConfig(table="test_table")
        config = SourceConfig(datastore="POSTGRES", batch_size=100, table=table_config, adapter=None)
        adapter = get_adapter("POSTGRES", self.datastores, config, "source")
        self.assertIsInstance(adapter, PostgresAdapter)



if __name__ == '__main__':
    unittest.main()

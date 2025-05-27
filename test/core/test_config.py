import unittest
from pydantic import ValidationError
from core.config import Config, DatabaseConfig, WebhookConfig, NatsConfig, RedisConfig, HTTPConfig, PipelineConfig, SourceConfig, SinkConfig, StateConfig, ReconciliationConfig, EnrichmentConfig, JoinConfig, FilterConfig, FieldConfig, MergeStrategyConfig,TableConfig


class TestConfigs(unittest.TestCase):

    def test_database_config(self):
        data = {
            "name": "db1",
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "postgres",
            "database": "postgres"
        }
        model = DatabaseConfig.model_validate(data)
        self.assertEqual(model.name, "db1")

    def test_webhook_config(self):
        data = {
            "name": "webhook1",
            "type": "webhook",
            "base_url": "http://mybaseurl"
        }
        model = WebhookConfig.model_validate(data)
        self.assertEqual(model.base_url, "http://mybaseurl")

    def test_nats_config(self):
        data = {
            "name": "nats_source",
            "type": "nats",
            "servers": ["nats://localhost:4222"],
            "token": "your_nats_token"
        }
        model = NatsConfig.model_validate(data)
        self.assertEqual(model.name, "nats_source")

    def test_redis_config(self):
        data = {
            "name": "redis1",
            "type": "redis",
            "host": "redis.host.local",
            "port": 6379,
            "db": 1
        }
        model = RedisConfig.model_validate(data)
        self.assertEqual(model.type, "redis")

    def test_http_config(self):
        data = {
            "name": "http1",
            "type": "http",
            "url": "http://myurl.com"
        }
        model = HTTPConfig.model_validate(data)
        self.assertEqual(model.url, "http://myurl.com")

    def test_source_config_with_joins_and_filters(self):
        data = {
            "datastore": "db1",
            "batch_size": 100,
            "table": {"table": "users", "alias": "u", "dbschema": "public"},
            "joins": [
                {"table": "orders", "alias": "o", "type": "left", "on": "users.id = o.user_id"},
                {"table": "payments", "alias": "p", "type": "right", "on": "users.id = p.user_id"}
            ],
            "filters": [
                {"column": "u.active", "operator": "=", "value": True},
                {"column": "u.status", "operator": "=", "value": "success"}
            ],
            "fields": [
                {"column": "u.name", "dtype": "str", "alias": "name"},
                {"column": "u.id", "dtype": "int"}
            ]
        }
        model = SourceConfig.model_validate(data)
        self.assertEqual(len(model.joins), 2)
        self.assertEqual(model.filters[0].value, True)

    def test_sink_config_basic(self):
        data = {
            "datastore": "db2",
            "table": {
                "table": "users"       
            },
            "batch_size": 100,
            "merge_strategy": {"strategy": "delete_insert", "allow_delete": True},
            "unique_key": ["sname", "sid"],
            "filters": [{"column": "order_total", "operator": ">", "value": 1000}],
            "fields": [{"column": "sname", "dtype": "str", "source_column": "u.name"}]
        }
        model = SinkConfig.model_validate(data)
        self.assertTrue(model.merge_strategy.allow_delete)
        self.assertIn("sname", model.unique_key)
        self.assertEqual(model.table.table, "users")

    def test_state_config_source(self):
        data = {
            "datastore": "db2",
            "table": {
                "table": "users_state"
            },
            "filters": [{"column": "deleted", "operator": "=", "value": "FALSE"}],
            "fields": [{"column": "uid", "dtype": "int", "source_column": "u.id"}]
        }
        model = StateConfig.model_validate(data)
        self.assertEqual(model.table.table, "users_state")
        # self.assertTrue(model.use_source)
        # self.assertEqual(model.hash_column, "checksum")

    def test_reconciliation_config(self):
        data = {
            "strategy": "md5sum_hash",
            "partition_column": "created_at",
            "partition_column_type": "datetime",
            "start": "lambda: datetime.datetime(2020,2,1)",
            "end": "lambda: datetime.datetime.now()",
            "initial_partition_interval": 31536000,
            "source_pfield": {
                "partition_column": "created_at"
            },
            "sink_pfield": {
                "partition_column": "created_at",
                "hash_column": "checksum"
            }
        }
        model = ReconciliationConfig.model_validate(data)
        self.assertEqual(model.strategy, "md5sum_hash")
        self.assertEqual(model.source_state_pfield, model.source_pfield)

    
        

    def test_enrichment_config(self):
        data = {
            "externalstore": "redis1",
            "name": "redis1",
            "type": "redis",
            "key_template": "user:{{ user_id }}",
            "output": "lambda v: {'redis_name': v}"
        }
        model = EnrichmentConfig.model_validate(data)
        self.assertEqual(model.externalstore, "redis1")

    def xtest_pipeline_config_provide_sink(self):
        #not required anymore
        source_data = {
            "datastore": "db1",
            "batch_size": 50,
            "table": {"table": "users"},
            "fields": [{"column": "u.id", "dtype": "int"}]
        }
        sink_data = {
            "datastore": "db2",
            "table": "users",
            "batch_size": 50,
            "merge_strategy": {"strategy": "upsert", "allow_delete": False},
            "unique_key": ["id"],
            "filters": [],
            "fields": [{"column": "id", "dtype": "int", "source_column": "u.id"}]
        }
        pipeline = PipelineConfig.model_validate({
            "name": "test",
            "source": source_data,
            "sink": sink_data,
            "reconciliation": [],
            "enrichment": []
        })
        # provide_sink_to_source should attach context and re-validate source
        self.assertTrue(hasattr(pipeline.source, "__context__"))
    
    # Edge-case tests:
    def test_missing_required_field(self):
        # Missing 'host' in DatabaseConfig
        data = {
            "name": "db_missing",
            "type": "mysql",
            "port": 3306,
            "username": "user",
            "password": "pass",
            "database": "db"
        }
        with self.assertRaises(ValidationError):
            DatabaseConfig.model_validate(data)

    def test_invalid_literal_type(self):
        # Invalid type for WebhookConfig
        data = {"name": "web1", "type": "http_wrong", "base_url": "url"}
        with self.assertRaises(ValidationError):
            WebhookConfig.model_validate(data)

    def test_dynamic_jinja_parsing(self):
        from core.config import parse_dynamic_field
        tmpl = "TMPL(Hello {{ name }})"
        func = parse_dynamic_field(tmpl)
        self.assertTrue(callable(func))
        self.assertEqual(func(name="World"), "Hello World")

    def test_dynamic_lambda_parsing(self):
        from core.config import parse_dynamic_field
        lam = "lambda x: x * 2"
        func = parse_dynamic_field(lam)
        self.assertTrue(callable(func))
        self.assertEqual(func(5), 10)

    def test_external_store_discriminator_error(self):
        # Wrong discriminator should fail
        data = {"name": "store1", "type": "unknown", "url": "x"}
        with self.assertRaises(ValidationError):
            HTTPConfig.model_validate(data)
    
    def test_source_field_population(self):
        source_data = {
            "datastore": "db1", 
            "batch_size": 10, 
            "table": {
                "table": "mytable",
                "alias": "t1"
            },
            "fields": [{"column" : "t1.id"},{"column" : "name"}]
        }
        source = SourceConfig(**source_data)
        self.assertEqual(source.datastore,"db1")
        self.assertEqual([x.alias for x in source.fields],["t1__id","name"])
    

    def test_source_auto_fields_from_sink(self):
        # When fields=None, fields are populated from sink
        source_data = {
            "datastore": "db1", 
            "batch_size": 10, 
            "table": {
                "table": "t1"
            }
        }
        sink_data = {
            "datastore": "db1", 
            "table": {
                "table": "t1"
            }, 
            "batch_size": 10,
            "merge_strategy": {
                "strategy": "upsert", 
                "allow_delete": False
            },
            "unique_key": ["id"], 
            "filters": [],
            "fields": [{"column": "id", "dtype": "int", "source_column": "id"}]
        }
        pipeline = PipelineConfig.model_validate({
            "name": "edge",
            "source": source_data,
            "sink": sink_data,
            "reconciliation": [],
            "enrichment": []
        })
        # After provide_sink, source.fields should include sink.fields
        cols = [f.column for f in pipeline.source.fields]
        self.assertIn("id", cols)

    def test_source_auto_fields_from_sink1(self):
        # When fields=None, fields are populated from sink
        source_data = {
            "datastore": "db1", 
            "batch_size": 10, 
            "table": {
                "table": "source_table",
                "alias": "t1"
            },
            "fields": [
                {"column" : "t1.id"},
                {"column" : "t1.name"},
                {"column": "t1.name", "alias":"myaliasname"}
            ]
        }
        sink_data = {
            "datastore": "db1", 
            "table": {
                "table": "sink_table"
            }, 
            "batch_size": 10,
            "merge_strategy": {
                "strategy": "upsert", 
                "allow_delete": False
            },
            "unique_key": ["id"], 
            "filters": [],
            "fields": [
                {"column": "id", "dtype": "int", "source_column": "t1.id"},
                {"column": "name", "dtype": "str", "source_column": "t1__name"},
                {"column": "unnamed", "dtype": "str", "source_column": "t1.not_defined_in_source"},
                {"column": "redis_column", "dtype": "str", "source_column": "name", "source":"redis1"},
            ]
        }
        pipeline = PipelineConfig.model_validate({
            "name": "edge",
            "source": source_data,
            "sink": sink_data,
            "reconciliation": [],
            "enrichment": []
        })
        # After provide_sink, source.fields should include sink.fields
        cols = [f.column for f in pipeline.source.fields]
        self.assertEqual([x.column for x in pipeline.source.fields],["t1.id","t1.name","t1.name","t1.not_defined_in_source"])
        self.assertEqual([x.alias for x in pipeline.source.fields],["t1__id","t1__name","myaliasname","t1__not_defined_in_source"])
        self.assertEqual([x.column for x in pipeline.sink.fields],["id","name","unnamed","redis_column"])
        self.assertEqual([x.alias for x in pipeline.sink.fields],["id","name","unnamed","redis_column"])


if __name__ == "__main__":
    unittest.main()
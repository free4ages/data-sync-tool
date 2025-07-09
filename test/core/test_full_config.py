import unittest
from core.config import Config, DatabaseConfig, StoreMeta, WebhookConfig, NatsConfig, RedisConfig, HTTPConfig, PipelineConfig, SourceConfig, SinkConfig, StateConfig, ReconciliationConfig, EnrichmentConfig, JoinConfig, FilterConfig, FieldConfig, MergeStrategyConfig,TableConfig

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = Config(
            datastores=[
                DatabaseConfig(
                    name="db1",
                    type="postgres",
                    host="localhost",
                    port=5432,
                    username="postgres",
                    password="postgres",
                    database="postgres"
                ),
                DatabaseConfig(
                    name="db2",
                    type="mysql",
                    host="localhost",
                    port=3306,
                    username="mysql",
                    password="mysql",
                    database="mysqldb"
                ),
                DatabaseConfig(
                    name="db3",
                    type="clickhouse",
                    host="localhost",
                    port=8236,
                    username="clickhouse",
                    password="clickhouse",
                    database="clickhousedb"
                ),
                # WebhookConfig(
                #     name="webhook1",
                #     type="webhook",
                #     base_url="http://mybaseurl"
                # ),
                # NatsConfig(
                #     name="nats_source",
                #     type="nats",
                #     servers=["nats://localhost:4222"],
                #     token="your_nats_token",
                #     subject="user.events",
                #     queue="event_routing",
                #     max_msgs=200,
                #     per_msg_timeout=2.0,
                #     total_timeout=30.0
                # )
            ],
            externalstores=[
                RedisConfig(
                    name="redis1",
                    type="redis",
                    host="redis.host.local",
                    port=6379,
                    db=1
                ),
                HTTPConfig(
                    name="http1",
                    type="http",
                    url="http://myurl.com"
                )
            ],
            pipelines=[
                PipelineConfig(
                    name="data_move_db1_users_to_db2_users",
                    source=SourceConfig(
                        datastore="db1",
                        table=TableConfig(
                            table="users",
                            alias="u",
                            dbschema="public"
                        ),
                        joins=[
                            JoinConfig(
                                table="orders",
                                alias="o",
                                type="left",
                                on="users.id = o.user_id"
                            ),
                            JoinConfig(
                                table="payments",
                                alias="p",
                                type="right",
                                on="users.id = p.user_id"
                            )
                        ],
                        filters=[
                            FilterConfig(
                                column="u.active",
                                operator="=",
                                value=True
                            ),
                            FilterConfig(
                                column="u.status",
                                operator="=",
                                value="success"
                            ),
                            FilterConfig(
                                column="o.total",
                                operator=">",
                                value=1000
                            )
                        ],
                        fields=[
                            FieldConfig(
                                column="u.name",
                                dtype="str",
                                alias="name"
                            ),
                            FieldConfig(
                                column="u.id",
                                dtype="int"
                            ),
                            FieldConfig(
                                column="o.total",
                                dtype="float"
                            )
                        ]
                    ),
                    sink=SinkConfig(
                        datastore="db2",
                        table=TableConfig(
                            table="users",
                            dbschema="myschema"
                        ),
                        batch_size=100,
                        merge_strategy=MergeStrategyConfig(
                            strategy="delete_insert",
                            allow_delete=True
                        ),
                        meta_columns=StoreMeta(
                            unique_columns=["sname", "sid"]
                        ),
                        filters=[
                            FilterConfig(
                                column="order_total",
                                operator=">",
                                value=1000
                            )
                        ],
                        fields=[
                            FieldConfig(
                                column="sname",
                                dtype="str",
                                source_column="u.name"
                            ),
                            FieldConfig(
                                column="sid",
                                dtype="int",
                                source_column="u.id"
                            ),
                            FieldConfig(
                                column="order_total",
                                dtype="float",
                                source_column="o.total"
                            ),
                            FieldConfig(
                                column="template_column",
                                dtype="str",
                                source_column="TMPL({{ name }} - ${{ o__total }})"
                            ),
                            FieldConfig(
                                column="function_column",
                                dtype="str",
                                source_column="lambda r: 1 if r['o__total']>1000 else 0"
                            ),
                            FieldConfig(
                                column="redis_name",
                                dtype="str",
                                source_column="ename",
                                source="redis1"
                            ),
                            FieldConfig(
                                column="http_name",
                                dtype="str",
                                source_column="http_name",
                                source="http1"
                            )
                        ]
                    ),
                    sourcestate=StateConfig(
                        datastore="db2",
                        table=TableConfig(
                            table = "users_state",
                            dbschema="myschema"
                        ),
                        filters=[
                            FilterConfig(
                                column="deleted",
                                operator="=",
                                value=False
                            )
                        ],
                        fields=[
                            FieldConfig(
                                column="uid",
                                dtype="int",
                                source_column="u.id"
                            ),
                            FieldConfig(
                                column="mname",
                                dtype="str",
                                source_column="u.name"
                            ),
                            FieldConfig(
                                column="checksum",
                                dtype="str"
                            )
                        ]
                    ),
                    sinkstate=StateConfig(
                        datastore="db2",
                        table=TableConfig(
                            table="users",
                            dbschema="myschema"
                        ),
                        filters=[
                            FilterConfig(
                                column="deleted",
                                operator="=",
                                value=False
                            )
                        ],
                        fields=[
                            FieldConfig(
                                column="mid",
                                dtype="int",
                                source_column="sid"
                            ),
                            FieldConfig(
                                column="mname",
                                dtype="int",
                                source_column="sname"
                            ),
                            FieldConfig(
                                column="checksum",
                                dtype="str"
                            )
                        ]
                    ),
                    reconciliation=[
                        ReconciliationConfig(
                            strategy="md5sum_hash",
                            # partition_column="created_at",
                            partition_column_type="datetime",
                            start="lambda: datetime.datetime(2020,2,1)",
                            end="lambda: datetime.datetime.now()",
                            initial_partition_interval=1*365*24*60*60,
                            # source_meta_columns=StoreMeta(
                            #     partition_column='created_at'
                            # ),
                            # sink_meta_columns=StoreMeta(
                            #     partition_column='checksum',
                            #     hash_column='checksum'
                            # )
                        ),
                        ReconciliationConfig(
                            strategy="hash_md5_hash",
                            # partition_column="created_at",
                            partition_column_type="datetime",
                            start="lambda: datetime.datetime(2020,2,1)",
                            end="lambda: datetime.datetime(2024,1,1)",
                            # source_order_column="u.id",
                            initial_partition_interval=1*365*24*60*60,
                            # source_meta_columns=StoreMeta(
                            #     partition_column='created_at'
                            # ),
                            # sink_meta_columns=StoreMeta(
                            #     partition_column='checksum',
                            #     hash_column='checksum'
                            # )
                        ),
                        ReconciliationConfig(
                            strategy="hash_md5_hash",
                            # partition_column="id",
                            partition_column_type="uuid",
                            start="",
                            end="",
                            # source_order_column="u.id",
                            initial_partition_interval=1,
                            # source_meta_columns=StoreMeta(
                            #     partition_column='created_at'
                            # ),
                            # sink_meta_columns=StoreMeta(
                            #     partition_column='checksum',
                            #     hash_column='checksum'
                            # )
                        )
                    ],
                    enrichment=[
                        EnrichmentConfig(
                            externalstore="redis1",
                            name="redis1",
                            type="redis",
                            key_template="user:{{ user_id }}",
                            output='lambda v: {"redis_name":v}'
                        ),
                        EnrichmentConfig(
                            externalstore="http1",
                            name="http1",
                            type="http",
                            path="TMPL(/creditscore?user_id={{user_id}})",
                            params={
                                "user_id": "{{ user_id }}"
                            }
                        ),
                        EnrichmentConfig(
                            type="http",
                            url="TMPL(http:://myurl.com/creditscore?user_id={{user_id}})"
                        )
                    ]
                )
            ]
        )

    def test_datastores(self):
        self.assertEqual(len(self.config.datastores), 3)
        self.assertEqual(self.config.datastores[0].name, "db1")
        self.assertEqual(self.config.datastores[1].type, "mysql")
        self.assertEqual(self.config.datastores[2].database, "clickhousedb")
        # self.assertEqual(self.config.datastores[3].base_url, "http://mybaseurl")
        # self.assertEqual(self.config.datastores[4].servers, ["nats://localhost:4222"])

    def test_externalstores(self):
        self.assertEqual(len(self.config.externalstores), 2)
        self.assertEqual(self.config.externalstores[0].name, "redis1")
        self.assertEqual(self.config.externalstores[1].url, "http://myurl.com")

    def test_pipelines(self):
        self.assertEqual(len(self.config.pipelines), 1)
        pipeline = self.config.pipelines[0]
        self.assertEqual(pipeline.name, "data_move_db1_users_to_db2_users")
        self.assertEqual(pipeline.source.datastore, "db1")
        self.assertEqual(pipeline.sink.table.table, "users")
        self.assertEqual(pipeline.sourcestate.table.table, "users_state")
        self.assertEqual(pipeline.sinkstate.table.table, "users")
        self.assertEqual(len(pipeline.reconciliation), 3)
        self.assertEqual(len(pipeline.enrichment), 3)

    def test_source_config(self):
        source = self.config.pipelines[0].source
        self.assertEqual(source.table.table, "users")
        self.assertEqual(source.joins[0].table, "orders")
        self.assertEqual(source.filters[0].column, "u.active")
        self.assertEqual(source.fields[0].column, "u.name")

    def test_sink_config(self):
        sink = self.config.pipelines[0].sink
        self.assertEqual(sink.table.table, "users")
        self.assertEqual(sink.merge_strategy.strategy, "delete_insert")
        self.assertEqual(sink.meta_columns.unique_columns, ["sname", "sid"])
        self.assertEqual(sink.fields[0].column, "sname")
        # self.assertEqual(sink.fields[3].source_column, "TMPL({{ name }} - ${{ o__total }})")
        # self.assertEqual(sink.fields[4].source_column, "lambda r: 1 if r['o__total']>1000 else 0")
        self.assertTrue(callable(sink.fields[3].source_column))
        self.assertEqual(sink.fields[3].source_column({"name": "test", "o__total":50}), "test - $50")
        self.assertTrue(callable(sink.fields[4].source_column))
        self.assertEqual(sink.fields[4].source_column({"o__total": 2000}), 1)

    def test_state_config(self):
        sourcestate = self.config.pipelines[0].sourcestate
        self.assertEqual(sourcestate.datastore, "db2")
        self.assertEqual(sourcestate.filters[0].column, "deleted")
        self.assertEqual(sourcestate.fields[0].column, "uid")

        sinkstate = self.config.pipelines[0].sinkstate
        self.assertEqual(sinkstate.datastore, "db2")
        self.assertEqual(sinkstate.filters[0].column, "deleted")
        self.assertEqual(sinkstate.fields[0].column, "mid")

    def test_reconciliation_config(self):
        reconciliation = self.config.pipelines[0].reconciliation
        self.assertEqual(reconciliation[0].strategy, "md5sum_hash")
        # self.assertEqual(reconciliation[1].partition_column, "created_at")
        self.assertEqual(reconciliation[2].partition_column_type, "uuid")


    def test_enrichment_config(self):
        enrichment = self.config.pipelines[0].enrichment
        self.assertEqual(enrichment[0].externalstore, "redis1")
        self.assertTrue(callable(enrichment[1].path))
        self.assertTrue(callable(enrichment[2].url))

if __name__ == '__main__':
    unittest.main()

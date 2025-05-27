from core.config import Config, DatabaseConfig, WebhookConfig, NatsConfig, RedisConfig, HTTPConfig, PipelineConfig, SourceConfig, SinkConfig, StateConfig, ReconciliationConfig, EnrichmentConfig, JoinConfig, FilterConfig, FieldConfig, MergeStrategyConfig,TableConfig

config = Config(
            datastores=[
                # DatabaseConfig(
                #     name="db1",
                #     type="postgres",
                #     host="localhost",
                #     port=5432,
                #     username="postgres",
                #     password="postgres",
                #     database="postgres"
                # ),
                # DatabaseConfig(
                #     name="db2",
                #     type="mysql",
                #     host="localhost",
                #     port=3306,
                #     username="mysql",
                #     password="mysql",
                #     database="mysqldb"
                # ),
                # DatabaseConfig(
                #     name="db3",
                #     type="clickhouse",
                #     host="localhost",
                #     port=8236,
                #     username="clickhouse",
                #     password="clickhouse",
                #     database="clickhousedb"
                # ),
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
                # RedisConfig(
                #     name="redis1",
                #     type="redis",
                #     host="redis.host.local",
                #     port=6379,
                #     db=1
                # ),
                HTTPConfig(
                    name="http1",
                    type="http",
                    url="http://myurl.com"
                )
            ],
            pipelines=[
                # PipelineConfig(
                #     name="data_move_db1_users_to_db2_users",
                #     source=SourceConfig(
                #         datastore="db1",
                #         batch_size=100,
                #         table=TableConfig(
                #             table="users",
                #             alias="u",
                #             dbschema="public"
                #         ),
                #         joins=[
                #             JoinConfig(
                #                 table="orders",
                #                 alias="o",
                #                 type="left",
                #                 on="users.id = o.user_id"
                #             ),
                #             JoinConfig(
                #                 table="payments",
                #                 alias="p",
                #                 type="right",
                #                 on="users.id = p.user_id"
                #             )
                #         ],
                #         filters=[
                #             FilterConfig(
                #                 column="u.active",
                #                 operator="=",
                #                 value=True
                #             ),
                #             FilterConfig(
                #                 column="u.status",
                #                 operator="=",
                #                 value="success"
                #             ),
                #             FilterConfig(
                #                 column="o.total",
                #                 operator=">",
                #                 value=1000
                #             )
                #         ],
                #         fields=[
                #             FieldConfig(
                #                 column="u.name",
                #                 dtype="str",
                #                 alias="name"
                #             ),
                #             FieldConfig(
                #                 column="u.id",
                #                 dtype="int"
                #             ),
                #             FieldConfig(
                #                 column="o.total",
                #                 dtype="float"
                #             )
                #         ]
                #     ),
                #     sink=SinkConfig(
                #         datastore="db2",
                #         table="users",
                #         dbschema="myschema",
                #         batch_size=100,
                #         merge_strategy=MergeStrategyConfig(
                #             strategy="delete_insert",
                #             allow_delete=True
                #         ),
                #         unique_key=["sname", "sid"],
                #         filters=[
                #             FilterConfig(
                #                 column="order_total",
                #                 operator=">",
                #                 value=1000
                #             )
                #         ],
                #         fields=[
                #             FieldConfig(
                #                 column="sname",
                #                 dtype="str",
                #                 scolumn="u.name"
                #             ),
                #             FieldConfig(
                #                 column="sid",
                #                 dtype="int",
                #                 scolumn="u.id"
                #             ),
                #             FieldConfig(
                #                 column="order_total",
                #                 dtype="float",
                #                 scolumn="o.total"
                #             ),
                #             FieldConfig(
                #                 column="template_column",
                #                 dtype="str",
                #                 scolumn="TMPL({{ name }} - ${{ o__total }})"
                #             ),
                #             FieldConfig(
                #                 column="function_column",
                #                 dtype="str",
                #                 scolumn="lambda r: 1 if r['o__total']>1000 else 0"
                #             ),
                #             FieldConfig(
                #                 column="redis_name",
                #                 dtype="str",
                #                 scolumn="ename",
                #                 source="redis1"
                #             ),
                #             FieldConfig(
                #                 column="http_name",
                #                 dtype="str",
                #                 scolumn="http_name",
                #                 source="http1"
                #             )
                #         ]
                #     ),
                #     sourcestate=StateConfig(
                #         datastore="db2",
                #         table="users_state",
                #         dbschema="myschema",
                #         filters=[
                #             FilterConfig(
                #                 column="deleted",
                #                 operator="=",
                #                 value=False
                #             )
                #         ],
                #         hash_column="checksum",
                #         fields=[
                #             FieldConfig(
                #                 column="uid",
                #                 dtype="int",
                #                 scolumn="u.id"
                #             ),
                #             FieldConfig(
                #                 column="mname",
                #                 dtype="str",
                #                 scolumn="u.name"
                #             ),
                #             FieldConfig(
                #                 column="checksum",
                #                 dtype="str"
                #             )
                #         ]
                #     ),
                #     sinkstate=StateConfig(
                #         datastore="db2",
                #         table="users",
                #         dbschema="myschema",
                #         filters=[
                #             FilterConfig(
                #                 column="deleted",
                #                 operator="=",
                #                 value=False
                #             )
                #         ],
                #         hash_column="checksum",
                #         fields=[
                #             FieldConfig(
                #                 column="mid",
                #                 dtype="int",
                #                 scolumn="sid"
                #             ),
                #             FieldConfig(
                #                 column="mname",
                #                 dtype="int",
                #                 scolumn="sname"
                #             ),
                #             FieldConfig(
                #                 column="checksum",
                #                 dtype="str"
                #             )
                #         ]
                #     ),
                #     reconciliation=[
                #         ReconciliationConfig(
                #             strategy="md5sum_hash",
                #             partition_column="created_at",
                #             partition_column_type="datetime",
                #             start="lambda: datetime.datetime(2020,2,1)",
                #             end="lambda: datetime.datetime.now()",
                #             initial_partition_interval=1*365*24*60*60
                #         ),
                #         ReconciliationConfig(
                #             strategy="hash_md5_hash",
                #             partition_column="created_at",
                #             partition_column_type="datetime",
                #             start="lambda: datetime.datetime(2020,2,1)",
                #             end="lambda: datetime.datetime.now()",
                #             source_order_column="u.id",
                #             initial_partition_interval=1*365*24*60*60
                #         ),
                #         ReconciliationConfig(
                #             strategy="hash_md5_hash",
                #             partition_column="id",
                #             partition_column_type="uuid",
                #             start="",
                #             end="",
                #             source_order_column="u.id",
                #             initial_partition_interval=1
                #         )
                #     ],
                #     enrichment=[
                #         # EnrichmentConfig(
                #         #     externalstore="redis1",
                #         #     name="redis1",
                #         #     type="redis",
                #         #     key_template="user:{{ user_id }}",
                #         #     output='lambda v: {"redis_name":v}'
                #         # ),
                #         # EnrichmentConfig(
                #         #     externalstore="http1",
                #         #     name="http1",
                #         #     type="http",
                #         #     path="TMPL(/creditscore?user_id={{user_id}})"
                #         # ),
                #         # EnrichmentConfig(
                #         #     type="http",
                #         #     url="TMPL(http:://myurl.com/creditscore?user_id={{user_id}})"
                #         # )
                #     ]
                # )
            ]
)
import pytest
import os
from unittest.mock import MagicMock, patch
from datetime import datetime

import psycopg2
import pytz
from core.config import MD5_SUM_HASH, HASH_MD5_HASH, FieldConfig, ReconciliationConfig, PipelineConfig
from engine.reconcile import prepare_data_blocks, Block
from adapters.postgres import PostgresAdapter
from adapters.base import Adapter


@pytest.fixture(scope="module")
def postgres_connection_params():
    """Connection parameters for the PostgreSQL Docker container."""
    return {
        'host': os.environ.get('POSTGRES_HOST', 'localhost'),
        'port': os.environ.get('POSTGRES_PORT', '5432'),
        'user': os.environ.get('POSTGRES_USER', 'synctool'),
        'password': os.environ.get('POSTGRES_PASSWORD', 'synctool'),
        'dbname': os.environ.get('POSTGRES_DB', 'synctool')
    }


@pytest.fixture(scope="module")
def postgres_source_adapter(postgres_connection_params):
    """Create a PostgresAdapter for the source."""
    adapter = PostgresAdapter(
        store_config={
            'host': postgres_connection_params['host'],
            'port': postgres_connection_params['port'],
            'username': postgres_connection_params['user'],
            'password': postgres_connection_params['password'],
            'database': postgres_connection_params['dbname'],
        },
        adapter_config=MagicMock(
            fields=[
                FieldConfig(column="id"),
                FieldConfig(column="name"),
                FieldConfig(column="value"),
                FieldConfig(column="created_at")
            ],
            table=MagicMock(table="source_table", dbschema="public", alias="src"),
            filters=[],
            joins=[],
            batch_size=1000,
            meta_columns = MagicMock(
                hash_column="hash_value",
                partition_column="created_at",
                order_column="id"
            )
        ),
        role='source'
    )
    adapter.connect()
    yield adapter
    adapter.close()


@pytest.fixture(scope="module")
def postgres_sink_adapter(postgres_connection_params):
    """Create a PostgresAdapter for the sink."""
    adapter = PostgresAdapter(
        store_config={
            'host': postgres_connection_params['host'],
            'port': postgres_connection_params['port'],
            'username': postgres_connection_params['user'],
            'password': postgres_connection_params['password'],
            'database': postgres_connection_params['dbname'],
        },
        adapter_config=MagicMock(
            fields=[
                FieldConfig(column="id"),
                FieldConfig(column="name"),
                FieldConfig(column="value"),
                FieldConfig(column="created_at")
            ],
            table=MagicMock(table="sink_table", dbschema="public", alias="snk"),
            filters=[],
            joins=[],
            batch_size=1000,
            meta_columns = MagicMock(
                hash_column="hash_value",
                partition_column="created_at",
                order_column="id"
            )
        ),
        role='sink'
    )
    adapter.connect()
    yield adapter
    adapter.close()


@pytest.fixture
def int_mock_pipeline(postgres_source_adapter, postgres_sink_adapter):
    """Create a mock Pipeline object."""
    pipeline = MagicMock()
    pipeline.source = MagicMock(adapter_config=MagicMock(batch_size=1000))
    pipeline.sink = MagicMock(adapter_config=MagicMock(batch_size=1000))
    pipeline.sourcestate = postgres_source_adapter
    pipeline.sinkstate = postgres_sink_adapter
    pipeline.sourcestate.adapter_config.meta_columns=MagicMock(
        hash_column="hash_value",
        partition_column="id",
        order_column="id"
    )
    pipeline.sinkstate.adapter_config.meta_columns=MagicMock(
        hash_column="hash_value",
        partition_column="id",
        order_column="id"
    )
    return pipeline

@pytest.fixture
def datetime_mock_pipeline(postgres_source_adapter, postgres_sink_adapter):
    """Create a mock Pipeline object."""
    pipeline = MagicMock()
    pipeline.source = MagicMock(adapter_config=MagicMock(batch_size=1000))
    pipeline.sink = MagicMock(adapter_config=MagicMock(batch_size=1000))
    pipeline.sourcestate = postgres_source_adapter
    pipeline.sinkstate = postgres_sink_adapter
    pipeline.sourcestate.adapter_config.meta_columns=MagicMock(
        hash_column="hash_value",
        partition_column="created_at",
        order_column="id"
    )
    pipeline.sinkstate.adapter_config.meta_columns=MagicMock(
        hash_column="hash_value",
        partition_column="created_at",
        order_column="id"
    )
    return pipeline

@pytest.fixture
def int_reconciliation_config():
    """Create a ReconciliationConfig for int partition column."""
    config = MagicMock(spec=ReconciliationConfig)
    config.partition_column_type = "int"
    config.strategy = HASH_MD5_HASH
    # config.source_meta_columns = MagicMock(
    #     hash_column="hash_value",
    #     partition_column="id",
    #     order_column="id"
    # )
    # config.source_state_meta_columns = config.source_meta_columns
    # config.sink_meta_columns = MagicMock(
    #     hash_column="hash_value",
    #     partition_column="id",
    #     order_column="id"
    # )
    # config.sink_state_meta_columns = config.sink_meta_columns
    config.filters = []
    config.joins = []
    config.initial_partition_interval = 10000
    config.start=None
    config.end=None
    return config


@pytest.fixture
def datetime_reconciliation_config():
    """Create a ReconciliationConfig for datetime partition column."""
    config = MagicMock(spec=ReconciliationConfig)
    config.partition_column_type = "datetime"
    config.strategy = HASH_MD5_HASH
    # config.source_meta_columns = MagicMock(
    #     hash_column="hash_value",
    #     partition_column="created_at",
    #     order_column="id"
    # )
    # config.source_state_meta_columns = config.source_meta_columns
    # config.sink_meta_columns = MagicMock(
    #     hash_column="hash_value",
    #     partition_column="created_at",
    #     order_column="id"
    # )
    # config.sink_state_meta_columns = config.sink_meta_columns
    config.filters = []
    config.joins = []
    config.initial_partition_interval = 86400  # 1 day in seconds
    config.start=None
    config.end=None
    return config


class TestPrepareDataBlocks:
    """Tests for prepare_data_blocks function with PostgresAdapter."""

    def test_prepare_data_blocks_int_partition(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with integer partition column."""
        # Execute function with int partition column
        start = 1
        end = 40000
        max_block_size = 5000
        interval_reduction_factor = 5
        
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=9999,
            max_block_size=max_block_size,
            interval_reduction_factor=interval_reduction_factor,
            start=start,
            end=end
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated some blocks"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        
        # Verify we have all types of statuses ('N', 'M', 'A', 'D')
        status_types = set(statuses)
        assert 'N' in status_types, "Should have matching blocks"
        assert 'M' in status_types, "Should have mismatched blocks"
        assert 'A' in status_types, "Should have blocks present in source but not sink"
        assert 'D' in status_types, "Should have blocks present in sink but not source"
        
        # Verify block properties
        for block in blocks:
            assert isinstance(block, Block), "Should be a Block instance"
            assert block.start >= start, "Block start should be >= overall start"
            assert block.end <= end, "Block end should be <= overall end"
            assert block.num_rows > 0, "Block should have rows"
            assert isinstance(block.hash, str), "Block hash should be a string"
        
        # Verify no block exceeds max_block_size
        for block, status in zip(blocks, statuses):
            if status in ('M', 'A'):
                assert block.num_rows <= max_block_size, \
                    f"Block with {block.num_rows} rows exceeds max_block_size of {max_block_size}"

    def test_prepare_data_blocks_int_specific_ranges(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with specific integer ranges to verify status mapping."""
        # Test range where data is matching (1-10000)
        blocks_matching, statuses_matching = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=5000,
            max_block_size=5000,
            interval_reduction_factor=5,
            start=1,
            end=10000
        )
        
        # All blocks should be matching ('N')
        assert all(status == 'N' for status in statuses_matching), \
            "All blocks in range 1-10000 should be matching"
        
        # Test range where data is mismatched (10001-20000)
        blocks_mismatched, statuses_mismatched = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=5000,
            max_block_size=5000,
            interval_reduction_factor=5,
            start=10001,
            end=20000
        )
        
        # All blocks should be mismatched ('M')
        assert all(status == 'M' for status in statuses_mismatched), \
            "All blocks in range 10001-20000 should be mismatched"
        
        # Test range where data is in source but not sink (20001-30000)
        blocks_source_only, statuses_source_only = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=5000,
            max_block_size=5000,
            interval_reduction_factor=5,
            start=20001,
            end=30000
        )
        
        # All blocks should be added ('A')
        assert all(status == 'A' for status in statuses_source_only), \
            "All blocks in range 20001-30000 should be added (in source but not sink)"
        
        # Test range where data is in sink but not source (30001-40000)
        blocks_sink_only, statuses_sink_only = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=5000,
            max_block_size=5000,
            interval_reduction_factor=5,
            start=30001,
            end=40000
        )
        
        # All blocks should be deleted ('D')
        assert all(status == 'D' for status in statuses_sink_only), \
            "All blocks in range 30001-40000 should be deleted (in sink but not source)"

    def test_prepare_data_blocks_block_size_constraints(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with different block size constraints."""
        # Test with small block size to force deep recursion
        small_block_size = 100
        blocks_small, statuses_small = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=10000,
            max_block_size=small_block_size,
            interval_reduction_factor=5,
            start=1,
            end=1000
        )
        
        # Verify no block exceeds the small block size
        for block, status in zip(blocks_small, statuses_small):
            if status in ('M', 'A'):
                assert block.num_rows <= small_block_size, \
                    f"Block with {block.num_rows} rows exceeds max_block_size of {small_block_size}"
        
        # Test with large block size
        large_block_size = 10000
        blocks_large, statuses_large = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=5000,
            max_block_size=large_block_size,
            interval_reduction_factor=5,
            start=1,
            end=1000
        )
        
        # Should have fewer blocks with large block size
        assert len(blocks_large) <= len(blocks_small), \
            "Should have fewer blocks with larger block size"

    def test_prepare_data_blocks_with_merge(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with conditions that trigger block merging."""
        # Configure to produce many small blocks with same status that should be merged
        max_block_size = 500
        # sink_max_block_size = 5000  # Large enough to allow merging
        # import pdb;pdb.set_trace()
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=1000,
            max_block_size=max_block_size,
            interval_reduction_factor=5,
            start=20001,  # Range with only 'A' status blocks
            end=22000
        )
        
        # Should have merged some blocks
        # Since all blocks have the same status ('A'), they should be merged
        # based on the max_block_size
        for block in blocks:
            # Blocks can exceed max_block_size after merging,
            # but should not exceed max_block_size
            assert block.num_rows <= max_block_size, \
                f"Merged block with {block.num_rows} rows exceeds max_block_size of {max_block_size}"
        

    def test_prepare_data_blocks_datetime_partition(self, datetime_mock_pipeline, datetime_reconciliation_config):
        """Test prepare_data_blocks with datetime partition column."""
        # Define datetime range
        start = datetime(2023, 1, 1, tzinfo=pytz.utc)
        end = datetime(2023, 1, 31, tzinfo=pytz.utc)
        
        blocks, statuses = prepare_data_blocks(
            pipeline=datetime_mock_pipeline,
            r_config=datetime_reconciliation_config,
            initial_partition_interval=86400,  # 1 day in seconds
            max_block_size=5000,
            interval_reduction_factor=4,
            start=start,
            end=end
        )
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks for datetime range"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        
        # Verify datetime blocks
        for block in blocks:
            assert isinstance(block, Block), "Should be a Block instance"
            assert isinstance(block.start, datetime), "Block start should be a datetime object"
            assert isinstance(block.end, datetime), "Block end should be a datetime object"
            assert block.start >= start, "Block start should be >= overall start"
            assert block.end <= end, "Block end should be <= overall end"

    def test_prepare_data_blocks_force_update(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with force_update flag."""
        # Execute with force_update=True
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=9999,
            max_block_size=5000,
            interval_reduction_factor=5,
            start=1,
            end=40000,
            force_update=True
        )
        
        # Assertions (should still get valid results with force_update)
        assert len(blocks) > 0, "Should have generated blocks with force_update"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        
        # Status distribution should be the same as regular execution
        status_counts = {status: statuses.count(status) for status in set(statuses)}
        assert 'N' in status_counts, "Should have matching blocks with force_update"
        assert 'M' in status_counts, "Should have mismatched blocks with force_update"
        assert 'A' in status_counts, "Should have added blocks with force_update"
        assert 'D' in status_counts, "Should have deleted blocks with force_update"

    def test_prepare_data_blocks_custom_intervals(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with custom interval settings."""
        # Test with custom interval parameters
        initial_partition_interval = 2000  # Small interval
        interval_reduction_factor = 10  # Large reduction factor
        
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=initial_partition_interval,
            max_block_size=500,
            interval_reduction_factor=interval_reduction_factor,
            start=1,
            end=10000
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks with custom intervals"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        
        # Verify small block sizes due to custom interval settings
        block_sizes = [block.num_rows for block,status in zip(blocks,statuses) if status in ('M','A')]
        max_block_size = max(block_sizes) if block_sizes else 0
        assert max_block_size <= 500, "Max block size should respect max_block_size"

    def test_prepare_data_blocks_full_datetime(self, datetime_mock_pipeline, datetime_reconciliation_config):
        """Test prepare_data_blocks with custom interval settings."""
        # Test with custom interval parameters
        initial_partition_interval = 7*86400  # Small interval
        interval_reduction_factor = 64  # Large reduction factor
        
        blocks, statuses = prepare_data_blocks(
            pipeline=datetime_mock_pipeline,
            r_config=datetime_reconciliation_config,
            initial_partition_interval=initial_partition_interval,
            max_block_size=1,
            interval_reduction_factor=interval_reduction_factor,
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks with custom intervals"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='A']) == 10000, "Should have 10000 new rows in source"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='D']) == 10000, "Should have 10000 extra rows in sink"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='N']) == 10437, "Should have 10437 common rows as 437 numbers between 10000 to 20000 have same mod for 19 and 23"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='M']) == 9563, "Should have 9563 modified rows"
 
        # Verify small block sizes due to custom interval settings
        # block_sizes = [block.num_rows for block,status in zip(blocks,statuses) if status in ('M','A')]
        # max_block_size = max(block_sizes) if block_sizes else 0
        # assert max_block_size <= 5000, "Max block size should respect max_block_size"

    def test_prepare_data_blocks_full_int(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with custom interval settings."""
        # Test with custom interval parameters
        initial_partition_interval = 10000  # Small interval
        interval_reduction_factor = 10  # Large reduction factor
        
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=initial_partition_interval,
            max_block_size=1,
            interval_reduction_factor=interval_reduction_factor,
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks with custom intervals"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='A']) == 10000, "Should have 10000 new rows in source"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='D']) == 10000, "Should have 10000 extra rows in sink"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='N']) == 10437, "Should have 10437 common rows as 437 numbers between 10000 to 20000 have same mod for 19 and 23"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='M']) == 9563, "Should have 9563 modified rows"
 
        # Verify small block sizes due to custom interval settings
        block_sizes = [block.num_rows for block,status in zip(blocks,statuses) if status in ('M','A')]
        max_block_size = max(block_sizes) if block_sizes else 0
        assert max_block_size <= 5000, "Max block size should respect max_block_size"

    def test_prepare_data_blocks_from_raw_field_int(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with custom interval settings."""
        # Test with custom interval parameters
        initial_partition_interval = 10000  # Small interval
        interval_reduction_factor = 10  # Large reduction factor
        int_mock_pipeline.sourcestate.adapter_config.meta_columns.hash_column=None
        int_mock_pipeline.sinkstate.adapter_config.meta_columns.hash_column=None
        # int_reconciliation_config.source_state_meta_columns.hash_column=None
        # int_reconciliation_config.sink_state_meta_columns.hash_column=None
        
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=initial_partition_interval,
            max_block_size=1,
            interval_reduction_factor=interval_reduction_factor,
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks with custom intervals"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='A']) == 10000, "Should have 10000 new rows in source"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='D']) == 10000, "Should have 10000 extra rows in sink"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='N']) == 10437, "Should have 10437 common rows as 437 numbers between 10000 to 20000 have same mod for 19 and 23"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='M']) == 9563, "Should have 9563 modified rows"
 
        # Verify small block sizes due to custom interval settings
        block_sizes = [block.num_rows for block,status in zip(blocks,statuses) if status in ('M','A')]
        max_block_size = max(block_sizes) if block_sizes else 0
        assert max_block_size <= 5000, "Max block size should respect max_block_size"

    def test_prepare_data_blocks_from_raw_field_datetime(self, datetime_mock_pipeline, datetime_reconciliation_config):
        """Test prepare_data_blocks with custom interval settings."""
        # Test with custom interval parameters
        initial_partition_interval = 7*86400  # Small interval
        interval_reduction_factor = 64  # Large reduction factor
        datetime_mock_pipeline.sourcestate.adapter_config.meta_columns.hash_column=None
        datetime_mock_pipeline.sinkstate.adapter_config.meta_columns.hash_column=None
        # datetime_reconciliation_config.source_state_meta_columns.hash_column=None
        # datetime_reconciliation_config.sink_state_meta_columns.hash_column=None
        
        blocks, statuses = prepare_data_blocks(
            pipeline=datetime_mock_pipeline,
            r_config=datetime_reconciliation_config,
            initial_partition_interval=initial_partition_interval,
            max_block_size=1,
            interval_reduction_factor=interval_reduction_factor,
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks with custom intervals"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='A']) == 10000, "Should have 10000 new rows in source"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='D']) == 10000, "Should have 10000 extra rows in sink"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='N']) == 10437, "Should have 10437 common rows as 437 numbers between 10000 to 20000 have same mod for 19 and 23"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='M']) == 9563, "Should have 9563 modified rows"
 
        # Verify small block sizes due to custom interval settings
        block_sizes = [block.num_rows for block,status in zip(blocks,statuses) if status in ('M','A')]
        max_block_size = max(block_sizes) if block_sizes else 0
        assert max_block_size <= 5000, "Max block size should respect max_block_size"

    def test_prepare_data_blocks_sum_md5_hash_int(self, int_mock_pipeline, int_reconciliation_config):
        """Test prepare_data_blocks with custom interval settings."""
        # Test with custom interval parameters
        initial_partition_interval = 10000  # Small interval
        interval_reduction_factor = 10  # Large reduction factor
        int_mock_pipeline.sourcestate.adapter_config.meta_columns.hash_column=None
        int_mock_pipeline.sinkstate.adapter_config.meta_columns.hash_column=None
        # int_reconciliation_config.source_state_meta_columns.hash_column=None
        # int_reconciliation_config.sink_state_meta_columns.hash_column=None
        int_reconciliation_config.strategy = MD5_SUM_HASH
        
        blocks, statuses = prepare_data_blocks(
            pipeline=int_mock_pipeline,
            r_config=int_reconciliation_config,
            initial_partition_interval=initial_partition_interval,
            max_block_size=1,
            interval_reduction_factor=interval_reduction_factor,
        )
        # import pdb;pdb.set_trace()
        
        # Assertions
        assert len(blocks) > 0, "Should have generated blocks with custom intervals"
        assert len(blocks) == len(statuses), "Should have same number of blocks and statuses"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='A']) == 10000, "Should have 10000 new rows in source"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='D']) == 10000, "Should have 10000 extra rows in sink"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='N']) == 10437, "Should have 10437 common rows as 437 numbers between 10000 to 20000 have same mod for 19 and 23"
        assert sum([b.num_rows for b,status in zip(blocks,statuses) if status=='M']) == 9563, "Should have 9563 modified rows"
 
        # Verify small block sizes due to custom interval settings
        block_sizes = [block.num_rows for block,status in zip(blocks,statuses) if status in ('M','A')]
        max_block_size = max(block_sizes) if block_sizes else 0
        assert max_block_size <= 5000, "Max block size should respect max_block_size"
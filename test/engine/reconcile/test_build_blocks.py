import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from engine.reconcile import build_blocks, Block, calculate_blocks
from core.config import HASH_MD5_HASH, MD5_SUM_HASH, ReconciliationConfig
from adapters.base import Adapter


class TestBuildBlocks:
    """Tests for the build_blocks function in reconcile.py"""

    @pytest.fixture
    def mock_source_adapter(self):
        adapter = MagicMock(spec=Adapter)
        adapter.adapter_config = MagicMock(
            fields=["id", "name", "value"],
            table=MagicMock(table="source_table", dbschema="public", alias="src"),
            filters=[],
            joins=[]
        )
        return adapter

    @pytest.fixture
    def mock_sink_adapter(self):
        adapter = MagicMock(spec=Adapter)
        adapter.adapter_config = MagicMock(
            fields=["id", "name", "value"],
            table=MagicMock(table="sink_table", dbschema="public", alias="snk"),
            filters=[],
            joins=[]
        )
        return adapter

    @pytest.fixture
    def mock_reconciliation_config(self):
        config = MagicMock(spec=ReconciliationConfig)
        config.partition_column_type = "int"
        config.strategy = HASH_MD5_HASH
        config.source_pfield = MagicMock(
            hash_column="hash_col",
            partition_column="id",
            order_column="id"
        )
        config.source_state_pfield = MagicMock(
            partition_column="id"
        )
        config.sink_pfield = MagicMock(
            hash_column="hash_col",
            partition_column="id",
            order_column="id"
        )
        config.filters = []
        config.joins = []
        return config

    def test_basic_int_blocks(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test basic block calculation with int partition column."""
        # Setup source blocks data
        source_data = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 37}
        ]
        # Setup sink blocks data  
        sink_data = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 37}
        ]
        
        # Configure mocks
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data
        
        # Set test parameters
        start = 13
        end = 50
        intervals = [50, 10, 3, 1]
        
        # Run function
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=1000,
            intervals=intervals
        )
        
        # Assertions
        assert len(blocks) == 1
        assert statuses[0] == 'N'  # Matching blocks
        assert blocks[0].start == start
        assert blocks[0].end == end
        assert blocks[0].num_rows == 37
        assert blocks[0].hash == "hash1"

    def test_different_partition_types(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test with different partition column types."""
        # Setup data
        source_data = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 99}
        ]
        sink_data = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 99}
        ]
        
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data

        # Test with int partition type
        mock_reconciliation_config.partition_column_type = "int"
        mock_reconciliation_config.source_pfield.partition_column = "id"
        mock_reconciliation_config.sink_pfield.partition_column = "id"
        mock_reconciliation_config.source_state_pfield.partition_column = "id"
        
        intervals = [100, 10, 1]
        
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, 1, 99, mock_reconciliation_config,
            max_block_size=1000,
            intervals=intervals
        )
        
        assert len(blocks) == 1
        assert statuses[0] == 'N'
        

        # Test with datetime partition type

        source_data = [
            {"blockname": "19358", "blockhash": "hash1", "row_count": 100}
        ]
        sink_data = [
            {"blockname": "19358", "blockhash": "hash1", "row_count": 100}
        ]
        
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 2)
        # import pdb;pdb.set_trace()
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start_time, end_time, mock_reconciliation_config,
            max_block_size=1000, 
            intervals=[86400, 3600, 60]
        )
        
        assert len(blocks) == 1
        assert statuses[0] == 'N'

    def test_status_mapping(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test different status mappings (N, M, A, D)."""
        # Setup data with different statuses
        source_data = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 100},  # Match
            {"blockname": "1", "blockhash": "hash2", "row_count": 100},  # Mismatch
            {"blockname": "2", "blockhash": "hash3", "row_count": 100},  # Added (missing in sink)
        ]
        
        sink_data = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 100},  # Match
            {"blockname": "1", "blockhash": "diff_hash", "row_count": 100},  # Mismatch
            {"blockname": "3", "blockhash": "hash4", "row_count": 100},  # Deleted (missing in source)
        ]
        
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data
        
        start = 13
        end = 57
        intervals = [50, 10, 3, 1]
        
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=1000,
            intervals=intervals
        )
        
        # Check all expected statuses are present
        assert 'N' in statuses  # Match
        assert 'M' in statuses  # Mismatch
        assert 'A' in statuses  # Added
        assert 'D' in statuses  # Deleted
        
        # Check total number of blocks
        assert len(blocks) == 4
        assert len(statuses) == 4

    def test_block_merging(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test merging of adjacent blocks with same status."""
        # Create data with adjacent blocks to be merged
        source_data = [
            {"blockname": "19358", "blockhash": "hash1", "row_count": 100},
            {"blockname": "19359", "blockhash": "hash2", "row_count": 100},
            {"blockname": "19360", "blockhash": "hash3", "row_count": 100},
        ]
        
        # Empty sink to create all 'A' statuses (for merging)
        sink_data = []
        
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 4)
        
        # Set sink_max_block_size to allow merging all blocks
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=100000, 
            intervals=[86400*3, 3600, 60]
        )
        
        # Check merged result
        assert len(blocks) == 1  # All three blocks should be merged
        assert blocks[0].num_rows == 300  # Sum of all row counts
        assert statuses[0] == 'A'  # All added (missing from sink)

    def test_recursive_calculation(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test that blocks exceeding max_block_size are recursively processed."""
        # Source data with large block
        source_data_level1 = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 1500}
        ]
        # Sink data with mismatching hash
        sink_data_level1 = [
            {"blockname": "0", "blockhash": "hash2", "row_count": 1500}
        ]
        
        # Level 2 data (after recursive calculation)
        source_data_level2 = [
            {"blockname": "0-0", "blockhash": "hash3", "row_count": 700},
            {"blockname": "0-1", "blockhash": "hash4", "row_count": 800}
        ]
        sink_data_level2 = [
            {"blockname": "0-0", "blockhash": "hash5", "row_count": 700},
            {"blockname": "0-1", "blockhash": "hash4", "row_count": 800}  # One matching, one mismatched
        ]
        
        # Configure mock to return different results based on level
        def mock_source_fetch(query):
            if query.select[2].metadata.level == 2:  # Level 2 query
                return source_data_level2
            return source_data_level1
            
        def mock_sink_fetch(query):
            if query.select[2].metadata.level == 2:  # Level 2 query
                return sink_data_level2
            return sink_data_level1
        
        mock_source_adapter.fetch.side_effect = mock_source_fetch
        mock_sink_adapter.fetch.side_effect = mock_sink_fetch
        
        # Configure for datetime
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        
        # Call with max_block_size smaller than the row count to trigger recursion
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=1000,
            intervals=[86400, 3600, 60]
        )
        
        # Verify we have both matching and mismatched blocks
        assert 'M' in statuses  # At least one mismatched
        assert 'N' in statuses  # At least one matching
        
        # Verify we have blocks at level 2
        assert any(block.level == 2 for block in blocks)

    def test_multilevel_recursion(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test recursion through multiple levels."""
        # Mock level 1 data
        source_data_level1 = [{"blockname": "0", "blockhash": "hash1", "row_count": 5000}]
        sink_data_level1 = [{"blockname": "0", "blockhash": "hash2", "row_count": 5000}]
        
        # Mock level 2 data
        source_data_level2 = [{"blockname": "0-0", "blockhash": "hash3", "row_count": 3000}]
        sink_data_level2 = [{"blockname": "0-0", "blockhash": "hash4", "row_count": 3000}]
        
        # Mock level 3 data
        source_data_level3 = [
            {"blockname": "0-0-0", "blockhash": "hash5", "row_count": 1500},
            {"blockname": "0-0-1", "blockhash": "hash6", "row_count": 1500}
        ]
        sink_data_level3 = [
            {"blockname": "0-0-0", "blockhash": "hash7", "row_count": 1500},
            {"blockname": "0-0-1", "blockhash": "hash6", "row_count": 1500}  # This one matches
        ]
        
        # Configure mocks based on query level
        def mock_source_fetch(query):
            level = query.select[2].metadata.level
            if level == 3:  # Level 3
                return source_data_level3
            elif level == 2:  # Level 2
                return source_data_level2
            return source_data_level1
            
        def mock_sink_fetch(query):
            level = query.select[2].metadata.level
            if level == 3:  # Level 3
                return sink_data_level3
            elif level == 2:  # Level 2
                return sink_data_level2
            return sink_data_level1
        
        mock_source_adapter.fetch.side_effect = mock_source_fetch
        mock_sink_adapter.fetch.side_effect = mock_sink_fetch
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        
        # Call with max_block_size that will trigger multiple levels of recursion
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=2000, 
            intervals=[86400, 3600, 60, 1]
        )
        
        # Verify blocks at all expected levels
        # assert any(block.level == 1 for block in blocks)
        # assert any(block.level == 2 for block in blocks)
        assert any(block.level == 3 for block in blocks)
        
        # Verify we have both matching and mismatched statuses
        assert 'M' in statuses
        assert 'N' in statuses

    def test_max_level_constraint(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test that recursion stops at max_level."""
        # Setup data that would normally recurse beyond max_level
        # Always return mismatched data with high row count to trigger recursion
        def mock_source_fetch(query):
            return [{"blockname": "0", "blockhash": "hash1", "row_count": 10000}]
        
        def mock_sink_fetch(query):
            return [{"blockname": "0", "blockhash": "hash2", "row_count": 10000}]
        
        mock_source_adapter.fetch.side_effect = mock_source_fetch
        mock_sink_adapter.fetch.side_effect = mock_sink_fetch
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        
        # Set max_level to 2
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=5000,
            intervals=[86400, 3600]
        )
        
        # No block should have level > 2
        assert all(block.level <= 2 for block in blocks)
        
        # All should be marked as mismatched
        assert all(status == 'M' for status in statuses)

    def test_block_time_ordering(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test that blocks are ordered by start time/value."""
        # Create blocks with different start times but same level
        source_data = [
            {"blockname": "2", "blockhash": "hash1", "row_count": 100},
            {"blockname": "0", "blockhash": "hash2", "row_count": 100},
            {"blockname": "1", "blockhash": "hash3", "row_count": 100},
        ]
        
        sink_data = [
            {"blockname": "2", "blockhash": "hash1", "row_count": 100},
            {"blockname": "0", "blockhash": "hash2", "row_count": 100},
            {"blockname": "1", "blockhash": "hash3", "row_count": 100},
        ]
        
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        # Create datetime start/end times
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 4)
        
        blocks, _ = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=1000, 
            intervals=[86400, 3600, 60]
        )
        
        # Check blocks are ordered by start time
        for i in range(1, len(blocks)):
            assert blocks[i-1].start <= blocks[i].start

    def test_level_based_ordering(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test ordering of blocks at different levels."""
        # Create data with mixed levels
        source_data_level1 = [{"blockname": "0", "blockhash": "hash1", "row_count": 1500}]
        sink_data_level1 = [{"blockname": "0", "blockhash": "hash2", "row_count": 1500}]
        
        source_data_level2 = [
            {"blockname": "0-0", "blockhash": "hash3", "row_count": 700},
            {"blockname": "0-1", "blockhash": "hash4", "row_count": 800}
        ]
        sink_data_level2 = [
            {"blockname": "0-0", "blockhash": "hash5", "row_count": 700},
            {"blockname": "0-1", "blockhash": "hash6", "row_count": 800}
        ]
        
        def mock_source_fetch(query):
            if query.select[2].metadata.level == 2:
                return source_data_level2
            return source_data_level1
            
        def mock_sink_fetch(query):
            if query.select[2].metadata.level == 2:
                return sink_data_level2
            return sink_data_level1
        
        mock_source_adapter.fetch.side_effect = mock_source_fetch
        mock_sink_adapter.fetch.side_effect = mock_sink_fetch
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        
        with patch('engine.reconcile.calculate_blocks', wraps=calculate_blocks) as mock_calc:
            blocks, _ = build_blocks(
                mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
                max_block_size=1000, 
                intervals=[86400, 3600, 60]
            )
        
        # Check level ordering - deeper levels (higher numbers) should come first
        # This is important because the results of recursive calls are extended into the final_blocks list
        for i in range(1, len(blocks)):
            if blocks[i-1].start == blocks[i].start and blocks[i-1].end == blocks[i].end:
                assert blocks[i-1].level >= blocks[i].level

    def test_order_after_merging(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test block ordering is maintained after merging adjacent blocks."""
        # Create data with blocks that should be merged
        source_data = [
            {"blockname": "19358", "blockhash": "hash1", "row_count": 100},
            {"blockname": "19359", "blockhash": "hash2", "row_count": 100},
            {"blockname": "19360", "blockhash": "hash3", "row_count": 100},
            {"blockname": "19361", "blockhash": "hash4", "row_count": 100},
        ]
        
        # Sink is missing blocks 1 and 2 to create adjacent 'A' status blocks
        sink_data = [
            {"blockname": "19358", "blockhash": "hash1", "row_count": 100},
            {"blockname": "19361", "blockhash": "hash4", "row_count": 100},
        ]
        
        mock_source_adapter.fetch.return_value = source_data
        mock_sink_adapter.fetch.return_value = sink_data
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 5)
        
        # Set max_block_size high enough to merge blocks
        blocks, statuses = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=1000, 
            intervals=[86400*4, 3600, 60]
        )
        
        # Verify blocks are merged and in correct order
        assert len(blocks) == 3  # Should be 3 blocks (0, merged 1 and 2, 3)
        
        # Verify blocks are ordered by start time
        for i in range(1, len(blocks)):
            assert blocks[i-1].start <= blocks[i].start
            
        # Verify the correct blocks were merged
        assert 'A' in statuses  # Should have at least one 'added' status
        assert 'N' in statuses  # Should have at least one 'matching' status
        
        # Find the merged block
        merged_block = None
        for i, (block, status) in enumerate(zip(blocks, statuses)):
            if status == 'A' and block.num_rows == 200:  # Merged block has 200 rows
                merged_block = block
                break
        
        assert merged_block is not None

    def xtest_max_block_size_constraint(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
        """Test that source blocks don't exceed the max_block_size threshold."""
        # Create a mix of blocks, some exceeding the max_block_size threshold
        source_data_level1 = [
            {"blockname": "0", "blockhash": "hash1", "row_count": 1500},  # Exceeds limit
            {"blockname": "1", "blockhash": "hash2", "row_count": 800},   # Below limit
            {"blockname": "2", "blockhash": "hash3", "row_count": 1200},  # Exceeds limit
        ]
        
        sink_data_level1 = [
            {"blockname": "0", "blockhash": "hash4", "row_count": 1500},
            {"blockname": "1", "blockhash": "hash5", "row_count": 800},
            {"blockname": "2", "blockhash": "hash6", "row_count": 1200},
        ]
        
        # Level 2 data for blocks that were split due to exceeding size
        source_data_level2_block0 = [
            {"blockname": "0-0", "blockhash": "hash7", "row_count": 700},
            {"blockname": "0-1", "blockhash": "hash8", "row_count": 800},
        ]
        
        source_data_level2_block2 = [
            {"blockname": "2-0", "blockhash": "hash9", "row_count": 600},
            {"blockname": "2-1", "blockhash": "hash10", "row_count": 600},
        ]
        
        sink_data_level2_block0 = [
            {"blockname": "0-0", "blockhash": "hash11", "row_count": 700},
            {"blockname": "0-1", "blockhash": "hash12", "row_count": 800},
        ]
        
        sink_data_level2_block2 = [
            {"blockname": "2-0", "blockhash": "hash13", "row_count": 600},
            {"blockname": "2-1", "blockhash": "hash14", "row_count": 600},
        ]
        
        # Configure mock responses based on the query
        def mock_source_fetch(query):
            level = query.select[2].metadata.level
            if level == 1:
                return source_data_level1
            elif level == 2:
                if "0" in str(query.filters[0].value):
                    return source_data_level2_block0
                else:
                    return source_data_level2_block2
            
        def mock_sink_fetch(query):
            level = query.select[2].metadata.level
            if level == 1:
                return sink_data_level1
            elif level == 2:
                if "0" in str(query.filters[0].value):
                    return sink_data_level2_block0
                else:
                    return sink_data_level2_block2
        
        mock_source_adapter.fetch.side_effect = mock_source_fetch
        mock_sink_adapter.fetch.side_effect = mock_sink_fetch
        
        mock_reconciliation_config.partition_column_type = "datetime"
        mock_reconciliation_config.source_pfield.partition_column = "created_at"
        mock_reconciliation_config.sink_pfield.partition_column = "created_at"
        mock_reconciliation_config.source_state_pfield.partition_column = "created_at"
        
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 4)
        
        # Set max_block_size to 1000
        max_block_size = 1000
        blocks, _ = build_blocks(
            mock_source_adapter, mock_sink_adapter, start, end, mock_reconciliation_config,
            max_block_size=max_block_size, 
            intervals=[86400, 3600, 60]
        )
        
        # Verify no block exceeds max_block_size
        for block in blocks:
            assert block.num_rows <= max_block_size, \
                f"Block with {block.num_rows} rows exceeds max_block_size of {max_block_size}"
        
        # Verify we have more blocks than we started with (due to splitting)
        assert len(blocks) >= 5, "Expected blocks to be split due to size constraints"
        
        # Verify we have blocks at different levels
        assert any(block.level == 1 for block in blocks)
        assert any(block.level == 2 for block in blocks)

    # def xtest_sink_max_block_size_constraint(self, mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    #     """Test that merged blocks don't exceed the sink_max_block_size threshold."""
    #     # Create many small blocks with the same status to be merged
    #     source_data = [
    #         {"blockname": "0", "blockhash": "hash1", "row_count": 300},
    #         {"blockname": "1", "blockhash": "hash2", "row_count": 300},
    #         {"blockname": "2", "blockhash": "hash3", "row_count": 300},
    #         {"blockname": "3", "blockhash": "hash4", "row_count": 300},
    #         {"blockname": "4", "blockhash": "hash5", "row_count":

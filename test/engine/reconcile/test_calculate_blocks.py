import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from core.config import MD5_SUM_HASH, FieldConfig, ReconciliationConfig
from engine.reconcile import calculate_blocks, Block, to_blocks
from adapters.base import Adapter

@pytest.fixture
def mock_source_adapter():
    adapter = Mock(spec=Adapter)
    adapter.adapter_config = Mock(
        fields=[
            FieldConfig(column="id"), 
            FieldConfig(column="name"), 
            FieldConfig(column="value")
        ],
        table=Mock(table="source_table", dbschema="public", alias="src"),
        filters=[],
        joins=[]
    )
    return adapter

@pytest.fixture
def mock_sink_adapter():
    adapter = Mock(spec=Adapter)
    adapter.adapter_config = Mock(
        fields=[
            FieldConfig(column="id"), 
            FieldConfig(column="name"), 
            FieldConfig(column="value")
        ],
        table=Mock(table="sink_table", dbschema="public", alias="snk"),
        filters=[],
        joins=[]
    )
    return adapter

@pytest.fixture
def mock_reconciliation_config():
    config = Mock(spec=ReconciliationConfig)
    config.partition_column_type = "int"
    config.strategy = MD5_SUM_HASH
    config.source_state_pfield = Mock(
        hash_column="hash_col",
        partition_column="id",
        order_column="id"
    )
    config.sink_state_pfield = Mock(
        hash_column="hash_col",
        partition_column="id",
        order_column="id"
    )
    config.source_pfield = config.source_state_pfield
    config.sink_pfield = config.sink_state_pfield
    config.filters = []
    config.joins = []
    return config

def test_calculate_blocks_matching_data(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 50
    intervals=[50,10,3,1]
    
    # Mock source data
    source_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 37
    }]
    mock_source_adapter.fetch.return_value = source_data
    
    # Mock sink data with matching hash and row count
    sink_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 37
    }]
    mock_sink_adapter.fetch.return_value = sink_data
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 1
    assert len(statuses) == 1
    assert statuses[0] == 'N'  # N indicates matching blocks
    assert isinstance(blocks[0], Block)
    assert blocks[0].num_rows == 37
    assert blocks[0].hash == "abc123"
    assert blocks[0].start==13
    assert blocks[0].end==50

def test_calculate_blocks_mismatched_data(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 50
    intervals=[50,10,3,1]
    
    # Mock source data
    source_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 37
    }]
    mock_source_adapter.fetch.return_value = source_data
    
    # Mock sink data with different hash
    sink_data = [{
        "blockname": "0",
        "blockhash": "def456",
        "row_count": 37
    }]
    mock_sink_adapter.fetch.return_value = sink_data
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 1
    assert len(statuses) == 1
    assert statuses[0] == 'M'  # M indicates mismatched blocks
    assert isinstance(blocks[0], Block)
    assert blocks[0].num_rows == 37

def test_calculate_blocks_recursive_split(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 50
    intervals=[50,10,3,1]
    
    # Mock source data for first level
    source_data_level1 = [{
        "blockname": "0",
        "blockhash": "abc124",
        "row_count": 37  # Greater than max_block_size
    }]
    
    # Mock source data for second level
    source_data_level2 = [
        {
            "blockname": "0-1",
            "blockhash": "def456",
            "row_count": 7
        },
        {
            "blockname": "0-2",
            "blockhash": "ghi788",
            "row_count": 10
        },
                {
            "blockname": "0-3",
            "blockhash": "ghi782",
            "row_count": 10
        },
                {
            "blockname": "0-4",
            "blockhash": "ghi785",
            "row_count": 10
        },
    ]
    
    mock_source_adapter.fetch.side_effect = [source_data_level1, source_data_level2]
    
    # Mock sink data
    sink_data_level1 = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 37
    }]
    
    sink_data_level2 = [
        {
            "blockname": "0-1",
            "blockhash": "def456",
            "row_count": 7
        },
        {
            "blockname": "0-2",
            "blockhash": "ghi785",
            "row_count": 10
        },
                {
            "blockname": "0-3",
            "blockhash": "ghi783",
            "row_count": 10
        },
                {
            "blockname": "0-4",
            "blockhash": "ghi785",
            "row_count": 10
        }
    ]
    
    mock_sink_adapter.fetch.side_effect = [sink_data_level1, sink_data_level2]
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=20  # Set lower than initial block size to force split
    )
    
    # Assertions
    assert len(blocks) == 4
    assert len(statuses) == 4
    assert statuses == ["N", "M", "M", "N"]
    # assert all(status == 'N' for status in statuses)  # All blocks should match
    assert all(isinstance(block, Block) for block in blocks)
    assert [block.num_rows  for block in blocks] == [7,10,10,10]

def test_calculate_blocks_missing_sink_data(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 50
    intervals=[50,10,3,1]
    
    # Mock source data
    source_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 37
    }]
    mock_source_adapter.fetch.return_value = source_data
    
    # Mock empty sink data
    mock_sink_adapter.fetch.return_value = []
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 1
    assert len(statuses) == 1
    assert statuses[0] == 'A'  # A indicates data present in source but missing in sink
    assert isinstance(blocks[0], Block)
    assert blocks[0].num_rows == 37
    assert blocks[0].hash == "abc123"

def test_calculate_blocks_no_data(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 50
    intervals=[50,10,3,1]
    
    # Mock empty source data
    mock_source_adapter.fetch.return_value = []
    
    # Mock empty sink data
    mock_sink_adapter.fetch.return_value = []
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 0
    assert len(statuses) == 0

def test_calculate_blocks_missing_source_data(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 50
    intervals=[50,10,3,1]
    
    # Mock empty source data
    mock_source_adapter.fetch.return_value = []
    
    # Mock sink data
    sink_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 37
    }]
    mock_sink_adapter.fetch.return_value = sink_data
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 1
    assert len(statuses) == 1
    assert statuses[0] == 'D'  # D indicates data present in sink but missing in source
    assert isinstance(blocks[0], Block)
    assert blocks[0].num_rows == 37
    assert blocks[0].hash == "abc123"

def test_calculate_blocks_large_data(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 57
    intervals=[50,10,3,1]
    
    # Mock source data for first level
    source_data_level1 = [
        {
            "blockname": "0",
            "blockhash": "abc124",
            "row_count": 37  # Greater than max_block_size
        },
        {
            "blockname": "1",
            "blockhash": "abc125",
            "row_count": 8  # Greater than max_block_size
        }
    ]
    
    # Mock source data for second level
    source_data_level2 = [
        {
            "blockname": "0-1",
            "blockhash": "def456",
            "row_count": 7
        },
        {
            "blockname": "0-2",
            "blockhash": "ghi788",
            "row_count": 10
        },
                {
            "blockname": "0-3",
            "blockhash": "ghi782",
            "row_count": 10
        },
                {
            "blockname": "0-4",
            "blockhash": "ghi785",
            "row_count": 10
        }
    ]
    
    mock_source_adapter.fetch.side_effect = [source_data_level1, source_data_level2]
    
    # Mock sink data
    sink_data_level1 = [
        {
            "blockname": "0",
            "blockhash": "abc123",
            "row_count": 37
        },
        {
            "blockname": "1",
            "blockhash": "abc125",
            "row_count": 8  # Greater than max_block_size
        }
    ]
    
    sink_data_level2 = [
        {
            "blockname": "0-1",
            "blockhash": "def456",
            "row_count": 7
        },
        {
            "blockname": "0-2",
            "blockhash": "ghi785",
            "row_count": 10
        },
                {
            "blockname": "0-3",
            "blockhash": "ghi783",
            "row_count": 10
        },
        {
            "blockname": "0-4",
            "blockhash": "ghi785",   
            "row_count": 9      #rowcount modified
        }
    ]
    
    mock_sink_adapter.fetch.side_effect = [sink_data_level1, sink_data_level2]
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=20  # Set lower than initial block size to force split
    )
    
    # Assertions
    assert len(blocks) == 5
    assert len(statuses) == 5
    assert statuses == ["N", "M", "M", "M", "N"]
    assert all(isinstance(block, Block) for block in blocks)
    assert [block.num_rows for block in blocks] == [7,10,10,10,8]

def test_adapter_query_args(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    """Test that adapters are called with proper start, end and level arguments in Query object"""
    # Setup test data similar to test_calculate_blocks_large_data
    start = 13
    end = 57
    intervals = [50, 10, 3, 1]
    
    # Set up mock data similar to test_calculate_blocks_large_data
    source_data_level1 = [
        {
            "blockname": "0",
            "blockhash": "abc124",
            "row_count": 37  # Greater than max_block_size
        },
        {
            "blockname": "1",
            "blockhash": "abc125",
            "row_count": 8
        }
    ]
    
    source_data_level2 = [
        {
            "blockname": "0-1",
            "blockhash": "def456",
            "row_count": 7
        },
        {
            "blockname": "0-2",
            "blockhash": "ghi788",
            "row_count": 10
        },
        {
            "blockname": "0-3",
            "blockhash": "ghi782",
            "row_count": 10
        },
        {
            "blockname": "0-4",
            "blockhash": "ghi785",
            "row_count": 10
        }
    ]
    
    mock_source_adapter.fetch.side_effect = [source_data_level1, source_data_level2]
    
    # Mock sink data
    sink_data_level1 = [
        {
            "blockname": "0",
            "blockhash": "abc123",
            "row_count": 37
        },
        {
            "blockname": "1",
            "blockhash": "abc125",
            "row_count": 8
        }
    ]
    
    sink_data_level2 = [
        {
            "blockname": "0-1",
            "blockhash": "def456",
            "row_count": 7
        },
        {
            "blockname": "0-2",
            "blockhash": "ghi785",
            "row_count": 10
        },
        {
            "blockname": "0-3",
            "blockhash": "ghi783",
            "row_count": 10
        },
        {
            "blockname": "0-4",
            "blockhash": "ghi785",
            "row_count": 9
        }
    ]
    
    mock_sink_adapter.fetch.side_effect = [sink_data_level1, sink_data_level2]
    
    # Execute function
    calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=20  # Set lower than initial block size to force split
    )
    
    # Check that adapters were called with correct args in Query objects
    # First level calls
    assert mock_source_adapter.fetch.call_count == 2
    assert mock_sink_adapter.fetch.call_count == 2
    
    # Verify first level calls (level 1)
    source_query_level1 = mock_source_adapter.fetch.call_args_list[0][0][0]
    sink_query_level1 = mock_sink_adapter.fetch.call_args_list[0][0][0]
    
    # Check level in metadata
    assert source_query_level1.select[2].metadata.level == 1
    assert sink_query_level1.select[2].metadata.level == 1
    
    # Check start and end in filters
    assert source_query_level1.filters[0].value == start
    assert source_query_level1.filters[1].value == end
    assert sink_query_level1.filters[0].value == start
    assert sink_query_level1.filters[1].value == end
    
    # Verify second level calls (level 2)
    source_query_level2 = mock_source_adapter.fetch.call_args_list[1][0][0]
    sink_query_level2 = mock_sink_adapter.fetch.call_args_list[1][0][0]
    
    # Check level in metadata
    assert source_query_level2.select[2].metadata.level == 2
    assert sink_query_level2.select[2].metadata.level == 2
    
    # We can't check exact start/end for level 2 as they depend on implementation details,
    # but we can verify they are different from level 1
    # and that both adapters receive the same values
    level2_start = source_query_level2.filters[0].value
    level2_end = source_query_level2.filters[1].value
    assert level2_start == 13 and level2_end ==50  # Different from level 1
    assert sink_query_level2.filters[0].value == level2_start
    assert sink_query_level2.filters[1].value == level2_end

def test_calculate_blocks_partition_column_int(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start = 13
    end = 57
    intervals=[50,10,3,1]

    # Mock source data
    source_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 50
    }]
    mock_source_adapter.fetch.return_value = source_data
    
    # Mock sink data with matching hash and row count
    sink_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 50
    }]
    mock_sink_adapter.fetch.return_value = sink_data
    
    # Update mock reconciliation config for int partition column
    mock_reconciliation_config.partition_column_type = "int"
    mock_reconciliation_config.source_pfield.partition_column = "id"
    mock_reconciliation_config.sink_pfield.partition_column = "id"
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start,
        end=end,
        level=1,
        r_config=mock_reconciliation_config,
        intervals=intervals,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 1
    assert len(statuses) == 1
    assert statuses[0] == 'N'  # N indicates matching blocks
    assert isinstance(blocks[0], Block)
    assert blocks[0].num_rows == 50
    assert blocks[0].hash == "abc123"

def xtest_calculate_blocks_partition_column_str(mock_source_adapter, mock_sink_adapter, mock_reconciliation_config):
    # Setup test data
    start_time = "A"
    end_time = "Z"
    
    # Mock source data
    source_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 50
    }]
    mock_source_adapter.fetch.return_value = source_data
    
    # Mock sink data with matching hash and row count
    sink_data = [{
        "blockname": "0",
        "blockhash": "abc123",
        "row_count": 50
    }]
    mock_sink_adapter.fetch.return_value = sink_data
    
    # Update mock reconciliation config for str partition column
    mock_reconciliation_config.partition_column_type = "str"
    mock_reconciliation_config.source_pfield.partition_column = "name"
    mock_reconciliation_config.sink_pfield.partition_column = "name"
    
    # Execute function
    blocks, statuses = calculate_blocks(
        sourcestate=mock_source_adapter,
        sinkstate=mock_sink_adapter,
        start=start_time,
        end=end_time,
        level=0,
        r_config=mock_reconciliation_config,
        interval_factor=10,
        interval_reduction_factor=10,
        max_block_size=100
    )
    
    # Assertions
    assert len(blocks) == 1
    assert len(statuses) == 1
    assert statuses[0] == 'N'  # N indicates matching blocks
    assert isinstance(blocks[0], Block)
    assert blocks[0].num_rows == 50
    assert blocks[0].hash == "abc123"

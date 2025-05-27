# SyncTool Prepare Data Blocks Test

This directory contains tests for the `prepare_data_blocks` method in the SyncTool reconciliation engine.

## Test Setup

### Sample Data Structure

The tests use a PostgreSQL database with real sample data to thoroughly test the `prepare_data_blocks` functionality. The data is structured to cover all possible reconciliation scenarios:

1. **Matching Data (IDs 1-10000)**
   - Data exists in both source and sink tables with identical values
   - All blocks should have 'N' (matching) status

2. **Mismatched Data (IDs 10001-20000)**
   - Data exists in both source and sink tables but with different values
   - Source uses `(id % 19)` while sink uses `(id % 23)` for value calculation
   - All blocks should have 'M' (mismatched) status

3. **Source-Only Data (IDs 20001-30000)**
   - Data exists only in the source table
   - All blocks should have 'A' (added) status

4. **Sink-Only Data (IDs 30001-40000)**
   - Data exists only in the sink table
   - All blocks should have 'D' (deleted) status

### Tables Schema

Both source and sink tables have identical schemas:

```sql
CREATE TABLE source_table/sink_table (
    id INT PRIMARY KEY,                  -- Integer partition column
    name VARCHAR(100),                   -- String field
    value DECIMAL(10, 2),                -- Decimal field
    hash_value VARCHAR(32),              -- MD5 hash field
    created_at TIMESTAMP NOT NULL        -- Datetime partition column
);
```

## Running the Tests

1. Start the PostgreSQL Docker container:
   ```
   make start
   ```

2. Run the prepare_data_blocks tests:
   ```
   make test-prepare-data-blocks
   ```

3. Cleanup after testing:
   ```
   make clean
   ```

## Test Coverage

The tests cover:

- Integer partition column testing (extensive cases)
- Datetime partition column testing
- Different block sizes
- Block merging
- Custom interval settings
- Force update scenarios

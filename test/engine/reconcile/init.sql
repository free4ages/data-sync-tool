-- Create source table with integer partition column
CREATE TABLE source_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    value DECIMAL(10, 2),
    hash_value VARCHAR(32),
    created_at TIMESTAMP NOT NULL
);

-- Create sink table with same schema
CREATE TABLE sink_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    value DECIMAL(10, 2),
    hash_value VARCHAR(32),
    created_at TIMESTAMP NOT NULL
);

-- Insert data into source table
-- Range 1-10000: All matching in sink
INSERT INTO source_table
SELECT 
    id,
    'Item ' || id AS name,
    (id % 100)::decimal + ((id % 17) / 10.0)::decimal AS value,
    md5('Item ' || id || '-' || ((id % 100) + ((id % 17) / 10.0))) AS hash_value,
    TIMESTAMP '2023-01-01 00:00:00' + (id * INTERVAL '1 minute')
FROM generate_series(1, 10000) AS id;

-- Range 10001-20000: Mismatched data (different values)
INSERT INTO source_table
SELECT 
    id,
    'Item ' || id AS name,
    (id % 100)::decimal + ((id % 19) / 10.0)::decimal AS value,
    md5('Item ' || id || '-' || ((id % 100) + ((id % 19) / 10.0))) AS hash_value,
    TIMESTAMP '2023-01-07 23:40:00' + (id * INTERVAL '1 minute')
FROM generate_series(10001, 20000) AS id;

-- Range 20001-30000: Data in source but not in sink
INSERT INTO source_table
SELECT 
    id,
    'Item ' || id AS name,
    (id % 100)::decimal + ((id % 13) / 10.0)::decimal AS value,
    md5('Item ' || id || '-' || ((id % 100) + ((id % 13) / 10.0))) AS hash_value,
    TIMESTAMP '2023-01-14 23:20:00' + (id * INTERVAL '1 minute')
FROM generate_series(20001, 30000) AS id;

-- Range 1-10000: All matching in source
INSERT INTO sink_table
SELECT 
    id,
    'Item ' || id AS name,
    (id % 100)::decimal + ((id % 17) / 10.0)::decimal AS value,
    md5('Item ' || id || '-' || ((id % 100) + ((id % 17) / 10.0))) AS hash_value,
    TIMESTAMP '2023-01-01 00:00:00' + (id * INTERVAL '1 minute')
FROM generate_series(1, 10000) AS id;

-- Range 10001-20000: Mismatched data (different values)
INSERT INTO sink_table
SELECT 
    id,
    'Item ' || id AS name,
    (id % 100)::decimal + ((id % 23) / 10.0)::decimal AS value,  -- Different calculation
    md5('Item ' || id || '-' || ((id % 100) + ((id % 23) / 10.0))) AS hash_value,
    TIMESTAMP '2023-01-07 23:40:00' + (id * INTERVAL '1 minute')
FROM generate_series(10001, 20000) AS id;

-- Range 30001-40000: Data in sink but not in source
INSERT INTO sink_table
SELECT 
    id,
    'Item ' || id AS name,
    (id % 100)::decimal + ((id % 11) / 10.0)::decimal AS value,
    md5('Item ' || id || '-' || ((id % 100) + ((id % 11) / 10.0))) AS hash_value,
    TIMESTAMP '2023-01-21 23:00:00' + (id * INTERVAL '1 minute')
FROM generate_series(30001, 40000) AS id;

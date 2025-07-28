# DataSyncTool (Work In progress)

DataSyncTool is a powerful and flexible data synchronization tool designed to efficiently move and transform data across various database systems and external services.

## Overview

DataSyncTool enables seamless data pipelines between different datastores with support for complex transformations, reconciliation strategies, and data enrichment. It's built to handle various data synchronization scenarios from simple table-to-table transfers to complex ETL workflows with joins, filters, and external data enrichment.

## Features

- **Multiple Database Support**: Connect to PostgreSQL, MySQL, ClickHouse, and more
- **External Service Integration**: Enrich data with Redis, HTTP APIs, and webhooks
- **NATS Messaging**: Support for NATS pub/sub messaging system
- **Flexible Data Pipelines**: Define complex data movement with joins, filters, and transformations
- **Advanced Reconciliation**: Multiple strategies for data reconciliation including hash-based and timestamp-based approaches
- **Data Enrichment**: Enhance your data with external sources during synchronization
- **Templating**: Use Jinja templates and Python lambdas for dynamic data transformation
- **Batch Processing**: Configure batch sizes for optimal performance

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/synctool.git
cd synctool

# Install dependencies
pip install -r requirements.txt
```

## Configuration

DataSyncTool uses YAML configuration files to define datastores and pipelines. Below is an overview of the configuration structure:

### Datastores

Define your data sources and destinations:

```yaml
datastores:
  - name: db1
    type: postgres
    host: localhost
    port: 5432
    username: postgres
    password: postgres
    database: postgres

  - name: db2
    type: mysql
    host: localhost
    port: 3306
    username: mysql
    password: mysql
    database: mysqldb
```

Supported datastore types:
- `postgres`: PostgreSQL databases
- `mysql`: MySQL databases
- `clickhouse`: ClickHouse databases
- `webhook`: HTTP webhook endpoints
- `nats`: NATS messaging system

### External Stores

Define external data sources for enrichment:

```yaml
externalstores:
  - name: redis1
    type: redis
    host: redis.host.local
    port: 6379
    db: 1
  - name: http1
    type: http
    url: "http://myurl.com"
```

### Pipelines

Define your data synchronization pipelines:

```yaml
pipelines:
  - name: data_move_db1_users_to_db2_users
    source:
      datastore: db1
      batch_size: 100
      table:
        table: users
        alias: u
        dbschema: public
      # Additional source configuration...
    
    sink:
      datastore: db2
      table: users
      dbschema: 'myschema'
      # Additional sink configuration...
    
    # Optional reconciliation, enrichment, etc.
```

## Pipeline Components

### Source Configuration

```yaml
source:
  datastore: db1  # Reference to a defined datastore
  batch_size: 100  # Number of records to process in each batch
  table:
    table: users  # Source table name
    alias: u      # Optional table alias
    dbschema: public  # Optional schema name
  joins:  # Optional table joins
    - table: orders
      alias: o
      type: left
      on: "users.id = o.user_id"
  filters:  # Optional filtering conditions
    - column: 'u.active'
      operator: '='
      value: TRUE
  fields:  # Optional field selection and type definition
    - column: 'u.name'
      dtype: 'str'
      alias: 'name'
```

### Sink Configuration

```yaml
sink:
  datastore: db2  # Reference to a defined datastore
  table: users    # Destination table
  dbschema: 'myschema'  # Optional schema
  batch_size: 100  # Batch size for inserts/updates
  merge_strategy:
    strategy: delete_insert  # Strategy for handling existing data
    allow_delete: true
  unique_key:  # Columns that uniquely identify records
    - sname
    - sid
  fields:  # Field mapping from source to destination
    - column: 'sname'  # Destination column
      dtype: 'str'     # Data type
      source_column: 'u.name'  # Source column
```

### Reconciliation Strategies

```yaml
reconciliation:
  - name: full_sync
    strategy: md5sum_hash
    partition_column: created_at
    partition_column_type: datetime
    start: 'lambda: datetime.datetime(2020,2,1)'
    end: 'lambda: datetime.datetime.now()'
    initial_partition_interval: 1*365*24*60*60
```

### Data Enrichment

```yaml
enrichment:
  - externalstore: redis1
    name: redis1
    type: redis
    key_template: "user:{{ user_id }}"
    output: 'lambda v: {redis_name":v}'
  - externalstore: http1
    name: http1
    type: http
    path: "TMPL(/creditscore?user_id={{user_id}})"
```

## Merge Strategies

DataSyncTool supports several merge strategies for handling data in the destination:

- `delete_insert`: Delete matching records before inserting
- `upsert`: Insert new records or update existing ones
- `collapse`: Add nullify row before insert

## Advanced Features

### Templating

Use Jinja templates for dynamic content:

```yaml
source_column: 'TMPL({{ name }} - ${{ o__total }})'
```

### Lambda Functions

Use Python lambda functions for custom transformations:

```yaml
source_column: 'lambda r: 1 if r["o__total"]>1000 else 0'
```

## Usage

Run a synchronization pipeline:

```bash
python main.py --config config.yaml --pipeline data_move_db1_users_to_db2_users
```

## Development

### Testing

```bash
# Run tests
pytest

# Run specific test
pytest test/engine/reconcile/test_calculate_blocks.py
```

## License

[Include your license information here]

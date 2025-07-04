# APData - Advanced Database Cloning Tool

A high-performance CLI tool for cloning data between MySQL and DynamoDB databases with advanced filtering, parallel processing, and optimized data transfer strategies.

## Features

### MySQL Improvements

- **Schema/Data Separation**: Export schema and data separately for better control
- **Foreign Key Handling**: Automatically manages FK constraints during import
- **Optimized Transfers**: Uses single transactions and optimized mysqldump flags
- **Filtered Cloning**: Clone specific rows using WHERE clauses with batch processing
- **Dependency Management**: Smart table ordering to handle foreign key relationships
- **Prefix-Based Cloning**: Automatically clone all tables matching client.environment pattern

### DynamoDB Improvements

- **Interactive Table Selection**: Checkbox interface for selecting specific tables when using component-based cloning
- **Filtered Scanning**: Use FilterExpressions to clone only specific items
- **Parallel Processing**: Configurable concurrency with parallel scan segments
- **Batch Operations**: Efficient batch writes with automatic retry logic
- **Memory Efficient**: Stream processing to handle large tables without memory issues
- **Structure Cloning**: Automatically recreate table schemas with GSI/LSI support
- **Prefix-Based Cloning**: Automatically discover and clone all tables matching client.environment pattern

## Installation

```bash
go build -o apdata
```

## Prerequisites

### AWS Configuration

The tool requires AWS credentials to be configured for DynamoDB operations. Set up your credentials using one of these methods:

```bash
# Option 1: Using AWS CLI (recommended)
aws configure

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1

# Option 3: AWS credentials file
# Create ~/.aws/credentials with:
# [default]
# aws_access_key_id = your-access-key
# aws_secret_access_key = your-secret-key
```

**Required AWS Permissions for DynamoDB:**

- `dynamodb:ListTables`
- `dynamodb:DescribeTable`
- `dynamodb:CreateTable`
- `dynamodb:Scan`
- `dynamodb:BatchWriteItem`

### MySQL Requirements

MySQL operations require direct database access with standard connection credentials.

## Configuration

The configuration system uses a JSON file to store database connection details for different clients and environments.

### Configuration File Location

The config file is stored at `~/.apdata/config.json` (or `.apdata/config.json` in current directory if home directory is not accessible).

### Config Structure

The config file has two main sections:

```json
{
  "mysql": {
    "client/env": {
      "Host": "hostname",
      "Port": 3306,
      "User": "username",
      "Password": "password",
      "Database": "database_name"
    }
  },
  "dynamodb": {
    "client/env": {
      "Region": "us-east-1"
    }
  }
}
```

### Connection String Format

You reference configurations using the format `client/env`:

- `client` = Your client/company name
- `env` = Environment (prod, staging, local, etc.)

Examples:

- `acme/prod` = Acme client's production environment
- `acme/staging` = Acme client's staging environment
- `acme/local` = Acme client's local environment

### Default Config

When you first run the tool, it creates a default config:

```json
{
  "mysql": {
    "example/local": {
      "Host": "localhost",
      "Port": 3306,
      "User": "root",
      "Password": "password",
      "Database": "testdb"
    }
  },
  "dynamodb": {
    "example/local": {
      "Region": "us-east-1"
    }
  }
}
```

**Note for DynamoDB**: The configuration only requires the AWS region. Table names are discovered automatically using prefix-based discovery, and the tool connects to AWS DynamoDB (not local instances).

### Config Management

The config system:

1. **Auto-creates** the config file with examples on first run
2. **Validates** connection strings match the `client/env` format
3. **Looks up** the appropriate database config based on the connection string
4. **Supports** multiple clients and environments in the same config file

### Security Note

The config file is created with `0600` permissions (readable only by the owner) since it contains database passwords.

To add your own configurations, edit `~/.apdata/config.json` and add entries following the same pattern as the examples.

## Usage

### Basic Commands

```bash
# Clone entire MySQL database (all tables with exact names)
./apdata clone mysql --source acme/dev --dest my-client/dev

# Clone DynamoDB data with prefix-based table discovery
./apdata clone dynamodb --source acme/dev --dest my-client/dev

# Clone DynamoDB data for a specific component with interactive selection
./apdata clone dynamodb --source acme/dev --dest my-client/dev --component-name my-service --interactive

# Clone both databases (MySQL: exact names, DynamoDB: prefix-based)
./apdata clone all --source acme/dev --dest my-client/dev
```

### Database Cloning Approaches

The tool uses different cloning strategies for each database type:

**MySQL**: Clones exact table names as they exist in the source database
- **Full Database Clone**: Clones all tables in the database (recreates destination database)
- **Single Table Clone**: When using `--table`, only affects that specific table (preserves other tables)
- **Filtered Clone**: When using `--table` with `--where`, clones only matching records to the specific table
- No automatic prefix manipulation - copies tables with their exact names
- Use different source/dest databases for isolation

**DynamoDB**: Uses prefix-based discovery and cloning for multi-tenant table patterns
- Finds all tables starting with `client.environment.*` and clones them to `newclient.newenvironment.*`
- Example: `acme.dev.users` → `my-client.dev.users`
- Example: `acme.dev.sessions` → `my-client.dev.sessions`

### Filtering Records

The tool supports different filtering approaches for each database type:

**MySQL Filtering** (using `--where` with `--table`):
```bash
# SQL-style WHERE clauses (requires specific table)
# Note: --where can only be used with --table for single table filtering
./apdata clone mysql --source acme/dev --dest my-client/dev --table users --where "created_at > '2024-01-01'"
./apdata clone mysql --source acme/dev --dest my-client/dev --table orders --where "status = 'active' AND amount > 100"
```

**Important**: The `--where` flag:
- Can only be used with MySQL (not DynamoDB)  
- Requires `--table` to specify which table to filter
- Replaces all data in the destination table with filtered results from source
- Preserves other tables in the destination database unchanged

**DynamoDB Filtering** (using `--filter`):
```bash
# DynamoDB FilterExpression syntax with automatic value binding
./apdata clone dynamodb --source acme/dev --dest my-client/dev --filter "DocumentType = 'CatalogStyle'"
./apdata clone dynamodb --source acme/dev --dest my-client/dev --filter "attribute_exists(active) AND Price > 50"
```

### Component-Based Cloning with Interactive Selection

For more granular control, you can specify a component name and use interactive selection:

```bash
# Interactive checkbox selection for a specific component
./apdata clone dynamodb --source acme/dev --dest my-client/dev --component-name my-component --interactive
```

This will:

1. **Discover** all tables matching `client.environment.component-name.*`
2. **Display** a checkbox interface with arrow key navigation
3. **Allow selection** of specific tables using space bar
4. **Confirm** your selection before proceeding
5. **Clone** only the selected tables with high-performance optimization

**Interactive Interface:**

```
📋 Found 5 table(s) matching your component criteria.
Use ↑/↓ to navigate, SPACE to select/deselect, ENTER to confirm

? Select tables to clone: [Use arrows to move, space to select, <enter> to submit]
❯ ⬜ my-client.dev.my-component.users
  ⬜ my-client.dev.my-component.sessions
  ✅ my-client.dev.my-component.logs
  ⬜ my-client.dev.my-component.metrics
  ⬜ my-client.dev.my-component.cache
```

### Advanced MySQL Options

```bash
# Clone schema only
./apdata clone mysql --source acme/prod --dest acme/local --schema-only

# Clone data only (skip schema)
./apdata clone mysql --source acme/prod --dest acme/local --data-only

# Clone specific table
./apdata clone mysql --source acme/prod --dest acme/local --table users

# Clone with WHERE clause filter
./apdata clone mysql --source acme/prod --dest acme/local --table users --where "created_at > '2024-01-01'"

# Schema export uses parallel processing by default
./apdata clone mysql --source acme/prod --dest acme/local
```

### Advanced DynamoDB Options

```bash
# Interactive table selection with checkbox interface
./apdata clone dynamodb --source acme/prod --dest acme/local --component-name my-service --interactive

# Clone with filter expression
./apdata clone dynamodb --source acme/prod --dest acme/local --filter "attribute_exists(active)"

# Custom concurrency (default: 25)
./apdata clone dynamodb --source acme/prod --dest acme/local --concurrency 50

# Clone specific table (bypasses prefix-based discovery)
./apdata clone dynamodb --source acme/prod --dest acme/local --table my-table
```

### Global Options

```bash
# Enable verbose logging
./apdata clone mysql --source acme/prod --dest acme/local --verbose

# Get help
./apdata clone --help
```

## Command Reference

### Required Flags

- `--source`: Source connection string in format `client/env`
- `--dest`: Destination connection string in format `client/env`

### Optional Flags

#### MySQL-Specific Flags
- `--table`: Specific table name to clone 
- `--where`: WHERE clause for filtering records (requires `--table`, e.g., `"created_at > '2024-01-01'"`)
- `--schema-only`: Clone schema only
- `--data-only`: Clone data only

#### DynamoDB-Specific Flags  
- `--component-name`: Component name for prefix-based cloning (e.g., `connectors-data-api`)
- `--interactive`: Enable interactive checkbox selection when using --component-name
- `--filter`: Filter expression for record filtering (e.g., `"DocumentType = 'CatalogStyle'"`)
- `--concurrency`: Number of concurrent workers (default: 25)

#### Global Flags
- `--verbose`: Enable verbose logging

## Examples

### MySQL Examples

```bash
# Full database clone
./apdata clone mysql --source acme/prod --dest acme/local

# Clone specific table with filtered data (only affects that table)
./apdata clone mysql --source acme/prod --dest acme/local --table users --where "last_login > '2024-01-01'"

# Clone specific table without filter (only affects that table)
./apdata clone mysql --source acme/prod --dest acme/local --table users

# Setup new environment (schema only)
./apdata clone mysql --source acme/prod --dest acme/staging --schema-only
```

### DynamoDB Examples

```bash
# Interactive table selection for a component
./apdata clone dynamodb --source acme/prod --dest acme/local --component-name user-service --interactive

# Clone active records only (connects to AWS DynamoDB)
./apdata clone dynamodb --source acme/prod --dest acme/local --filter "attribute_exists(active)"

# Clone specific records by primary key
./apdata clone dynamodb --source acme/prod --dest acme/local --filter "DocumentType = 'CatalogStyle'"

# High-performance clone with increased concurrency
./apdata clone dynamodb --source acme/prod --dest acme/local --concurrency 50
```

### Prefix-Based Cloning Examples

```bash
# Interactive selection for component-specific tables
./apdata clone dynamodb --source acme/dev --dest my-client/dev --component-name connectors-data-api --interactive

# Clone all acme.dev.* DynamoDB tables to my-client.dev.*
./apdata clone dynamodb --source acme/dev --dest my-client/dev --verbose

# Clone all MySQL tables with exact names
./apdata clone mysql --source acme/dev --dest my-client/dev --verbose

# Clone both databases (MySQL: exact names, DynamoDB: prefix-based)
./apdata clone all --source acme/dev --dest my-client/dev --verbose

# Apply filters during prefix-based cloning
./apdata clone dynamodb --source acme/dev --dest my-client/dev --filter "attribute_exists(active)"

# Clone only specific records using primary key filter
./apdata clone dynamodb --source acme/dev --dest my-client/dev --filter "DocumentType = 'CatalogStyle'"
```

### Multi-Database Examples

```bash
# Clone everything with verbose output
./apdata clone all --source acme/prod --dest acme/local --verbose

# Clone between different clients
./apdata clone mysql --source acme/prod --dest beta/staging
```

## Testing

The project includes a comprehensive test suite covering all modules with 85+ tests.

### Running Tests

```bash
# Run all tests
go test ./...

# Run with verbose output
go test ./... -v

# Run specific package
go test ./mysql -v
go test ./config -v
go test ./internal -v

# Run specific test
go test ./config -run TestParseConnectionString
```

### Test Coverage

- **MySQL Module**: Connection handling, schema operations, error detection, performance optimization
- **DynamoDB Module**: Performance optimization, metrics tracking, parallel scanning, error handling
- **Config Module**: Connection string parsing, configuration management
- **Internal Module**: Spinner functionality, verbose mode behavior
- **Command Module**: CLI validation, error formatting
- **Integration Tests**: End-to-end component interaction

All tests work without external dependencies and include proper cleanup and state management.

## Performance Optimization

The tool includes several performance optimizations:

### MySQL Performance Features

**Standard Schema Export:**

- Uses optimized mysqldump flags (`--single-transaction`, `--quick`, `--lock-tables=false`)
- Skips unnecessary comments and uses compact output
- Includes detailed timing measurements and progress tracking

**Optimized Schema Export (Default):**

- Uses highly optimized mysqldump flags for maximum performance
- Includes tables, routines, triggers, and events in a single efficient operation
- Uses optimized import settings (`--disable-keys`, `--single-transaction`)
- Provides detailed performance breakdowns showing time spent in each phase

**Intelligent Data Cloning (Default):**

- Analyzes table sizes to choose optimal processing strategy for each table
- **Large tables (>100K rows)**: Chunked streaming with progress tracking
- **Medium tables (10K-100K rows)**: Optimized mysqldump with extended inserts
- **Small tables (<10K rows)**: Parallel processing with 3x concurrency
- Real-time progress tracking with rows/second metrics
- Memory-efficient processing to handle datasets of any size

**Performance Monitoring:**

- Logs detailed timing for each operation (recreate, export, import)
- Shows percentage breakdown of time spent in each phase
- Reports file sizes and transfer rates
- Enables easy identification of bottlenecks

### Usage Examples

```bash
# Monitor performance with verbose logging (intelligent processing enabled by default)
./apdata clone mysql --source acme/prod --dest acme/local --verbose

# Clone data only (skips schema) - uses intelligent table processing
./apdata clone mysql --source acme/prod --dest acme/local --data-only --verbose

# Clone specific large table with chunked processing
./apdata clone mysql --source acme/prod --dest acme/local --table large_logs --verbose
```

The intelligent data cloning provides significant performance improvements for:

- **Large datasets**: Chunked processing prevents memory issues
- **Mixed table sizes**: Optimal strategy per table size
- **Production databases**: Parallel processing for small tables
- **Long-running operations**: Real-time progress tracking and ETA

### DynamoDB Performance Features

**Intelligent Parallel Scanning:**

- Automatically determines optimal concurrency based on table characteristics
- Uses parallel scan segments for maximum throughput (default: 10 concurrent segments)
- Sequential scanning with filters for targeted data extraction
- Real-time progress tracking with items/second metrics

**Optimized Batch Processing:**

- Configurable batch sizes up to DynamoDB's 25-item limit for maximum efficiency
- Intelligent retry logic with exponential backoff and jitter
- Memory-efficient streaming to handle tables of any size
- Comprehensive error handling and automatic recovery

**Advanced Metrics & Monitoring:**

- **Real-time Metrics**: Items processed, throughput per second, bytes processed, error counts
- **Progress Tracking**: Visual progress indicators with percentage completion and ETA
- **Performance Analysis**: Detailed timing for scan vs write operations
- **Resource Monitoring**: Active segment count, batch write statistics, retry metrics

**Loading Indicators & User Experience:**

- **Visual Spinners**: Shows animated loading indicators for all DynamoDB operations
- **Operation-Specific Messages**: "Analyzing table size", "Cloning DynamoDB table", "Processing segments"
- **Real-time Updates**: Dynamic progress messages with current throughput and completion percentage
- **Intelligent Display**: Automatically adapts to verbose mode for optimal user experience

**Memory & Resource Optimization:**

- **Channel Buffering**: Intelligently sized channels based on concurrency and batch size
- **Atomic Operations**: Thread-safe metrics collection for concurrent operations
- **Context Cancellation**: Proper cleanup and graceful shutdown on errors or interruption
- **Resource Efficiency**: Minimal memory footprint even for million-item tables

### Usage Examples

```bash
# High-performance DynamoDB clone with verbose metrics
./apdata clone dynamodb --source acme/prod --dest acme/local --verbose

# Optimized clone with custom concurrency for large tables
./apdata clone dynamodb --source acme/prod --dest acme/local --concurrency 20 --verbose

# Clone with filter and performance monitoring
./apdata clone dynamodb --source acme/prod --dest acme/local --filter "attribute_exists(active)" --verbose
```

The DynamoDB performance optimizations provide significant improvements for:

- **Large tables**: Parallel scanning with up to 20x performance improvement
- **High-throughput scenarios**: Optimized batch processing maximizes write capacity utilization
- **Production workloads**: Intelligent defaults work well without tuning
- **Monitoring & observability**: Comprehensive metrics for performance analysis

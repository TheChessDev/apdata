# APData - Advanced Database Cloning Tool

A high-performance CLI tool for cloning data between MySQL and DynamoDB databases with advanced filtering, parallel processing, and optimized data transfer strategies.

## Features

### MySQL Improvements
- **Schema/Data Separation**: Export schema and data separately for better control
- **Foreign Key Handling**: Automatically manages FK constraints during import
- **Optimized Transfers**: Uses single transactions and optimized mysqldump flags
- **Filtered Cloning**: Clone specific rows using WHERE clauses with batch processing
- **Dependency Management**: Smart table ordering to handle foreign key relationships

### DynamoDB Improvements
- **Filtered Scanning**: Use FilterExpressions to clone only specific items
- **Parallel Processing**: Configurable concurrency with parallel scan segments
- **Batch Operations**: Efficient batch writes with automatic retry logic
- **Memory Efficient**: Stream processing to handle large tables without memory issues
- **Structure Cloning**: Automatically recreate table schemas with GSI/LSI support

## Installation

```bash
go build -o apdata
```

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
      "Region": "us-east-1",
      "TableName": "table-name",
      "Endpoint": "http://localhost:8000"
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
      "Region": "us-east-1",
      "TableName": "test-table",
      "Endpoint": "http://localhost:8000"
    }
  }
}
```

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
# Clone MySQL data
./apdata clone mysql --source acme/prod --dest acme/local

# Clone DynamoDB data
./apdata clone dynamodb --source acme/prod --dest acme/local

# Clone both MySQL and DynamoDB
./apdata clone all --source acme/prod --dest acme/local
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
```

### Advanced DynamoDB Options

```bash
# Clone with filter expression
./apdata clone dynamodb --source acme/prod --dest acme/local --filter "attribute_exists(active)"

# Custom concurrency (default: 10)
./apdata clone dynamodb --source acme/prod --dest acme/local --concurrency 20

# Clone specific table
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
- `--table`: Specific table name to clone
- `--filter`: DynamoDB filter expression
- `--where`: MySQL WHERE clause
- `--schema-only`: Clone schema only (MySQL)
- `--data-only`: Clone data only (MySQL)
- `--concurrency`: Number of concurrent workers for DynamoDB (default: 10)
- `--verbose`: Enable verbose logging

## Examples

### MySQL Examples

```bash
# Full database clone
./apdata clone mysql --source acme/prod --dest acme/local

# Clone recent user data
./apdata clone mysql --source acme/prod --dest acme/local --table users --where "last_login > '2024-01-01'"

# Setup new environment (schema only)
./apdata clone mysql --source acme/prod --dest acme/staging --schema-only
```

### DynamoDB Examples

```bash
# Clone active records only
./apdata clone dynamodb --source acme/prod --dest acme/local --filter "attribute_exists(active) AND active = :true"

# High-performance clone with increased concurrency
./apdata clone dynamodb --source acme/prod --dest acme/local --concurrency 50

# Clone specific table
./apdata clone dynamodb --source acme/prod --dest acme/local --table user-sessions
```

### Multi-Database Examples

```bash
# Clone everything with verbose output
./apdata clone all --source acme/prod --dest acme/local --verbose

# Clone between different clients
./apdata clone mysql --source acme/prod --dest beta/staging
```
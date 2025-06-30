package mysql

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"apdata/internal"
	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

type Cloner struct {
	Source Config
	Dest   Config
}

func NewCloner(source, dest Config) *Cloner {
	return &Cloner{
		Source: source,
		Dest:   dest,
	}
}

func (c *Cloner) CloneSchema() error {
	totalStart := time.Now()
	internal.Logger.Info("Starting schema clone", "source", c.Source.Database, "dest", c.Dest.Database)

	// Always recreate database first to ensure clean import
	recreateStart := time.Now()
	if err := c.recreateDatabase(); err != nil {
		return fmt.Errorf("failed to recreate database: %w", err)
	}
	recreateDuration := time.Since(recreateStart)
	internal.Logger.Info("Database recreation completed", "duration", recreateDuration)

	// Prepare optimized mysqldump command
	exportStart := time.Now()
	args := []string{
		"--no-data",                 // Schema only
		"--skip-add-drop-table",     // Don't add DROP TABLE statements  
		"--skip-disable-keys",       // Don't disable keys
		"--routines",                // Include stored procedures/functions
		"--triggers",                // Include triggers
		"--events",                  // Include events
		"--single-transaction",      // Performance: Consistent snapshot
		"--quick",                   // Performance: Retrieve rows one at a time
		"--lock-tables=false",       // Performance: Don't lock tables
		"--skip-lock-tables",        // Avoid FLUSH TABLES WITH READ LOCK
		"--no-tablespaces",          // Avoid tablespace operations
		"--skip-comments",           // Performance: Skip comment generation
		"--compact",                 // Performance: Less verbose output
		"--default-character-set=utf8mb4", // Explicit charset
		"--set-gtid-purged=OFF",     // RDS compatibility: avoid GTID issues
		"--column-statistics=0",     // RDS compatibility: disable column statistics
		fmt.Sprintf("--host=%s", c.Source.Host),
		fmt.Sprintf("--port=%d", c.Source.Port),
		fmt.Sprintf("--user=%s", c.Source.User),
	}

	if c.Source.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Source.Password))
	}

	args = append(args, c.Source.Database)
	cmd := exec.Command("mysqldump", args...)

	internal.Logger.Debug("Exporting MySQL schema", "database", c.Source.Database, "host", c.Source.Host, "args", strings.Join(args, " "))

	schemaFile := fmt.Sprintf("%s_schema.sql", c.Source.Database)

	var exportError error
	err := internal.SimpleSpinner(fmt.Sprintf("Exporting schema from %s", c.Source.Database), func() error {
		file, err := os.Create(schemaFile)
		if err != nil {
			return fmt.Errorf("failed to create schema file: %w", err)
		}
		defer file.Close()

		// Capture stderr for better error reporting
		var stderr strings.Builder
		cmd.Stdout = file
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			stderrOutput := stderr.String()
			internal.Logger.Debug("Export error details", "stderr", stderrOutput, "error", err)
			
			// Store the error for later evaluation
			exportError = fmt.Errorf("mysqldump failed: %w (stderr: %s)", err, stderrOutput)
			return exportError
		}

		return nil
	})

	exportDuration := time.Since(exportStart)
	internal.Logger.Debug("Schema export completed", "duration", exportDuration, "file", schemaFile)

	// Check if export failed but we still got a usable schema file (common with RDS privilege issues)
	if err != nil {
		if fileInfo, statErr := os.Stat(schemaFile); statErr == nil && fileInfo.Size() > 0 {
			if exportError != nil && strings.Contains(exportError.Error(), "FLUSH") && strings.Contains(exportError.Error(), "Access denied") {
				internal.Logger.Warn("MySQL privilege warning (common with RDS)", "warning", "FLUSH TABLES permission not available, but schema export succeeded")
				internal.Logger.Info("Schema file was created despite privilege warning", "size_bytes", fileInfo.Size())
				
				// Let's check the content of the schema file to see if it has actual schema
				if content, readErr := os.ReadFile(schemaFile); readErr == nil {
					contentStr := string(content)
					if strings.Contains(contentStr, "CREATE TABLE") || strings.Contains(contentStr, "CREATE PROCEDURE") || strings.Contains(contentStr, "CREATE FUNCTION") {
						internal.Logger.Info("Schema file contains valid database objects, continuing with import")
						// Continue with import despite the privilege warning
					} else {
						previewLen := 200
						if len(contentStr) < previewLen {
							previewLen = len(contentStr)
						}
						internal.Logger.Error("Schema file does not contain valid database objects", "content_preview", contentStr[:previewLen])
						
						// Try a fallback approach with minimal flags for RDS
						internal.Logger.Info("Attempting fallback export with minimal flags for RDS compatibility")
						return c.tryFallbackSchemaExport(schemaFile)
					}
				}
			} else {
				return fmt.Errorf("failed to export schema: %w", err)
			}
		} else {
			return fmt.Errorf("failed to export schema: %w", err)
		}
	}

	// Get file size for performance metrics
	if fileInfo, err := os.Stat(schemaFile); err == nil {
		internal.Logger.Info("Schema file created", "size_bytes", fileInfo.Size(), "size_mb", float64(fileInfo.Size())/1024/1024)
	}

	internal.Logger.Debug("Importing schema to destination", "host", c.Dest.Host, "database", c.Dest.Database)

	importStart := time.Now()
	err = internal.SimpleSpinner(fmt.Sprintf("Importing schema to %s", c.Dest.Database), func() error {
		return c.importSQL(schemaFile)
	})
	importDuration := time.Since(importStart)
	internal.Logger.Debug("Schema import completed", "duration", importDuration)

	// Clean up schema file
	os.Remove(schemaFile)

	totalDuration := time.Since(totalStart)
	internal.Logger.Debug("Schema clone completed", "total_duration", totalDuration,
		"recreate_pct", fmt.Sprintf("%.1f%%", float64(recreateDuration.Nanoseconds())/float64(totalDuration.Nanoseconds())*100),
		"export_pct", fmt.Sprintf("%.1f%%", float64(exportDuration.Nanoseconds())/float64(totalDuration.Nanoseconds())*100),
		"import_pct", fmt.Sprintf("%.1f%%", float64(importDuration.Nanoseconds())/float64(totalDuration.Nanoseconds())*100))

	// Finish the schema cloning operation line
	internal.FinishLine()

	return err
}

func (c *Cloner) tryFallbackSchemaExport(schemaFile string) error {
	internal.Logger.Info("Trying fallback schema export with basic flags")
	
	// Use minimal flags that should work with most RDS configurations
	args := []string{
		"--no-data",                 // Schema only
		"--routines=false",          // Skip routines to avoid permission issues
		"--triggers=false",          // Skip triggers to avoid permission issues
		"--events=false",            // Skip events to avoid permission issues
		"--single-transaction",      // Basic consistency
		"--lock-tables=false",       // Don't lock tables
		"--set-gtid-purged=OFF",     // RDS compatibility
		"--column-statistics=0",     // RDS compatibility
		fmt.Sprintf("--host=%s", c.Source.Host),
		fmt.Sprintf("--port=%d", c.Source.Port),
		fmt.Sprintf("--user=%s", c.Source.User),
	}

	if c.Source.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Source.Password))
	}

	args = append(args, c.Source.Database)
	cmd := exec.Command("mysqldump", args...)

	internal.Logger.Debug("Fallback mysqldump command", "args", strings.Join(args, " "))

	file, err := os.Create(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to create fallback schema file: %w", err)
	}
	defer file.Close()

	var stderr strings.Builder
	cmd.Stdout = file
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		stderrOutput := stderr.String()
		internal.Logger.Error("Fallback export also failed", "stderr", stderrOutput, "error", err)
		return fmt.Errorf("fallback mysqldump also failed: %w (stderr: %s)", err, stderrOutput)
	}

	// Check if fallback created useful content
	if content, readErr := os.ReadFile(schemaFile); readErr == nil {
		contentStr := string(content)
		if strings.Contains(contentStr, "CREATE TABLE") {
			internal.Logger.Info("Fallback export successful - found tables in schema")
			return nil
		} else {
			previewLen := min(int64(len(contentStr)), 200)
			internal.Logger.Error("Fallback export failed - no tables found", "content_preview", contentStr[:previewLen])
			return fmt.Errorf("fallback export failed - no valid tables in schema file")
		}
	}

	return fmt.Errorf("fallback export failed - could not read schema file")
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (c *Cloner) CloneData(tables []string) error {
	totalStart := time.Now()
	internal.Logger.Debug("Starting data clone operation")

	if len(tables) == 0 {
		var err error
		err = internal.SimpleSpinner("Discovering database tables", func() error {
			tables, err = c.getAllTables()
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to get table list: %w", err)
		}
		internal.Logger.Debug("Found tables to clone", "count", len(tables), "tables", tables)
	}

	// Get table sizes for intelligent processing
	var tableSizes map[string]int64
	err := internal.SimpleSpinner("Analyzing table sizes", func() error {
		var err error
		tableSizes, err = c.getTableSizes(tables)
		return err
	})
	if err != nil {
		internal.Logger.Warn("Could not get table sizes, using default processing", "error", err)
		tableSizes = make(map[string]int64)
	}

	// Categorize tables by size for optimal processing strategy
	largeTables, mediumTables, smallTables := c.categorizeTablesBySize(tables, tableSizes)
	
	internal.Logger.Debug("Table size analysis", 
		"large_tables", len(largeTables),
		"medium_tables", len(mediumTables), 
		"small_tables", len(smallTables),
		"total_tables", len(tables))

	var totalRows int64
	var processedRows int64

	// Estimate total work for progress tracking
	for _, size := range tableSizes {
		totalRows += size
	}
	internal.Logger.Debug("Estimated total rows to clone", "rows", totalRows)

	// Process large tables individually with streaming
	for i, table := range largeTables {
		err := internal.SimpleSpinner(fmt.Sprintf("Cloning large table %s (%d/%d)", table, i+1, len(largeTables)), func() error {
			return c.cloneTableWithStreaming(table, tableSizes[table], &processedRows, totalRows)
		})
		if err != nil {
			return fmt.Errorf("failed to clone large table %s: %w", table, err)
		}
	}

	// Process medium tables with optimized mysqldump
	for i, table := range mediumTables {
		err := internal.SimpleSpinner(fmt.Sprintf("Cloning medium table %s (%d/%d)", table, i+1, len(mediumTables)), func() error {
			return c.cloneTableOptimized(table, tableSizes[table], &processedRows, totalRows)
		})
		if err != nil {
			return fmt.Errorf("failed to clone medium table %s: %w", table, err)
		}
	}

	// Process small tables in parallel batches
	if len(smallTables) > 0 {
		smallTablesErr := internal.SimpleSpinner(fmt.Sprintf("Cloning %d small tables in parallel", len(smallTables)), func() error {
			return c.cloneSmallTablesParallel(smallTables, tableSizes, &processedRows, totalRows)
		})
		if smallTablesErr != nil {
			return fmt.Errorf("failed to clone small tables: %w", smallTablesErr)
		}
	}

	totalDuration := time.Since(totalStart)
	internal.Logger.Debug("Data clone completed", 
		"total_duration", totalDuration,
		"total_rows_processed", processedRows,
		"avg_rows_per_sec", float64(processedRows)/totalDuration.Seconds(),
		"tables_processed", len(tables))

	// Finish the data cloning operation line
	internal.FinishLine()

	return nil
}


func (c *Cloner) CloneWithFilter(table, whereClause string) error {
	internal.Logger.Debug("Starting filtered clone", "table", table, "where", whereClause)

	sourceDB, err := c.connectSource()
	if err != nil {
		return err
	}
	defer sourceDB.Close()

	destDB, err := c.connectDest()
	if err != nil {
		return err
	}
	defer destDB.Close()

	columns, err := c.getTableColumns(sourceDB, table)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	internal.Logger.Debug("Executing query", "query", query)

	var spinner *internal.Spinner
	if !internal.VerboseMode {
		spinner = internal.NewSpinner(fmt.Sprintf("Querying table %s", table))
		spinner.Start()
	}

	rows, err := sourceDB.Query(query)
	if err != nil {
		if spinner != nil {
			spinner.Error(fmt.Sprintf("Failed to query table %s", table))
		}
		return fmt.Errorf("failed to query source: %w", err)
	}
	defer rows.Close()

	if spinner != nil {
		spinner.UpdateMessage(fmt.Sprintf("Processing rows from table %s", table))
	}

	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1]
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(columns, ", "), placeholders)

	if _, err := destDB.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("failed to disable FK checks: %w", err)
	}
	defer destDB.Exec("SET FOREIGN_KEY_CHECKS=1")

	stmt, err := destDB.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	batchSize := 1000
	batch := make([][]interface{}, 0, batchSize)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch = append(batch, values)

		if len(batch) >= batchSize {
			if err := c.insertBatch(stmt, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		internal.Logger.Debug("Inserting final batch", "table", table, "batchSize", len(batch))
		if err := c.insertBatch(stmt, batch); err != nil {
			if spinner != nil {
				spinner.Error(fmt.Sprintf("Failed to insert final batch for %s", table))
			}
			return err
		}
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("Filtered clone completed: %s", table))
	}
	return rows.Err()
}

func (c *Cloner) connectSource() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		c.Source.User, c.Source.Password, c.Source.Host, c.Source.Port, c.Source.Database)
	return sql.Open("mysql", dsn)
}

func (c *Cloner) connectDest() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		c.Dest.User, c.Dest.Password, c.Dest.Host, c.Dest.Port, c.Dest.Database)
	return sql.Open("mysql", dsn)
}

func (c *Cloner) importSQL(filename string) error {
	// Performance optimized mysql import
	args := []string{
		fmt.Sprintf("--host=%s", c.Dest.Host),
		fmt.Sprintf("--port=%d", c.Dest.Port),
		fmt.Sprintf("--user=%s", c.Dest.User),
		"--force",                    // Performance: Continue on errors
		"--default-character-set=utf8mb4", // Performance: Set charset explicitly
	}

	if c.Dest.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Dest.Password))
	}

	args = append(args, c.Dest.Database)
	cmd := exec.Command("mysql", args...)

	internal.Logger.Debug("Importing SQL file", "file", filename, "args", strings.Join(args, " "))

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Capture stderr for better error reporting
	var stderr strings.Builder
	cmd.Stdin = file
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		stderrOutput := stderr.String()
		internal.Logger.Debug("Import error details", "stderr", stderrOutput, "error", err)
		return fmt.Errorf("failed to import SQL: %w (stderr: %s)", err, stderrOutput)
	}

	internal.Logger.Debug("SQL file imported successfully", "file", filename)
	return nil
}

func (c *Cloner) importTableData(filename string) error {
	tempFile := filename + ".temp"
	content := "SET FOREIGN_KEY_CHECKS=0;\n"

	originalContent, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	content += string(originalContent)
	content += "\nSET FOREIGN_KEY_CHECKS=1;\n"

	if err := os.WriteFile(tempFile, []byte(content), 0644); err != nil {
		return err
	}
	defer os.Remove(tempFile)

	return c.importSQL(tempFile)
}

func (c *Cloner) recreateDatabase() error {
	return internal.SimpleSpinner(fmt.Sprintf("Recreating database %s", c.Dest.Database), func() error {
		var dsn string
		if c.Dest.Password != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/",
				c.Dest.User, c.Dest.Password, c.Dest.Host, c.Dest.Port)
		} else {
			dsn = fmt.Sprintf("%s@tcp(%s:%d)/",
				c.Dest.User, c.Dest.Host, c.Dest.Port)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("failed to connect to MySQL: %w", err)
		}
		defer db.Close()

		internal.Logger.Debug("Dropping database", "database", c.Dest.Database)
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", c.Dest.Database))
		if err != nil {
			return fmt.Errorf("failed to drop database: %w", err)
		}

		internal.Logger.Debug("Creating database", "database", c.Dest.Database)
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE `%s`", c.Dest.Database))
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}

		return nil
	})
}

// getTablesWithPrefix returns tables that start with the given prefix
func (c *Cloner) getTablesWithPrefix(prefix string) ([]string, error) {
	db, err := c.connectSource()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		// Filter tables that match our prefix
		if strings.HasPrefix(table, prefix+"_") {
			tables = append(tables, table)
			internal.Logger.Debug("Found matching table", "table", table, "prefix", prefix)
		}
	}

	internal.Logger.Info("Table discovery completed", "prefix", prefix, "tables_found", len(tables))
	return tables, rows.Err()
}

// CloneTablesWithPrefix clones all tables that match the source prefix to destination with new prefix
func (c *Cloner) CloneTablesWithPrefix(sourcePrefix, destPrefix string, schemaOnly, dataOnly bool) error {
	if sourcePrefix == "" || destPrefix == "" {
		return fmt.Errorf("both sourcePrefix and destPrefix must be specified for prefix-based cloning")
	}
	
	totalStart := time.Now()
	internal.Logger.Debug("Starting prefix-based MySQL clone", 
		"source_prefix", sourcePrefix, 
		"dest_prefix", destPrefix)
	
	// Discover source tables
	var sourceTables []string
	err := internal.SimpleSpinner(fmt.Sprintf("Discovering tables with prefix %s", sourcePrefix), func() error {
		var discoverErr error
		sourceTables, discoverErr = c.getTablesWithPrefix(sourcePrefix)
		return discoverErr
	})
	if err != nil {
		return fmt.Errorf("failed to discover source tables: %w", err)
	}
	
	if len(sourceTables) == 0 {
		internal.Logger.Warn("No tables found with specified prefix", "prefix", sourcePrefix)
		return fmt.Errorf("no tables found with prefix: %s", sourcePrefix)
	}
	
	internal.Logger.Debug("Found tables to clone", "count", len(sourceTables), "tables", sourceTables)
	
	var totalErrors int64
	
	// Clone each table
	for i, sourceTable := range sourceTables {
		// Generate destination table name by replacing prefix
		destTable := strings.Replace(sourceTable, sourcePrefix+"_", destPrefix+"_", 1)
		
		// Create temporary cloner for this specific table pair
		tableCloner := &Cloner{
			Source: Config{
				Host:     c.Source.Host,
				Port:     c.Source.Port,
				User:     c.Source.User,
				Password: c.Source.Password,
				Database: c.Source.Database,
			},
			Dest: Config{
				Host:     c.Dest.Host,
				Port:     c.Dest.Port,
				User:     c.Dest.User,
				Password: c.Dest.Password,
				Database: c.Dest.Database,
			},
		}
		
		// Clone table with progress indicator
		err := internal.SimpleSpinner(fmt.Sprintf("Cloning table %s → %s (%d/%d)", sourceTable, destTable, i+1, len(sourceTables)), func() error {
			// Clone schema first if requested
			if !dataOnly {
				if err := tableCloner.cloneTableSchema(sourceTable, destTable); err != nil {
					return fmt.Errorf("failed to clone schema: %w", err)
				}
			}
			
			// Clone data if requested
			if !schemaOnly {
				if err := tableCloner.cloneTableData(sourceTable, destTable); err != nil {
					return fmt.Errorf("failed to clone data: %w", err)
				}
			}
			
			return nil
		})
		
		if err != nil {
			internal.Logger.Error("Failed to clone table", 
				"source_table", sourceTable,
				"dest_table", destTable,
				"error", err)
			totalErrors++
			continue
		}
	}
	
	totalDuration := time.Since(totalStart)
	
	if totalErrors > 0 {
		internal.Logger.Error("Prefix-based clone completed with errors", 
			"total_duration", totalDuration,
			"tables_processed", len(sourceTables),
			"tables_failed", totalErrors)
		return fmt.Errorf("cloning completed with %d errors out of %d tables", totalErrors, len(sourceTables))
	}
	
	internal.Logger.Debug("Prefix-based clone completed successfully", 
		"total_duration", totalDuration,
		"tables_cloned", len(sourceTables),
		"source_prefix", sourcePrefix,
		"dest_prefix", destPrefix)
	
	// Finish the prefix-based cloning operation line
	internal.FinishLine()
	
	return nil
}

// cloneTableSchema creates a specific table in the destination with a new name
func (c *Cloner) cloneTableSchema(sourceTable, destTable string) error {
	sourceDB, err := c.connectSource()
	if err != nil {
		return err
	}
	defer sourceDB.Close()

	destDB, err := c.connectDest()
	if err != nil {
		return err
	}
	defer destDB.Close()

	// Get CREATE TABLE statement for source table
	var createTableSQL string
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`", sourceTable)
	row := sourceDB.QueryRow(query)
	
	var tableName string
	if err := row.Scan(&tableName, &createTableSQL); err != nil {
		return fmt.Errorf("failed to get CREATE TABLE statement: %w", err)
	}

	// Replace source table name with destination table name in the CREATE statement
	modifiedSQL := strings.Replace(createTableSQL, fmt.Sprintf("CREATE TABLE `%s`", sourceTable), fmt.Sprintf("CREATE TABLE `%s`", destTable), 1)
	
	internal.Logger.Debug("Creating table schema", "dest_table", destTable)
	
	// Drop destination table if it exists
	_, err = destDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", destTable))
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}
	
	// Create the table with modified name
	_, err = destDB.Exec(modifiedSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	internal.Logger.Debug("Table schema created successfully", "dest_table", destTable)
	return nil
}

// cloneTableData copies data from source table to destination table with potentially different names
func (c *Cloner) cloneTableData(sourceTable, destTable string) error {
	sourceDB, err := c.connectSource()
	if err != nil {
		return err
	}
	defer sourceDB.Close()

	destDB, err := c.connectDest()
	if err != nil {
		return err
	}
	defer destDB.Close()

	// Get table columns
	columns, err := c.getTableColumns(sourceDB, sourceTable)
	if err != nil {
		return err
	}

	// Build SELECT query
	selectQuery := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(columns, ", "), sourceTable)
	
	internal.Logger.Debug("Querying source table", "table", sourceTable)
	
	rows, err := sourceDB.Query(selectQuery)
	if err != nil {
		return fmt.Errorf("failed to query source table: %w", err)
	}
	defer rows.Close()

	// Prepare INSERT statement for destination
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1]
	insertQuery := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		destTable, strings.Join(columns, ", "), placeholders)

	// Disable foreign key checks
	if _, err := destDB.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("failed to disable FK checks: %w", err)
	}
	defer destDB.Exec("SET FOREIGN_KEY_CHECKS=1")

	stmt, err := destDB.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	batchSize := 1000
	batch := make([][]interface{}, 0, batchSize)
	rowCount := 0

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch = append(batch, values)
		rowCount++

		if len(batch) >= batchSize {
			if err := c.insertBatch(stmt, batch); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	// Insert final batch
	if len(batch) > 0 {
		if err := c.insertBatch(stmt, batch); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
	}

	internal.Logger.Debug("Table data cloned successfully", "source_table", sourceTable, "dest_table", destTable, "rows", rowCount)
	return rows.Err()
}

func (c *Cloner) getAllTables() ([]string, error) {
	db, err := c.connectSource()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

func (c *Cloner) getTableColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE %s", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var field, type_, null, key, default_, extra string
		if err := rows.Scan(&field, &type_, &null, &key, &default_, &extra); err != nil {
			return nil, err
		}
		columns = append(columns, field)
	}

	return columns, rows.Err()
}

func (c *Cloner) sortTablesByDependency(tables []string) []string {
	return tables
}

func (c *Cloner) insertBatch(stmt *sql.Stmt, batch [][]interface{}) error {
	for _, values := range batch {
		if _, err := stmt.Exec(values...); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}
	return nil
}

// getTableSizes retrieves row counts for all tables to enable intelligent processing
func (c *Cloner) getTableSizes(tables []string) (map[string]int64, error) {
	db, err := c.connectSource()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tableSizes := make(map[string]int64)
	
	for _, table := range tables {
		var count int64
		query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		
		if err := db.QueryRow(query).Scan(&count); err != nil {
			internal.Logger.Warn("Could not get row count for table", "table", table, "error", err)
			tableSizes[table] = 0
		} else {
			tableSizes[table] = count
		}
	}
	
	return tableSizes, nil
}

// categorizeTablesBySize splits tables into size categories for optimal processing
func (c *Cloner) categorizeTablesBySize(tables []string, tableSizes map[string]int64) (large, medium, small []string) {
	const (
		largeThreshold  = 100000  // Tables with >100K rows
		mediumThreshold = 10000   // Tables with >10K rows
	)
	
	for _, table := range tables {
		rowCount := tableSizes[table]
		if rowCount > largeThreshold {
			large = append(large, table)
		} else if rowCount > mediumThreshold {
			medium = append(medium, table)
		} else {
			small = append(small, table)
		}
	}
	
	return large, medium, small
}

// cloneTableWithStreaming handles large tables with chunked streaming to avoid memory issues
func (c *Cloner) cloneTableWithStreaming(table string, estimatedRows int64, processedRows *int64, totalRows int64) error {
	internal.Logger.Info("Starting streaming clone for large table", "table", table)
	
	// Use chunked approach for large tables
	const chunkSize = 10000
	chunks := (estimatedRows / chunkSize) + 1
	
	for chunk := int64(0); chunk < chunks; chunk++ {
		offset := chunk * chunkSize
		
		// Show progress
		progress := float64(*processedRows) / float64(totalRows) * 100
		internal.Logger.Debug("Processing chunk", "table", table, "chunk", chunk+1, "total_chunks", chunks, 
			"offset", offset, "overall_progress", fmt.Sprintf("%.1f%%", progress))
		
		whereClause := fmt.Sprintf("1=1 LIMIT %d OFFSET %d", chunkSize, offset)
		
		chunkErr := internal.SimpleSpinner(fmt.Sprintf("Processing %s chunk %d/%d", table, chunk+1, chunks), func() error {
			return c.CloneWithFilter(table, whereClause)
		})
		
		if chunkErr != nil {
			return fmt.Errorf("failed to clone chunk %d of table %s: %w", chunk, table, chunkErr)
		}
		
		*processedRows += min(chunkSize, estimatedRows-offset)
	}
	
	return nil
}

// cloneTableOptimized uses optimized mysqldump for medium-sized tables
func (c *Cloner) cloneTableOptimized(table string, estimatedRows int64, processedRows *int64, totalRows int64) error {
	start := time.Now()
	
	args := []string{
		"--no-create-info",
		"--skip-disable-keys",
		"--single-transaction",
		"--quick",
		"--lock-tables=false",
		"--skip-lock-tables",        // RDS compatibility
		"--set-gtid-purged=OFF",     // RDS compatibility
		"--column-statistics=0",     // RDS compatibility
		"--extended-insert",         // Performance: use multi-row inserts
		"--default-character-set=utf8mb4",
		fmt.Sprintf("--host=%s", c.Source.Host),
		fmt.Sprintf("--port=%d", c.Source.Port),
		fmt.Sprintf("--user=%s", c.Source.User),
	}

	if c.Source.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Source.Password))
	}

	args = append(args, c.Source.Database, table)
	cmd := exec.Command("mysqldump", args...)

	internal.Logger.Debug("Exporting table data with optimized flags", "table", table, "estimated_rows", estimatedRows)

	dataFile := fmt.Sprintf("%s_%s_data.sql", c.Source.Database, table)
	file, err := os.Create(dataFile)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer file.Close()
	defer os.Remove(dataFile) // Clean up after import

	// Export with spinner
	exportErr := internal.SimpleSpinner(fmt.Sprintf("Exporting %s data", table), func() error {
		// Capture stderr for error reporting
		var stderr strings.Builder
		cmd.Stdout = file
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			stderrOutput := stderr.String()
			internal.Logger.Debug("Export error details", "table", table, "stderr", stderrOutput, "error", err)
			return fmt.Errorf("failed to export table data: %w (stderr: %s)", err, stderrOutput)
		}
		return nil
	})
	
	if exportErr != nil {
		return exportErr
	}

	exportDuration := time.Since(start)
	
	// Get actual file size
	if fileInfo, err := os.Stat(dataFile); err == nil {
		internal.Logger.Debug("Table data exported", "table", table, "file", dataFile, 
			"size_mb", float64(fileInfo.Size())/1024/1024, "export_duration", exportDuration)
	}

	// Import the data
	importStart := time.Now()
	importErr := internal.SimpleSpinner(fmt.Sprintf("Importing %s data", table), func() error {
		return c.importTableData(dataFile)
	})
	if importErr != nil {
		return fmt.Errorf("failed to import table data: %w", importErr)
	}
	
	importDuration := time.Since(importStart)
	*processedRows += estimatedRows
	
	progress := float64(*processedRows) / float64(totalRows) * 100
	internal.Logger.Info("Table cloned", "table", table, "rows", estimatedRows,
		"export_duration", exportDuration, "import_duration", importDuration,
		"progress", fmt.Sprintf("%.1f%%", progress))
	
	return nil
}

// cloneSmallTablesParallel processes small tables in parallel for efficiency
func (c *Cloner) cloneSmallTablesParallel(tables []string, tableSizes map[string]int64, processedRows *int64, totalRows int64) error {
	const maxConcurrency = 3 // Conservative concurrency to avoid overwhelming the database
	
	sem := make(chan struct{}, maxConcurrency)
	errChan := make(chan error, len(tables))
	
	for _, table := range tables {
		go func(tableName string) {
			sem <- struct{}{} // Acquire semaphore
			defer func() { <-sem }() // Release semaphore
			
			err := c.cloneTableOptimized(tableName, tableSizes[tableName], processedRows, totalRows)
			errChan <- err
		}(table)
	}
	
	// Wait for all goroutines to complete
	var errors []error
	for i := 0; i < len(tables); i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err)
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("failed to clone %d small tables: %v", len(errors), errors)
	}
	
	internal.Logger.Info("All small tables processed successfully", "count", len(tables))
	return nil
}

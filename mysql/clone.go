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
	internal.Logger.Info("Schema export completed", "duration", exportDuration, "file", schemaFile)

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
	internal.Logger.Info("Schema import completed", "duration", importDuration)

	// Clean up schema file
	os.Remove(schemaFile)

	totalDuration := time.Since(totalStart)
	internal.Logger.Info("Schema clone completed", "total_duration", totalDuration,
		"recreate_pct", fmt.Sprintf("%.1f%%", float64(recreateDuration.Nanoseconds())/float64(totalDuration.Nanoseconds())*100),
		"export_pct", fmt.Sprintf("%.1f%%", float64(exportDuration.Nanoseconds())/float64(totalDuration.Nanoseconds())*100),
		"import_pct", fmt.Sprintf("%.1f%%", float64(importDuration.Nanoseconds())/float64(totalDuration.Nanoseconds())*100))

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
			internal.Logger.Error("Fallback export failed - no tables found", "content_preview", contentStr[:min(len(contentStr), 200)])
			return fmt.Errorf("fallback export failed - no valid tables in schema file")
		}
	}

	return fmt.Errorf("fallback export failed - could not read schema file")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *Cloner) CloneData(tables []string) error {
	if len(tables) == 0 {
		var err error
		tables, err = c.getAllTables()
		if err != nil {
			return fmt.Errorf("failed to get table list: %w", err)
		}
		internal.Logger.Debug("Found tables to clone", "count", len(tables), "tables", tables)
	}

	orderedTables := c.sortTablesByDependency(tables)

	for i, table := range orderedTables {
		internal.Logger.Info("Cloning table", "table", table, "progress", fmt.Sprintf("%d/%d", i+1, len(orderedTables)))
		if err := c.cloneTable(table); err != nil {
			return fmt.Errorf("failed to clone table %s: %w", table, err)
		}
		internal.Logger.Debug("Table cloned successfully", "table", table)
	}

	return nil
}

func (c *Cloner) cloneTable(table string) error {
	args := []string{
		"--no-create-info",
		"--skip-disable-keys",
		"--single-transaction",
		"--quick",
		"--lock-tables=false",
		fmt.Sprintf("--host=%s", c.Source.Host),
		fmt.Sprintf("--port=%d", c.Source.Port),
		fmt.Sprintf("--user=%s", c.Source.User),
	}

	if c.Source.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Source.Password))
	}

	args = append(args, c.Source.Database, table)
	cmd := exec.Command("mysqldump", args...)

	internal.Logger.Debug("Exporting table data", "table", table, "database", c.Source.Database)

	dataFile := fmt.Sprintf("%s_%s_data.sql", c.Source.Database, table)
	file, err := os.Create(dataFile)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer file.Close()

	cmd.Stdout = file
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to export table data: %w", err)
	}

	internal.Logger.Debug("Table data exported", "table", table, "file", dataFile)
	return c.importTableData(dataFile)
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

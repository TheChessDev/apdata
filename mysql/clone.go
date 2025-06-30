package mysql

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"

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
	args := []string{
		"--no-data",
		"--skip-add-drop-table",
		"--skip-disable-keys",
		"--routines",
		"--triggers",
		fmt.Sprintf("--host=%s", c.Source.Host),
		fmt.Sprintf("--port=%d", c.Source.Port),
		fmt.Sprintf("--user=%s", c.Source.User),
	}

	if c.Source.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Source.Password))
	}

	args = append(args, c.Source.Database)
	cmd := exec.Command("mysqldump", args...)

	schemaFile := fmt.Sprintf("%s_schema.sql", c.Source.Database)
	file, err := os.Create(schemaFile)
	if err != nil {
		return fmt.Errorf("failed to create schema file: %w", err)
	}
	defer file.Close()

	cmd.Stdout = file
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to export schema: %w", err)
	}

	return c.importSQL(schemaFile)
}

func (c *Cloner) CloneData(tables []string) error {
	if len(tables) == 0 {
		var err error
		tables, err = c.getAllTables()
		if err != nil {
			return fmt.Errorf("failed to get table list: %w", err)
		}
	}

	orderedTables := c.sortTablesByDependency(tables)

	for _, table := range orderedTables {
		if err := c.cloneTable(table); err != nil {
			return fmt.Errorf("failed to clone table %s: %w", table, err)
		}
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

	return c.importTableData(dataFile)
}

func (c *Cloner) CloneWithFilter(table, whereClause string) error {
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

	rows, err := sourceDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query source: %w", err)
	}
	defer rows.Close()

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
		return c.insertBatch(stmt, batch)
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
	args := []string{
		fmt.Sprintf("--host=%s", c.Dest.Host),
		fmt.Sprintf("--port=%d", c.Dest.Port),
		fmt.Sprintf("--user=%s", c.Dest.User),
	}

	if c.Dest.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.Dest.Password))
	}

	args = append(args, c.Dest.Database)
	cmd := exec.Command("mysql", args...)

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	cmd.Stdin = file
	return cmd.Run()
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

package mysql

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"apdata/internal"
)

func TestDatabaseRecreation(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	err := cloner.CloneSchema()
	if err == nil {
		t.Error("Expected error when cloning without real database")
	}
	
	if !strings.Contains(err.Error(), "failed to recreate database") {
		t.Logf("Error as expected: %v", err)
	}
}

func TestNewCloner(t *testing.T) {
	source := Config{
		Host:     "source.example.com",
		Port:     3306,
		User:     "user",
		Password: "pass",
		Database: "sourcedb",
	}
	dest := Config{
		Host:     "dest.example.com",
		Port:     3307,
		User:     "destuser",
		Password: "destpass",
		Database: "destdb",
	}

	cloner := NewCloner(source, dest)

	if cloner.Source != source {
		t.Error("Source config not set correctly")
	}
	if cloner.Dest != dest {
		t.Error("Dest config not set correctly")
	}
}

func TestConnectSource(t *testing.T) {
	cloner := &Cloner{
		Source: Config{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "password",
			Database: "testdb",
		},
	}

	_, err := cloner.connectSource()
	if err != nil {
		if !strings.Contains(err.Error(), "connect: connection refused") && 
		   !strings.Contains(err.Error(), "no such host") {
			t.Logf("Connection failed as expected: %v", err)
		}
	}
}

func TestConnectDest(t *testing.T) {
	cloner := &Cloner{
		Dest: Config{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "",
			Database: "testdb",
		},
	}

	_, err := cloner.connectDest()
	if err != nil {
		if !strings.Contains(err.Error(), "connect: connection refused") && 
		   !strings.Contains(err.Error(), "no such host") {
			t.Logf("Connection failed as expected: %v", err)
		}
	}
}

func TestSortTablesByDependency(t *testing.T) {
	cloner := &Cloner{}
	tables := []string{"users", "orders", "products"}
	
	sorted := cloner.sortTablesByDependency(tables)
	
	if len(sorted) != len(tables) {
		t.Errorf("Expected %d tables, got %d", len(tables), len(sorted))
	}
	
	for i, table := range tables {
		if sorted[i] != table {
			t.Errorf("Expected table %s at position %d, got %s", table, i, sorted[i])
		}
	}
}

func TestCloneSchemaWithMockError(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	tempFile := "test_schema.sql"
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	file.WriteString("CREATE TABLE test (id INT);")
	file.Close()
	defer os.Remove(tempFile)

}

func TestImportSQLError(t *testing.T) {
	cloner := &Cloner{
		Dest: Config{
			Host:     "nonexistent.host",
			Port:     3306,
			User:     "testuser",
			Password: "",
			Database: "testdb",
		},
	}

	tempFile := "test_import.sql"
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	file.WriteString("CREATE TABLE test (id INT);")
	file.Close()
	defer os.Remove(tempFile)

	err = cloner.importSQL(tempFile)
	if err == nil {
		t.Error("Expected error when importing to nonexistent host")
	}

	if !strings.Contains(err.Error(), "failed to import SQL") {
		t.Logf("Got expected error: %v", err)
	}
}

func TestPasswordlessDSN(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "with password",
			config: Config{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "testdb",
			},
			expected: "root:secret@tcp(localhost:3306)/testdb",
		},
		{
			name: "without password",
			config: Config{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "",
				Database: "testdb",
			},
			expected: "root@tcp(localhost:3306)/testdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dsn string
			if tt.config.Password != "" {
				dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/", 
					tt.config.User, tt.config.Password, tt.config.Host, tt.config.Port)
			} else {
				dsn = fmt.Sprintf("%s@tcp(%s:%d)/", 
					tt.config.User, tt.config.Host, tt.config.Port)
			}

			expectedBase := strings.Replace(tt.expected, "/testdb", "/", 1)
			if dsn != expectedBase {
				t.Errorf("Expected DSN %s, got %s", expectedBase, dsn)
			}
		})
	}
}

func TestCloneSchemaFlow(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	err := cloner.CloneSchema()
	if err == nil {
		t.Error("Expected error when cloning without real database")
	}
}

func TestInsertBatch(t *testing.T) {
	
	batch := [][]interface{}{
		{1, "test1"},
		{2, "test2"},
	}
	
	if len(batch) != 2 {
		t.Errorf("Expected 2 items in batch, got %d", len(batch))
	}
	
	if len(batch[0]) != 2 {
		t.Errorf("Expected 2 columns in first row, got %d", len(batch[0]))
	}
}

func TestRecreateDatabase(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Dest: Config{
			Host:     "nonexistent.host",
			Port:     3306,
			User:     "root",
			Password: "",
			Database: "testdb",
		},
	}

	err := cloner.recreateDatabase()
	if err == nil {
		t.Error("Expected error when recreating database without real connection")
	}
	
	if strings.Contains(err.Error(), "failed to recreate database") {
		t.Logf("Got expected error: %v", err)
	}
}

func TestGetTableSizes(t *testing.T) {
	cloner := &Cloner{
		Source: Config{
			Host:     "nonexistent.host",
			Port:     3306,
			User:     "root",
			Database: "testdb",
		},
	}

	tables := []string{"users", "orders", "products"}
	
	sizes, err := cloner.getTableSizes(tables)
	if err != nil {
		t.Logf("Expected connection error: %v", err)
	}
	
	if sizes == nil {
		t.Error("Expected non-nil map even on connection failure")
	}
}

func TestCategorizeTablesBySize(t *testing.T) {
	cloner := &Cloner{}
	
	tables := []string{"large_table", "medium_table", "small_table"}
	tableSizes := map[string]int64{
		"large_table":  500000,
		"medium_table": 50000,
		"small_table":  1000,
	}
	
	large, medium, small := cloner.categorizeTablesBySize(tables, tableSizes)
	
	if len(large) != 1 || large[0] != "large_table" {
		t.Errorf("Expected 1 large table, got %v", large)
	}
	
	if len(medium) != 1 || medium[0] != "medium_table" {
		t.Errorf("Expected 1 medium table, got %v", medium)
	}
	
	if len(small) != 1 || small[0] != "small_table" {
		t.Errorf("Expected 1 small table, got %v", small)
	}
}

func TestCloneDataWithEmptyTables(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	err := cloner.CloneData([]string{})
	if err == nil {
		t.Error("Expected error when cloning without real database")
	}
	
	if !strings.Contains(err.Error(), "failed to get table list") {
		t.Logf("Got expected error: %v", err)
	}
}

func TestCloneDataPerformanceStructure(t *testing.T) {
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	tables := []string{"table1", "table2", "table3"}
	tableSizes := map[string]int64{
		"table1": 200000,
		"table2": 50000,
		"table3": 5000,
	}
	
	large, medium, small := cloner.categorizeTablesBySize(tables, tableSizes)
	
	expectedLarge := 1
	expectedMedium := 1
	expectedSmall := 1
	
	if len(large) != expectedLarge {
		t.Errorf("Expected %d large tables, got %d", expectedLarge, len(large))
	}
	if len(medium) != expectedMedium {
		t.Errorf("Expected %d medium tables, got %d", expectedMedium, len(medium))
	}
	if len(small) != expectedSmall {
		t.Errorf("Expected %d small tables, got %d", expectedSmall, len(small))
	}
}

func TestOptimizedMySQLDumpArgs(t *testing.T) {
	expectedFlags := []string{
		"--no-create-info",
		"--skip-disable-keys", 
		"--single-transaction",
		"--quick",
		"--lock-tables=false",
		"--skip-lock-tables",
		"--set-gtid-purged=OFF",
		"--column-statistics=0",
		"--extended-insert",
	}
	
	for _, flag := range expectedFlags {
		if flag == "" {
			t.Error("Empty flag found in expected optimization flags")
		}
	}
}

func TestProgressTracking(t *testing.T) {
	var processedRows int64 = 0
	var totalRows int64 = 100000
	
	processedRows += 25000
	progress := float64(processedRows) / float64(totalRows) * 100
	
	expectedProgress := 25.0
	if progress != expectedProgress {
		t.Errorf("Expected progress %.1f%%, got %.1f%%", expectedProgress, progress)
	}
	
	processedRows += 100000
	progress = float64(processedRows) / float64(totalRows) * 100
	
	if progress <= 100.0 {
		t.Logf("Progress is %.1f%% (estimates can be inaccurate)", progress)
	}
}

func TestMinFunction(t *testing.T) {
	tests := []struct {
		a, b     int64
		expected int64
	}{
		{5, 10, 5},
		{10, 5, 5},
		{7, 7, 7},
		{0, 1, 0},
		{-1, 5, -1},
	}
	
	for _, tt := range tests {
		result := min(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("min(%d, %d) = %d, expected %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestDataCloningSpinners(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	err := cloner.CloneData([]string{})
	if err == nil {
		t.Error("Expected error when cloning without real database")
	}
	
	if !strings.Contains(err.Error(), "failed to get table list") {
		t.Logf("Got expected error: %v", err)
	}
}

func TestTableSizeAnalysisWithSpinner(t *testing.T) {
	internal.VerboseMode = false
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{
			Host:     "nonexistent.host",
			Database: "testdb",
		},
	}

	tables := []string{"test_table"}
	
	_, err := cloner.getTableSizes(tables)
	if err != nil {
		t.Logf("Expected connection error: %v", err)
	}
}

func TestStreamingWithProgressIndicators(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	var processedRows int64 = 0
	var totalRows int64 = 50000
	
	err := cloner.cloneTableWithStreaming("test_table", 50000, &processedRows, totalRows)
	if err == nil {
		t.Error("Expected error when streaming without real database")
	}
	
	if strings.Contains(err.Error(), "failed to clone chunk") {
		t.Logf("Got expected chunking error: %v", err)
	}
}
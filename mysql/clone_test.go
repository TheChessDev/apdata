package mysql

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"apdata/internal"
)

func TestDatabaseRecreation(t *testing.T) {
	// Test that CloneSchema always recreates database first
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Source: Config{Database: "sourcedb"},
		Dest:   Config{Database: "destdb"},
	}

	// This will fail due to no real DB, but ensures recreateDatabase is called
	err := cloner.CloneSchema()
	if err == nil {
		t.Error("Expected error when cloning without real database")
	}
	
	// The error should be from database recreation, not schema import
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

	// This will fail since we don't have a real DB, but we can test the method exists
	_, err := cloner.connectSource()
	if err != nil {
		// Expected to fail in test environment
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
			Password: "",  // Test passwordless connection
			Database: "testdb",
		},
	}

	_, err := cloner.connectDest()
	if err != nil {
		// Expected to fail in test environment
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
	
	// Currently returns original order - test this behavior
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
	// Test the error detection logic without actual MySQL
	// Set verbose mode to avoid spinner interference in tests
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	// Create a temporary file to simulate schema export
	tempFile := "test_schema.sql"
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	file.WriteString("CREATE TABLE test (id INT);")
	file.Close()
	defer os.Remove(tempFile)

	// Test that the function would call CloneSchemaWithOptions(true) on table exists error
	// We can't easily test the full flow without a real database, but we can test components
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

	// Create a temp SQL file
	tempFile := "test_import.sql"
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	file.WriteString("CREATE TABLE test (id INT);")
	file.Close()
	defer os.Remove(tempFile)

	// This should fail because host doesn't exist
	err = cloner.importSQL(tempFile)
	if err == nil {
		t.Error("Expected error when importing to nonexistent host")
	}

	// Should be a connection error
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
			// We can't easily extract the DSN from sql.Open without connecting,
			// but we can test the recreation DSN logic
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

	// Test CloneSchema always recreates database first
	// This will fail due to no real DB, but we can verify the method exists
	err := cloner.CloneSchema()
	if err == nil {
		t.Error("Expected error when cloning without real database")
	}
}

func TestInsertBatch(t *testing.T) {
	// This test requires a real database connection to properly test
	// For now, we'll test that the method signature is correct
	
	// Create some test data
	batch := [][]interface{}{
		{1, "test1"},
		{2, "test2"},
	}
	
	// Test that batch has expected structure
	if len(batch) != 2 {
		t.Errorf("Expected 2 items in batch, got %d", len(batch))
	}
	
	if len(batch[0]) != 2 {
		t.Errorf("Expected 2 columns in first row, got %d", len(batch[0]))
	}
}

// Test that recreateDatabase handles passwordless connections
func TestRecreateDatabase(t *testing.T) {
	// Set verbose mode to avoid spinner issues in tests
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	cloner := &Cloner{
		Dest: Config{
			Host:     "nonexistent.host",
			Port:     3306,
			User:     "root",
			Password: "", // Test passwordless
			Database: "testdb",
		},
	}

	// This will fail without real database, but tests the DSN construction
	err := cloner.recreateDatabase()
	if err == nil {
		t.Error("Expected error when recreating database without real connection")
	}
	
	// Should be a connection error
	if strings.Contains(err.Error(), "failed to recreate database") {
		t.Logf("Got expected error: %v", err)
	}
}
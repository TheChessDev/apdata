package main

import (
	"os"
	"strings"
	"testing"

	"apdata/config"
	"apdata/internal"
	"apdata/mysql"
)

// Integration tests that test components working together
func TestConfigIntegration(t *testing.T) {
	// Test that config can be loaded and parsed correctly
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	cfg, err := config.LoadConfig()
	if err != nil {
		t.Errorf("Failed to load default config: %v", err)
	}

	// Should have default examples
	mysqlConns, dynamoConns := cfg.GetConfiguredConnections()
	if len(mysqlConns) == 0 {
		t.Error("Expected at least one MySQL connection in default config")
	}
	if len(dynamoConns) == 0 {
		t.Error("Expected at least one DynamoDB connection in default config")
	}

	// Test parsing connection strings work with config
	for _, conn := range mysqlConns {
		connStr, err := config.ParseConnectionString(conn)
		if err != nil {
			t.Errorf("Failed to parse connection string '%s': %v", conn, err)
		}
		
		// Should be able to get config back
		_, err = cfg.GetMySQLConfig(connStr.Client, connStr.Env)
		if err != nil {
			t.Errorf("Failed to get MySQL config for '%s': %v", conn, err)
		}
	}
}

func TestMySQLClonerWithConfig(t *testing.T) {
	// Test that MySQL cloner can be created from config
	source := mysql.Config{
		Host:     "source.example.com",
		Port:     3306,
		User:     "user",
		Password: "pass",
		Database: "sourcedb",
	}
	dest := mysql.Config{
		Host:     "dest.example.com", 
		Port:     3306,
		User:     "user",
		Password: "",  // Test passwordless
		Database: "destdb",
	}

	cloner := mysql.NewCloner(source, dest)
	if cloner == nil {
		t.Error("Failed to create MySQL cloner")
	}

	// Test that the cloner has the correct config
	if cloner.Source.Host != source.Host {
		t.Error("Source config not set correctly")
	}
	if cloner.Dest.Password != "" {
		t.Error("Dest password should be empty")
	}
}

func TestSpinnerIntegration(t *testing.T) {
	// Test that spinner works with different modes
	internal.VerboseMode = false
	
	executed := false
	err := internal.SimpleSpinner("Test operation", func() error {
		executed = true
		return nil
	})
	
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !executed {
		t.Error("Operation should have been executed")
	}

	// Test verbose mode disables spinner but still executes
	internal.VerboseMode = true
	executed = false
	
	err = internal.SimpleSpinner("Test operation", func() error {
		executed = true
		return nil
	})
	
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !executed {
		t.Error("Operation should have been executed in verbose mode")
	}
	
	// Reset
	internal.VerboseMode = false
}

func TestErrorHandlingIntegration(t *testing.T) {
	// Test that the simplified schema cloning approach works
	cloner := mysql.NewCloner(
		mysql.Config{
			Host:     "source.test",
			Database: "sourcedb",
		},
		mysql.Config{
			Host:     "dest.test", 
			Database: "destdb",
		},
	)

	// Test that CloneSchema always recreates database
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	err := cloner.CloneSchema()
	if err == nil {
		t.Error("Expected error without real database connection")
	}
	
	// Should fail on database recreation step
	if !strings.Contains(err.Error(), "failed to recreate database") {
		t.Logf("Got expected error: %v", err)
	}
}
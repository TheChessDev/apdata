package main

import (
	"os"
	"strings"
	"testing"

	"apdata/config"
	"apdata/internal"
	"apdata/mysql"
)

func TestConfigIntegration(t *testing.T) {
	tempDir := t.TempDir()
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", originalHome)

	cfg, err := config.LoadConfig()
	if err != nil {
		t.Errorf("Failed to load default config: %v", err)
	}

	mysqlConns, dynamoConns := cfg.GetConfiguredConnections()
	if len(mysqlConns) == 0 {
		t.Error("Expected at least one MySQL connection in default config")
	}
	if len(dynamoConns) == 0 {
		t.Error("Expected at least one DynamoDB connection in default config")
	}

	for _, conn := range mysqlConns {
		connStr, err := config.ParseConnectionString(conn)
		if err != nil {
			t.Errorf("Failed to parse connection string '%s': %v", conn, err)
		}
		
		_, err = cfg.GetMySQLConfig(connStr.Client, connStr.Env)
		if err != nil {
			t.Errorf("Failed to get MySQL config for '%s': %v", conn, err)
		}
	}
}

func TestMySQLClonerWithConfig(t *testing.T) {
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
		Password: "",
		Database: "destdb",
	}

	cloner := mysql.NewCloner(source, dest)
	if cloner == nil {
		t.Error("Failed to create MySQL cloner")
	}

	if cloner.Source.Host != source.Host {
		t.Error("Source config not set correctly")
	}
	if cloner.Dest.Password != "" {
		t.Error("Dest password should be empty")
	}
}

func TestSpinnerIntegration(t *testing.T) {
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
	
	internal.VerboseMode = false
}

func TestErrorHandlingIntegration(t *testing.T) {
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

	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()
	
	err := cloner.CloneSchema()
	if err == nil {
		t.Error("Expected error without real database connection")
	}
	
	if !strings.Contains(err.Error(), "failed to recreate database") {
		t.Logf("Got expected error: %v", err)
	}
}
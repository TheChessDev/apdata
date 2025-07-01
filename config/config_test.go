package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"apdata/dynamodb"
	"apdata/mysql"
)

func TestParseConnectionString(t *testing.T) {
	tests := []struct {
		name        string
		connStr     string
		expectedErr bool
		expected    *ConnectionString
	}{
		{
			name:    "valid connection string",
			connStr: "acme/prod",
			expected: &ConnectionString{
				Client: "acme",
				Env:    "prod",
			},
		},
		{
			name:    "valid with different values",
			connStr: "beta/staging", 
			expected: &ConnectionString{
				Client: "beta",
				Env:    "staging",
			},
		},
		{
			name:        "invalid - no slash",
			connStr:     "acmeprod",
			expectedErr: true,
		},
		{
			name:        "invalid - multiple slashes",
			connStr:     "acme/prod/extra",
			expectedErr: true,
		},
		{
			name:        "invalid - empty",
			connStr:     "",
			expectedErr: true,
		},
		{
			name:        "invalid - only slash",
			connStr:     "/",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseConnectionString(tt.connStr)
			
			if tt.expectedErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if result.Client != tt.expected.Client {
				t.Errorf("Expected client %s, got %s", tt.expected.Client, result.Client)
			}
			
			if result.Env != tt.expected.Env {
				t.Errorf("Expected env %s, got %s", tt.expected.Env, result.Env)
			}
		})
	}
}

func TestConfigMethods(t *testing.T) {
	config := &Config{
		MySQL: make(map[string]mysql.Config),
		DynamoDB: make(map[string]dynamodb.Config),
	}

	mysqlConfig := mysql.Config{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Database: "testdb",
	}
	
	config.SetMySQLConfig("test", "local", mysqlConfig)
	
	retrieved, err := config.GetMySQLConfig("test", "local")
	if err != nil {
		t.Errorf("Unexpected error getting MySQL config: %v", err)
	}
	
	if *retrieved != mysqlConfig {
		t.Error("Retrieved MySQL config doesn't match set config")
	}

	dynamoConfig := dynamodb.Config{
		Region: "us-east-1",
	}
	
	config.SetDynamoDBConfig("test", "local", dynamoConfig)
	
	retrievedDynamo, err := config.GetDynamoDBConfig("test", "local")
	if err != nil {
		t.Errorf("Unexpected error getting DynamoDB config: %v", err)
	}
	
	if *retrievedDynamo != dynamoConfig {
		t.Error("Retrieved DynamoDB config doesn't match set config")
	}

	_, err = config.GetMySQLConfig("nonexistent", "config")
	if err == nil {
		t.Error("Expected error for non-existent MySQL config")
	}

	_, err = config.GetDynamoDBConfig("nonexistent", "config")
	if err == nil {
		t.Error("Expected error for non-existent DynamoDB config")
	}
}

func TestGetConfiguredConnections(t *testing.T) {
	config := &Config{
		MySQL: map[string]mysql.Config{
			"client1/prod": {},
			"client1/staging": {},
			"client2/local": {},
		},
		DynamoDB: map[string]dynamodb.Config{
			"client1/prod": {},
			"client3/test": {},
		},
	}

	mysqlConns, dynamoConns := config.GetConfiguredConnections()

	expectedMySQL := 3
	if len(mysqlConns) != expectedMySQL {
		t.Errorf("Expected %d MySQL connections, got %d", expectedMySQL, len(mysqlConns))
	}

	expectedDynamo := 2
	if len(dynamoConns) != expectedDynamo {
		t.Errorf("Expected %d DynamoDB connections, got %d", expectedDynamo, len(dynamoConns))
	}

	mysqlMap := make(map[string]bool)
	for _, conn := range mysqlConns {
		mysqlMap[conn] = true
	}

	if !mysqlMap["client1/prod"] {
		t.Error("Expected client1/prod in MySQL connections")
	}
}

func TestConfigSerialization(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_config.json")

	config := &Config{
		MySQL: map[string]mysql.Config{
			"test/local": {
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Database: "testdb",
			},
		},
		DynamoDB: map[string]dynamodb.Config{
			"test/local": {
				Region: "us-east-1",
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Errorf("Failed to marshal config: %v", err)
	}

	err = os.WriteFile(tempFile, data, 0644)
	if err != nil {
		t.Errorf("Failed to write config file: %v", err)
	}

	readData, err := os.ReadFile(tempFile)
	if err != nil {
		t.Errorf("Failed to read config file: %v", err)
	}

	var loadedConfig Config
	err = json.Unmarshal(readData, &loadedConfig)
	if err != nil {
		t.Errorf("Failed to unmarshal config: %v", err)
	}

	mysqlConfig, err := loadedConfig.GetMySQLConfig("test", "local")
	if err != nil {
		t.Errorf("Failed to get MySQL config: %v", err)
	}

	if mysqlConfig.Host != "localhost" || mysqlConfig.Port != 3306 {
		t.Error("MySQL config data was corrupted during serialization")
	}

	dynamoConfig, err := loadedConfig.GetDynamoDBConfig("test", "local")
	if err != nil {
		t.Errorf("Failed to get DynamoDB config: %v", err)
	}

	if dynamoConfig.Region != "us-east-1" {
		t.Error("DynamoDB config data was corrupted during serialization")
	}
}
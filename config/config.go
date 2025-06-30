package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"apdata/dynamodb"
	"apdata/mysql"
)

type Config struct {
	MySQL    map[string]mysql.Config    `json:"mysql"`
	DynamoDB map[string]dynamodb.Config `json:"dynamodb"`
}

type ConnectionString struct {
	Client string
	Env    string
}

func ParseConnectionString(connStr string) (*ConnectionString, error) {
	parts := []string{}
	for i, char := range connStr {
		if char == '/' {
			parts = append(parts, connStr[:i])
			parts = append(parts, connStr[i+1:])
			break
		}
	}

	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid connection string format: %s (expected client/env)", connStr)
	}

	return &ConnectionString{
		Client: parts[0],
		Env:    parts[1],
	}, nil
}

func LoadConfig() (*Config, error) {
	configPath := getConfigPath()

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return createDefaultConfig(configPath)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func (c *Config) SaveConfig() error {
	configPath := getConfigPath()

	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func (c *Config) GetMySQLConfig(client, env string) (*mysql.Config, error) {
	key := fmt.Sprintf("%s/%s", client, env)
	config, exists := c.MySQL[key]
	if !exists {
		return nil, fmt.Errorf("MySQL config not found for %s", key)
	}
	return &config, nil
}

func (c *Config) GetDynamoDBConfig(client, env string) (*dynamodb.Config, error) {
	key := fmt.Sprintf("%s/%s", client, env)
	config, exists := c.DynamoDB[key]
	if !exists {
		return nil, fmt.Errorf("DynamoDB config not found for %s", key)
	}
	return &config, nil
}

func (c *Config) SetMySQLConfig(client, env string, config mysql.Config) {
	if c.MySQL == nil {
		c.MySQL = make(map[string]mysql.Config)
	}
	key := fmt.Sprintf("%s/%s", client, env)
	c.MySQL[key] = config
}

func (c *Config) SetDynamoDBConfig(client, env string, config dynamodb.Config) {
	if c.DynamoDB == nil {
		c.DynamoDB = make(map[string]dynamodb.Config)
	}
	key := fmt.Sprintf("%s/%s", client, env)
	c.DynamoDB[key] = config
}

func getConfigPath() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ".apdata/config.json"
	}
	return filepath.Join(homeDir, ".apdata", "config.json")
}

func createDefaultConfig(configPath string) (*Config, error) {
	config := &Config{
		MySQL: map[string]mysql.Config{
			"example/local": {
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Database: "testdb",
			},
		},
		DynamoDB: map[string]dynamodb.Config{
			"example/local": {
				Region:    "us-east-1",
				TableName: "test-table",
				Endpoint:  "http://localhost:8000",
			},
		},
	}

	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := config.SaveConfig(); err != nil {
		return nil, fmt.Errorf("failed to save default config: %w", err)
	}

	fmt.Printf("Created default config at %s\n", configPath)
	fmt.Println("Please edit the config file to add your database connections.")

	return config, nil
}

func (c *Config) GetConfiguredConnections() ([]string, []string) {
	var mysql, dynamodb []string

	for key := range c.MySQL {
		mysql = append(mysql, key)
	}

	for key := range c.DynamoDB {
		dynamodb = append(dynamodb, key)
	}

	return mysql, dynamodb
}


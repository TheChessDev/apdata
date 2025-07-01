package cmd

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"apdata/config"
	"apdata/dynamodb"
	"apdata/internal"
	"apdata/mysql"
)

var cloneCmd = &cobra.Command{
	Use:   "clone [mysql|dynamodb|all]",
	Short: "Clone data from cloud to local",
	Long: `Clone data from cloud to local environments.

Examples:
  # Clone all tables for a component
  apdata clone dynamodb --source allpoint/dev --dest julian/dev --component-name connectors-data-api

  # Interactive checkbox selection for a component  
  apdata clone dynamodb --source allpoint/dev --dest julian/dev --component-name connectors-data-api --interactive

  # Clone a specific MySQL table
  apdata clone mysql --source allpoint/dev --dest julian/dev --table specific-table-name`,
	Args:          cobra.ExactArgs(1),
	RunE:          runClone,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func runClone(cmd *cobra.Command, args []string) error {
	cloneType := args[0]

	source, _ := cmd.Flags().GetString("source")
	dest, _ := cmd.Flags().GetString("dest")
	table, _ := cmd.Flags().GetString("table")
	componentName, _ := cmd.Flags().GetString("component-name")
	filter, _ := cmd.Flags().GetString("filter")
	schemaOnly, _ := cmd.Flags().GetBool("schema-only")
	dataOnly, _ := cmd.Flags().GetBool("data-only")
	whereClause, _ := cmd.Flags().GetString("where")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	verbose, _ := cmd.Flags().GetBool("verbose")
	interactive, _ := cmd.Flags().GetBool("interactive")

	if verbose {
		internal.SetLogLevel("debug")
	} else {
		internal.SetLogLevel("error")
	}

	if source == "" || dest == "" {
		return fmt.Errorf("both --source and --dest are required")
	}

	if interactive && componentName == "" {
		return fmt.Errorf("--interactive flag can only be used with --component-name")
	}

	if table != "" && (cloneType == "dynamodb" || cloneType == "all") {
		return fmt.Errorf("--table flag is only supported for MySQL. For DynamoDB, use --component-name with --interactive for table selection")
	}

	if componentName != "" && cloneType == "mysql" {
		return fmt.Errorf("--component-name flag is only supported for DynamoDB. For MySQL, use --table for specific tables or omit for all tables")
	}

	if filter != "" && cloneType == "mysql" {
		return fmt.Errorf("--filter flag is only supported for DynamoDB. For MySQL, use --where for filtering records")
	}

	if whereClause != "" {
		if cloneType != "mysql" {
			return fmt.Errorf("--where flag is only supported for MySQL. For DynamoDB, use --filter for filtering records")
		}
		if table == "" {
			return fmt.Errorf("--where flag requires --table to specify which table to filter. Use --where with --table for record filtering")
		}
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	internal.Logger.Info("Starting clone operation",
		"type", cloneType,
		"source", source,
		"dest", dest,
		"table", table)

	switch cloneType {
	case "mysql":
		if err := cloneMySQLData(cfg, source, dest, table, whereClause, schemaOnly, dataOnly); err != nil {
			return formatError(err)
		}
	case "dynamodb":
		if err := cloneDynamoDBData(cfg, source, dest, table, componentName, filter, concurrency, interactive); err != nil {
			return formatError(err)
		}
	case "all":
		if err := cloneMySQLData(cfg, source, dest, table, whereClause, schemaOnly, dataOnly); err != nil {
			return formatError(err)
		}
		if err := cloneDynamoDBData(cfg, source, dest, table, componentName, filter, concurrency, interactive); err != nil {
			return formatError(err)
		}
	default:
		return formatError(fmt.Errorf("unsupported clone type: %s", cloneType))
	}

	return nil
}

func formatError(err error) error {
	errStr := err.Error()

	if strings.Contains(errStr, "AWS credentials") || strings.Contains(errStr, "aws configure") {
		return fmt.Errorf("❌ AWS credentials not configured. Please run 'aws configure' or set AWS environment variables.")
	}

	if strings.Contains(errStr, "ResourceNotFoundException") {
		return fmt.Errorf("❌ DynamoDB table not found. Please check your table name and AWS region.")
	}

	if strings.Contains(errStr, "UnauthorizedOperation") || strings.Contains(errStr, "AccessDenied") {
		return fmt.Errorf("❌ AWS permissions denied. Please ensure your AWS credentials have DynamoDB access permissions.")
	}

	if strings.Contains(errStr, "connection refused") {
		return fmt.Errorf("❌ Cannot connect to MySQL server. Please check your connection settings.")
	}

	if strings.Contains(errStr, "Access denied") && strings.Contains(errStr, "FLUSH") {
		return fmt.Errorf("❌ MySQL user lacks required privileges (RELOAD). This is common with RDS instances. The export should still work - please check if the operation completed successfully.")
	}

	if strings.Contains(errStr, "Access denied") {
		return fmt.Errorf("❌ MySQL authentication failed. Please check your username and password.")
	}

	if strings.Contains(errStr, "Unknown database") {
		return fmt.Errorf("❌ Database does not exist. Please check your database name.")
	}

	return fmt.Errorf("❌ %s", errStr)
}

func cloneMySQLData(cfg *config.Config, source, dest, table, whereClause string, schemaOnly, dataOnly bool) error {
	sourceConn, err := config.ParseConnectionString(source)
	if err != nil {
		return fmt.Errorf("invalid source connection string: %w", err)
	}

	destConn, err := config.ParseConnectionString(dest)
	if err != nil {
		return fmt.Errorf("invalid destination connection string: %w", err)
	}

	sourceConfig, err := cfg.GetMySQLConfig(sourceConn.Client, sourceConn.Env)
	if err != nil {
		return fmt.Errorf("failed to get source MySQL config: %w", err)
	}

	destConfig, err := cfg.GetMySQLConfig(destConn.Client, destConn.Env)
	if err != nil {
		return fmt.Errorf("failed to get destination MySQL config: %w", err)
	}

	cloner := mysql.NewCloner(*sourceConfig, *destConfig)

	start := time.Now()
	defer func() {
		internal.Logger.Info("MySQL clone completed", "duration", time.Since(start))
	}()

	if !dataOnly {
		if table == "" {
			internal.Logger.Info("Cloning MySQL schema")
			if err := cloner.CloneSchema(); err != nil {
				return fmt.Errorf("failed to clone schema: %w", err)
			}
		}
	}

	if !schemaOnly {
		internal.Logger.Info("Cloning MySQL data")

		if whereClause != "" && table != "" {
			if err := cloner.EnsureTableExists(table); err != nil {
				return fmt.Errorf("failed to ensure table exists: %w", err)
			}
			if err := cloner.CloneWithFilter(table, whereClause); err != nil {
				return fmt.Errorf("failed to clone table with filter: %w", err)
			}
		} else if table != "" {
			if err := cloner.EnsureTableExists(table); err != nil {
				return fmt.Errorf("failed to ensure table exists: %w", err)
			}
			if err := cloner.CloneData([]string{table}); err != nil {
				return fmt.Errorf("failed to clone table: %w", err)
			}
		} else {
			if err := cloner.CloneData(nil); err != nil {
				return fmt.Errorf("failed to clone data: %w", err)
			}
		}
	}

	return nil
}

func cloneDynamoDBData(cfg *config.Config, source, dest, table, componentName, filter string, concurrency int, interactive bool) error {
	sourceConn, err := config.ParseConnectionString(source)
	if err != nil {
		return fmt.Errorf("invalid source connection string: %w", err)
	}

	destConn, err := config.ParseConnectionString(dest)
	if err != nil {
		return fmt.Errorf("invalid destination connection string: %w", err)
	}

	sourceConfig, err := cfg.GetDynamoDBConfig(sourceConn.Client, sourceConn.Env)
	if err != nil {
		return fmt.Errorf("failed to get source DynamoDB config: %w", err)
	}

	destConfig, err := cfg.GetDynamoDBConfig(destConn.Client, destConn.Env)
	if err != nil {
		return fmt.Errorf("failed to get destination DynamoDB config: %w", err)
	}

	start := time.Now()
	defer func() {
		internal.Logger.Info("DynamoDB clone completed", "duration", time.Since(start))
	}()

	ctx := context.Background()

	sourcePrefix := fmt.Sprintf("%s.%s", sourceConn.Client, sourceConn.Env)
	destPrefix := fmt.Sprintf("%s.%s", destConn.Client, destConn.Env)
	
	if componentName != "" {
		sourcePrefix = fmt.Sprintf("%s.%s", sourcePrefix, componentName)
		destPrefix = fmt.Sprintf("%s.%s", destPrefix, componentName)
	}
	
	internal.Logger.Debug("Starting prefix-based DynamoDB clone", 
		"source_prefix", sourcePrefix, 
		"dest_prefix", destPrefix)
	
	cloner, err := dynamodb.NewCloner(*sourceConfig, *destConfig)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB cloner: %w", err)
	}

	options := dynamodb.CloneOptions{
		Concurrency:  concurrency,
		BatchSize:    25,
		SourcePrefix: sourcePrefix,
		DestPrefix:   destPrefix,
	}

	if filter != "" {
		processedFilter, attributeValues, err := processFilterExpression(filter)
		if err != nil {
			internal.Logger.Warn("Filter expression processing warning", "filter", filter, "error", err)
			options.FilterExpression = &filter
		} else {
			options.FilterExpression = &processedFilter
			if len(attributeValues) > 0 {
				options.ExpressionAttributeValues = attributeValues
				internal.Logger.Debug("Auto-bound filter values", "filter", processedFilter, "values", attributeValues)
			}
		}
		
		if strings.Contains(filter, "#") {
			internal.Logger.Warn("Filter expression contains attribute names (#). Please ensure they are properly configured.")
		}
	}

	if interactive && componentName != "" {
		if err := cloner.CloneTablesWithPrefixInteractive(ctx, options); err != nil {
			return fmt.Errorf("failed to clone DynamoDB tables with interactive selection: %w", err)
		}
	} else {
		if err := cloner.CloneTablesWithPrefix(ctx, options); err != nil {
			return fmt.Errorf("failed to clone DynamoDB tables with prefix: %w", err)
		}
	}

	return nil
}

func processFilterExpression(filter string) (string, map[string]types.AttributeValue, error) {
	
	
	attributeValues := make(map[string]types.AttributeValue)
	processedFilter := filter
	valueCounter := 1
	
	patterns := []struct {
		regex   string
		handler func(matches []string) (string, types.AttributeValue)
	}{
		{
			regex: `(\w+)\s*=\s*'([^']*)'`,
			handler: func(matches []string) (string, types.AttributeValue) {
				return matches[2], &types.AttributeValueMemberS{Value: matches[2]}
			},
		},
		{
			regex: `(\w+)\s*=\s*"([^"]*)"`,
			handler: func(matches []string) (string, types.AttributeValue) {
				return matches[2], &types.AttributeValueMemberS{Value: matches[2]}
			},
		},
		{
			regex: `(\w+)\s*=\s*(\d+(?:\.\d+)?)`,
			handler: func(matches []string) (string, types.AttributeValue) {
				return matches[2], &types.AttributeValueMemberN{Value: matches[2]}
			},
		},
	}
	
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern.regex)
		matches := re.FindAllStringSubmatch(processedFilter, -1)
		
		for _, match := range matches {
			if len(match) >= 3 {
				attributeName := match[1]
				_, attributeValue := pattern.handler(match)
				
				paramName := fmt.Sprintf(":val%d", valueCounter)
				attributeValues[paramName] = attributeValue
				
				originalExpression := match[0]
				newExpression := fmt.Sprintf("%s = %s", attributeName, paramName)
				processedFilter = strings.Replace(processedFilter, originalExpression, newExpression, 1)
				
				valueCounter++
			}
		}
	}
	
	return processedFilter, attributeValues, nil
}

func init() {
	rootCmd.AddCommand(cloneCmd)

	cloneCmd.Flags().String("source", "", "Source in format client/env (required)")
	cloneCmd.Flags().String("dest", "", "Destination in format client/env (required)")
	cloneCmd.MarkFlagRequired("source")
	cloneCmd.MarkFlagRequired("dest")

	cloneCmd.Flags().String("table", "", "Optional table name (MySQL only)")
	cloneCmd.Flags().String("component-name", "", "Optional component name for prefix-based cloning (DynamoDB only, e.g., connectors-data-api)")
	cloneCmd.Flags().String("filter", "", "Optional filter expression (DynamoDB only, e.g., \"DocumentType = 'CatalogStyle'\")")
	cloneCmd.Flags().String("where", "", "Optional WHERE clause (MySQL only, requires --table, e.g., \"created_at > '2024-01-01'\")")
	cloneCmd.Flags().Bool("schema-only", false, "Clone schema only (MySQL)")
	cloneCmd.Flags().Bool("data-only", false, "Clone data only (MySQL)")
	cloneCmd.Flags().Int("concurrency", 25, "Number of concurrent workers for DynamoDB")
	cloneCmd.Flags().Bool("verbose", false, "Enable verbose logging")
	cloneCmd.Flags().Bool("interactive", false, "Enable interactive checkbox selection when using --component-name")
}

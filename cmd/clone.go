package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"apdata/config"
	"apdata/dynamodb"
	"apdata/internal"
	"apdata/mysql"
)

var cloneCmd = &cobra.Command{
	Use:           "clone [mysql|dynamodb|all]",
	Short:         "Clone data from cloud to local",
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
	filter, _ := cmd.Flags().GetString("filter")
	schemaOnly, _ := cmd.Flags().GetBool("schema-only")
	dataOnly, _ := cmd.Flags().GetBool("data-only")
	whereClause, _ := cmd.Flags().GetString("where")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	verbose, _ := cmd.Flags().GetBool("verbose")

	if verbose {
		internal.SetLogLevel("debug")
	} else {
		internal.SetLogLevel("error")
	}

	if source == "" || dest == "" {
		return fmt.Errorf("both --source and --dest are required")
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
		if err := cloneDynamoDBData(cfg, source, dest, table, filter, concurrency); err != nil {
			return formatError(err)
		}
	case "all":
		if err := cloneMySQLData(cfg, source, dest, table, whereClause, schemaOnly, dataOnly); err != nil {
			return formatError(err)
		}
		if err := cloneDynamoDBData(cfg, source, dest, table, filter, concurrency); err != nil {
			return formatError(err)
		}
	default:
		return formatError(fmt.Errorf("unsupported clone type: %s", cloneType))
	}

	return nil
}

func formatError(err error) error {
	errStr := err.Error()

	if strings.Contains(errStr, "connection refused") {
		return fmt.Errorf("❌ Cannot connect to MySQL server. Please check your connection settings.")
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
		internal.Logger.Info("Cloning MySQL schema")
		if err := cloner.CloneSchema(); err != nil {
			return fmt.Errorf("failed to clone schema: %w", err)
		}
	}

	if !schemaOnly {
		internal.Logger.Info("Cloning MySQL data")

		if whereClause != "" && table != "" {
			if err := cloner.CloneWithFilter(table, whereClause); err != nil {
				return fmt.Errorf("failed to clone table with filter: %w", err)
			}
		} else if table != "" {
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

func cloneDynamoDBData(cfg *config.Config, source, dest, table, filter string, concurrency int) error {
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

	if table != "" {
		sourceConfig.TableName = table
		destConfig.TableName = table
	}

	cloner, err := dynamodb.NewCloner(*sourceConfig, *destConfig)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB cloner: %w", err)
	}

	start := time.Now()
	defer func() {
		internal.Logger.Info("DynamoDB clone completed", "duration", time.Since(start))
	}()

	ctx := context.Background()

	internal.Logger.Info("Cloning DynamoDB table structure")
	if err := cloner.CloneTableStructure(ctx); err != nil {
		internal.Logger.Warn("Failed to clone table structure (table might already exist)", "error", err)
	}

	options := dynamodb.CloneOptions{
		Concurrency: concurrency,
		BatchSize:   25,
	}

	if filter != "" {
		options.FilterExpression = &filter
		if strings.Contains(filter, ":") || strings.Contains(filter, "#") {
			internal.Logger.Warn("Filter expression contains attribute names/values. Please ensure they are properly configured.")
		}
	}

	err = internal.WithSpinnerConditional("Getting item count", func() error {
		count, err := cloner.GetItemCount(ctx, options.FilterExpression)
		if err != nil {
			internal.Logger.Warn("Failed to get item count", "error", err)
		} else {
			internal.Logger.Info("Cloning DynamoDB data", "itemCount", count)
		}
		return nil
	}, !internal.VerboseMode)
	if err != nil {
		return err
	}

	if err := cloner.CloneTable(ctx, options); err != nil {
		return fmt.Errorf("failed to clone DynamoDB table: %w", err)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(cloneCmd)

	cloneCmd.Flags().String("source", "", "Source in format client/env (required)")
	cloneCmd.Flags().String("dest", "", "Destination in format client/env (required)")
	cloneCmd.MarkFlagRequired("source")
	cloneCmd.MarkFlagRequired("dest")

	cloneCmd.Flags().String("table", "", "Optional table name")
	cloneCmd.Flags().String("filter", "", "Optional filter expression for DynamoDB")
	cloneCmd.Flags().String("where", "", "Optional WHERE clause for MySQL")
	cloneCmd.Flags().Bool("schema-only", false, "Clone schema only (MySQL)")
	cloneCmd.Flags().Bool("data-only", false, "Clone data only (MySQL)")
	cloneCmd.Flags().Int("concurrency", 10, "Number of concurrent workers for DynamoDB")
	cloneCmd.Flags().Bool("verbose", false, "Enable verbose logging")
}

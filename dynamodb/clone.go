package dynamodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Config struct {
	Region    string
	TableName string
	Endpoint  string // Optional for local DynamoDB
}

type Cloner struct {
	SourceConfig Config
	DestConfig   Config
	SourceClient *dynamodb.Client
	DestClient   *dynamodb.Client
}

type CloneOptions struct {
	FilterExpression     *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	Concurrency         int
	BatchSize           int
}

func NewCloner(sourceConfig, destConfig Config) (*Cloner, error) {
	sourceClient, err := createClient(sourceConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create source client: %w", err)
	}

	destClient, err := createClient(destConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination client: %w", err)
	}

	return &Cloner{
		SourceConfig: sourceConfig,
		DestConfig:   destConfig,
		SourceClient: sourceClient,
		DestClient:   destClient,
	}, nil
}

func createClient(cfg Config) (*dynamodb.Client, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, err
	}

	client := dynamodb.NewFromConfig(awsConfig)
	
	// Use custom endpoint if provided (for local DynamoDB)
	if cfg.Endpoint != "" {
		client = dynamodb.NewFromConfig(awsConfig, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	return client, nil
}

// CloneTable clones a DynamoDB table with optional filtering and parallel processing
func (c *Cloner) CloneTable(ctx context.Context, options CloneOptions) error {
	// Set defaults
	if options.Concurrency == 0 {
		options.Concurrency = 10
	}
	if options.BatchSize == 0 {
		options.BatchSize = 25 // DynamoDB batch write limit
	}

	// Create channels for parallel processing
	itemChan := make(chan map[string]types.AttributeValue, options.Concurrency*2)
	errorChan := make(chan error, options.Concurrency)
	
	// Start scanner goroutine
	go func() {
		defer close(itemChan)
		err := c.scanTable(ctx, itemChan, options)
		if err != nil {
			errorChan <- err
		}
	}()

	// Start writer goroutines
	var wg sync.WaitGroup
	for i := 0; i < options.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.writeItems(ctx, itemChan, options.BatchSize)
			if err != nil {
				errorChan <- err
			}
		}()
	}

	// Wait for all writers to complete
	go func() {
		wg.Wait()
		close(errorChan)
	}()

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// scanTable scans the source table and sends items to the channel
func (c *Cloner) scanTable(ctx context.Context, itemChan chan<- map[string]types.AttributeValue, options CloneOptions) error {
	input := &dynamodb.ScanInput{
		TableName: aws.String(c.SourceConfig.TableName),
	}

	// Add filter expression if provided
	if options.FilterExpression != nil {
		input.FilterExpression = options.FilterExpression
		input.ExpressionAttributeNames = options.ExpressionAttributeNames
		input.ExpressionAttributeValues = options.ExpressionAttributeValues
	}

	// Use parallel scan if no filter is applied (more efficient for full table scans)
	if options.FilterExpression == nil {
		return c.parallelScan(ctx, itemChan, input, options.Concurrency)
	}

	// Sequential scan with filter
	paginator := dynamodb.NewScanPaginator(c.SourceClient, input)
	
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to scan page: %w", err)
		}

		for _, item := range page.Items {
			select {
			case itemChan <- item:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// parallelScan performs parallel scanning for better performance on large tables
func (c *Cloner) parallelScan(ctx context.Context, itemChan chan<- map[string]types.AttributeValue, baseInput *dynamodb.ScanInput, segments int) error {
	var wg sync.WaitGroup
	errChan := make(chan error, segments)

	for segment := 0; segment < segments; segment++ {
		wg.Add(1)
		go func(seg int) {
			defer wg.Done()
			
			input := *baseInput
			input.Segment = aws.Int32(int32(seg))
			input.TotalSegments = aws.Int32(int32(segments))

			paginator := dynamodb.NewScanPaginator(c.SourceClient, &input)
			
			for paginator.HasMorePages() {
				page, err := paginator.NextPage(ctx)
				if err != nil {
					errChan <- fmt.Errorf("failed to scan segment %d: %w", seg, err)
					return
				}

				for _, item := range page.Items {
					select {
					case itemChan <- item:
					case <-ctx.Done():
						errChan <- ctx.Err()
						return
					}
				}
			}
		}(segment)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// writeItems batches items and writes them to the destination table
func (c *Cloner) writeItems(ctx context.Context, itemChan <-chan map[string]types.AttributeValue, batchSize int) error {
	batch := make([]types.WriteRequest, 0, batchSize)

	for item := range itemChan {
		batch = append(batch, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})

		if len(batch) >= batchSize {
			if err := c.writeBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Write remaining items
	if len(batch) > 0 {
		return c.writeBatch(ctx, batch)
	}

	return nil
}

// writeBatch writes a batch of items with retry logic for unprocessed items
func (c *Cloner) writeBatch(ctx context.Context, batch []types.WriteRequest) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			c.DestConfig.TableName: batch,
		},
	}

	maxRetries := 3
	backoff := time.Second

	for retry := 0; retry < maxRetries; retry++ {
		result, err := c.DestClient.BatchWriteItem(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}

		// Check for unprocessed items
		unprocessed := result.UnprocessedItems[c.DestConfig.TableName]
		if len(unprocessed) == 0 {
			return nil
		}

		// Retry with unprocessed items
		input.RequestItems[c.DestConfig.TableName] = unprocessed
		
		// Exponential backoff
		time.Sleep(backoff)
		backoff *= 2
	}

	return fmt.Errorf("failed to write batch after %d retries", maxRetries)
}

// GetItemCount returns the number of items in the source table
func (c *Cloner) GetItemCount(ctx context.Context, filterExpression *string) (int64, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(c.SourceConfig.TableName),
		Select:    types.SelectCount,
	}

	if filterExpression != nil {
		input.FilterExpression = filterExpression
	}

	var totalCount int64
	paginator := dynamodb.NewScanPaginator(c.SourceClient, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to count items: %w", err)
		}
		totalCount += int64(page.Count)
	}

	return totalCount, nil
}

// CloneTableStructure creates the destination table with the same structure as source
func (c *Cloner) CloneTableStructure(ctx context.Context) error {
	// Describe source table
	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(c.SourceConfig.TableName),
	}

	result, err := c.SourceClient.DescribeTable(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to describe source table: %w", err)
	}

	table := result.Table

	// Create destination table
	createInput := &dynamodb.CreateTableInput{
		TableName:              aws.String(c.DestConfig.TableName),
		KeySchema:              table.KeySchema,
		AttributeDefinitions:   table.AttributeDefinitions,
		BillingMode:           types.BillingModePayPerRequest, // Use on-demand for simplicity
	}

	// Add GSIs if they exist
	if len(table.GlobalSecondaryIndexes) > 0 {
		createInput.GlobalSecondaryIndexes = make([]types.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
		for i, gsi := range table.GlobalSecondaryIndexes {
			createInput.GlobalSecondaryIndexes[i] = types.GlobalSecondaryIndex{
				IndexName:  gsi.IndexName,
				KeySchema:  gsi.KeySchema,
				Projection: gsi.Projection,
			}
		}
	}

	// Add LSIs if they exist
	if len(table.LocalSecondaryIndexes) > 0 {
		createInput.LocalSecondaryIndexes = make([]types.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
		for i, lsi := range table.LocalSecondaryIndexes {
			createInput.LocalSecondaryIndexes[i] = types.LocalSecondaryIndex{
				IndexName:  lsi.IndexName,
				KeySchema:  lsi.KeySchema,
				Projection: lsi.Projection,
			}
		}
	}

	_, err = c.DestClient.CreateTable(ctx, createInput)
	if err != nil {
		return fmt.Errorf("failed to create destination table: %w", err)
	}

	// Wait for table to be active
	waiter := dynamodb.NewTableExistsWaiter(c.DestClient)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.DestConfig.TableName),
	}, 5*time.Minute)
}
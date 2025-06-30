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

	"apdata/internal"
)

type Config struct {
	Region    string
	TableName string
	Endpoint  string
}

type Cloner struct {
	SourceConfig Config
	DestConfig   Config
	SourceClient *dynamodb.Client
	DestClient   *dynamodb.Client
}

type CloneOptions struct {
	FilterExpression          *string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	Concurrency               int
	BatchSize                 int
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

	if cfg.Endpoint != "" {
		client = dynamodb.NewFromConfig(awsConfig, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	return client, nil
}

func (c *Cloner) CloneTable(ctx context.Context, options CloneOptions) error {
	if options.Concurrency == 0 {
		options.Concurrency = 10
	}
	if options.BatchSize == 0 {
		options.BatchSize = 25
	}

	internal.Logger.Debug("Starting DynamoDB clone", "table", c.SourceConfig.TableName, "concurrency", options.Concurrency, "batchSize", options.BatchSize)

	var spinner *internal.Spinner
	if !internal.VerboseMode {
		spinner = internal.NewSpinner(fmt.Sprintf("Cloning DynamoDB table %s", c.SourceConfig.TableName))
		spinner.Start()
	}

	itemChan := make(chan map[string]types.AttributeValue, options.Concurrency*2)
	errorChan := make(chan error, options.Concurrency)

	go func() {
		defer close(itemChan)
		err := c.scanTable(ctx, itemChan, options)
		if err != nil {
			errorChan <- err
		}
	}()

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

	go func() {
		wg.Wait()
		close(errorChan)
	}()

	for err := range errorChan {
		if err != nil {
			if spinner != nil {
				spinner.Error(fmt.Sprintf("Failed to clone table %s", c.SourceConfig.TableName))
			}
			return err
		}
	}

	if spinner != nil {
		spinner.Success(fmt.Sprintf("DynamoDB table %s cloned successfully", c.SourceConfig.TableName))
	}
	return nil
}

func (c *Cloner) scanTable(ctx context.Context, itemChan chan<- map[string]types.AttributeValue, options CloneOptions) error {
	input := &dynamodb.ScanInput{
		TableName: aws.String(c.SourceConfig.TableName),
	}

	if options.FilterExpression != nil {
		input.FilterExpression = options.FilterExpression
		input.ExpressionAttributeNames = options.ExpressionAttributeNames
		input.ExpressionAttributeValues = options.ExpressionAttributeValues
	}

	if options.FilterExpression == nil {
		internal.Logger.Debug("Using parallel scan (no filter)", "segments", options.Concurrency)
		return c.parallelScan(ctx, itemChan, input, options.Concurrency)
	} else {
		internal.Logger.Debug("Using sequential scan with filter", "filter", *options.FilterExpression)
	}

	paginator := dynamodb.NewScanPaginator(c.SourceClient, input)

	pageCount := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to scan page: %w", err)
		}
		pageCount++
		internal.Logger.Debug("Processed scan page", "page", pageCount, "items", len(page.Items))

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

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

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

	if len(batch) > 0 {
		return c.writeBatch(ctx, batch)
	}

	return nil
}

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

		unprocessed := result.UnprocessedItems[c.DestConfig.TableName]
		if len(unprocessed) == 0 {
			internal.Logger.Debug("Batch write completed", "items", len(batch))
			return nil
		}

		internal.Logger.Debug("Retrying unprocessed items", "retry", retry+1, "unprocessed", len(unprocessed))

		input.RequestItems[c.DestConfig.TableName] = unprocessed

		time.Sleep(backoff)
		backoff *= 2
	}

	return fmt.Errorf("failed to write batch after %d retries", maxRetries)
}

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

func (c *Cloner) CloneTableStructure(ctx context.Context) error {
	internal.Logger.Debug("Cloning table structure", "sourceTable", c.SourceConfig.TableName, "destTable", c.DestConfig.TableName)

	var spinner *internal.Spinner
	if !internal.VerboseMode {
		spinner = internal.NewSpinner(fmt.Sprintf("Creating table structure for %s", c.DestConfig.TableName))
		spinner.Start()
	}

	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(c.SourceConfig.TableName),
	}

	result, err := c.SourceClient.DescribeTable(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to describe source table: %w", err)
	}

	table := result.Table

	createInput := &dynamodb.CreateTableInput{
		TableName:            aws.String(c.DestConfig.TableName),
		KeySchema:            table.KeySchema,
		AttributeDefinitions: table.AttributeDefinitions,
		BillingMode:          types.BillingModePayPerRequest,
	}

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

	internal.Logger.Debug("Destination table created, waiting for it to become active")
	if spinner != nil {
		spinner.UpdateMessage(fmt.Sprintf("Waiting for table %s to become active", c.DestConfig.TableName))
	}

	waiter := dynamodb.NewTableExistsWaiter(c.DestClient)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.DestConfig.TableName),
	}, 5*time.Minute)

	if err == nil {
		if spinner != nil {
			spinner.Success(fmt.Sprintf("Table structure created: %s", c.DestConfig.TableName))
		}
		internal.Logger.Debug("Table structure cloned successfully")
	} else {
		if spinner != nil {
			spinner.Error(fmt.Sprintf("Failed to create table structure: %s", c.DestConfig.TableName))
		}
	}
	return err
}


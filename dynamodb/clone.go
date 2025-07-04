package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"apdata/internal"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type Config struct {
	Region string
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
	ProgressCallback          func(processed, total int64)
	SourcePrefix              string
	DestPrefix                string
}

type CloneMetrics struct {
	StartTime        time.Time
	ItemsProcessed   int64
	ItemsTotal       int64
	BytesProcessed   int64
	ErrorCount       int64
	RetryCount       int64
	SegmentsActive   int32
	BatchesWritten   int64
	AvgItemSize      int64
	ThroughputPerSec float64
	ThrottleCount    int64
	LastThrottleTime time.Time
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
		return nil, fmt.Errorf("failed to load AWS config - ensure AWS credentials are configured via 'aws configure' or environment variables: %w", err)
	}

	creds, err := awsConfig.Credentials.Retrieve(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve AWS credentials - ensure AWS credentials are configured via 'aws configure' or environment variables: %w", err)
	}
	
	if creds.AccessKeyID == "" {
		return nil, fmt.Errorf("AWS credentials not found - please run 'aws configure' to set up your credentials")
	}

	internal.Logger.Debug("AWS credentials validated", "region", cfg.Region, "access_key_id", creds.AccessKeyID[:8]+"...")

	client := dynamodb.NewFromConfig(awsConfig)
	return client, nil
}

func (c *Cloner) CloneTable(ctx context.Context, sourceTableName, destTableName string, options CloneOptions) error {
	return c.cloneTableWithSpinner(ctx, sourceTableName, destTableName, options, true)
}

func (c *Cloner) cloneTableWithSpinner(ctx context.Context, sourceTableName, destTableName string, options CloneOptions, showSpinner bool) error {
	totalStart := time.Now()
	internal.Logger.Debug("Starting DynamoDB clone operation", "source_table", sourceTableName, "dest_table", destTableName)

	if options.Concurrency == 0 {
		options.Concurrency = c.getOptimalConcurrency()
	}
	if options.BatchSize == 0 {
		options.BatchSize = c.getOptimalBatchSize()
	}

	metrics := &CloneMetrics{
		StartTime: totalStart,
	}

	var itemCountErr error
	err := internal.SimpleSpinner("Analyzing table size", func() error {
		metrics.ItemsTotal, itemCountErr = c.GetItemCount(ctx, sourceTableName, options.FilterExpression)
		return itemCountErr
	})
	if err != nil {
		internal.Logger.Warn("Could not get item count, proceeding without progress tracking", "error", err)
		metrics.ItemsTotal = -1
	}

	if metrics.ItemsTotal > 0 && options.Concurrency == c.getOptimalConcurrency() {
		optimizedConcurrency := c.getOptimalConcurrencyForTable(metrics.ItemsTotal)
		if optimizedConcurrency > options.Concurrency {
			internal.Logger.Debug("Optimizing concurrency for large table", 
				"original_concurrency", options.Concurrency,
				"optimized_concurrency", optimizedConcurrency,
				"item_count", metrics.ItemsTotal)
			options.Concurrency = optimizedConcurrency
		}
	}
	
	if metrics.ItemsTotal > 500000 {
		conservativeConcurrency := min(options.Concurrency, 15)
		internal.Logger.Debug("Using conservative concurrency for very large table", 
			"original_concurrency", options.Concurrency,
			"conservative_concurrency", conservativeConcurrency,
			"item_count", metrics.ItemsTotal,
			"reason", "reduce_throttling")
		options.Concurrency = conservativeConcurrency
	}

	internal.Logger.Debug("Table analysis completed", 
		"source_table", sourceTableName,
		"dest_table", destTableName,
		"estimated_items", metrics.ItemsTotal,
		"concurrency", options.Concurrency,
		"batch_size", options.BatchSize)

	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()
	
	if metrics.ItemsTotal > 0 {
		go c.monitorProgressWithSpinner(progressCtx, sourceTableName, metrics, options.ProgressCallback, showSpinner)
	}

	channelBuffer := options.Concurrency * options.BatchSize
	if metrics.ItemsTotal > 50000 {
		channelBuffer = channelBuffer * 2
	}
	
	itemChan := make(chan map[string]types.AttributeValue, channelBuffer)
	errorChan := make(chan error, options.Concurrency+1)

	go func() {
		defer close(itemChan)
		scanErr := c.scanTableWithMetrics(ctx, sourceTableName, itemChan, options, metrics)
		if scanErr != nil {
			atomic.AddInt64(&metrics.ErrorCount, 1)
			errorChan <- fmt.Errorf("scan error: %w", scanErr)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < options.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			writeErr := c.writeItemsWithMetrics(ctx, destTableName, itemChan, options, metrics, workerID)
			if writeErr != nil {
				atomic.AddInt64(&metrics.ErrorCount, 1)
				errorChan <- fmt.Errorf("writer %d error: %w", workerID, writeErr)
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(errorChan)
	}()

	var cloneErr error
	for err := range errorChan {
		if err != nil {
			cloneErr = err
			break
		}
	}

	totalDuration := time.Since(totalStart)
	if metrics.ItemsProcessed > 0 {
		metrics.ThroughputPerSec = float64(metrics.ItemsProcessed) / totalDuration.Seconds()
		if metrics.BytesProcessed > 0 {
			metrics.AvgItemSize = metrics.BytesProcessed / metrics.ItemsProcessed
		}
	}

	c.logCloneResults(metrics, totalDuration, cloneErr)

	internal.FinishLine()

	return cloneErr
}


func (c *Cloner) GetItemCount(ctx context.Context, tableName string, filterExpression *string) (int64, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
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

func (c *Cloner) CloneTableStructure(ctx context.Context, sourceTableName, destTableName string) error {
	return c.cloneTableStructureWithSpinner(ctx, sourceTableName, destTableName, true)
}

func (c *Cloner) cloneTableStructureWithSpinner(ctx context.Context, sourceTableName, destTableName string, showSpinner bool) error {
	internal.Logger.Debug("Cloning table structure", "sourceTable", sourceTableName, "destTable", destTableName)

	var spinner *internal.Spinner
	if !internal.VerboseMode && showSpinner {
		spinner = internal.NewSpinner(fmt.Sprintf("Creating table structure for %s", destTableName))
		spinner.Start()
	}

	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(sourceTableName),
	}

	result, err := c.SourceClient.DescribeTable(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to describe source table: %w", err)
	}

	table := result.Table

	createInput := &dynamodb.CreateTableInput{
		TableName:            aws.String(destTableName),
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
		if strings.Contains(err.Error(), "ResourceInUseException") && strings.Contains(err.Error(), "Table already exists") {
			internal.Logger.Debug("Destination table already exists, skipping creation", "destTable", destTableName)
			
			if spinner != nil {
				spinner.UpdateMessage(fmt.Sprintf("Table %s already exists, verifying availability", destTableName))
			}
			
			waiter := dynamodb.NewTableExistsWaiter(c.DestClient)
			err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(destTableName),
			}, 5*time.Minute)
			
			if err == nil {
				if spinner != nil {
					spinner.Success(fmt.Sprintf("Table structure ready: %s", destTableName))
				}
				internal.Logger.Debug("Existing table structure verified successfully")
				return nil
			} else {
				if spinner != nil {
					spinner.Error(fmt.Sprintf("Failed to verify existing table: %s", destTableName))
				}
				return fmt.Errorf("failed to verify existing table: %w", err)
			}
		}
		return fmt.Errorf("failed to create destination table: %w", err)
	}

	internal.Logger.Debug("Destination table created, waiting for it to become active")
	if spinner != nil {
		spinner.UpdateMessage(fmt.Sprintf("Waiting for table %s to become active", destTableName))
	}

	waiter := dynamodb.NewTableExistsWaiter(c.DestClient)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(destTableName),
	}, 5*time.Minute)

	if err == nil {
		if spinner != nil {
			spinner.Success(fmt.Sprintf("Table structure created: %s", destTableName))
		}
		internal.Logger.Debug("Table structure cloned successfully")
	} else {
		if spinner != nil {
			spinner.Error(fmt.Sprintf("Failed to create table structure: %s", destTableName))
		}
	}
	return err
}

func (c *Cloner) getOptimalConcurrency() int {
	return 25
}

func (c *Cloner) getOptimalConcurrencyForTable(itemCount int64) int {
	switch {
	case itemCount > 100000:
		return 75
	case itemCount > 50000:
		return 50
	case itemCount > 10000:
		return 35
	case itemCount > 1000:
		return 25
	default:
		return 15
	}
}

func (c *Cloner) getOptimalBatchSize() int {
	return 25
}

func (c *Cloner) monitorProgress(ctx context.Context, sourceTableName string, metrics *CloneMetrics, callback func(processed, total int64)) {
	c.monitorProgressWithSpinner(ctx, sourceTableName, metrics, callback, true)
}

func (c *Cloner) monitorProgressWithSpinner(ctx context.Context, sourceTableName string, metrics *CloneMetrics, callback func(processed, total int64), showSpinner bool) {
	monitoringInterval := 2 * time.Second
	if metrics.ItemsTotal > 50000 {
		monitoringInterval = 5 * time.Second
	}
	
	ticker := time.NewTicker(monitoringInterval)
	defer ticker.Stop()

	var spinner *internal.Spinner
	if !internal.VerboseMode && showSpinner {
		spinner = internal.NewSpinner(fmt.Sprintf("Cloning DynamoDB table %s", sourceTableName))
		spinner.Start()
	}

	lastProcessed := int64(0)
	
	for {
		select {
		case <-ctx.Done():
			if spinner != nil {
				if metrics.ErrorCount > 0 {
					spinner.Error(fmt.Sprintf("Failed to clone table %s", sourceTableName))
				} else {
					spinner.Success(fmt.Sprintf("DynamoDB table %s cloned successfully", sourceTableName))
				}
			}
			return
		case <-ticker.C:
			processed := atomic.LoadInt64(&metrics.ItemsProcessed)
			
			if metrics.ItemsTotal > 0 {
				progress := float64(processed) / float64(metrics.ItemsTotal) * 100
				
				itemsSinceLastCheck := processed - lastProcessed
				throughputPerSec := float64(itemsSinceLastCheck) / 2.0
				
				if spinner != nil {
					spinner.UpdateMessage(fmt.Sprintf("Cloning %s: %.1f%% (%d/%d items, %.0f items/sec)", 
						sourceTableName, progress, processed, metrics.ItemsTotal, throughputPerSec))
				}
				
				internal.Logger.Debug("Clone progress", 
					"table", sourceTableName,
					"progress_pct", fmt.Sprintf("%.1f%%", progress),
					"processed", processed,
					"total", metrics.ItemsTotal,
					"throughput_per_sec", fmt.Sprintf("%.0f", throughputPerSec),
					"active_segments", atomic.LoadInt32(&metrics.SegmentsActive))
			} else {
				if spinner != nil {
					spinner.UpdateMessage(fmt.Sprintf("Cloning %s: %d items processed", 
						sourceTableName, processed))
				}
			}
			
			if callback != nil {
				callback(processed, metrics.ItemsTotal)
			}
			
			lastProcessed = processed
		}
	}
}

func (c *Cloner) scanTableWithMetrics(ctx context.Context, sourceTableName string, itemChan chan<- map[string]types.AttributeValue, options CloneOptions, metrics *CloneMetrics) error {
	input := &dynamodb.ScanInput{
		TableName: aws.String(sourceTableName),
	}

	if options.FilterExpression != nil {
		input.FilterExpression = options.FilterExpression
		input.ExpressionAttributeNames = options.ExpressionAttributeNames
		input.ExpressionAttributeValues = options.ExpressionAttributeValues
	}

	if options.FilterExpression == nil {
		segments := options.Concurrency
		if metrics.ItemsTotal > 100000 {
			segments = min(segments*2, 100)
		} else if metrics.ItemsTotal > 50000 {
			segments = min(segments+10, 50)
		}
		
		internal.Logger.Debug("Using parallel scan for optimal performance", 
			"segments", segments, 
			"table_size", metrics.ItemsTotal,
			"concurrency", options.Concurrency)
		return c.parallelScanWithMetrics(ctx, itemChan, input, segments, metrics)
	} else {
		internal.Logger.Debug("Using sequential scan with filter", "filter", *options.FilterExpression)
		return c.sequentialScanWithMetrics(ctx, itemChan, input, metrics)
	}
}

func (c *Cloner) parallelScanWithMetrics(ctx context.Context, itemChan chan<- map[string]types.AttributeValue, baseInput *dynamodb.ScanInput, segments int, metrics *CloneMetrics) error {
	var wg sync.WaitGroup
	errChan := make(chan error, segments)

	for segment := 0; segment < segments; segment++ {
		wg.Add(1)
		atomic.AddInt32(&metrics.SegmentsActive, 1)
		
		go func(seg int) {
			defer wg.Done()
			defer atomic.AddInt32(&metrics.SegmentsActive, -1)

			internal.Logger.Debug("Starting scan segment", "segment", seg, "total_segments", segments)

			input := *baseInput
			input.Segment = aws.Int32(int32(seg))
			input.TotalSegments = aws.Int32(int32(segments))

			paginator := dynamodb.NewScanPaginator(c.SourceClient, &input)
			pageCount := 0

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(ctx)
				if err != nil {
					errChan <- fmt.Errorf("failed to scan segment %d page %d: %w", seg, pageCount, err)
					return
				}
				pageCount++

				internal.Logger.Debug("Processed scan page", 
					"segment", seg, 
					"page", pageCount, 
					"items", len(page.Items),
					"scanned_count", page.ScannedCount,
					"consumed_capacity", page.ConsumedCapacity)

				for _, item := range page.Items {
					itemSize := c.estimateItemSize(item)
					atomic.AddInt64(&metrics.BytesProcessed, itemSize)
					
					select {
					case itemChan <- item:
					case <-ctx.Done():
						errChan <- ctx.Err()
						return
					}
				}
			}

			internal.Logger.Debug("Segment scan completed", "segment", seg, "pages", pageCount)
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

func (c *Cloner) sequentialScanWithMetrics(ctx context.Context, itemChan chan<- map[string]types.AttributeValue, input *dynamodb.ScanInput, metrics *CloneMetrics) error {
	paginator := dynamodb.NewScanPaginator(c.SourceClient, input)
	pageCount := 0

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to scan page %d: %w", pageCount, err)
		}
		pageCount++

		internal.Logger.Debug("Processed scan page", 
			"page", pageCount, 
			"items", len(page.Items),
			"scanned_count", page.ScannedCount)

		for _, item := range page.Items {
			itemSize := c.estimateItemSize(item)
			atomic.AddInt64(&metrics.BytesProcessed, itemSize)
			
			select {
			case itemChan <- item:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	internal.Logger.Debug("Sequential scan completed", "pages", pageCount)
	return nil
}

func (c *Cloner) writeItemsWithMetrics(ctx context.Context, destTableName string, itemChan <-chan map[string]types.AttributeValue, options CloneOptions, metrics *CloneMetrics, workerID int) error {
	batch := make([]types.WriteRequest, 0, options.BatchSize)
	batchCount := 0

	internal.Logger.Debug("Writer started", "worker_id", workerID)

	for item := range itemChan {
		batch = append(batch, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})

		if len(batch) >= options.BatchSize {
			batchCount++
			if err := c.writeBatchWithMetrics(ctx, destTableName, batch, metrics, workerID, batchCount); err != nil {
				return fmt.Errorf("worker %d batch %d error: %w", workerID, batchCount, err)
			}
			atomic.AddInt64(&metrics.ItemsProcessed, int64(len(batch)))
			atomic.AddInt64(&metrics.BatchesWritten, 1)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		batchCount++
		if err := c.writeBatchWithMetrics(ctx, destTableName, batch, metrics, workerID, batchCount); err != nil {
			return fmt.Errorf("worker %d final batch error: %w", workerID, err)
		}
		atomic.AddInt64(&metrics.ItemsProcessed, int64(len(batch)))
		atomic.AddInt64(&metrics.BatchesWritten, 1)
	}

	internal.Logger.Debug("Writer completed", "worker_id", workerID, "batches_written", batchCount)
	return nil
}

func (c *Cloner) writeBatchWithMetrics(ctx context.Context, destTableName string, batch []types.WriteRequest, metrics *CloneMetrics, workerID, batchNum int) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			destTableName: batch,
		},
	}

	maxRetries := 20
	baseBackoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second
	originalBatchSize := len(batch)

	for retry := 0; retry < maxRetries; retry++ {
		start := time.Now()
		result, err := c.DestClient.BatchWriteItem(ctx, input)
		duration := time.Since(start)

		if err != nil {
			atomic.AddInt64(&metrics.ErrorCount, 1)
			
			isThrottling := c.isThrottlingError(err)
			if isThrottling {
				atomic.AddInt64(&metrics.ThrottleCount, 1)
				metrics.LastThrottleTime = time.Now()
			}
			
			internal.Logger.Debug("Batch write error", 
				"worker_id", workerID,
				"batch_num", batchNum,
				"retry", retry,
				"error", err,
				"duration", duration,
				"is_throttling", isThrottling,
				"total_throttles", atomic.LoadInt64(&metrics.ThrottleCount))
			
			if retry == maxRetries-1 {
				return fmt.Errorf("batch write failed after %d retries: %w", maxRetries, err)
			}
			
			backoff := c.calculateBackoff(baseBackoff, maxBackoff, retry, isThrottling, workerID)
			time.Sleep(backoff)
			continue
		}

		unprocessed := result.UnprocessedItems[destTableName]
		if len(unprocessed) == 0 {
			internal.Logger.Debug("Batch write successful", 
				"worker_id", workerID,
				"batch_num", batchNum,
				"items", originalBatchSize,
				"duration", duration,
				"consumed_capacity", result.ConsumedCapacity)
			return nil
		}

		atomic.AddInt64(&metrics.RetryCount, 1)
		
		retryPercentage := float64(len(unprocessed)) / float64(originalBatchSize) * 100
		
		internal.Logger.Debug("Retrying unprocessed items", 
			"worker_id", workerID,
			"batch_num", batchNum,
			"retry", retry+1, 
			"unprocessed", len(unprocessed),
			"original_size", originalBatchSize,
			"retry_percentage", fmt.Sprintf("%.1f%%", retryPercentage))

		input.RequestItems[destTableName] = unprocessed
		
		isHighThrottling := retryPercentage > 50.0
		backoff := c.calculateBackoff(baseBackoff, maxBackoff, retry, isHighThrottling, workerID)
		time.Sleep(backoff)
	}

	return fmt.Errorf("failed to write batch after %d retries, %d items still unprocessed", 
		maxRetries, len(input.RequestItems[destTableName]))
}

func (c *Cloner) isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "throttling") || 
		   strings.Contains(errStr, "provisionedthroughputexceeded") ||
		   strings.Contains(errStr, "requestlimitexceeded") ||
		   strings.Contains(errStr, "throughputexceeded")
}

func (c *Cloner) calculateBackoff(baseBackoff, maxBackoff time.Duration, retry int, isThrottling bool, workerID int) time.Duration {
	backoff := baseBackoff * time.Duration(1<<uint(retry))
	
	if isThrottling {
		backoff *= 3
	}
	
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	
	jitter := time.Duration(workerID*100) * time.Millisecond
	backoff += jitter
	
	return backoff
}

func (c *Cloner) estimateItemSize(item map[string]types.AttributeValue) int64 {
	return int64(len(item) * 50)
}

func (c *Cloner) DiscoverTablesWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	internal.Logger.Info("Discovering tables with prefix", "prefix", prefix)
	
	var tables []string
	var lastEvaluatedTableName *string
	
	for {
		input := &dynamodb.ListTablesInput{
			Limit: aws.Int32(100),
		}
		
		if lastEvaluatedTableName != nil {
			input.ExclusiveStartTableName = lastEvaluatedTableName
		}
		
		result, err := c.SourceClient.ListTables(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %w", err)
		}
		
		for _, tableName := range result.TableNames {
			if strings.HasPrefix(tableName, prefix+".") {
				tables = append(tables, tableName)
				internal.Logger.Debug("Found matching table", "table", tableName, "prefix", prefix)
			}
		}
		
		if result.LastEvaluatedTableName == nil {
			break
		}
		lastEvaluatedTableName = result.LastEvaluatedTableName
	}
	
	internal.Logger.Info("Table discovery completed", "prefix", prefix, "tables_found", len(tables))
	return tables, nil
}

func (c *Cloner) CloneTablesWithPrefixInteractive(ctx context.Context, options CloneOptions) error {
	if options.SourcePrefix == "" || options.DestPrefix == "" {
		return fmt.Errorf("both SourcePrefix and DestPrefix must be specified for prefix-based cloning")
	}
	
	var allTables []string
	err := internal.SimpleSpinner(fmt.Sprintf("Discovering tables with prefix %s", options.SourcePrefix), func() error {
		var discoverErr error
		allTables, discoverErr = c.DiscoverTablesWithPrefix(ctx, options.SourcePrefix)
		return discoverErr
	})
	if err != nil {
		return fmt.Errorf("failed to discover source tables: %w", err)
	}
	
	if len(allTables) == 0 {
		internal.Logger.Warn("No tables found with specified prefix", "prefix", options.SourcePrefix)
		return fmt.Errorf("no tables found with prefix: %s", options.SourcePrefix)
	}
	
	selector := internal.NewTableSelector(allTables)
	selectedTables, err := selector.SelectTables()
	if err != nil {
		return fmt.Errorf("table selection failed: %w", err)
	}
	
	internal.Logger.Debug("Selected tables for cloning", "count", len(selectedTables), "tables", selectedTables)
	
	return c.cloneSelectedTables(ctx, selectedTables, options)
}

func (c *Cloner) CloneTablesWithPrefix(ctx context.Context, options CloneOptions) error {
	if options.SourcePrefix == "" || options.DestPrefix == "" {
		return fmt.Errorf("both SourcePrefix and DestPrefix must be specified for prefix-based cloning")
	}
	
	internal.Logger.Debug("Starting prefix-based DynamoDB clone", 
		"source_prefix", options.SourcePrefix, 
		"dest_prefix", options.DestPrefix)
	
	var sourceTables []string
	err := internal.SimpleSpinner(fmt.Sprintf("Discovering tables with prefix %s", options.SourcePrefix), func() error {
		var discoverErr error
		sourceTables, discoverErr = c.DiscoverTablesWithPrefix(ctx, options.SourcePrefix)
		return discoverErr
	})
	if err != nil {
		return fmt.Errorf("failed to discover source tables: %w", err)
	}
	
	if len(sourceTables) == 0 {
		internal.Logger.Warn("No tables found with specified prefix", "prefix", options.SourcePrefix)
		return fmt.Errorf("no tables found with prefix: %s", options.SourcePrefix)
	}
	
	internal.Logger.Debug("Found tables to clone", "count", len(sourceTables), "tables", sourceTables)
	
	return c.cloneSelectedTables(ctx, sourceTables, options)
}

func (c *Cloner) cloneSelectedTables(ctx context.Context, selectedTables []string, options CloneOptions) error {
	totalStart := time.Now()
	var totalItemsCloned int64
	var totalErrors int64
	
	for i, sourceTable := range selectedTables {
		destTable := strings.Replace(sourceTable, options.SourcePrefix+".", options.DestPrefix+".", 1)
		
		internal.Logger.Debug("Cloning table", 
			"progress", fmt.Sprintf("%d/%d", i+1, len(selectedTables)),
			"source_table", sourceTable, 
			"dest_table", destTable)
		
		tableCloner := &Cloner{
			SourceConfig: Config{
				Region: c.SourceConfig.Region,
			},
			DestConfig: Config{
				Region: c.DestConfig.Region,
			},
			SourceClient: c.SourceClient,
			DestClient:   c.DestClient,
		}
		
		err := internal.SimpleSpinner(fmt.Sprintf("Cloning table %s → %s (%d/%d)", sourceTable, destTable, i+1, len(selectedTables)), func() error {
			if structureErr := tableCloner.cloneTableStructureWithSpinner(ctx, sourceTable, destTable, false); structureErr != nil {
				return fmt.Errorf("failed to create table structure: %w", structureErr)
			}
			
			tableOptions := options
			tableOptions.ProgressCallback = func(processed, total int64) {
				if options.ProgressCallback != nil {
					options.ProgressCallback(totalItemsCloned+processed, -1)
				}
			}
			
			if dataErr := tableCloner.cloneTableWithSpinner(ctx, sourceTable, destTable, tableOptions, false); dataErr != nil {
				return fmt.Errorf("failed to clone table data: %w", dataErr)
			}
			
			return nil
		})
		
		if err != nil {
			if internal.VerboseMode {
				internal.Logger.Error("Failed to clone table", 
					"source_table", sourceTable,
					"dest_table", destTable,
					"error", err)
			}
			totalErrors++
			continue
		}
		
		if itemCount, countErr := tableCloner.GetItemCount(ctx, sourceTable, options.FilterExpression); countErr == nil {
			totalItemsCloned += itemCount
		}
		
		internal.Logger.Debug("Table cloned successfully", 
			"source_table", sourceTable,
			"dest_table", destTable,
			"progress", fmt.Sprintf("%d/%d", i+1, len(selectedTables)))
	}
	
	totalDuration := time.Since(totalStart)
	
	if totalErrors > 0 {
		internal.Logger.Error("Prefix-based clone completed with errors", 
			"total_duration", totalDuration,
			"tables_processed", len(selectedTables),
			"tables_failed", totalErrors,
			"items_cloned", totalItemsCloned)
		return fmt.Errorf("cloning completed with %d errors out of %d tables", totalErrors, len(selectedTables))
	}
	
	internal.Logger.Debug("Prefix-based clone completed successfully", 
		"total_duration", totalDuration,
		"tables_cloned", len(selectedTables),
		"total_items_cloned", totalItemsCloned,
		"source_prefix", options.SourcePrefix,
		"dest_prefix", options.DestPrefix)
	
	internal.FinishLine()
	
	return nil
}

func (c *Cloner) logCloneResults(metrics *CloneMetrics, duration time.Duration, err error) {
	if err != nil {
		internal.Logger.Error("DynamoDB clone failed", 
			"duration", duration,
			"items_processed", metrics.ItemsProcessed,
			"items_total", metrics.ItemsTotal,
			"error_count", metrics.ErrorCount,
			"retry_count", metrics.RetryCount,
			"throttle_count", metrics.ThrottleCount,
			"error", err)
		return
	}

	successRate := float64(metrics.ItemsProcessed) / float64(metrics.ItemsTotal) * 100
	if metrics.ItemsTotal <= 0 {
		successRate = 100
	}

	internal.Logger.Debug("DynamoDB clone completed successfully",
		"duration", duration,
		"items_processed", metrics.ItemsProcessed,
		"items_total", metrics.ItemsTotal,
		"success_rate", fmt.Sprintf("%.1f%%", successRate),
		"throughput_per_sec", fmt.Sprintf("%.1f", metrics.ThroughputPerSec),
		"avg_item_size_bytes", metrics.AvgItemSize,
		"total_bytes_processed", metrics.BytesProcessed,
		"batches_written", metrics.BatchesWritten,
		"error_count", metrics.ErrorCount,
		"retry_count", metrics.RetryCount,
		"throttle_count", metrics.ThrottleCount)
}


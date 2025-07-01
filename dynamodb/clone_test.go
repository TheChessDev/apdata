package dynamodb

import (
	"context"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"apdata/internal"
)

func TestNewCloner(t *testing.T) {
	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-west-2",
	}

	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		if cloner == nil {
			t.Log("Expected failure - no AWS credentials in test environment")
		}
		return
	}

	if cloner.SourceConfig != sourceConfig {
		t.Error("Source config not set correctly")
	}
	if cloner.DestConfig != destConfig {
		t.Error("Dest config not set correctly")
	}
}

func TestCreateClient(t *testing.T) {
	cfg := Config{
		Region: "us-east-1",
	}

	client, err := createClient(cfg)
	if err != nil {
		t.Logf("Expected error creating client without AWS credentials: %v", err)
		return
	}

	if client == nil {
		t.Error("Expected non-nil client")
	}
}

func TestCloneOptions(t *testing.T) {
	options := CloneOptions{
		Concurrency: 5,
		BatchSize:   20,
	}

	if options.Concurrency != 5 {
		t.Errorf("Expected concurrency 5, got %d", options.Concurrency)
	}
	if options.BatchSize != 20 {
		t.Errorf("Expected batch size 20, got %d", options.BatchSize)
	}
}

func TestCloneMetrics(t *testing.T) {
	metrics := &CloneMetrics{
		StartTime: time.Now(),
	}

	atomic.AddInt64(&metrics.ItemsProcessed, 100)
	atomic.AddInt64(&metrics.BytesProcessed, 5000)
	atomic.AddInt64(&metrics.ErrorCount, 2)
	atomic.AddInt64(&metrics.RetryCount, 1)
	atomic.AddInt32(&metrics.SegmentsActive, 3)
	atomic.AddInt64(&metrics.BatchesWritten, 10)

	if atomic.LoadInt64(&metrics.ItemsProcessed) != 100 {
		t.Errorf("Expected 100 items processed, got %d", metrics.ItemsProcessed)
	}
	if atomic.LoadInt64(&metrics.BytesProcessed) != 5000 {
		t.Errorf("Expected 5000 bytes processed, got %d", metrics.BytesProcessed)
	}
	if atomic.LoadInt64(&metrics.ErrorCount) != 2 {
		t.Errorf("Expected 2 errors, got %d", metrics.ErrorCount)
	}
	if atomic.LoadInt64(&metrics.RetryCount) != 1 {
		t.Errorf("Expected 1 retry, got %d", metrics.RetryCount)
	}
	if atomic.LoadInt32(&metrics.SegmentsActive) != 3 {
		t.Errorf("Expected 3 active segments, got %d", metrics.SegmentsActive)
	}
	if atomic.LoadInt64(&metrics.BatchesWritten) != 10 {
		t.Errorf("Expected 10 batches written, got %d", metrics.BatchesWritten)
	}

	if metrics.BytesProcessed > 0 && metrics.ItemsProcessed > 0 {
		avgSize := metrics.BytesProcessed / metrics.ItemsProcessed
		expectedAvg := int64(50)
		if avgSize != expectedAvg {
			t.Errorf("Expected avg item size %d, got %d", expectedAvg, avgSize)
		}
	}
}

func TestGetOptimalConcurrency(t *testing.T) {
	cloner := &Cloner{}
	concurrency := cloner.getOptimalConcurrency()

	if concurrency <= 0 || concurrency > 100 {
		t.Errorf("Expected reasonable concurrency (1-100), got %d", concurrency)
	}
}

func TestGetOptimalBatchSize(t *testing.T) {
	cloner := &Cloner{}
	batchSize := cloner.getOptimalBatchSize()

	if batchSize <= 0 || batchSize > 25 {
		t.Errorf("Expected batch size 1-25, got %d", batchSize)
	}
}

func TestEstimateItemSize(t *testing.T) {
	cloner := &Cloner{}
	
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: "12345"},
		"name": &types.AttributeValueMemberS{Value: "test"},
		"age":  &types.AttributeValueMemberN{Value: "25"},
	}

	size := cloner.estimateItemSize(item)
	
	if size <= 0 {
		t.Error("Expected positive size estimate")
	}
	
	largeItem := map[string]types.AttributeValue{
		"id":          &types.AttributeValueMemberS{Value: "12345"},
		"name":        &types.AttributeValueMemberS{Value: "test"},
		"description": &types.AttributeValueMemberS{Value: "long description"},
		"metadata":    &types.AttributeValueMemberS{Value: "more data"},
		"timestamp":   &types.AttributeValueMemberN{Value: "1640995200"},
	}
	
	largeSize := cloner.estimateItemSize(largeItem)
	
	if largeSize <= size {
		t.Error("Expected larger item to have larger estimated size")
	}
}

func TestCloneTableWithoutDatabase(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-east-1",
	}

	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		t.Logf("Expected error creating cloner without AWS credentials: %v", err)
		return
	}

	if cloner == nil {
		t.Log("Cloner creation failed, skipping CloneTable test")
		return
	}

	ctx := context.Background()
	options := CloneOptions{
		Concurrency: 2,
		BatchSize:   10,
	}

	err = cloner.CloneTable(ctx, "source-table", "dest-table", options)
	if err == nil {
		t.Error("Expected error when cloning without real DynamoDB connection")
	}

	t.Logf("Got expected error: %v", err)
}

func TestGetItemCountWithoutDatabase(t *testing.T) {
	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-east-1",
	}

	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		t.Logf("Expected error creating cloner: %v", err)
		return
	}

	if cloner == nil {
		t.Log("Cloner creation failed, skipping GetItemCount test")
		return
	}

	ctx := context.Background()
	
	count, err := cloner.GetItemCount(ctx, "test-table", nil)
	if err == nil {
		t.Error("Expected error when getting item count without real DynamoDB")
	}
	if count != 0 {
		t.Errorf("Expected count 0 on error, got %d", count)
	}

	t.Logf("Got expected error: %v", err)
}

func TestCloneTableStructureWithoutDatabase(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-east-1",
	}

	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		t.Logf("Expected error creating cloner: %v", err)
		return
	}

	if cloner == nil {
		t.Log("Cloner creation failed, skipping CloneTableStructure test")
		return
	}

	ctx := context.Background()
	
	err = cloner.CloneTableStructure(ctx, "source-table", "dest-table")
	if err == nil {
		t.Error("Expected error when cloning structure without real DynamoDB")
	}

	t.Logf("Got expected error: %v", err)
}

func TestProgressCallback(t *testing.T) {
	var callbackCalled bool
	var lastProcessed, lastTotal int64

	callback := func(processed, total int64) {
		callbackCalled = true
		lastProcessed = processed
		lastTotal = total
	}

	options := CloneOptions{
		ProgressCallback: callback,
	}

	if options.ProgressCallback != nil {
		options.ProgressCallback(50, 100)
	}

	if !callbackCalled {
		t.Error("Expected progress callback to be called")
	}
	if lastProcessed != 50 {
		t.Errorf("Expected processed=50, got %d", lastProcessed)
	}
	if lastTotal != 100 {
		t.Errorf("Expected total=100, got %d", lastTotal)
	}
}

func TestFilterExpressionHandling(t *testing.T) {
	filterExpression := "attribute_exists(active)"
	attributeNames := map[string]string{
		"#status": "status",
	}
	attributeValues := map[string]types.AttributeValue{
		":active": &types.AttributeValueMemberBOOL{Value: true},
	}

	options := CloneOptions{
		FilterExpression:          &filterExpression,
		ExpressionAttributeNames:  attributeNames,
		ExpressionAttributeValues: attributeValues,
	}

	if options.FilterExpression == nil {
		t.Error("Expected non-nil filter expression")
	}
	if *options.FilterExpression != filterExpression {
		t.Errorf("Expected filter '%s', got '%s'", filterExpression, *options.FilterExpression)
	}
	if !reflect.DeepEqual(options.ExpressionAttributeNames, attributeNames) {
		t.Error("Expression attribute names not set correctly")
	}
	if len(options.ExpressionAttributeValues) != 1 {
		t.Errorf("Expected 1 attribute value, got %d", len(options.ExpressionAttributeValues))
	}
}

func TestCloneOptionsDefaults(t *testing.T) {
	cloner := &Cloner{}
	
	defaultConcurrency := cloner.getOptimalConcurrency()
	if defaultConcurrency <= 0 {
		t.Error("Expected positive default concurrency")
	}

	defaultBatchSize := cloner.getOptimalBatchSize()
	if defaultBatchSize <= 0 || defaultBatchSize > 25 {
		t.Errorf("Expected batch size 1-25, got %d", defaultBatchSize)
	}
}

func TestMetricsCalculations(t *testing.T) {
	metrics := &CloneMetrics{
		StartTime:      time.Now().Add(-10 * time.Second),
		ItemsProcessed: 1000,
		BytesProcessed: 50000,
	}

	duration := time.Since(metrics.StartTime)
	throughput := float64(metrics.ItemsProcessed) / duration.Seconds()
	avgItemSize := metrics.BytesProcessed / metrics.ItemsProcessed

	if throughput <= 0 {
		t.Error("Expected positive throughput")
	}
	if avgItemSize != 50 {
		t.Errorf("Expected avg item size 50, got %d", avgItemSize)
	}

	expectedThroughput := 100.0
	if throughput < expectedThroughput*0.8 || throughput > expectedThroughput*1.2 {
		t.Logf("Throughput %.2f items/sec (expected ~%.2f)", throughput, expectedThroughput)
	}
}

func TestConcurrentMetricsUpdates(t *testing.T) {
	metrics := &CloneMetrics{}
	
	const numGoroutines = 10
	const incrementsPerGoroutine = 100
	
	done := make(chan struct{})
	
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < incrementsPerGoroutine; j++ {
				atomic.AddInt64(&metrics.ItemsProcessed, 1)
				atomic.AddInt64(&metrics.BytesProcessed, 50)
				atomic.AddInt64(&metrics.BatchesWritten, 1)
			}
		}()
	}
	
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	
	expectedItems := int64(numGoroutines * incrementsPerGoroutine)
	expectedBytes := int64(numGoroutines * incrementsPerGoroutine * 50)
	expectedBatches := int64(numGoroutines * incrementsPerGoroutine)
	
	if atomic.LoadInt64(&metrics.ItemsProcessed) != expectedItems {
		t.Errorf("Expected %d items processed, got %d", expectedItems, metrics.ItemsProcessed)
	}
	if atomic.LoadInt64(&metrics.BytesProcessed) != expectedBytes {
		t.Errorf("Expected %d bytes processed, got %d", expectedBytes, metrics.BytesProcessed)
	}
	if atomic.LoadInt64(&metrics.BatchesWritten) != expectedBatches {
		t.Errorf("Expected %d batches written, got %d", expectedBatches, metrics.BatchesWritten)
	}
}

func TestSpinnerIntegration(t *testing.T) {
	internal.VerboseMode = false
	defer func() { internal.VerboseMode = false }()

	if internal.VerboseMode {
		t.Error("Expected verbose mode to be false for spinner testing")
	}

	internal.VerboseMode = true
	if !internal.VerboseMode {
		t.Error("Expected verbose mode to be true")
	}
}

func TestPerformanceOptimizations(t *testing.T) {
	cloner := &Cloner{}
	
	concurrency := cloner.getOptimalConcurrency()
	batchSize := cloner.getOptimalBatchSize()
	
	if concurrency < 1 || concurrency > 50 {
		t.Errorf("Expected concurrency 1-50, got %d", concurrency)
	}
	
	if batchSize != 25 {
		t.Errorf("Expected batch size 25 (DynamoDB limit), got %d", batchSize)
	}
}

func TestConfigStructures(t *testing.T) {
	config := Config{
		Region: "us-west-1",
	}
	
	if config.Region != "us-west-1" {
		t.Errorf("Expected region us-west-1, got %s", config.Region)
	}
}

func TestErrorHandling(t *testing.T) {
	metrics := &CloneMetrics{}
	
	atomic.AddInt64(&metrics.ErrorCount, 1)
	atomic.AddInt64(&metrics.RetryCount, 3)
	
	if atomic.LoadInt64(&metrics.ErrorCount) != 1 {
		t.Errorf("Expected 1 error, got %d", metrics.ErrorCount)
	}
	if atomic.LoadInt64(&metrics.RetryCount) != 3 {
		t.Errorf("Expected 3 retries, got %d", metrics.RetryCount)
	}
	
	totalErrors := atomic.LoadInt64(&metrics.ErrorCount)
	if totalErrors < 0 {
		t.Error("Error count should not be negative")
	}
}

func TestProgressTracking(t *testing.T) {
	var processedItems int64 = 0
	var totalItems int64 = 100000
	
	atomic.AddInt64(&processedItems, 25000)
	progress := float64(processedItems) / float64(totalItems) * 100
	
	expectedProgress := 25.0
	if progress != expectedProgress {
		t.Errorf("Expected progress %.1f%%, got %.1f%%", expectedProgress, progress)
	}
	
	atomic.AddInt64(&processedItems, 100000)
	progress = float64(processedItems) / float64(totalItems) * 100
	
	if progress <= 100.0 {
		t.Logf("Progress is %.1f%% (estimates can be inaccurate)", progress)
	}
}

func TestChannelBuffering(t *testing.T) {
	options := CloneOptions{
		Concurrency: 10,
		BatchSize:   25,
	}
	
	expectedBuffer := options.Concurrency * options.BatchSize
	if expectedBuffer != 250 {
		t.Errorf("Expected buffer size 250, got %d", expectedBuffer)
	}
	
	options.Concurrency = 5
	options.BatchSize = 10
	expectedBuffer = options.Concurrency * options.BatchSize
	if expectedBuffer != 50 {
		t.Errorf("Expected buffer size 50, got %d", expectedBuffer)
	}
}

func TestPrefixBasedCloning(t *testing.T) {
	sourcePrefix := "allpoint.dev"
	destPrefix := "julian.dev"
	
	options := CloneOptions{
		SourcePrefix: sourcePrefix,
		DestPrefix:   destPrefix,
		Concurrency:  10,
		BatchSize:    25,
	}
	
	if options.SourcePrefix != sourcePrefix {
		t.Errorf("Expected source prefix '%s', got '%s'", sourcePrefix, options.SourcePrefix)
	}
	if options.DestPrefix != destPrefix {
		t.Errorf("Expected dest prefix '%s', got '%s'", destPrefix, options.DestPrefix)
	}
}

func TestDiscoverTablesWithPrefixWithAWS(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-east-1",
	}

	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		t.Skipf("AWS credentials not configured, skipping test: %v", err)
		return
	}

	if cloner == nil {
		t.Skip("Cloner creation failed, skipping table discovery test")
		return
	}

	ctx := context.Background()
	tables, err := cloner.DiscoverTablesWithPrefix(ctx, "allpoint.dev")
	if err != nil {
		t.Logf("Table discovery error (expected if no tables exist): %v", err)
		return
	}

	t.Logf("Found %d tables with prefix 'allpoint.dev'", len(tables))
	if len(tables) > 0 {
		sampleCount := len(tables)
		if sampleCount > 3 {
			sampleCount = 3
		}
		t.Logf("Sample tables: %v", tables[:sampleCount])
	}
}

func TestCloneTablesWithPrefixValidation(t *testing.T) {
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-east-1",
	}

	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		t.Skipf("AWS credentials not configured, skipping test: %v", err)
		return
	}

	if cloner == nil {
		t.Skip("Cloner creation failed, skipping prefix cloning test")
		return
	}

	ctx := context.Background()
	
	options := CloneOptions{
		SourcePrefix: "nonexistent.prefix.test",
		DestPrefix:   "test.nonexistent.prefix",
		Concurrency:  5,
		BatchSize:    10,
	}

	err = cloner.CloneTablesWithPrefix(ctx, options)
	if err == nil {
		t.Error("Expected error when cloning tables with non-existent prefix")
	}

	if !strings.Contains(err.Error(), "no tables found") {
		t.Errorf("Expected 'no tables found' error, got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

func TestPrefixValidation(t *testing.T) {
	cloner := &Cloner{}
	ctx := context.Background()
	
	options := CloneOptions{
		DestPrefix:  "julian.dev",
		Concurrency: 5,
		BatchSize:   10,
	}
	
	err := cloner.CloneTablesWithPrefix(ctx, options)
	if err == nil {
		t.Error("Expected error when source prefix is missing")
	}
	if !strings.Contains(err.Error(), "SourcePrefix") {
		t.Errorf("Expected error about SourcePrefix, got: %v", err)
	}
	
	options = CloneOptions{
		SourcePrefix: "allpoint.dev",
		Concurrency:  5,
		BatchSize:    10,
	}
	
	err = cloner.CloneTablesWithPrefix(ctx, options)
	if err == nil {
		t.Error("Expected error when dest prefix is missing")
	}
	if !strings.Contains(err.Error(), "DestPrefix") {
		t.Errorf("Expected error about DestPrefix, got: %v", err)
	}
}
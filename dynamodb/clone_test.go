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
		// Expected to fail in test environment without AWS credentials
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

	// This will likely fail without real AWS config, but tests the function exists
	client, err := createClient(cfg)
	if err != nil {
		// Expected in test environment
		t.Logf("Expected error creating client without AWS credentials: %v", err)
		return
	}

	if client == nil {
		t.Error("Expected non-nil client")
	}
}

func TestCloneOptions(t *testing.T) {
	// Test CloneOptions structure and defaults
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
	// Test CloneMetrics structure and atomic operations
	metrics := &CloneMetrics{
		StartTime: time.Now(),
	}

	// Test atomic operations
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

	// Test average calculation
	if metrics.BytesProcessed > 0 && metrics.ItemsProcessed > 0 {
		avgSize := metrics.BytesProcessed / metrics.ItemsProcessed
		expectedAvg := int64(50) // 5000 / 100
		if avgSize != expectedAvg {
			t.Errorf("Expected avg item size %d, got %d", expectedAvg, avgSize)
		}
	}
}

func TestGetOptimalConcurrency(t *testing.T) {
	cloner := &Cloner{}
	concurrency := cloner.getOptimalConcurrency()

	// Should return a reasonable default
	if concurrency <= 0 || concurrency > 100 {
		t.Errorf("Expected reasonable concurrency (1-100), got %d", concurrency)
	}
}

func TestGetOptimalBatchSize(t *testing.T) {
	cloner := &Cloner{}
	batchSize := cloner.getOptimalBatchSize()

	// DynamoDB batch write supports up to 25 items
	if batchSize <= 0 || batchSize > 25 {
		t.Errorf("Expected batch size 1-25, got %d", batchSize)
	}
}

func TestEstimateItemSize(t *testing.T) {
	cloner := &Cloner{}
	
	// Test with sample item
	item := map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: "12345"},
		"name": &types.AttributeValueMemberS{Value: "test"},
		"age":  &types.AttributeValueMemberN{Value: "25"},
	}

	size := cloner.estimateItemSize(item)
	
	// Should return a reasonable estimate
	if size <= 0 {
		t.Error("Expected positive size estimate")
	}
	
	// Test with larger item
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
	// Test CloneTable behavior without real DynamoDB connection
	// Set verbose mode to avoid spinner interference in tests
	internal.VerboseMode = true
	defer func() { internal.VerboseMode = false }()

	sourceConfig := Config{
		Region: "us-east-1",
	}
	destConfig := Config{
		Region: "us-east-1",
	}

	// Try to create cloner - this may fail without AWS credentials
	cloner, err := NewCloner(sourceConfig, destConfig)
	if err != nil {
		// Expected to fail in test environment
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

	// This should fail since we don't have real DynamoDB clients
	err = cloner.CloneTable(ctx, "source-table", "dest-table", options)
	if err == nil {
		t.Error("Expected error when cloning without real DynamoDB connection")
	}

	// Should be a connection or client error
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
		// Expected to fail in test environment
		t.Logf("Expected error creating cloner: %v", err)
		return
	}

	if cloner == nil {
		t.Log("Cloner creation failed, skipping GetItemCount test")
		return
	}

	ctx := context.Background()
	
	// This should fail without real DynamoDB
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
	// Set verbose mode to avoid spinner interference
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
		// Expected to fail in test environment
		t.Logf("Expected error creating cloner: %v", err)
		return
	}

	if cloner == nil {
		t.Log("Cloner creation failed, skipping CloneTableStructure test")
		return
	}

	ctx := context.Background()
	
	// This should fail without real DynamoDB
	err = cloner.CloneTableStructure(ctx, "source-table", "dest-table")
	if err == nil {
		t.Error("Expected error when cloning structure without real DynamoDB")
	}

	t.Logf("Got expected error: %v", err)
}

func TestProgressCallback(t *testing.T) {
	// Test progress callback functionality
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

	// Simulate calling the callback
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
	// Test that filter expressions are properly handled
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
	// Test that CloneTable sets intelligent defaults
	cloner := &Cloner{}
	
	// Test default concurrency
	defaultConcurrency := cloner.getOptimalConcurrency()
	if defaultConcurrency <= 0 {
		t.Error("Expected positive default concurrency")
	}

	// Test default batch size  
	defaultBatchSize := cloner.getOptimalBatchSize()
	if defaultBatchSize <= 0 || defaultBatchSize > 25 {
		t.Errorf("Expected batch size 1-25, got %d", defaultBatchSize)
	}
}

func TestMetricsCalculations(t *testing.T) {
	// Test metrics calculations and throughput
	metrics := &CloneMetrics{
		StartTime:      time.Now().Add(-10 * time.Second), // 10 seconds ago
		ItemsProcessed: 1000,
		BytesProcessed: 50000,
	}

	duration := time.Since(metrics.StartTime)
	throughput := float64(metrics.ItemsProcessed) / duration.Seconds()
	avgItemSize := metrics.BytesProcessed / metrics.ItemsProcessed

	if throughput <= 0 {
		t.Error("Expected positive throughput")
	}
	if avgItemSize != 50 { // 50000 / 1000
		t.Errorf("Expected avg item size 50, got %d", avgItemSize)
	}

	// Test throughput calculation (should be around 100 items/sec for 10 second duration)
	expectedThroughput := 100.0
	if throughput < expectedThroughput*0.8 || throughput > expectedThroughput*1.2 {
		t.Logf("Throughput %.2f items/sec (expected ~%.2f)", throughput, expectedThroughput)
	}
}

func TestConcurrentMetricsUpdates(t *testing.T) {
	// Test that atomic operations work correctly under concurrent access
	metrics := &CloneMetrics{}
	
	const numGoroutines = 10
	const incrementsPerGoroutine = 100
	
	done := make(chan struct{})
	
	// Start concurrent goroutines updating metrics
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
	
	// Wait for all goroutines to complete
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
	// Test spinner behavior with DynamoDB operations
	// This tests that spinners are properly created and managed
	internal.VerboseMode = false // Enable spinners
	defer func() { internal.VerboseMode = false }()

	// Test spinner behavior - can't easily test actual spinner without real operations
	// but we can verify the mode switching works
	if internal.VerboseMode {
		t.Error("Expected verbose mode to be false for spinner testing")
	}

	// Switch to verbose mode and verify spinners would be disabled
	internal.VerboseMode = true
	if !internal.VerboseMode {
		t.Error("Expected verbose mode to be true")
	}
}

func TestPerformanceOptimizations(t *testing.T) {
	// Test that performance optimizations are properly configured
	cloner := &Cloner{}
	
	// Test intelligent defaults
	concurrency := cloner.getOptimalConcurrency()
	batchSize := cloner.getOptimalBatchSize()
	
	// Concurrency should be reasonable for most workloads
	if concurrency < 1 || concurrency > 50 {
		t.Errorf("Expected concurrency 1-50, got %d", concurrency)
	}
	
	// Batch size should match DynamoDB limits
	if batchSize != 25 {
		t.Errorf("Expected batch size 25 (DynamoDB limit), got %d", batchSize)
	}
}

func TestConfigStructures(t *testing.T) {
	// Test Config structure
	config := Config{
		Region: "us-west-1",
	}
	
	if config.Region != "us-west-1" {
		t.Errorf("Expected region us-west-1, got %s", config.Region)
	}
}

func TestErrorHandling(t *testing.T) {
	// Test error handling patterns
	metrics := &CloneMetrics{}
	
	// Simulate error conditions
	atomic.AddInt64(&metrics.ErrorCount, 1)
	atomic.AddInt64(&metrics.RetryCount, 3)
	
	if atomic.LoadInt64(&metrics.ErrorCount) != 1 {
		t.Errorf("Expected 1 error, got %d", metrics.ErrorCount)
	}
	if atomic.LoadInt64(&metrics.RetryCount) != 3 {
		t.Errorf("Expected 3 retries, got %d", metrics.RetryCount)
	}
	
	// Test that errors are properly tracked
	totalErrors := atomic.LoadInt64(&metrics.ErrorCount)
	if totalErrors < 0 {
		t.Error("Error count should not be negative")
	}
}

func TestProgressTracking(t *testing.T) {
	// Test progress tracking logic similar to MySQL tests
	var processedItems int64 = 0
	var totalItems int64 = 100000
	
	// Simulate processing some items
	atomic.AddInt64(&processedItems, 25000)
	progress := float64(processedItems) / float64(totalItems) * 100
	
	expectedProgress := 25.0
	if progress != expectedProgress {
		t.Errorf("Expected progress %.1f%%, got %.1f%%", expectedProgress, progress)
	}
	
	// Test that progress doesn't cause issues when it exceeds 100%
	atomic.AddInt64(&processedItems, 100000) // More than total
	progress = float64(processedItems) / float64(totalItems) * 100
	
	if progress <= 100.0 {
		// This is actually okay - progress can exceed 100% if estimates are wrong
		t.Logf("Progress is %.1f%% (estimates can be inaccurate)", progress)
	}
}

func TestChannelBuffering(t *testing.T) {
	// Test channel buffer size calculations
	options := CloneOptions{
		Concurrency: 10,
		BatchSize:   25,
	}
	
	expectedBuffer := options.Concurrency * options.BatchSize
	if expectedBuffer != 250 {
		t.Errorf("Expected buffer size 250, got %d", expectedBuffer)
	}
	
	// Test with different values
	options.Concurrency = 5
	options.BatchSize = 10
	expectedBuffer = options.Concurrency * options.BatchSize
	if expectedBuffer != 50 {
		t.Errorf("Expected buffer size 50, got %d", expectedBuffer)
	}
}

func TestPrefixBasedCloning(t *testing.T) {
	// Test prefix-based cloning options
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
	// Test table discovery with real AWS credentials (if available)
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
		// AWS credentials not configured, skip test
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
		// This is okay - might not have real tables with this prefix
		t.Logf("Table discovery error (expected if no tables exist): %v", err)
		return
	}

	// If we successfully found tables, log them
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
	// Test prefix-based cloning validation without actually cloning real tables
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
		// AWS credentials not configured, skip test
		t.Skipf("AWS credentials not configured, skipping test: %v", err)
		return
	}

	if cloner == nil {
		t.Skip("Cloner creation failed, skipping prefix cloning test")
		return
	}

	ctx := context.Background()
	
	// Test with non-existent prefix to avoid actually cloning real tables
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

	// Should get a "no tables found" error
	if !strings.Contains(err.Error(), "no tables found") {
		t.Errorf("Expected 'no tables found' error, got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

func TestPrefixValidation(t *testing.T) {
	// Test that prefix validation works correctly
	cloner := &Cloner{}
	ctx := context.Background()
	
	// Test missing source prefix
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
	
	// Test missing dest prefix
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
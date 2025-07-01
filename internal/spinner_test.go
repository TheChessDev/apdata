package internal

import (
	"errors"
	"testing"
	"time"
)

func TestSimpleSpinner(t *testing.T) {
	VerboseMode = false
	
	err := SimpleSpinner("Test operation", func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestSimpleSpinnerWithError(t *testing.T) {
	VerboseMode = false
	
	expectedErr := errors.New("test error")
	err := SimpleSpinner("Test operation", func() error {
		time.Sleep(100 * time.Millisecond)
		return expectedErr
	})
	
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestSimpleSpinnerVerboseMode(t *testing.T) {
	VerboseMode = true
	defer func() { VerboseMode = false }()
	
	operationCalled := false
	err := SimpleSpinner("Test operation", func() error {
		operationCalled = true
		return nil
	})
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if !operationCalled {
		t.Error("Operation should still be called in verbose mode")
	}
}

func TestNewSpinner(t *testing.T) {
	spinner := NewSpinner("Test message")
	
	if spinner.message != "Test message" {
		t.Errorf("Expected message 'Test message', got '%s'", spinner.message)
	}
	
	if len(spinner.frames) == 0 {
		t.Error("Expected frames to be populated")
	}
	
	if spinner.interval != 100*time.Millisecond {
		t.Errorf("Expected interval 100ms, got %v", spinner.interval)
	}
}

func TestSpinnerStartStop(t *testing.T) {
	spinner := NewSpinner("Test")
	
	spinner.Start()
	if !spinner.active {
		t.Error("Spinner should be active after Start()")
	}
	
	spinner.Stop()
	time.Sleep(150 * time.Millisecond)
	if spinner.active {
		t.Error("Spinner should not be active after Stop()")
	}
}

func TestSpinnerDoubleStart(t *testing.T) {
	spinner := NewSpinner("Test")
	
	spinner.Start()
	if !spinner.active {
		t.Error("Spinner should be active after first Start()")
	}
	
	spinner.Start()
	if !spinner.active {
		t.Error("Spinner should still be active after second Start()")
	}
	
	spinner.Stop()
}

func TestSpinnerUpdateMessage(t *testing.T) {
	spinner := NewSpinner("Original message")
	
	spinner.UpdateMessage("Updated message")
	
	if spinner.message != "Updated message" {
		t.Errorf("Expected message 'Updated message', got '%s'", spinner.message)
	}
}

func TestWithSpinner(t *testing.T) {
	err := WithSpinner("Test operation", func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestWithSpinnerConditional(t *testing.T) {
	err := WithSpinnerConditional("Test operation", func() error {
		return nil
	}, true)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	err = WithSpinnerConditional("Test operation", func() error {
		return nil
	}, false)
	
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestLogLevelSettings(t *testing.T) {
	originalVerboseMode := VerboseMode
	defer func() { VerboseMode = originalVerboseMode }()
	
	SetLogLevel("debug")
	if !VerboseMode {
		t.Error("VerboseMode should be true when log level is debug")
	}
	
	VerboseMode = false
	SetLogLevel("info")
	if VerboseMode {
		t.Error("VerboseMode should be false when log level is info")
	}
	
	VerboseMode = false
	SetLogLevel("warn")
	if VerboseMode {
		t.Error("VerboseMode should be false when log level is warn")
	}
	
	VerboseMode = false
	SetLogLevel("error")
	if VerboseMode {
		t.Error("VerboseMode should be false when log level is error")
	}
	
	VerboseMode = false
	SetLogLevel("unknown")
	if VerboseMode {
		t.Error("VerboseMode should be false for unknown log level")
	}
}
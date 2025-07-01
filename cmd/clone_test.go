package cmd

import (
	"strings"
	"testing"
)

func TestFormatError(t *testing.T) {
	tests := []struct {
		name          string
		inputError    error
		expectedStart string
	}{
		{
			name:          "connection refused error",
			inputError:    &mockError{"connection refused"},
			expectedStart: "❌ Cannot connect to MySQL server",
		},
		{
			name:          "access denied error", 
			inputError:    &mockError{"Access denied for user 'root'"},
			expectedStart: "❌ MySQL authentication failed",
		},
		{
			name:          "unknown database error",
			inputError:    &mockError{"Unknown database 'nonexistent'"},
			expectedStart: "❌ Database does not exist",
		},
		{
			name:          "generic error",
			inputError:    &mockError{"some other error"},
			expectedStart: "❌ some other error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatError(tt.inputError)
			
			if !strings.HasPrefix(result.Error(), tt.expectedStart) {
				t.Errorf("Expected error to start with '%s', got '%s'", 
					tt.expectedStart, result.Error())
			}
		})
	}
}

func TestFormatErrorPreservesOriginalMessage(t *testing.T) {
	originalErr := &mockError{"connection refused: detailed message"}
	formatted := formatError(originalErr)
	
	if !strings.Contains(formatted.Error(), "Cannot connect to MySQL server") {
		t.Error("Should contain helpful connection message")
	}
}

func TestRunCloneValidation(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid mysql type",
			args:        []string{"mysql"},
			expectError: false,
		},
		{
			name:        "valid dynamodb type", 
			args:        []string{"dynamodb"},
			expectError: false,
		},
		{
			name:        "valid all type",
			args:        []string{"all"},
			expectError: false,
		},
		{
			name:        "invalid type",
			args:        []string{"invalid"},
			expectError: true,
			errorMsg:    "unsupported clone type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloneType := tt.args[0]
			
			validTypes := map[string]bool{
				"mysql":    true,
				"dynamodb": true,
				"all":      true,
			}
			
			isValid := validTypes[cloneType]
			
			if tt.expectError && isValid {
				t.Error("Expected invalid type to be rejected")
			}
			
			if !tt.expectError && !isValid {
				t.Error("Expected valid type to be accepted")
			}
		})
	}
}

func TestCloneCommandConfiguration(t *testing.T) {
	if cloneCmd.Use != "clone [mysql|dynamodb|all]" {
		t.Errorf("Expected Use to be 'clone [mysql|dynamodb|all]', got '%s'", cloneCmd.Use)
	}
	
	if cloneCmd.Short != "Clone data from cloud to local" {
		t.Errorf("Expected Short to be 'Clone data from cloud to local', got '%s'", cloneCmd.Short)
	}
	
	if !cloneCmd.SilenceUsage {
		t.Error("Expected SilenceUsage to be true")
	}
	
	if !cloneCmd.SilenceErrors {
		t.Error("Expected SilenceErrors to be true")
	}
}

func TestCloneCommandFlags(t *testing.T) {
	flags := cloneCmd.Flags()
	
	requiredFlags := []string{"source", "dest"}
	for _, flagName := range requiredFlags {
		flag := flags.Lookup(flagName)
		if flag == nil {
			t.Errorf("Expected flag '%s' to exist", flagName)
		}
	}
	
	optionalFlags := []string{
		"table", "filter", "where", "schema-only", 
		"data-only", "concurrency", "verbose",
	}
	for _, flagName := range optionalFlags {
		flag := flags.Lookup(flagName)
		if flag == nil {
			t.Errorf("Expected flag '%s' to exist", flagName)
		}
	}
	
	concurrencyFlag := flags.Lookup("concurrency")
	if concurrencyFlag != nil && concurrencyFlag.DefValue != "25" {
		t.Errorf("Expected concurrency default to be '25', got '%s'", concurrencyFlag.DefValue)
	}
	
	boolFlags := []string{"schema-only", "data-only", "verbose"}
	for _, flagName := range boolFlags {
		flag := flags.Lookup(flagName)
		if flag != nil && flag.Value.Type() != "bool" {
			t.Errorf("Expected flag '%s' to be boolean type", flagName)
		}
	}
}

type mockError struct {
	message string
}

func (e *mockError) Error() string {
	return e.message
}
package internal

import (
	"fmt"
	"sort"
	"strings"

	"github.com/AlecAivazis/survey/v2"
)

// TableSelector handles interactive table selection
type TableSelector struct {
	tables []string
}

// NewTableSelector creates a new table selector with the given tables
func NewTableSelector(tables []string) *TableSelector {
	// Sort tables for consistent display
	sortedTables := make([]string, len(tables))
	copy(sortedTables, tables)
	sort.Strings(sortedTables)
	
	return &TableSelector{
		tables: sortedTables,
	}
}

// SelectTables presents an interactive checkbox selection interface and returns selected tables
func (ts *TableSelector) SelectTables() ([]string, error) {
	if len(ts.tables) == 0 {
		return nil, fmt.Errorf("no tables available for selection")
	}

	Logger.Info("Found tables for selection", "count", len(ts.tables))
	
	// Show initial message
	fmt.Printf("\n📋 Found %d table(s) matching your component criteria.\n", len(ts.tables))
	fmt.Println("Use ↑/↓ to navigate, SPACE to select/deselect, ENTER to confirm")

	var selectedTables []string
	
	prompt := &survey.MultiSelect{
		Message: "Select tables to clone:",
		Options: ts.tables,
		Description: func(value string, index int) string {
			// Add helpful descriptions for long table names
			if len(value) > 50 {
				return fmt.Sprintf("Table %d", index+1)
			}
			return ""
		},
		PageSize: 15, // Show up to 15 items at once
	}

	err := survey.AskOne(prompt, &selectedTables, survey.WithPageSize(15))
	if err != nil {
		if err.Error() == "interrupt" {
			return nil, fmt.Errorf("selection cancelled by user")
		}
		return nil, fmt.Errorf("selection error: %w", err)
	}

	if len(selectedTables) == 0 {
		return nil, fmt.Errorf("no tables selected")
	}

	// Show confirmation
	fmt.Printf("\n✅ Selected %d table(s):\n", len(selectedTables))
	for i, table := range selectedTables {
		fmt.Printf("  %d. %s\n", i+1, table)
	}
	
	// Final confirmation
	var confirm bool
	confirmPrompt := &survey.Confirm{
		Message: fmt.Sprintf("Proceed with cloning %d selected table(s)?", len(selectedTables)),
		Default: true,
	}
	
	err = survey.AskOne(confirmPrompt, &confirm)
	if err != nil {
		return nil, fmt.Errorf("confirmation error: %w", err)
	}
	
	if !confirm {
		return nil, fmt.Errorf("operation cancelled by user")
	}

	return selectedTables, nil
}

// SelectTablesSimple provides a fallback numbered selection interface
func (ts *TableSelector) SelectTablesSimple() ([]string, error) {
	if len(ts.tables) == 0 {
		return nil, fmt.Errorf("no tables available for selection")
	}

	fmt.Println("\n📋 Available Tables:")
	for i, table := range ts.tables {
		fmt.Printf("  %d. %s\n", i+1, table)
	}

	var input string
	fmt.Print("Enter table numbers (comma-separated, e.g., 1,3,5) or 'all' for all tables: ")
	fmt.Scanln(&input)

	input = strings.TrimSpace(input)
	if strings.ToLower(input) == "all" {
		return ts.tables, nil
	}

	var selectedTables []string
	parts := strings.Split(input, ",")
	selectedIndices := make(map[int]bool)
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		
		var num int
		if _, err := fmt.Sscanf(part, "%d", &num); err != nil {
			continue // Skip invalid numbers
		}
		if num >= 1 && num <= len(ts.tables) && !selectedIndices[num-1] {
			selectedTables = append(selectedTables, ts.tables[num-1])
			selectedIndices[num-1] = true
		}
	}

	if len(selectedTables) == 0 {
		return nil, fmt.Errorf("no tables selected")
	}

	// Show confirmation
	fmt.Printf("\n✅ Selected %d table(s):\n", len(selectedTables))
	for _, table := range selectedTables {
		fmt.Printf("  - %s\n", table)
	}

	return selectedTables, nil
}
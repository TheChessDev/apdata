package internal

import (
	"fmt"
	"os"
	"time"
)

func SimpleSpinner(message string, operation func() error) error {
	if VerboseMode {
		return operation()
	}

	done := make(chan bool)
	errCh := make(chan error)

	// Start the spinner in a goroutine
	go func() {
		frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				fmt.Printf("\r%s %s", frames[i%len(frames)], message)
				os.Stdout.Sync()
				i++
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Run the operation in another goroutine
	go func() {
		errCh <- operation()
	}()

	// Wait for operation to complete
	err := <-errCh
	done <- true

	// Clear the spinner line and show result
	fmt.Print("\r\033[K")
	if err != nil {
		fmt.Printf("❌ Failed: %s", message)
	} else {
		fmt.Printf("✅ %s", message)
	}
	os.Stdout.Sync()

	return err
}

// FinishLine moves to the next line after a series of related operations
func FinishLine() {
	if !VerboseMode {
		fmt.Print("\n")
		os.Stdout.Sync()
	}
}
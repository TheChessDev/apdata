package internal

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Spinner struct {
	frames   []string
	interval time.Duration
	message  string
	writer   io.Writer
	active   bool
	mu       sync.Mutex
	done     chan struct{}
}

func NewSpinner(message string) *Spinner {
	return &Spinner{
		frames:   []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		interval: 100 * time.Millisecond,
		message:  message,
		writer:   os.Stdout,
		done:     make(chan struct{}),
	}
}

func (s *Spinner) Start() {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return
	}
	s.active = true
	s.mu.Unlock()

	go func() {
		frameIndex := 0
		for {
			select {
			case <-s.done:
				s.clearLine()
				return
			default:
				s.mu.Lock()
				frame := s.frames[frameIndex%len(s.frames)]
				fmt.Fprintf(s.writer, "\r%s %s", frame, s.message)
				if f, ok := s.writer.(*os.File); ok {
					f.Sync()
				}
				s.mu.Unlock()
				
				frameIndex++
				time.Sleep(s.interval)
			}
		}
	}()
}

func (s *Spinner) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.active {
		return
	}
	
	s.active = false
	close(s.done)
	s.done = make(chan struct{})
}

func (s *Spinner) Success(message string) {
	s.Stop()
	fmt.Fprintf(s.writer, "\r✅ %s\n", message)
}

func (s *Spinner) Error(message string) {
	s.Stop()
	fmt.Fprintf(s.writer, "\r❌ %s\n", message)
}

func (s *Spinner) UpdateMessage(message string) {
	s.mu.Lock()
	s.message = message
	s.mu.Unlock()
}

func (s *Spinner) clearLine() {
	fmt.Fprint(s.writer, "\r\033[K")
}

func WithSpinner(message string, operation func() error) error {
	return WithSpinnerConditional(message, operation, true)
}

func WithSpinnerConditional(message string, operation func() error, showSpinner bool) error {
	if !showSpinner {
		return operation()
	}
	
	spinner := NewSpinner(message)
	spinner.Start()
	
	err := operation()
	
	if err != nil {
		spinner.Error(fmt.Sprintf("Failed: %s", message))
		return err
	}
	
	spinner.Success(message)
	return nil
}
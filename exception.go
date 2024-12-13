package dagsterpipes

import (
	"fmt"
	"runtime"
)

// Exception represents a structured error with detailed context.
type Exception struct {
	Name    string     `json:"name"`
	Message string     `json:"message"`
	Cause   *Exception `json:"cause"`
	Stack   []string   `json:"stack"`
	Context []string   `json:"context"`
}

// NewException creates a new Exception from a given error.
// If includeStackTrace is true, it captures the stack trace.
func NewException(err error, includeStackTrace bool) *Exception {
	if err == nil {
		return nil
	}

	var stack []string
	if includeStackTrace {
		stack = captureStackTrace()
	}

	return &Exception{
		Name:    getTypeName(err),
		Message: err.Error(),
		Cause:   NewException(getCause(err), false),
		Stack:   stack,
		Context: []string{},
	}
}

// captureStackTrace captures the current stack trace as a slice of strings.
func captureStackTrace() []string {
	var stack []string

	pcs := make([]uintptr, 10)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])

	for {
		frame, more := frames.Next()
		stack = append(stack, frame.Function+" at "+frame.File+":"+string(rune(frame.Line)))

		if !more {
			break
		}
	}

	return stack
}

// getTypeName gets the type name of an error.
func getTypeName(err error) string {
	if err == nil {
		return ""
	}

	return fmt.Sprintf("%T", err)
}

// getCause retrieves the cause of an error if it supports wrapping.
func getCause(err error) error {
	type causer interface {
		Unwrap() error
	}

	if c, ok := err.(causer); ok {
		return c.Unwrap()
	}

	return nil
}

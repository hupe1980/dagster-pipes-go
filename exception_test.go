package dagsterpipes

import (
	"errors"
	"fmt"
	"testing"
)

func TestException(t *testing.T) {
	t.Run("NewException", func(t *testing.T) {
		t.Run("NoError", func(t *testing.T) {
			exception := NewException(nil, true)
			if exception != nil {
				t.Fatalf("Expected nil exception, got %v", exception)
			}
		})

		t.Run("SimpleError", func(t *testing.T) {
			err := errors.New("simple error")
			exception := NewException(err, false)

			if exception == nil {
				t.Fatal("Expected non-nil exception")
			}

			if exception.Name != "*errors.errorString" {
				t.Fatalf("Expected Name to be '*errors.errorString', got %s", exception.Name)
			}

			if exception.Message != "simple error" {
				t.Fatalf("Expected Message to be 'simple error', got %s", exception.Message)
			}

			if exception.Cause != nil {
				t.Fatalf("Expected Cause to be nil, got %v", exception.Cause)
			}

			if len(exception.Stack) != 0 {
				t.Fatalf("Expected empty Stack, got %v", exception.Stack)
			}
		})

		t.Run("WithStackTrace", func(t *testing.T) {
			err := errors.New("error with stack trace")
			exception := NewException(err, true)

			if exception == nil {
				t.Fatal("Expected non-nil exception")
			}

			if len(exception.Stack) == 0 {
				t.Fatal("Expected non-empty stack trace")
			}
		})

		t.Run("WithCause", func(t *testing.T) {
			rootErr := errors.New("root error")
			wrappedErr := fmt.Errorf("wrapped error: %w", rootErr)
			exception := NewException(wrappedErr, false)

			if exception == nil {
				t.Fatal("Expected non-nil exception")
			}

			if exception.Cause == nil {
				t.Fatal("Expected non-nil Cause")
			}

			if exception.Cause.Message != "root error" {
				t.Fatalf("Expected Cause Message to be 'root error', got %s", exception.Cause.Message)
			}
		})

		t.Run("NilCause", func(t *testing.T) {
			err := errors.New("error without cause")
			exception := NewException(err, false)

			if exception == nil {
				t.Fatal("Expected non-nil exception")
			}

			if exception.Cause != nil {
				t.Fatalf("Expected Cause to be nil, got %v", exception.Cause)
			}
		})
	})

	t.Run("captureStackTrace", func(t *testing.T) {
		stack := captureStackTrace()
		if len(stack) == 0 {
			t.Fatal("Expected non-empty stack trace")
		}

		if stack[0] == "" {
			t.Fatal("Expected first stack frame to be non-empty")
		}
	})

	t.Run("getTypeName", func(t *testing.T) {
		err := errors.New("some error")
		typeName := getTypeName(err)

		if typeName != "*errors.errorString" {
			t.Fatalf("Expected '*errors.errorString', got %s", typeName)
		}
	})

	t.Run("getCause", func(t *testing.T) {
		t.Run("NoCause", func(t *testing.T) {
			err := errors.New("no cause")
			cause := getCause(err)

			if cause != nil {
				t.Fatalf("Expected nil cause, got %v", cause)
			}
		})

		t.Run("WithCause", func(t *testing.T) {
			rootErr := errors.New("root error")
			wrappedErr := fmt.Errorf("wrapped error: %w", rootErr)
			cause := getCause(wrappedErr)

			if cause == nil {
				t.Fatal("Expected non-nil cause")
			}

			if cause.Error() != "root error" {
				t.Fatalf("Expected cause message to be 'root error', got %s", cause.Error())
			}
		})
	})
}

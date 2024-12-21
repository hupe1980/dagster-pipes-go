package dagsterpipes

import (
	"encoding/json"
	"fmt"
	"os"
)

// MessageChannel represents an interface for writing messages.
type MessageChannel interface {
	// WriteMessage writes a Message to the underlying channel.
	WriteMessage(message Message) error
	// Close closes the underlying channel and releases any associated resources.
	// After calling Close, the channel cannot be used for further writes.
	Close() error
}

// FileMessageWriterChannel implements the MessageChannel interface.
// It writes messages to a specified file.
type FileMessageWriterChannel struct {
	file      *os.File      // Open file handle for writing messages.
	messages  chan Message  // Channel for buffering messages.
	closeChan chan struct{} // Channel to signal the writer goroutine to stop.
	done      chan struct{} // Channel to indicate all writes are completed.
}

// NewFileMessageWriterChannel creates a new FileMessageWriterChannel.
// The provided path specifies the file location for message writing.
func NewFileMessageWriterChannel(path string) (*FileMessageWriterChannel, error) {
	// Open the file once in append mode and create it if it doesn't exist.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	fmw := &FileMessageWriterChannel{
		file:      file,
		messages:  make(chan Message, 30),
		closeChan: make(chan struct{}),
		done:      make(chan struct{}),
	}

	// Start the writer goroutine.
	go fmw.startWriter()

	return fmw, nil
}

// WriteMessage sends a message to the channel for asynchronous writing.
func (f *FileMessageWriterChannel) WriteMessage(message Message) error {
	select {
	case f.messages <- message:
		// Successfully enqueued message.
		return nil
	default:
		// Handle dropped messages if necessary.
		return fmt.Errorf("message channel buffer full, message dropped")
	}
}

// Close signals the writer to stop, waits for all writes to complete, and closes the file.
func (f *FileMessageWriterChannel) Close() error {
	// Signal the writer to stop.
	close(f.closeChan)

	// Wait for the writer to finish.
	<-f.done

	// Close the file and return any errors encountered.
	return f.file.Close()
}

// startWriter continuously writes messages from the channel to the file.
func (f *FileMessageWriterChannel) startWriter() {
	defer close(f.done) // Ensure completion signal is sent.

	for {
		select {
		case msg := <-f.messages:
			// Process message if received.
			f.writeToFile(msg)

		case <-f.closeChan:
			// After receiving the close signal, drain remaining messages.
			f.drainMessages()
			return
		}
	}
}

// drainMessages writes remaining messages in the channel to the file.
func (f *FileMessageWriterChannel) drainMessages() {
	for {
		select {
		case msg := <-f.messages:
			f.writeToFile(msg)
		default:
			// Exit the loop when no more messages are pending.
			return
		}
	}
}

// writeToFile serializes and writes a message to the file.
func (f *FileMessageWriterChannel) writeToFile(msg Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		// Handle serialization errors (e.g., log them).
		fmt.Printf("Error marshaling message: %v\n", err)
		return
	}

	_, err = f.file.Write(append(data, '\n'))
	if err != nil {
		// Handle file writing errors (e.g., log them).
		fmt.Printf("Error writing message to file: %v\n", err)
	}
}

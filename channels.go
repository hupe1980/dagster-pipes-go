package dagsterpipes

import (
	"encoding/json"
	"os"
	"sync"
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
	mu   sync.Mutex // Protects concurrent access to the file handle.
	file *os.File   // Open file handle for writing messages.
}

// NewFileMessageWriterChannel creates a new FileMessageWriterChannel.
// The provided path specifies the file location for message writing.
func NewFileMessageWriterChannel(path string) (*FileMessageWriterChannel, error) {
	// Open the file once in append mode and create it if it doesn't exist.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &FileMessageWriterChannel{file: file}, nil
}

// WriteMessage writes a Message to the file specified in the channel's Path.
// If the file does not exist, it creates it. Messages are appended to the file,
// with each message serialized as a JSON object followed by a newline.
func (f *FileMessageWriterChannel) WriteMessage(message Message) error {
	// Serialize the message to JSON.
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Lock to ensure only one goroutine writes at a time.
	f.mu.Lock()
	defer f.mu.Unlock()

	// Write the JSON data to the file with a newline.
	_, err = f.file.Write(append(data, '\n'))

	return err
}

// Close closes the underlying file handle. Should be called to release resources.
func (f *FileMessageWriterChannel) Close() error {
	// Lock to ensure no writes occur during closing.
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.file.Close()
}

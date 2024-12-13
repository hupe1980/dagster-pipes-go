package dagsterpipes

import (
	"encoding/json"
	"os"
)

// MessageChannel represents an interface for writing messages.
type MessageChannel interface {
	// WriteMessage writes a Message to the underlying channel.
	WriteMessage(message Message) error
}

// FileMessageWriterChannel implements the MessageChannel interface.
// It writes messages to a specified file.
type FileMessageWriterChannel struct {
	Path string // Path to the file where messages will be written.
}

// NewFileMessageWriterChannel creates a new FileMessageWriterChannel.
// The provided path specifies the file location for message writing.
func NewFileMessageWriterChannel(path string) *FileMessageWriterChannel {
	return &FileMessageWriterChannel{Path: path}
}

// WriteMessage writes a Message to the file specified in the channel's Path.
// If the file does not exist, it creates it. Messages are appended to the file,
// with each message serialized as a JSON object followed by a newline.
func (f *FileMessageWriterChannel) WriteMessage(message Message) error {
	// Open or create the file with append and write-only permissions.
	file, err := os.OpenFile(f.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close() // Ensure the file is closed after the operation.

	// Marshal the Message into JSON format.
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Write the JSON data to the file, appending a newline at the end.
	_, err = file.Write(append(data, '\n'))

	return err
}

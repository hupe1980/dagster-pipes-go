package dagsterpipes

import "errors"

// MessageWriter defines an interface for creating and managing message channels.
// It includes methods for opening a message channel and retrieving additional
// metadata about the opened channel.
type MessageWriter interface {
	// Open initializes a MessageChannel using the provided parameters.
	// Returns the created MessageChannel or an error if initialization fails.
	Open(params *MessagesParams) (MessageChannel, error)

	// GetOpenedExtras retrieves any additional metadata or information
	// associated with the opened message channel.
	GetOpenedExtras() map[string]any
}

// DefaultMessageWriter is the default implementation of the MessageWriter interface.
// It supports file-based message channels and serves as a fallback for unrecognized types.
type DefaultMessageWriter struct{}

// Open initializes a file-based MessageChannel if a valid path is provided in the parameters.
// Returns the created MessageChannel or an error if the path is missing or unsupported.
func (mw *DefaultMessageWriter) Open(params *MessagesParams) (MessageChannel, error) {
	if params.Path != "" {
		return NewFileMessageWriterChannel(params.Path), nil
	}

	// TODO: Extend support for additional message writer types.
	return nil, errors.New("no path provided")
}

// GetOpenedExtras provides additional metadata for the opened message channel.
// In the default implementation, this method returns an empty map.
func (mw *DefaultMessageWriter) GetOpenedExtras() map[string]any {
	return map[string]any{}
}

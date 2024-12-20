package dagsterpipes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
)

// Options defines configuration options for creating a new Context.
type Options[T any] struct {
	ParamsLoader  ParamsLoader[T]  // Loader for context parameters.
	ContextLoader ContextLoader[T] // Loader for the execution context.
	MessageWriter MessageWriter    // Writer for communication messages.
	Logger        *slog.Logger     // Logger instance for logging messages.
}

// Context represents a Dagster Pipes execution context.
type Context[T any] struct {
	data             *ContextData[T] // Contextual data for the process.
	messageChannel   MessageChannel  // Channel to communicate messages.
	materializedKeys map[string]any  // Tracks materialized assets to prevent duplicates.
	exception        *Exception      // Holds the exception if one is reported.
	closed           bool            // Indicates whether the context has been closed.
	logger           *slog.Logger    // Logger instance for logging messages.
	mu               sync.RWMutex    // Mutex to protect shared state
}

// NewContext initializes a new Context using the provided configuration functions.
// Validates the Dagster Pipes process and sets up context, messages, and communication channels.
func NewContext[T any](optFns ...func(o *Options[T])) (*Context[T], error) {
	opts := Options[T]{
		ParamsLoader:  &EnvVarParamsLoader[T]{},
		ContextLoader: &DefaultContextLoader[T]{},
		MessageWriter: &DefaultMessageWriter{},
		Logger:        slog.Default(),
	}

	for _, fn := range optFns {
		fn(&opts)
	}

	if !opts.ParamsLoader.IsDagsterPipesProcess() {
		return nil, errors.New("not a Dagster Pipes process")
	}

	contextParams, err := opts.ParamsLoader.LoadContextParams()
	if err != nil {
		return nil, err
	}

	data, err := opts.ContextLoader.LoadContext(contextParams)
	if err != nil {
		return nil, err
	}

	messagesParams, err := opts.ParamsLoader.LoadMessagesParams()
	if err != nil {
		return nil, err
	}

	messageChannel, err := opts.MessageWriter.Open(messagesParams)
	if err != nil {
		return nil, err
	}

	pc := &Context[T]{
		data:             data,
		messageChannel:   messageChannel,
		materializedKeys: make(map[string]any),
		closed:           false,
		logger:           opts.Logger,
	}

	if err := pc.writeMessage(MethodOpened, &Opened{Extras: opts.MessageWriter.OpenedExtras()}); err != nil {
		return nil, err
	}

	return pc, nil
}

// RunID retrieves the run identifier from the context data.
func (c *Context[T]) RunID() string {
	return c.data.RunID
}

// Extras retrieves additional data associated with the context.
func (c *Context[T]) Extras() T {
	return c.data.Extras
}

// AssetKeys retrieves the list of asset keys associated with the context.
func (c *Context[T]) AssetKeys() []string {
	return c.data.AssetKeys
}

// Close closes the context and sends a "closed" message.
// Ensures the context cannot be used after it is closed.
func (c *Context[T]) Close() error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	if err := c.writeMessage(MethodClosed, &Closed{Exception: c.exception}); err != nil {
		return err
	}

	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	return nil
}

// IsClosed checks if the context is closed.
func (c *Context[T]) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.closed
}

// ReportAssetMaterialization reports an asset materialization event.
// Ensures duplicate materializations for the same asset key are prevented.
func (c *Context[T]) ReportAssetMaterialization(materialization *AssetMaterialization) error {
	assetKey := materialization.AssetKey

	c.mu.RLock()
	if _, exists := c.materializedKeys[assetKey]; exists {
		c.mu.RUnlock()
		return fmt.Errorf("asset with key %s has already been materialized", assetKey)
	}
	c.mu.RUnlock()

	assetKey, err := c.resolveOptionallyPassedAssetKey(assetKey)
	if err != nil {
		return err
	}

	materialization.AssetKey = assetKey

	if err := c.writeMessage(MethodReportAssetMaterialization, materialization); err != nil {
		return err
	}

	c.mu.Lock()
	c.materializedKeys[assetKey] = struct{}{}
	c.mu.Unlock()

	return nil
}

// ReportAssetCheck sends a report for an asset check event.
func (c *Context[T]) ReportAssetCheck(check *AssetCheck) error {
	return c.writeMessage(MethodReportAssetCheck, check)
}

// ReportCustomMessage sends a custom message through the context.
func (c *Context[T]) ReportCustomMessage(msg *CustomMessage) error {
	return c.writeMessage(MethodReportCustomMessage, msg)
}

// ReportException records an exception in the context for later reporting.
func (c *Context[T]) ReportException(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.exception = NewException(err, true)

	return nil
}

// LogDebug logs a debug-level message using the context's logger.
// Also sends the log message to the message channel.
func (c *Context[T]) LogDebug(message string) error {
	return c.log(slog.LevelDebug, message)
}

// LogInfo logs an informational message using the context's logger.
// Also sends the log message to the message channel.
func (c *Context[T]) LogInfo(message string) error {
	return c.log(slog.LevelInfo, message)
}

// LogWarn logs a warning-level message using the context's logger.
// Also sends the log message to the message channel.
func (c *Context[T]) LogWarn(message string) error {
	return c.log(slog.LevelWarn, message)
}

// LogError logs an error-level message using the context's logger.
// Also sends the log message to the message channel.
func (c *Context[T]) LogError(message string) error {
	return c.log(slog.LevelError, message)
}

// log sends a log message at the specified level using the context's logger.
// Writes the same message to the message channel for external processing.
func (c *Context[T]) log(level slog.Level, message string) error {
	c.logger.Log(context.Background(), level, message)

	if err := c.writeMessage(MethodLog, &Log{Message: message, Level: level.String()}); err != nil {
		return err
	}

	return nil
}

// resolveOptionallyPassedAssetKey resolves the provided asset key based on context data.
// Handles validation and deduplication of asset keys.
func (c *Context[T]) resolveOptionallyPassedAssetKey(assetKey string) (string, error) {
	if !c.data.HasAssetKeys() {
		return "", errors.New("no asset keys were passed")
	}

	if assetKey != "" {
		if slices.Contains(c.data.AssetKeys, assetKey) {
			return assetKey, nil
		}

		return "", fmt.Errorf("asset key %s is not in the list of asset keys %v", assetKey, c.data.AssetKeys)
	}

	if c.data.IsMultiAsset() {
		return "", errors.New("multiple asset keys were passed, but no asset key was specified")
	}

	return c.data.AssetKeys[0], nil
}

// writeMessage sends a message to the message channel.
// Ensures the context is not closed before sending the message.
func (c *Context[T]) writeMessage(method Method, params any) error {
	if c.IsClosed() {
		return errors.New("cannot send message after pipes context is closed")
	}

	msg := Message{
		DagsterPipesVersion: ProtocolVersion,
		Method:              method,
		Params:              params,
	}

	return c.messageChannel.WriteMessage(msg)
}

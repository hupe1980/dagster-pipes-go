package dagsterpipes

import (
	"errors"
	"fmt"
	"slices"
)

// Options defines configuration for creating a new Context.
type Options[T any] struct {
	ParamsLoader  ParamsLoader[T]  // Loader for context parameters.
	ContextLoader ContextLoader[T] // Loader for the execution context.
	MessageWriter MessageWriter    // Writer for communication messages.
}

// Context represents a Dagster Pipes execution context.
type Context[T any] struct {
	data             *ContextData[T] // Contextual data for the process.
	messageChannel   MessageChannel  // Channel to communicate messages.
	materializedKeys map[string]any  // Tracks materialized assets to prevent duplicates.
	exception        *Exception      // Holds the exception if one is reported.
	closed           bool            // Indicates whether the context has been closed.
}

// NewContext initializes a new Context using the provided options functions.
// It validates the Dagster Pipes process and sets up the context, messages, and communication channels.
func NewContext[T any](optFns ...func(o *Options[T])) (*Context[T], error) {
	opts := Options[T]{
		ParamsLoader:  &EnvVarParamsLoader[T]{},
		ContextLoader: &DefaultContextLoader[T]{},
		MessageWriter: &DefaultMessageWriter{},
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

// Close closes the context and sends a closed message.
func (c *Context[T]) Close() error {
	if c.closed {
		return nil
	}

	if err := c.writeMessage(MethodClosed, &Closed{Exception: c.exception}); err != nil {
		return err
	}

	c.closed = true

	return nil
}

// IsClosed checks if the context is closed.
func (c *Context[T]) IsClosed() bool {
	return c.closed
}

// ReportAssetMaterialization reports an asset materialization event.
func (c *Context[T]) ReportAssetMaterialization(materialization *AssetMaterialization) error {
	assetKey := materialization.AssetKey

	if _, exists := c.materializedKeys[assetKey]; exists {
		return fmt.Errorf("asset with key %s has already been materialized", assetKey)
	}

	assetKey, err := c.resolveOptionallyPassedAssetKey(assetKey)
	if err != nil {
		return err
	}

	materialization.AssetKey = assetKey

	if err := c.writeMessage(MethodReportAssetMaterialization, materialization); err != nil {
		return err
	}

	c.materializedKeys[assetKey] = struct{}{}

	return nil
}

// ReportAssetCheck reports an asset check event.
func (c *Context[T]) ReportAssetCheck(check *AssetCheck) error {
	return c.writeMessage(MethodReportAssetCheck, check)
}

// ReportCustomMessage reports a custom message.
func (c *Context[T]) ReportCustomMessage(msg *CustomMessage) error {
	return c.writeMessage(MethodReportCustomMessage, msg)
}

// ReportException records an exception in the context.
func (c *Context[T]) ReportException(err error) error {
	c.exception = NewException(err, true)
	return nil
}

// resolveOptionallyPassedAssetKey resolves an optionally passed asset key.
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

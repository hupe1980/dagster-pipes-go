package dagsterpipes

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// ContextData represents the runtime context for a Dagster Pipes process,
// including information about asset keys, the run ID, and any additional metadata.
type ContextData[T any] struct {
	AssetKeys []string `json:"asset_keys"` // List of asset keys related to the current context.
	RunID     string   `json:"run_id"`     // Unique identifier for the current Dagster run.
	Extras    T        `json:"extras"`     // Additional context-specific metadata.
}

// HasAssetKeys checks if any asset keys are defined in the context.
func (d *ContextData[T]) HasAssetKeys() bool {
	return len(d.AssetKeys) > 0
}

// IsMultiAsset checks if multiple asset keys are defined in the context.
func (d *ContextData[T]) IsMultiAsset() bool {
	return len(d.AssetKeys) > 1
}

// ContextParams represents the parameters used to load a Dagster Pipes context.
type ContextParams[T any] struct {
	Data   *ContextData[T] `json:"data"`   // Context data provided inline.
	Path   string          `json:"path"`   // File path to load the context data from.
	Extras map[string]any  `json:"extras"` // Additional parameters.
}

// MessagesParams represents parameters for managing messages between Dagster Pipes processes.
type MessagesParams struct {
	Stdio string `json:"stdio"` // Configuration for standard I/O messaging.
	Path  string `json:"path"`  // File path for message exchange.
}

// ParamsLoader defines an interface for loading context and messaging parameters.
type ParamsLoader[T any] interface {
	LoadContextParams() (*ContextParams[T], error) // Load parameters for the Dagster context.
	LoadMessagesParams() (*MessagesParams, error)  // Load parameters for messaging configuration.
	IsDagsterPipesProcess() bool                   // Check if the current process is a Dagster Pipes process.
}

// EnvVarParamsLoader implements the ParamsLoader interface using environment variables.
type EnvVarParamsLoader[T any] struct{}

// LoadContextParams loads context parameters from the environment variable `DAGSTER_PIPES_CONTEXT`.
func (l *EnvVarParamsLoader[T]) LoadContextParams() (*ContextParams[T], error) {
	return loadParamsFromEnvVar[ContextParams[T]]("DAGSTER_PIPES_CONTEXT")
}

// LoadMessagesParams loads messaging parameters from the environment variable `DAGSTER_PIPES_MESSAGES`.
func (l *EnvVarParamsLoader[T]) LoadMessagesParams() (*MessagesParams, error) {
	return loadParamsFromEnvVar[MessagesParams]("DAGSTER_PIPES_MESSAGES")
}

// IsDagsterPipesProcess checks if the `DAGSTER_PIPES_CONTEXT` environment variable is set.
func (l *EnvVarParamsLoader[T]) IsDagsterPipesProcess() bool {
	_, exists := os.LookupEnv("DAGSTER_PIPES_CONTEXT")
	return exists
}

// loadParamsFromEnvVar decodes and loads parameters of type T from a specified environment variable.
func loadParamsFromEnvVar[T any](envVar string) (*T, error) {
	param := os.Getenv(envVar)

	var result T
	if err := decodeEnvVar(param, &result); err != nil {
		return nil, fmt.Errorf("failed to decode %s: %w", envVar, err)
	}

	return &result, nil
}

// decodeEnvVar decodes and decompresses a zlib-compressed, base64-encoded string into a Go object.
func decodeEnvVar(param string, v any) error {
	decoded, err := base64.StdEncoding.DecodeString(param)
	if err != nil {
		return err
	}

	reader, err := zlib.NewReader(bytes.NewReader(decoded))
	if err != nil {
		return err
	}
	defer reader.Close()

	var decompressed bytes.Buffer
	if _, err = decompressed.ReadFrom(reader); err != nil {
		return err
	}

	return json.Unmarshal(decompressed.Bytes(), v)
}

// ContextLoader defines an interface for loading a Dagster Pipes context.
type ContextLoader[T any] interface {
	LoadContext(params *ContextParams[T]) (*ContextData[T], error) // Load context data from the provided parameters.
}

// DefaultContextLoader is a default implementation of ContextLoader.
type DefaultContextLoader[T any] struct{}

// LoadContext loads context data from a file or inline data in the parameters.
func (l *DefaultContextLoader[T]) LoadContext(params *ContextParams[T]) (*ContextData[T], error) {
	if params.Path != "" {
		return loadContextDataFromFile[T](params.Path)
	}

	if params.Data != nil {
		return params.Data, nil
	}

	return nil, errors.New("invalid params: expected a value in key path or data")
}

// loadContextDataFromFile reads context data from the specified file path.
func loadContextDataFromFile[T any](path string) (*ContextData[T], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data ContextData[T]
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return nil, err
	}

	return &data, nil
}

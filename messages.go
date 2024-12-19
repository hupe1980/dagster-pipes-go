package dagsterpipes

import "encoding/json"

// Constants for metadata type inference.
const (
	MetadataTypeInfer = "__infer__"
)

// Method represents different types of communication methods.
type Method string

const (
	// MethodClosed indicates that the context is closed.
	MethodClosed Method = "closed"

	// MethodLog represents a log message.
	MethodLog Method = "log"

	// MethodOpened indicates that the context is opened.
	MethodOpened Method = "opened"

	// MethodReportAssetMaterialization reports an asset materialization event.
	MethodReportAssetMaterialization Method = "report_asset_materialization"

	// MethodReportAssetCheck reports an asset check event.
	MethodReportAssetCheck Method = "report_asset_check"

	// MethodReportCustomMessage sends a custom message.
	MethodReportCustomMessage Method = "report_custom_message"
)

// Opened represents the parameters for the "opened" method.
type Opened struct {
	Extras map[string]any `json:"extras"` // Additional metadata for the opened method.
}

// Closed represents the parameters for the "closed" method.
type Closed struct {
	Exception *Exception `json:"exception,omitempty"` // An optional exception if the context closed with an error.
}

// Log represents the parameters for the "log" method.
type Log struct {
	Message string `json:"message"` // The log message.
	Level   string `json:"level"`   // The log level (e.g., DEBUG, INFO).
}

// MetadataValue represents a metadata entry with a type and raw value.
type MetadataValue struct {
	RawValue any    `json:"raw_value"` // The raw value of the metadata.
	Type     string `json:"type"`      // The type of the metadata.
}

// AssetMaterialization represents an asset materialization event.
type AssetMaterialization struct {
	AssetKey    string         // The unique key of the asset being materialized.
	DataVersion string         // The version of the asset data.
	Metadata    map[string]any // Metadata associated with the asset materialization.
}

// MarshalJSON serializes AssetMaterialization into JSON, normalizing metadata.
func (a AssetMaterialization) MarshalJSON() ([]byte, error) {
	normalized := struct {
		AssetKey    string                   `json:"asset_key"`
		DataVersion string                   `json:"data_version"`
		Metadata    map[string]MetadataValue `json:"metadata"`
	}{
		AssetKey:    a.AssetKey,
		DataVersion: a.DataVersion,
		Metadata:    normalizeParamMetadata(a.Metadata),
	}

	return json.Marshal(normalized)
}

// AssetCheckSeverity represents the severity levels for asset checks.
type AssetCheckSeverity string

const (
	// AssetCheckSeverityWarn indicates a warning severity level.
	AssetCheckSeverityWarn AssetCheckSeverity = "WARN"

	// AssetCheckSeverityError indicates an error severity level.
	AssetCheckSeverityError AssetCheckSeverity = "ERROR"
)

// AssetCheck represents an asset check event.
type AssetCheck struct {
	AssetKey  string             // The key of the asset being checked.
	CheckName string             // The name of the check being performed.
	Passed    bool               // Whether the check passed or failed.
	Serverity AssetCheckSeverity // The severity of the check result.
	Metadata  map[string]any     // Metadata associated with the asset check.
}

// MarshalJSON serializes AssetCheck into JSON, normalizing metadata.
func (a AssetCheck) MarshalJSON() ([]byte, error) {
	normalized := struct {
		AssetKey  string                   `json:"asset_key"`
		CheckName string                   `json:"check_name"`
		Passed    bool                     `json:"passed"`
		Serverity AssetCheckSeverity       `json:"severity"`
		Metadata  map[string]MetadataValue `json:"metadata"`
	}{
		AssetKey:  a.AssetKey,
		CheckName: a.CheckName,
		Passed:    a.Passed,
		Serverity: a.Serverity,
		Metadata:  normalizeParamMetadata(a.Metadata),
	}

	return json.Marshal(normalized)
}

// CustomMessage represents a custom message to be sent.
type CustomMessage struct {
	Payload any `json:"payload"` // The payload of the custom message.
}

// Message represents a communication message in Dagster Pipes.
type Message struct {
	DagsterPipesVersion string `json:"__dagster_pipes_version"` // The version of Dagster Pipes protocol.
	Method              Method `json:"method"`                  // The communication method.
	Params              any    `json:"params"`                  // The parameters of the message.
}

// normalizeParamMetadata validates and normalizes the metadata parameter.
func normalizeParamMetadata(
	metadata map[string]any,
) map[string]MetadataValue {
	newMetadata := make(map[string]MetadataValue)

	for key, value := range metadata {
		switch v := value.(type) {
		case MetadataValue:
			newMetadata[key] = v
		default:
			newMetadata[key] = MetadataValue{
				RawValue: v,
				Type:     MetadataTypeInfer,
			}
		}
	}

	return newMetadata
}

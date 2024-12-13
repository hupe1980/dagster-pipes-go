// Package dagsterpipes provides tools and utilities for communication and
// interaction within Dagster pipelines. This package defines core components
// for handling contexts, messages, and protocol operations to enable seamless
// integration with Dagster's asset and pipeline infrastructure.
package dagsterpipes

// ProtocolVersion defines the current version of the Dagster Pipes protocol.
// This version is used to ensure compatibility between the client implementation
// and the Dagster system.
const (
	ProtocolVersion = "0.1"
)

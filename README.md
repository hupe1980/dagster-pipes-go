# dagster-pipes-go
![Build Status](https://github.com/hupe1980/dagster-pipes-go/workflows/Build/badge.svg) 
[![Go Reference](https://pkg.go.dev/badge/github.com/hupe1980/dagster-pipes-go.svg)](https://pkg.go.dev/github.com/hupe1980/dagster-pipes-go)
[![goreportcard](https://goreportcard.com/badge/github.com/hupe1980/dagster-pipes-go)](https://goreportcard.com/report/github.com/hupe1980/dagster-pipes-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`dagster-pipes-go` is a Go library for integrating with [Dagster Pipes](https://docs.dagster.io/concepts/pipes), enabling seamless communication between external processes and Dagster workflows. It provides a set of utilities for managing context, logging, reporting asset materializations, and more.

## Features

- **Context Management**: Open and manage Pipes contexts.
- **Message Handling**: Send and receive messages in a structured format.
- **Asset Reporting**: Report asset materializations and checks.
- **Custom Messaging**: Send custom messages for advanced use cases.
- **Error Handling**: Report exceptions gracefully.

## Installation

To install the library, use `go get`:

```sh
go get github.com/hupe1980/dagster-pipes-go
```

## How to use
```golang
package main

import (
	"log"

	dagsterpipes "github.com/hupe1980/dagster-pipes-go"
)

func main() {
	session, err := dagsterpipes.New()
	if err != nil {
		log.Fatalf("Error creating dagster pipes session: %v", err)
	}
	defer session.Close()

	if err := session.Run(func(context *dagsterpipes.Context) error {
		if err := context.ReportAssetMaterialization(&dagsterpipes.AssetMaterialization{
			AssetKey:    "asset",
			DataVersion: "1.0",
			Metadata: map[string]any{
				"foo": "bar",
			},
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Error running dagster pipes session: %v", err)
	}
}
```

## Contributing
Contributions are welcome! If you find bugs or want to suggest features, please open an issue or submit a pull request.

## License
This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
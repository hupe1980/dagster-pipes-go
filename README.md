# dagster-pipes-go

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

## Contributing
Contributions are welcome! If you find bugs or want to suggest features, please open an issue or submit a pull request.

## License
This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
package dagsterpipes

// RunFunc defines a function type that processes a Dagster Pipes context.
// It should return an error if any issues occur during execution.
type RunFunc func(context *Context) error

// Session represents a managed session for interacting with a Dagster Pipes context.
// It ensures proper handling of the context lifecycle.
type Session struct {
	context *Context // The underlying context for the session.
}

// New initializes a new Session by creating and opening a Dagster Pipes context.
// It accepts a variadic list of option functions to configure the context.
// Returns a pointer to the created Session or an error if the context fails to open.
func New(optFns ...func(o *Options)) (*Session, error) {
	context, err := NewContext(optFns...)
	if err != nil {
		return nil, err
	}

	return &Session{context: context}, nil
}

// Context retrieves the underlying context associated with the session.
func (s *Session) Context() *Context {
	return s.context
}

// Run executes the provided RunFunc with the session's context.
// If the RunFunc encounters an error, the error is reported using the context's
// ReportException method. If reporting the error also fails, that error is returned.
// Otherwise, nil is returned.
func (s *Session) Run(fn RunFunc) error {
	return s.runWithContext(fn)
}

// runWithContext is an internal helper that encapsulates error handling logic
// for executing the RunFunc and reporting exceptions.
func (s *Session) runWithContext(fn RunFunc) error {
	if err := fn(s.context); err != nil {
		// Attempt to report the exception
		if reportErr := s.context.ReportException(err); reportErr != nil {
			return reportErr
		}
	}

	return nil
}

// Close finalizes the session by closing the associated context.
// Returns an error if the context fails to close.
func (s *Session) Close() error {
	return s.context.Close()
}

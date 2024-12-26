// Package errors provides error wrapping with location information
package errors

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
)

// Format calls fmt.Errorf(format, a...) then wraps the error with the source
// location of the caller.
func Format(format string, a ...any) error {
	return wrap(fmt.Errorf(format, a...), 2)
}

// As calls errors.As
func As(err error, target any) bool {
	return errors.As(err, target)
}

// Is calls errors.Is
func Is(err error, target error) bool {
	return errors.Is(err, target)
}

// Join calls errors.Join
func Join(errs ...error) error {
	return errors.Join(errs...)
}

// New calls errors.New
func New(text string) error {
	return wrap(errors.New(text), 2)
}

// Unwrap calls errors.Unwrap
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// Source represents the Source file and line where an error was encountered
type Source struct {
	File string `json:"file"`
	Line int    `json:"line"`
}

func (s *Source) Loc() string {
	return fmt.Sprintf("%s:%d", filepath.Base(s.File), s.Line)
}

// ErrorAt wraps an error with the Source location the error was encountered at
// for tracing from a top level error handler.
type ErrorAt struct {
	Err    error
	Source Source
}

// Unwrap implements error wrapping.
func (e *ErrorAt) Unwrap() error {
	return e.Err
}

// Error returns the error string with Source location.
func (e *ErrorAt) Error() string {
	return e.Source.Loc() + ": " + e.Err.Error()
}

func wrap(err error, skip int) error {
	// Nothing to do
	if err == nil {
		return nil
	}

	// Already a holos error no need to do anything.
	var errAt *ErrorAt
	if errors.As(err, &errAt) {
		return err
	}

	// Try to wrap err with caller info
	if _, file, line, ok := runtime.Caller(skip); ok {
		return &ErrorAt{
			Err: err,
			Source: Source{
				File: file,
				Line: line,
			},
		}
	}

	return err
}

// Wrap wraps err in a ErrorAt or returns err if err is nil, already a ErrorAt,
// or caller info is not available.
func Wrap(err error) error {
	return wrap(err, 2)
}

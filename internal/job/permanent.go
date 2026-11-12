package job

import "errors"

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as a failure that retrying can't fix, such as a
// malformed payload. A handler that returns it sends the job straight to the
// dead-letter queue, whatever attempts it has left.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or an error it wraps, came from Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

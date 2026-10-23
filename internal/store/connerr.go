package store

import (
	"crypto/tls"
	"crypto/x509"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE codes and classes that decide whether a connection error is
// worth retrying (https://www.postgresql.org/docs/current/errcodes-appendix.html).
const (
	// sqlstateClassInvalidAuthorization (28xxx): wrong password, no
	// pg_hba.conf entry, unknown role. Retrying cannot fix it.
	sqlstateClassInvalidAuthorization = "28"
	// sqlstateInvalidCatalogName (3D000): the database does not exist.
	sqlstateInvalidCatalogName = "3D000"
)

// pgconnRefusedTLS is the exact text of pgconn's error when the server
// answers an SSLRequest with "N". pgconn creates it with errors.New in
// startTLS and exports no type or sentinel for it, so the text is the only
// handle; it is compared for equality on a single leaf error only.
const pgconnRefusedTLS = "server refused TLS connection"

// isTransientConnectError reports whether a failed connect or ping may
// succeed if retried.
//
// pgx tries several connection attempts per connect (each host, and with
// sslmode prefer or allow, TLS and plaintext) and joins their errors
// (errors.Join of per-attempt errors inside *pgconn.ConnectError). The
// failure is transient if any attempt failed transiently. With the default
// sslmode=prefer against a server without TLS, the TLS attempt is always
// refused; a "starting up" on the plaintext attempt must still be retried.
//
// For one attempt: the server being down, starting up (57P03), shutting
// down (57P01, 57P02), out of connections (53300), unresolvable or slow are
// transient. A server that was reached but rejects the configuration is
// permanent: an authorization failure (class 28), a missing database
// (3D000), or a TLS setup failure (refused, or the certificate does not
// verify). Anything else defaults to transient, so an unknown hiccup cannot
// fail a start that a retry would have saved.
func isTransientConnectError(err error) bool {
	if err == nil {
		return false
	}
	if permanent, decided := classifyNode(err); decided {
		return !permanent
	}
	switch u := err.(type) { //nolint:errorlint // walks the tree by hand: errors.As would cross attempts.
	case interface{ Unwrap() []error }:
		// Separate attempts: transient if any of them is.
		children := u.Unwrap()
		for _, child := range children {
			if isTransientConnectError(child) {
				return true
			}
		}
		return len(children) == 0
	case interface{ Unwrap() error }:
		if child := u.Unwrap(); child != nil {
			return isTransientConnectError(child)
		}
	}
	return true
}

// classifyNode classifies err by its own type only, without looking at what
// it wraps (that would cross from one joined attempt into another).
// decided is false when err says nothing by itself.
func classifyNode(err error) (permanent, decided bool) {
	switch e := err.(type) { //nolint:errorlint // this node only; wrapped errors are walked by the caller.
	case *pgconn.PgError:
		return strings.HasPrefix(e.Code, sqlstateClassInvalidAuthorization) || e.Code == sqlstateInvalidCatalogName, true
	case *tls.CertificateVerificationError, x509.UnknownAuthorityError, x509.HostnameError,
		x509.CertificateInvalidError:
		return true, true
	case interface{ Unwrap() error }, interface{ Unwrap() []error }:
		return false, false // a wrapper: decided by what it wraps
	}
	// A leaf error: pgconn's TLS refusal is recognized by its exact text.
	if err.Error() == pgconnRefusedTLS {
		return true, true
	}
	return false, false
}

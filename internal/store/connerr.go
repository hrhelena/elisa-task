package store

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
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

// pgconnRefusedTLS is the text of pgconn's (unexported, unwrapped) error for
// sslmode=require against a server without TLS.
const pgconnRefusedTLS = "server refused TLS connection"

// isTransientConnectError reports whether a failed connect or ping may
// succeed if retried. The server being down, starting up (57P03), shutting
// down (57P01, 57P02), out of connections (53300), unresolvable or slow are
// transient. A server that was reached but rejects the configuration is
// not: an authorization failure (class 28), a missing database (3D000), or
// a TLS setup failure (refused, or the certificate does not verify). Any
// other error defaults to transient, so an unknown hiccup cannot fail a
// start that a retry would have saved.
func isTransientConnectError(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		if strings.HasPrefix(pgErr.Code, sqlstateClassInvalidAuthorization) || pgErr.Code == sqlstateInvalidCatalogName {
			return false
		}
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return false
	}
	if _, ok := errors.AsType[x509.UnknownAuthorityError](err); ok {
		return false
	}
	if _, ok := errors.AsType[x509.HostnameError](err); ok {
		return false
	}
	if _, ok := errors.AsType[x509.CertificateInvalidError](err); ok {
		return false
	}
	return !strings.Contains(err.Error(), pgconnRefusedTLS)
}

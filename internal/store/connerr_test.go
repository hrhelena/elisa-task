package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// wrapped mimics how pgx reports a connect failure: the cause sits under
// per-host errors joined together and a "failed to connect" wrapper.
func wrapped(err error) error {
	return fmt.Errorf("failed to connect to `user=enron database=enron`: %w",
		errors.Join(fmt.Errorf("127.0.0.1:5432 (localhost): %w", err)))
}

func pgErr(code string) error { return &pgconn.PgError{Severity: "FATAL", Code: code} }

// Blocking finding 1 on PR #31: only failures that may pass are retried.
func TestIsTransientConnectError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// Permanent: the server answered and rejected the configuration.
		{"wrong password 28P01", pgErr("28P01"), false},
		{"no pg_hba entry 28000", pgErr("28000"), false},
		{"missing database 3D000", pgErr("3D000"), false},
		{"server refused TLS", errors.New("server refused TLS connection"), false},
		{"certificate does not verify", &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}, false},
		{"unknown authority", x509.UnknownAuthorityError{}, false},
		{"hostname mismatch", x509.HostnameError{Host: "db"}, false},
		{"expired certificate", x509.CertificateInvalidError{Reason: x509.Expired}, false},

		// Transient: the server is not (yet) able to serve us.
		{"starting up 57P03", pgErr("57P03"), true},
		{"admin shutdown 57P01", pgErr("57P01"), true},
		{"crash shutdown 57P02", pgErr("57P02"), true},
		{"too many connections 53300", pgErr("53300"), true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, true},
		{"DNS failure", &net.DNSError{Err: "no such host", Name: "postgres", IsNotFound: true}, true},
		{"timeout", context.DeadlineExceeded, true},
		{"unknown error", errors.New("unexpected EOF"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isTransientConnectError(tt.err); got != tt.want {
				t.Errorf("bare: isTransientConnectError(%v) = %v, want %v", tt.err, got, tt.want)
			}
			if got := isTransientConnectError(wrapped(tt.err)); got != tt.want {
				t.Errorf("wrapped: isTransientConnectError(%v) = %v, want %v", wrapped(tt.err), got, tt.want)
			}
		})
	}
}

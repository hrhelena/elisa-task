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

// connectErr builds an error shaped like pgx 5.11's connect error: one
// wrapped error per attempt (host, TLS or plaintext), joined, under a
// "failed to connect" wrapper, and wrapped again by Ping.
func connectErr(attempts ...error) error {
	perAttempt := make([]error, len(attempts))
	for i, err := range attempts {
		perAttempt[i] = fmt.Errorf("127.0.0.1:5432 (localhost): %w", err)
	}
	return fmt.Errorf("acquiring connection: %w",
		fmt.Errorf("failed to connect to `user=enron database=enron`: %w", errors.Join(perAttempt...)))
}

func pgErr(code string) error { return &pgconn.PgError{Severity: "FATAL", Code: code} }

// errRefusedTLS is pgconn's error for a server that answers SSLRequest with N.
var errRefusedTLS = errors.New("server refused TLS connection")

func TestIsTransientConnectError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		attempts []error
		want     bool
	}{
		// Permanent: the server answered and rejected the configuration.
		{"wrong password 28P01", []error{pgErr("28P01")}, false},
		{"no pg_hba entry 28000", []error{pgErr("28000")}, false},
		{"missing database 3D000", []error{pgErr("3D000")}, false},
		{"sslmode=require, server refused TLS", []error{errRefusedTLS}, false},
		{"certificate does not verify", []error{&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}, false},
		{"unknown authority", []error{x509.UnknownAuthorityError{}}, false},
		{"hostname mismatch", []error{x509.HostnameError{Host: "db"}}, false},
		{"expired certificate", []error{x509.CertificateInvalidError{Reason: x509.Expired}}, false},

		// Transient: the server is not (yet) able to serve us.
		{"starting up 57P03", []error{pgErr("57P03")}, true},
		{"admin shutdown 57P01", []error{pgErr("57P01")}, true},
		{"crash shutdown 57P02", []error{pgErr("57P02")}, true},
		{"too many connections 53300", []error{pgErr("53300")}, true},
		{"connection refused", []error{&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}, true},
		{"DNS failure", []error{&net.DNSError{Err: "no such host", Name: "postgres", IsNotFound: true}}, true},
		{"timeout", []error{context.DeadlineExceeded}, true},
		{"unknown error", []error{errors.New("unexpected EOF")}, true},
		{"message merely mentions the TLS refusal", []error{errors.New("proxy: server refused TLS connection, retry later")}, true},

		// Several attempts (sslmode prefer/allow, or several hosts):
		// transient if any attempt is (PR #31 round 2).
		{"prefer: TLS refused, then starting up", []error{errRefusedTLS, pgErr("57P03")}, true},
		{"allow: starting up, then TLS refused", []error{pgErr("57P03"), errRefusedTLS}, true},
		{"prefer: TLS refused, then too many connections", []error{errRefusedTLS, pgErr("53300")}, true},
		{"allow: too many connections, then TLS refused", []error{pgErr("53300"), errRefusedTLS}, true},
		{"prefer: TLS refused, then connection refused", []error{errRefusedTLS, &net.OpError{Op: "dial", Err: errors.New("connection refused")}}, true},
		{"two hosts: one wrong password, one down", []error{pgErr("28P01"), &net.OpError{Op: "dial", Err: errors.New("connection refused")}}, true},
		{"prefer: TLS refused, then wrong password", []error{errRefusedTLS, pgErr("28P01")}, false},
		{"allow: missing database, then TLS refused", []error{pgErr("3D000"), errRefusedTLS}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := connectErr(tt.attempts...)
			if got := isTransientConnectError(err); got != tt.want {
				t.Errorf("isTransientConnectError(%v) = %v, want %v", err, got, tt.want)
			}
			if len(tt.attempts) == 1 {
				if got := isTransientConnectError(tt.attempts[0]); got != tt.want {
					t.Errorf("bare: isTransientConnectError(%v) = %v, want %v", tt.attempts[0], got, tt.want)
				}
			}
		})
	}
}

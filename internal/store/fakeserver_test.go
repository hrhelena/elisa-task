package store

import (
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// startRefusingServer starts a fake Postgres server without TLS support
// that rejects every startup with a FATAL error carrying code, the way a
// real server reports "starting up" (57P03), "too many connections"
// (53300) or a wrong password (28P01). It returns its address.
func startRefusingServer(t *testing.T, code string) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			wg.Go(func() {
				defer func() { _ = conn.Close() }()
				refuseStartup(conn, code)
			})
		}
	})
	return ln.Addr().String()
}

// refuseStartup answers TLS and GSS encryption requests with "N" (not
// supported) and the startup message with a FATAL ErrorResponse.
func refuseStartup(conn net.Conn, code string) {
	backend := pgproto3.NewBackend(conn, conn)
	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.StartupMessage:
			backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: code, Message: "fake server refuses: " + code})
			_ = backend.Flush()
			return
		default:
			return
		}
	}
}

// PR #31 round 2: with sslmode unset (pgx defaults to prefer) against a
// server without TLS, every failure includes the refused TLS attempt. A
// transient refusal on the plaintext attempt must still be retried, and a
// permanent one must not.
func TestOpenWithDefaultSSLModeClassifiesThePlaintextAttempt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code, sslmode   string
		wantUnreachable bool
	}{
		{"57P03", "", true}, // the database system is starting up
		{"53300", "", true}, // too many connections
		{"57P03", "allow", true},
		{"28P01", "", false}, // wrong password
		{"3D000", "", false}, // missing database
		{"57P03", "require", false},
	}
	for _, tt := range tests {
		t.Run(tt.code+"/sslmode="+tt.sslmode, func(t *testing.T) {
			t.Parallel()
			url := "postgres://enron:s3cret@" + startRefusingServer(t, tt.code) + "/enron"
			if tt.sslmode != "" {
				url += "?sslmode=" + tt.sslmode
			}
			s, err := Open(t.Context(), url, slog.New(slog.DiscardHandler))
			if err == nil {
				s.Close()
				t.Fatal("Open succeeded against a refusing server")
			}
			if got := errors.Is(err, ErrUnreachable); got != tt.wantUnreachable {
				t.Errorf("errors.Is(err, ErrUnreachable) = %v, want %v; err: %v", got, tt.wantUnreachable, err)
			}
			if tt.sslmode != "require" && !strings.Contains(err.Error(), tt.code) {
				t.Errorf("err = %v, want it to report SQLSTATE %s", err, tt.code)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("err leaks the password: %v", err)
			}
		})
	}
}

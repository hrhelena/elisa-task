package store

import (
	"bytes"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func TestSanitizeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, want string
	}{
		{"empty", "", ""},
		{"valid ASCII unchanged", "Q3 report", "Q3 report"},
		{"valid multibyte unchanged", "Grüße – ✓", "Grüße – ✓"},
		{"NUL removed", "a\x00b", "ab"},
		{"only NULs", "\x00\x00", ""},
		{"invalid byte replaced", "a\xffb", "a�b"},
		{"run of invalid bytes becomes one replacement", "a\xff\xfe\xfdb", "a�b"},
		{"truncated multibyte sequence", "caf\xc3", "caf�"},
		// Gherkin: Text with NUL bytes and invalid UTF-8 is stored safely.
		{"NUL and invalid UTF-8 together", "Q3\x00 report \xff", "Q3 report �"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeString(tt.in); got != tt.want {
				t.Errorf("sanitizeString(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSanitizeOptional(t *testing.T) {
	t.Parallel()
	if got := sanitizeOptional(nil); got != nil {
		t.Errorf("sanitizeOptional(nil) = %q, want nil", *got)
	}
	empty := sanitizeOptional(ptr(""))
	if empty == nil || *empty != "" {
		t.Errorf("sanitizeOptional(\"\") = %v, want pointer to empty string", empty)
	}
	in := "x\x00\xff"
	got := sanitizeOptional(&in)
	if got == nil || *got != "x�" {
		t.Errorf("sanitizeOptional(%q) = %v, want %q", in, got, "x�")
	}
	if in != "x\x00\xff" {
		t.Errorf("input was modified: %q", in)
	}
}

func TestSanitizeList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"nil becomes empty", nil, []string{}},
		{"empty stays empty", []string{}, []string{}},
		{"elements sanitized", []string{"a@enron.com", "b\x00@enron.com", "\xff"}, []string{"a@enron.com", "b@enron.com", "�"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := sanitizeList(tt.in)
			if got == nil {
				t.Fatal("sanitizeList returned nil; it must return a non-nil slice so the column is '{}'")
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("sanitizeList(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestMessageRow(t *testing.T) {
	t.Parallel()
	sent := time.Date(2001, 5, 14, 23, 39, 0, 0, time.UTC)
	hash := bytes.Repeat([]byte{0xab}, 32)
	m := Message{
		SourcePath: "allen-p/_sent_mail/1.", Mailbox: "allen-p", Folder: "_sent_mail",
		MessageID: ptr("<1.JavaMail.evans@thyme>"), DateRaw: ptr("Mon, 14 May 2001 16:39:00 -0700"),
		FromAddr: ptr("phillip.allen@enron.com"), FromRaw: ptr("phillip.allen@enron.com"), XFrom: ptr("Phillip K Allen"),
		Subject: ptr("Q3\x00 report \xff"), SentAt: &sent,
		To: []string{"tim.belden@enron.com"}, ContentSHA256: hash, RunID: 7,
	}
	row, err := messageRow(&m)
	if err != nil {
		t.Fatalf("messageRow: %v", err)
	}
	if got, want := len(row), len(messageColumns()); got != want {
		t.Fatalf("row has %d values, columns list has %d", got, want)
	}
	col := func(name string) any {
		t.Helper()
		i := slices.Index(messageColumns(), name)
		if i < 0 {
			t.Fatalf("no column %q", name)
		}
		return row[i]
	}
	if got := col("subject").(*string); *got != "Q3 report �" {
		t.Errorf("subject = %q, want sanitized", *got)
	}
	if got := col("cc_addrs").([]string); got == nil || len(got) != 0 {
		t.Errorf("cc_addrs = %#v, want empty non-nil slice", got)
	}
	if got := col("run_id").(*int64); got == nil || *got != 7 {
		t.Errorf("run_id = %v, want 7", got)
	}
	if got := col("content_sha256").([]byte); !bytes.Equal(got, hash) {
		t.Errorf("content_sha256 = %x, want %x", got, hash)
	}
	if got := col("sent_at").(*time.Time); !got.Equal(sent) {
		t.Errorf("sent_at = %v, want %v", got, sent)
	}
}

func TestMessageRowNullables(t *testing.T) {
	t.Parallel()
	row, err := messageRow(&Message{SourcePath: "lay-k/inbox/1.", Mailbox: "lay-k", Folder: "inbox", ContentSHA256: []byte{}})
	if err != nil {
		t.Fatalf("messageRow: %v", err)
	}
	cols := messageColumns()
	for _, name := range []string{"message_id", "date_raw", "from_addr", "from_raw", "x_from", "subject"} {
		if got := row[slices.Index(cols, name)].(*string); got != nil {
			t.Errorf("%s = %q, want nil (NULL)", name, *got)
		}
	}
	if got := row[slices.Index(cols, "sent_at")].(*time.Time); got != nil {
		t.Errorf("sent_at = %v, want nil", got)
	}
	if got := row[slices.Index(cols, "run_id")].(*int64); got != nil {
		t.Errorf("run_id = %d, want nil for RunID 0", *got)
	}
	if got := row[slices.Index(cols, "content_sha256")].([]byte); got != nil {
		t.Errorf("content_sha256 = %#v, want nil for an empty hash", got)
	}
	for _, name := range []string{"to_addrs", "cc_addrs", "bcc_addrs"} {
		if got := row[slices.Index(cols, name)].([]string); got == nil {
			t.Errorf("%s is nil, want empty slice", name)
		}
	}
}

func TestMessageRowRejectsInvalidMessages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		msg     Message
		wantSub string
	}{
		{"empty source path", Message{Mailbox: "allen-p"}, "empty source path"},
		{"short hash", Message{SourcePath: "a/b/1.", ContentSHA256: []byte{1, 2, 3}}, "3 bytes"},
		{"long hash", Message{SourcePath: "a/b/1.", ContentSHA256: make([]byte, 33)}, "33 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := messageRow(&tt.msg)
			if !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("err = %v, want ErrInvalidMessage", err)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("err = %q, want it to mention %q", err, tt.wantSub)
			}
		})
	}
}

// TestInsertMessagesEmptyBatch checks that an empty batch is a no-op that
// never touches the database (the zero Postgres has no pool).
func TestInsertMessagesEmptyBatch(t *testing.T) {
	t.Parallel()
	n, err := (&Postgres{}).InsertMessages(t.Context(), nil)
	if err != nil || n != 0 {
		t.Fatalf("InsertMessages(nil) = %d, %v; want 0, nil", n, err)
	}
}

// TestTopSendersInvalidLimit covers the Gherkin scenario "Invalid limit".
// The limit is validated before any query, so no database is needed.
func TestTopSendersInvalidLimit(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, -1} {
		got, err := (&Postgres{}).TopSenders(t.Context(), limit)
		if !errors.Is(err, ErrInvalidLimit) {
			t.Errorf("TopSenders(%d) error = %v, want ErrInvalidLimit", limit, err)
		}
		if got != nil {
			t.Errorf("TopSenders(%d) = %v, want nil", limit, got)
		}
	}
}

// TestOpenFails covers the startup failure paths (main exits 1 on them).
// Errors must not echo the password from the URL into logs.
func TestOpenFails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, url, wantSub string
	}{
		{"malformed URL", "postgres://enron:s3cret@localhost:notaport/enron", "parsing database URL"},
		{"unknown pool option", "postgres://enron:s3cret@localhost/enron?pool_max_conns=many", "parsing database URL"},
		// Port 1 on loopback refuses connections immediately.
		{"unreachable server", "postgres://enron:s3cret@127.0.0.1:1/enron?connect_timeout=2", "connecting to postgres"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := Open(t.Context(), tt.url, slog.New(slog.DiscardHandler))
			if err == nil {
				s.Close()
				t.Fatal("Open succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("err = %q, want it to contain %q", err, tt.wantSub)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("err leaks the password: %q", err)
			}
		})
	}
}

func TestName(t *testing.T) {
	t.Parallel()
	if got := (&Postgres{}).Name(); got != "postgres" {
		t.Errorf("Name() = %q, want %q", got, "postgres")
	}
}

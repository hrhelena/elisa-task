package mailparse_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io/fs"
	"math/rand/v2"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf8"

	"github.com/hrhelena/elisa-task/internal/mailparse"
)

var testdata = os.DirFS("testdata")

func parseFixture(t *testing.T, name string) *mailparse.Message {
	t.Helper()
	data, err := fs.ReadFile(testdata, name)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	m, err := mailparse.Parse(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Parse(%s): unexpected error: %v", name, err)
	}
	return m
}

func parseString(t *testing.T, s string) *mailparse.Message {
	t.Helper()
	m, err := mailparse.Parse(strings.NewReader(s))
	if err != nil {
		t.Fatalf("Parse: unexpected error: %v", err)
	}
	return m
}

func requireSkip(t *testing.T, err error, want mailparse.SkipReason) {
	t.Helper()
	var se *mailparse.SkipError
	if !errors.As(err, &se) {
		t.Fatalf("error = %v, want *SkipError with reason %q", err, want)
	}
	if se.Reason != want {
		t.Fatalf("skip reason = %q, want %q (err: %v)", se.Reason, want, err)
	}
}

func TestParseFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string // Gherkin scenario
		fixture   string
		from      string
		fromRaw   string
		xFrom     string
		to        []string
		cc        []string
		subject   string
		date      time.Time
		dateRaw   string
		issues    []mailparse.Issue
		messageID string
	}{
		{
			name:      "Well-formed Enron message",
			fixture:   "allen-p_sent_1.eml",
			from:      "phillip.allen@enron.com",
			fromRaw:   "phillip.allen@enron.com",
			xFrom:     "Phillip K Allen",
			to:        []string{"tim.belden@enron.com"},
			date:      time.Date(2001, 5, 14, 23, 39, 0, 0, time.UTC),
			dateRaw:   "Mon, 14 May 2001 16:39:00 -0700 (PDT)",
			messageID: "<18782981.1075855378110.JavaMail.evans@thyme>",
		},
		{
			name:    "Folded recipient header",
			fixture: "folded_to.eml",
			from:    "melissa.becker@enron.com",
			fromRaw: "melissa.becker@enron.com",
			xFrom:   "Melissa Becker",
			to: []string{
				"wayne.andrews@enron.com", "sally.beck@enron.com", "philippe.bibi@enron.com",
				"rick.boehm@enron.com", "mark.brand@enron.com",
			},
			subject: "SAP rollout",
			date:    time.Date(2000, 9, 27, 10, 48, 0, 0, time.UTC),
			dateRaw: "Wed, 27 Sep 2000 03:48:00 -0700 (PDT)",
		},
		{
			name:    "Display-name sender is normalized",
			fixture: "display_name_sender.eml",
			from:    "phillip.allen@enron.com",
			fromRaw: "Phillip Allen <Phillip.Allen@Enron.com>",
			xFrom:   "Phillip K Allen",
			to:      []string{"tim.belden@enron.com"},
			subject: "Forecast",
			date:    time.Date(2001, 5, 14, 23, 39, 0, 0, time.UTC),
			dateRaw: "Mon, 14 May 2001 16:39:00 -0700 (PDT)",
		},
		{
			name:    "Broken Enron sender form is kept leniently",
			fixture: "broken_sender.eml",
			from:    ".ward@enron.com",
			fromRaw: "houston <.ward@enron.com>",
			xFrom:   "Ward, Kim S (Houston) </O=ENRON/OU=NA/CN=RECIPIENTS/CN=KWARD>",
			to:      []string{"kim.ward@enron.com"},
			subject: "Re: storage",
			date:    time.Date(2001, 10, 5, 17, 12, 0, 0, time.UTC),
			dateRaw: "Fri, 5 Oct 2001 10:12:00 -0700 (PDT)",
		},
		{
			// Also covers "Sender without a domain".
			name:    "X-From is stored but never used as the sender",
			fixture: "no_domain_sender.eml",
			from:    "",
			fromRaw: "pep <performance.>",
			xFrom:   "Pep Performance",
			to:      []string{"jeff.skilling@enron.com"},
			subject: "Performance review reminder",
			date:    time.Date(2001, 5, 15, 3, 58, 0, 0, time.UTC),
			dateRaw: "Mon, 14 May 2001 20:58:00 -0700 (PDT)",
			issues:  []mailparse.Issue{mailparse.IssueInvalidSender},
		},
		{
			name:    "Missing From header",
			fixture: "missing_from.eml",
			xFrom:   "Unknown",
			to:      []string{"tim.belden@enron.com"},
			subject: "No sender here",
			date:    time.Date(2001, 5, 15, 16, 0, 0, 0, time.UTC),
			dateRaw: "Tue, 15 May 2001 09:00:00 -0700 (PDT)",
			issues:  []mailparse.Issue{mailparse.IssueMissingSender},
		},
		{
			name:    "Unparseable date",
			fixture: "invalid_date.eml",
			from:    "phillip.allen@enron.com",
			fromRaw: "phillip.allen@enron.com",
			to:      []string{"tim.belden@enron.com"},
			subject: "When?",
			dateRaw: "sometime last week",
			issues:  []mailparse.Issue{mailparse.IssueInvalidDate},
		},
		{
			name:    "RFC 2047 subject",
			fixture: "rfc2047_subject.eml",
			from:    "phillip.allen@enron.com",
			fromRaw: "phillip.allen@enron.com",
			xFrom:   "André Müller",
			to:      []string{"tim.belden@enron.com"},
			subject: "Café meeting",
			date:    time.Date(2001, 5, 15, 16, 0, 0, 0, time.UTC),
			dateRaw: "Tue, 15 May 2001 09:00:00 -0700 (PDT)",
		},
		{
			name:    "Invalid recipients are dropped with one issue per header",
			fixture: "invalid_recipient.eml",
			from:    "melissa.becker@enron.com",
			fromRaw: "melissa.becker@enron.com",
			to:      []string{"wayne.andrews@enron.com", "sally.beck@enron.com"},
			cc:      []string{"townend'.'andy@enron.com"},
			subject: "Recipients with broken entries",
			date:    time.Date(2000, 9, 27, 10, 48, 0, 0, time.UTC),
			dateRaw: "Wed, 27 Sep 2000 03:48:00 -0700 (PDT)",
			issues:  []mailparse.Issue{mailparse.IssueInvalidRecipient, mailparse.IssueInvalidRecipient},
		},
		{
			name:    "Header block without a blank line",
			fixture: "no_blank_line.eml",
			from:    "phillip.allen@enron.com",
			fromRaw: "phillip.allen@enron.com",
			subject: "Header block ends without a blank line",
			date:    time.Date(2001, 5, 15, 16, 0, 0, 0, time.UTC),
			dateRaw: "Tue, 15 May 2001 09:00:00 -0700 (PDT)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := parseFixture(t, tc.fixture)

			if m.From != tc.from {
				t.Errorf("From = %q, want %q", m.From, tc.from)
			}
			if m.FromRaw != tc.fromRaw {
				t.Errorf("FromRaw = %q, want %q", m.FromRaw, tc.fromRaw)
			}
			if m.XFrom != tc.xFrom {
				t.Errorf("XFrom = %q, want %q", m.XFrom, tc.xFrom)
			}
			if !slices.Equal(m.To, tc.to) {
				t.Errorf("To = %q, want %q", m.To, tc.to)
			}
			if !slices.Equal(m.Cc, tc.cc) {
				t.Errorf("Cc = %q, want %q", m.Cc, tc.cc)
			}
			if m.Subject != tc.subject {
				t.Errorf("Subject = %q, want %q", m.Subject, tc.subject)
			}
			if !m.Date.Equal(tc.date) || m.Date.Location() != time.UTC {
				t.Errorf("Date = %v, want %v (UTC)", m.Date, tc.date)
			}
			if m.DateRaw != tc.dateRaw {
				t.Errorf("DateRaw = %q, want %q", m.DateRaw, tc.dateRaw)
			}
			if !slices.Equal(m.Issues, tc.issues) {
				t.Errorf("Issues = %q, want %q", m.Issues, tc.issues)
			}
			if tc.messageID != "" && m.MessageID != tc.messageID {
				t.Errorf("MessageID = %q, want %q", m.MessageID, tc.messageID)
			}
		})
	}
}

func TestParse_MissingDate(t *testing.T) {
	t.Parallel()
	m := parseString(t, "From: a@b.com\nSubject: s\n\nbody\n")
	if !m.Date.IsZero() || m.DateRaw != "" || !m.HasIssue(mailparse.IssueMissingDate) {
		t.Fatalf("Date = %v, DateRaw = %q, Issues = %q; want zero, empty, missing_date", m.Date, m.DateRaw, m.Issues)
	}
}

func TestParse_InvalidUTF8AndNULBytes(t *testing.T) {
	t.Parallel()
	in := "From: a@b.com\nDate: Mon, 14 May 2001 16:39:00 -0700\nSubject: bad \xff byte\x00 here\n\nbody\n"
	m := parseString(t, in)

	if !utf8.ValidString(m.Subject) || strings.ContainsRune(m.Subject, 0) {
		t.Fatalf("Subject %q is not valid UTF-8 without NUL", m.Subject)
	}
	if want := "bad � byte here"; m.Subject != want {
		t.Errorf("Subject = %q, want %q", m.Subject, want)
	}
	if !slices.Equal(m.Issues, []mailparse.Issue{mailparse.IssueInvalidEncoding}) {
		t.Errorf("Issues = %q, want [invalid_encoding]", m.Issues)
	}
}

func TestParse_InvalidBytesInBodyAreNotAnIssue(t *testing.T) {
	t.Parallel()
	m := parseString(t, "From: a@b.com\nDate: Mon, 14 May 2001 16:39:00 -0700\n\nbody \xff\x00\n")
	if len(m.Issues) != 0 {
		t.Errorf("Issues = %q, want none", m.Issues)
	}
}

func TestParse_CRLFLineEndings(t *testing.T) {
	t.Parallel()
	m := parseString(t, "From: A <A@B.com>\r\nTo: x@y.com,\r\n\tz@y.com\r\nSubject: hi\r\nDate: Mon, 14 May 2001 16:39:00 -0700\r\n\r\nbody\r\n")
	if m.From != "a@b.com" || m.Subject != "hi" || !slices.Equal(m.To, []string{"x@y.com", "z@y.com"}) || len(m.Issues) != 0 {
		t.Fatalf("got From=%q Subject=%q To=%q Issues=%q", m.From, m.Subject, m.To, m.Issues)
	}
}

func TestParse_EmptyFile(t *testing.T) {
	t.Parallel()
	m, err := mailparse.Parse(bytes.NewReader(nil))
	requireSkip(t, err, mailparse.SkipEmptyFile)
	if m != nil {
		t.Errorf("message = %+v, want nil", m)
	}
}

func TestParse_BinaryNonEmailFile(t *testing.T) {
	t.Parallel()
	for i := range 50 {
		// Deterministic pseudo-random bytes, one seed per iteration.
		data := make([]byte, 4<<10)
		_, _ = rand.NewChaCha8([32]byte{byte(i)}).Read(data)
		_, err := mailparse.Parse(bytes.NewReader(data))
		requireSkip(t, err, mailparse.SkipNotAnEmail)
	}
}

func TestParse_NotAnEmail(t *testing.T) {
	t.Parallel()
	for name, in := range map[string]string{
		"blank first line":         "\nFrom: a@b.com\n",
		"plain text":               "just some notes\nwith lines\n",
		"headers but none of mail": "Content-Type: text/plain\nX-Folder: inbox\n\nbody\n",
		"leading continuation":     " From: a@b.com\n",
	} {
		_, err := mailparse.Parse(strings.NewReader(in))
		t.Run(name, func(t *testing.T) { requireSkip(t, err, mailparse.SkipNotAnEmail) })
	}
}

func TestParse_OversizedHeaderBlock(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	b.WriteString("From: a@b.com\n")
	line := "X-Filler: " + strings.Repeat("x", 1000) + "\n"
	for b.Len() < 2<<20 {
		b.WriteString(line)
	}
	b.WriteString("\nbody\n")
	_, err := mailparse.Parse(strings.NewReader(b.String()))
	requireSkip(t, err, mailparse.SkipHeaderTooLarge)

	// A single 2 MiB line is too large as well.
	_, err = mailparse.Parse(strings.NewReader("Subject: " + strings.Repeat("x", 2<<20)))
	requireSkip(t, err, mailparse.SkipHeaderTooLarge)
}

func TestParse_LargeBodyIsFine(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("y", 3<<20)
	m := parseString(t, "From: a@b.com\nDate: Mon, 14 May 2001 16:39:00 -0700\n\n"+body)
	if len(m.Issues) != 0 {
		t.Errorf("Issues = %q, want none", m.Issues)
	}
}

func TestParse_ReadError(t *testing.T) {
	t.Parallel()
	errBoom := errors.New("boom")

	_, err := mailparse.Parse(iotest.ErrReader(errBoom))
	requireSkip(t, err, mailparse.SkipReadError)
	if !errors.Is(err, errBoom) {
		t.Errorf("error %v does not wrap the I/O error", err)
	}

	// Failure while reading the body.
	r := iotest.TimeoutReader(iotest.OneByteReader(strings.NewReader("From: a@b.com\n\nbody\n")))
	_, err = mailparse.Parse(r)
	requireSkip(t, err, mailparse.SkipReadError)
}

func TestParse_SameEmailInTwoFoldersHasTheSameHash(t *testing.T) {
	t.Parallel()
	a := parseFixture(t, "same_email_inbox.eml")
	b := parseFixture(t, "same_email_all_documents.eml")
	if a.MessageID == b.MessageID {
		t.Fatal("fixtures must differ in Message-ID")
	}
	if a.ContentSHA256 != b.ContentSHA256 {
		t.Errorf("hashes differ: %x vs %x", a.ContentSHA256, b.ContentSHA256)
	}
	if a.ContentSHA256 == ([32]byte{}) {
		t.Error("hash is zero")
	}

	// The hash covers the documented fields and the body.
	want := sha256.Sum256([]byte("phillip.allen@enron.com\ntim.belden@enron.com\njohn.lavorato@enron.com\nForecast\n" +
		"Mon, 14 May 2001 16:39:00 -0700 (PDT)\n\nHere is our forecast\n"))
	if a.ContentSHA256 != want {
		t.Errorf("hash = %x, want %x", a.ContentSHA256, want)
	}
}

func TestParse_HashChangesWithContent(t *testing.T) {
	t.Parallel()
	base := "From: a@b.com\nTo: c@d.com\nSubject: s\nDate: Mon, 14 May 2001 16:39:00 -0700\n\nbody\n"
	variants := []string{
		strings.Replace(base, "From: a@b.com", "From: x@b.com", 1),
		strings.Replace(base, "To: c@d.com", "To: x@d.com", 1),
		strings.Replace(base, "Subject: s", "Subject: t", 1),
		strings.Replace(base, "16:39:00", "16:39:01", 1),
		strings.Replace(base, "body", "bodY", 1),
		strings.Replace(base, "To: c@d.com", "To: c@d.com\nCc: e@f.com", 1),
	}
	h := parseString(t, base).ContentSHA256
	for _, v := range variants {
		if parseString(t, v).ContentSHA256 == h {
			t.Errorf("hash unchanged for variant:\n%s", v)
		}
	}
}

func TestParse_RepeatedAndObsoleteHeaders(t *testing.T) {
	t.Parallel()
	m := parseString(t, "to: c@d.com\nfrom : a@b.com\nTO: e@f.com\nSubject: first\nSubject: second\n\n")
	if m.From != "a@b.com" || m.Subject != "first" || !slices.Equal(m.To, []string{"c@d.com", "e@f.com"}) {
		t.Fatalf("got From=%q Subject=%q To=%q", m.From, m.Subject, m.To)
	}
}

func TestSkipError(t *testing.T) {
	t.Parallel()
	inner := errors.New("inner")
	err := error(&mailparse.SkipError{Reason: mailparse.SkipReadError, Err: inner})
	if !errors.Is(err, inner) {
		t.Error("SkipError does not unwrap")
	}
	if got := err.Error(); !strings.Contains(got, "read_error") || !strings.Contains(got, "inner") {
		t.Errorf("Error() = %q", got)
	}
	if got := (&mailparse.SkipError{Reason: mailparse.SkipEmptyFile}).Error(); got != "skip: empty_file" {
		t.Errorf("Error() = %q", got)
	}
}

func TestNormalizeAddress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw  string
		want string
		ok   bool
	}{
		{"phillip.allen@enron.com", "phillip.allen@enron.com", true},
		{"Phillip Allen <Phillip.Allen@Enron.com>", "phillip.allen@enron.com", true},
		{"houston <.ward@enron.com>", ".ward@enron.com", true},
		{"legal <.taylor@enron.com>", ".taylor@enron.com", true},
		{"pep <performance.>", "", false},
		{"", "", false},
		{"   ", "", false},
		{"no address here", "", false},
		{`"Allen, Phillip" <phillip.allen@enron.com>`, "phillip.allen@enron.com", true},
		{"'jeff.dasovich@enron.com'", "jeff.dasovich@enron.com", true},
		{"jeff.dasovich@enron.com.", "jeff.dasovich@enron.com", true},
		{"sent by jeff.dasovich@enron.com today", "jeff.dasovich@enron.com", true},
		{"a <b@c.com> <D@E.com>", "d@e.com", true},
		{"townend'.'andy@enron.com", "townend'.'andy@enron.com", true},
		{`<"linda".@enron@enron.com>`, "", false},
		{"user@localhost", "", false},
		{"@enron.com", "", false},
		{"name <unterminated@enron.com", "unterminated@enron.com", true},
		{`"john doe"@enron.com`, "", false},
		{"<john doe@enron.com>", "", false},
	}
	for _, tc := range tests {
		got, ok := mailparse.NormalizeAddress(tc.raw)
		if got != tc.want || ok != tc.ok {
			t.Errorf("NormalizeAddress(%q) = (%q, %v), want (%q, %v)", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestParseAddressList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw      string
		want     []string
		allValid bool
	}{
		{"", nil, true},
		{"a@b.com", []string{"a@b.com"}, true},
		{"A@B.com, \"Last, First\" <c@d.com>", []string{"a@b.com", "c@d.com"}, true},
		{"a@b.com, nxi <p.>, c@d.com", []string{"a@b.com", "c@d.com"}, false},
		{"a@b.com, c@d.com,", []string{"a@b.com", "c@d.com"}, true},
		{"houston <.ward@enron.com>, a@b.com", []string{".ward@enron.com", "a@b.com"}, true},
	}
	for _, tc := range tests {
		got, ok := mailparse.ParseAddressList(tc.raw)
		if !slices.Equal(got, tc.want) || ok != tc.allValid {
			t.Errorf("ParseAddressList(%q) = (%q, %v), want (%q, %v)", tc.raw, got, ok, tc.want, tc.allValid)
		}
	}
}

func TestParseDate(t *testing.T) {
	t.Parallel()
	want := time.Date(2001, 5, 14, 23, 39, 0, 0, time.UTC)
	tests := []struct {
		raw string
		ok  bool
	}{
		{"Mon, 14 May 2001 16:39:00 -0700 (PDT)", true},
		{"Mon, 14 May 2001 16:39:00 -0700", true},
		{"14 May 2001 16:39:00 -0700", true},
		{"Mon, 14 May 2001 16:39 -0700", true},
		{"Mon,  14 May 2001   16:39:00 -0700  (Pacific Daylight Time)", true},
		{"2001-05-14T16:39:00-07:00", true},
		{"2001-05-14 23:39:00", true},
		{"sometime last week", false},
		{"", false},
	}
	for _, tc := range tests {
		got, ok := mailparse.ParseDate(tc.raw)
		if ok != tc.ok || (ok && (!got.Equal(want) || got.Location() != time.UTC)) {
			t.Errorf("ParseDate(%q) = (%v, %v), want ok=%v %v", tc.raw, got, ok, tc.ok, want)
		}
	}
}

func FuzzParse(f *testing.F) {
	entries, err := fs.ReadDir(testdata, ".")
	if err != nil {
		f.Fatal(err)
	}
	for _, e := range entries {
		if path.Ext(e.Name()) != ".eml" {
			continue
		}
		data, err := fs.ReadFile(testdata, e.Name())
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte{})
	f.Add([]byte("From: a@b.com\nSubject: \xff\x00=?utf-8?B?w6k=?=\n\n"))
	f.Add([]byte("Subject: =?bogus?Q?x?= =?utf-8?Q?=ZZ?=\n To: ,,<>@\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := mailparse.Parse(bytes.NewReader(data))
		if err != nil {
			var se *mailparse.SkipError
			if !errors.As(err, &se) {
				t.Fatalf("error is not a *SkipError: %v", err)
			}
			if m != nil {
				t.Fatal("non-nil message with an error")
			}
			return
		}
		if m == nil {
			t.Fatal("nil message without an error")
		}
		strs := []string{m.MessageID, m.DateRaw, m.FromRaw, m.From, m.XFrom, m.Subject}
		strs = append(strs, m.To...)
		strs = append(strs, m.Cc...)
		strs = append(strs, m.Bcc...)
		for _, s := range strs {
			if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
				t.Fatalf("output %q is not valid UTF-8 without NUL", s)
			}
		}
		if m.From != "" {
			if got, ok := mailparse.NormalizeAddress(m.From); !ok || got != m.From {
				t.Fatalf("From %q is not a stable normalized address", m.From)
			}
		}
		if m.Date.IsZero() == (m.DateRaw != "" && !m.HasIssue(mailparse.IssueInvalidDate)) {
			t.Fatalf("Date %v inconsistent with DateRaw %q and Issues %q", m.Date, m.DateRaw, m.Issues)
		}
	})
}

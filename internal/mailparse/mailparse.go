// Package mailparse turns one maildir file into structured header data.
//
// It is pure: it reads from an io.Reader and never touches the file system or a database.
// Malformed input never makes Parse fail with anything but a *SkipError, and field-level
// problems are reported as Issues on an otherwise valid Message (ADR 0007).
package mailparse

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strings"
	"time"
)

// MaxHeaderBytes is the largest header block Parse accepts. Larger blocks are skipped
// with SkipHeaderTooLarge.
const MaxHeaderBytes = 1 << 20

// Message is the header data kept for one maildir file (ADR 0002). Every string field is
// valid UTF-8 without NUL bytes.
type Message struct {
	MessageID     string
	DateRaw       string
	Date          time.Time // UTC; zero if missing or unparseable
	FromRaw       string
	From          string // normalized sender, "" if missing or invalid (ADR 0012)
	XFrom         string // RFC 2047 decoded, never used as the sender
	To, Cc, Bcc   []string
	Subject       string // RFC 2047 decoded
	ContentSHA256 [32]byte
	Issues        []Issue // field-level problems; the message is still valid
}

// HasIssue reports whether m carries the given issue.
func (m *Message) HasIssue(i Issue) bool {
	for _, got := range m.Issues {
		if got == i {
			return true
		}
	}
	return false
}

// Issue is a field-level problem. The file is still a message and is stored.
type Issue string

// Field-level issues.
const (
	IssueInvalidSender    Issue = "invalid_sender"
	IssueMissingSender    Issue = "missing_sender"
	IssueInvalidDate      Issue = "invalid_date"
	IssueMissingDate      Issue = "missing_date"
	IssueInvalidEncoding  Issue = "invalid_encoding"
	IssueInvalidRecipient Issue = "invalid_recipient"
)

// SkipReason says why a file is not a message.
type SkipReason string

// File-level skip reasons.
const (
	SkipEmptyFile      SkipReason = "empty_file"
	SkipNotAnEmail     SkipReason = "not_an_email"
	SkipHeaderTooLarge SkipReason = "header_too_large"
	SkipReadError      SkipReason = "read_error"
)

// SkipError is returned by Parse when the input is not a message and must be skipped.
type SkipError struct {
	Reason SkipReason
	Err    error
}

func (e *SkipError) Error() string {
	if e.Err == nil {
		return "skip: " + string(e.Reason)
	}
	return fmt.Sprintf("skip: %s: %v", e.Reason, e.Err)
}

func (e *SkipError) Unwrap() error { return e.Err }

var (
	errEmpty          = errors.New("file is empty")
	errHeaderTooLarge = fmt.Errorf("header block exceeds %d bytes", MaxHeaderBytes)
	errNoHeader       = errors.New("no valid header line")
	errNoMailHeaders  = errors.New("none of From, Date, Message-ID, To, Subject present")
)

func skip(reason SkipReason, err error) *SkipError {
	return &SkipError{Reason: reason, Err: err}
}

// Canonical header keys (textproto.CanonicalMIMEHeaderKey form).
const (
	hdrMessageID = "Message-Id"
	hdrDate      = "Date"
	hdrFrom      = "From"
	hdrTo        = "To"
	hdrCc        = "Cc"
	hdrBcc       = "Bcc"
	hdrSubject   = "Subject"
	hdrXFrom     = "X-From"
)

// Parse reads one message. A *SkipError means the input is not a message and must be skipped;
// any other result is a message, possibly with Issues. The body is streamed into the content hash
// and never held in memory. Parse never panics.
func Parse(r io.Reader) (*Message, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	if _, err := br.Peek(1); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, skip(SkipEmptyFile, errEmpty)
		}
		return nil, skip(SkipReadError, err)
	}

	hdr, bodyPrefix, err := readHeader(br)
	if err != nil {
		return nil, err
	}
	if !hdr.hasAny(hdrFrom, hdrDate, hdrMessageID, hdrTo, hdrSubject) {
		return nil, skip(SkipNotAnEmail, errNoMailHeaders)
	}

	m := buildMessage(hdr)

	h := sha256.New()
	writeHashPrefix(h, m)
	h.Write(bodyPrefix)
	if _, err := io.Copy(h, br); err != nil {
		return nil, skip(SkipReadError, fmt.Errorf("reading body: %w", err))
	}
	h.Sum(m.ContentSHA256[:0])
	return m, nil
}

// header holds header values by canonical key, in file order, with RFC 5322 folding undone.
type header map[string][]string

func (h header) get(key string) (string, bool) {
	v, ok := h[key]
	if !ok {
		return "", false
	}
	return v[0], true
}

func (h header) hasAny(keys ...string) bool {
	for _, k := range keys {
		if _, ok := h[k]; ok {
			return true
		}
	}
	return false
}

// readHeader reads the header block up to the first blank line. It is deliberately more lenient
// than textproto.Reader.ReadMIMEHeader, which gives up on the first malformed line: here a line that
// is neither a header nor a continuation ends the block and is returned as the start of the body.
// The returned bodyPrefix must be hashed before the rest of br.
func readHeader(br *bufio.Reader) (hdr header, bodyPrefix []byte, err error) {
	hdr = header{}
	budget := MaxHeaderBytes
	var lastKey string
	for {
		line, rerr := readLine(br, &budget)
		if errors.Is(rerr, errHeaderTooLarge) {
			return nil, nil, skip(SkipHeaderTooLarge, rerr)
		}
		if rerr != nil && !errors.Is(rerr, io.EOF) {
			return nil, nil, skip(SkipReadError, fmt.Errorf("reading header: %w", rerr))
		}
		content := trimEOL(line)
		switch {
		case len(line) == 0:
			// End of input without a blank line: the message has no body.
		case len(content) == 0:
			// Blank line: end of the header block.
			rerr = io.EOF
		case content[0] == ' ' || content[0] == '\t':
			if lastKey == "" {
				return nil, nil, skip(SkipNotAnEmail, errNoHeader)
			}
			vals := hdr[lastKey]
			vals[len(vals)-1] += " " + strings.TrimSpace(string(content))
		default:
			key, value, ok := splitHeaderLine(content)
			if !ok {
				if lastKey == "" {
					return nil, nil, skip(SkipNotAnEmail, errNoHeader)
				}
				// Not a header line: the header block ended without a blank line.
				return hdr, bytes.Clone(line), nil
			}
			hdr[key] = append(hdr[key], value)
			lastKey = key
		}
		if rerr != nil {
			if lastKey == "" {
				return nil, nil, skip(SkipNotAnEmail, errNoHeader)
			}
			return hdr, nil, nil
		}
	}
}

// readLine returns the next line including its line ending, charging its length against budget.
// It returns errHeaderTooLarge once the budget is exhausted, and io.EOF with the final unterminated
// line (possibly empty) at the end of input.
func readLine(br *bufio.Reader, budget *int) ([]byte, error) {
	var line []byte
	for {
		frag, err := br.ReadSlice('\n')
		*budget -= len(frag)
		if *budget < 0 {
			return nil, errHeaderTooLarge
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			line = append(line, frag...)
			continue
		}
		if line == nil {
			return frag, err
		}
		return append(line, frag...), err
	}
}

func trimEOL(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte("\n"))
	return bytes.TrimSuffix(line, []byte("\r"))
}

// splitHeaderLine splits "Key: value". The key must be non-empty printable ASCII without
// spaces or colons (RFC 5322 ftext); whitespace before the colon (obsolete syntax) is tolerated.
func splitHeaderLine(line []byte) (key, value string, ok bool) {
	i := bytes.IndexByte(line, ':')
	if i <= 0 {
		return "", "", false
	}
	k := bytes.TrimRight(line[:i], " \t")
	if len(k) == 0 {
		return "", "", false
	}
	for _, c := range k {
		if c < 33 || c > 126 {
			return "", "", false
		}
	}
	return textproto.CanonicalMIMEHeaderKey(string(k)), strings.TrimSpace(string(line[i+1:])), true
}

// buildMessage derives the Message fields from the header block.
func buildMessage(hdr header) *Message {
	m := &Message{}
	var s sanitizer

	m.MessageID = s.clean(first(hdr, hdrMessageID))

	if raw, ok := hdr.get(hdrDate); ok {
		m.DateRaw = s.clean(strings.TrimSpace(raw))
		if t, ok := ParseDate(m.DateRaw); ok {
			m.Date = t
		} else {
			m.addIssue(IssueInvalidDate)
		}
	} else {
		m.addIssue(IssueMissingDate)
	}

	if raw, ok := hdr.get(hdrFrom); ok {
		m.FromRaw = s.clean(raw)
		if addr, ok := NormalizeAddress(m.FromRaw); ok {
			m.From = addr
		} else {
			m.addIssue(IssueInvalidSender)
		}
	} else {
		m.addIssue(IssueMissingSender)
	}

	m.XFrom = s.clean(decodeWords(s.clean(first(hdr, hdrXFrom))))
	m.Subject = s.clean(decodeWords(s.clean(first(hdr, hdrSubject))))

	for _, f := range []struct {
		key string
		dst *[]string
	}{{hdrTo, &m.To}, {hdrCc, &m.Cc}, {hdrBcc, &m.Bcc}} {
		vals, ok := hdr[f.key]
		if !ok {
			continue
		}
		addrs, allValid := ParseAddressList(s.clean(strings.Join(vals, ", ")))
		*f.dst = addrs
		if !allValid {
			m.addIssue(IssueInvalidRecipient)
		}
	}

	if s.replaced {
		m.addIssue(IssueInvalidEncoding)
	}
	return m
}

func first(hdr header, key string) string {
	v, _ := hdr.get(key)
	return v
}

func (m *Message) addIssue(i Issue) { m.Issues = append(m.Issues, i) }

// writeHashPrefix writes the header part of the content hash (ADR 0003): normalized From, To, Cc,
// Subject and raw Date. Message-ID and export-specific headers (X-Folder, X-FileName, X-Origin)
// are excluded so copies of one email in several folders hash the same.
func writeHashPrefix(w io.Writer, m *Message) {
	fmt.Fprintf(w, "%s\n%s\n%s\n%s\n%s\n\n",
		m.From, strings.Join(m.To, ","), strings.Join(m.Cc, ","), m.Subject, m.DateRaw)
}

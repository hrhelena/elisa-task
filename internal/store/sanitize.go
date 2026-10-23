package store

import (
	"strings"
	"unicode/utf8"
)

// sanitizeString makes s safe for a Postgres TEXT value: invalid UTF-8
// sequences become U+FFFD and NUL bytes, which Postgres rejects, are removed
// (ADR 0007). The parser is expected to do this already; the store repeats
// it as a last line of defense so one bad header can never fail a batch.
func sanitizeString(s string) string {
	if utf8.ValidString(s) && strings.IndexByte(s, 0) < 0 {
		return s
	}
	return strings.ReplaceAll(strings.ToValidUTF8(s, "�"), "\x00", "")
}

// sanitizeOptional sanitizes *p, keeping nil as nil (NULL).
func sanitizeOptional(p *string) *string {
	if p == nil {
		return nil
	}
	s := sanitizeString(*p)
	return &s
}

// sanitizeList sanitizes every element and turns nil into an empty, non-nil
// slice so it is stored as '{}' rather than NULL.
func sanitizeList(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = sanitizeString(s)
	}
	return out
}

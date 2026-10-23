package mailparse

import (
	"net/mail"
	"regexp"
	"strings"
	"time"
)

// fallbackDateLayouts are tried after net/mail.ParseDate on the normalized value (see
// normalizeDate). They only use numeric offsets, or no zone at all (read as UTC).
var fallbackDateLayouts = []string{
	"Mon, 2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04 -0700",
	"2 Jan 2006 15:04:05 -0700",
	"2 Jan 2006 15:04 -0700",
	"Mon, 2 Jan 06 15:04:05 -0700",
	"2 Jan 06 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04:05",
	"Mon Jan 2 15:04:05 2006",
	"Mon Jan 2 15:04:05 -0700 2006",
	time.RFC3339,
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05",
}

// namedZones maps the zone abbreviations accepted in Date headers to fixed offsets, so parsing
// never depends on the host's time.Local (time.Parse resolves abbreviations through it).
var namedZones = map[string]string{
	"UT": "+0000", "UTC": "+0000", "GMT": "+0000", "Z": "+0000",
	"EST": "-0500", "EDT": "-0400",
	"CST": "-0600", "CDT": "-0500",
	"MST": "-0700", "MDT": "-0600",
	"PST": "-0800", "PDT": "-0700",
}

var (
	trailingComment = regexp.MustCompile(`\s*\([^()]*\)\s*$`)
	numericOffset   = regexp.MustCompile(`^[+-]\d{4}$`)
	alphaZone       = regexp.MustCompile(`^[A-Za-z]{1,5}$`)
)

// ParseDate parses a Date header value and returns it in UTC. The result does not depend on the
// host time zone: named zones are first rewritten to numeric offsets (see normalizeDate), then
// net/mail.ParseDate and a few lenient layouts (without weekday, seconds or zone) are tried.
func ParseDate(raw string) (time.Time, bool) {
	s := normalizeDate(raw)
	if s == "" {
		return time.Time{}, false
	}
	if t, err := mail.ParseDate(s); err == nil {
		return t.UTC(), true
	}
	for _, layout := range fallbackDateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// normalizeDate strips a trailing "(TZ)" comment, collapses whitespace and removes named zones:
//   - with a numeric offset present, a trailing alphabetic zone is redundant and dropped;
//   - otherwise a known zone (namedZones) is replaced by its offset, wherever it appears;
//   - otherwise an unknown trailing alphabetic zone is read as UTC ("+0000"), explicitly.
func normalizeDate(raw string) string {
	fields := strings.Fields(trailingComment.ReplaceAllString(raw, ""))
	if len(fields) == 0 {
		return ""
	}
	hasOffset := false
	for _, f := range fields {
		if numericOffset.MatchString(f) {
			hasOffset = true
			break
		}
	}
	last := len(fields) - 1
	switch {
	case hasOffset:
		if last > 0 && alphaZone.MatchString(fields[last]) {
			fields = fields[:last]
		}
	default:
		replaced := false
		for i, f := range fields {
			if off, ok := namedZones[strings.ToUpper(f)]; ok {
				fields[i] = off
				replaced = true
			}
		}
		if !replaced && last > 0 && alphaZone.MatchString(fields[last]) {
			fields[last] = "+0000"
		}
	}
	return strings.Join(fields, " ")
}

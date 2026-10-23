package mailparse

import (
	"net/mail"
	"regexp"
	"strings"
	"time"
)

// fallbackDateLayouts are tried after net/mail.ParseDate, on the value with any trailing
// "(TZ)" comment removed and whitespace collapsed.
var fallbackDateLayouts = []string{
	"Mon, 2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04 -0700",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"2 Jan 2006 15:04:05 -0700",
	"2 Jan 2006 15:04 -0700",
	"2 Jan 2006 15:04:05 MST",
	"Mon, 2 Jan 06 15:04:05 -0700",
	"2 Jan 06 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04:05",
	"Mon Jan 2 15:04:05 2006",
	"Mon Jan 2 15:04:05 MST 2006",
	time.RFC3339,
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05",
}

var trailingComment = regexp.MustCompile(`\s*\([^()]*\)\s*$`)

// ParseDate parses a Date header value and returns it in UTC. It tries net/mail.ParseDate,
// then a few lenient layouts (without weekday, without seconds or zone) on the value with the
// trailing "(TZ)" comment stripped.
func ParseDate(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := mail.ParseDate(raw); err == nil {
		return t.UTC(), true
	}
	s := strings.Join(strings.Fields(trailingComment.ReplaceAllString(raw, "")), " ")
	for _, layout := range fallbackDateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

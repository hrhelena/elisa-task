package mailparse

import (
	"net/mail"
	"strings"
	"unicode"
)

// NormalizeAddress extracts a lowercase email address from a From/To/Cc/Bcc value (ADR 0012).
//
// It tries net/mail.ParseAddress first. On failure it falls back to the text inside the last
// <...> or, without one, the first whitespace-separated token containing "@". The candidate is
// trimmed of spaces, quotes, stray angle brackets, trailing "," or ";" and trailing dots, then
// lowercased and stripped of a "mailto:" prefix. It is valid when it has exactly one "@", a
// non-empty local part and a domain containing a "."; see validAddress for the characters that
// are rejected. Broken but usable forms such as ".ward@enron.com" are kept as written.
//
// NormalizeAddress is idempotent: if it returns (addr, true), NormalizeAddress(addr) returns
// (addr, true) too.
func NormalizeAddress(raw string) (addr string, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	var candidate string
	if a, err := mail.ParseAddress(raw); err == nil {
		candidate = a.Address
	} else {
		candidate = lenientCandidate(raw)
	}
	candidate = strings.ToLower(trimAddress(candidate))
	candidate = trimAddress(strings.TrimPrefix(candidate, "mailto:"))
	if !validAddress(candidate) {
		return "", false
	}
	return candidate, true
}

// lenientCandidate returns the text inside the last <...>, else the first token containing "@".
func lenientCandidate(raw string) string {
	if i := strings.LastIndexByte(raw, '<'); i >= 0 {
		if j := strings.IndexByte(raw[i+1:], '>'); j >= 0 {
			return raw[i+1 : i+1+j]
		}
	}
	for tok := range strings.FieldsSeq(raw) {
		if strings.Contains(tok, "@") {
			return tok
		}
	}
	return ""
}

// trimAddress removes surrounding spaces, quotes and stray angle brackets (from an unterminated
// "<"), and trailing list separators and dots, until nothing changes.
func trimAddress(s string) string {
	for {
		t := strings.TrimRight(strings.Trim(s, " \t\"'<>"), ".,;")
		if t == s {
			return s
		}
		s = t
	}
}

// addressSpecials are RFC 5322 specials that net/mail accepts inside a quoted local part but
// that change meaning when the unquoted result is parsed again: "a<b>"@c.com unquotes to
// a<b>@c.com, whose lenient re-parse picks "b", and a(b)@c.com reads "(b)" as a comment. An
// address containing one would not survive a second normalization, so it is not a usable sender
// key. "." and "@" are structural, and '"' is allowed because the Enron export produces
// local@domain forms such as munn"."mary@enron.com (like townend'.'andy@enron.com) that
// normalize stably.
const addressSpecials = `()<>[]:;,\`

// validAddress implements the ADR 0012 rule (exactly one "@", non-empty local part, domain with
// a "."). It also rejects whitespace, control characters and addressSpecials, which keeps
// NormalizeAddress idempotent.
func validAddress(s string) bool {
	if strings.ContainsAny(s, addressSpecials) ||
		strings.ContainsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return false
	}
	local, domain, found := strings.Cut(s, "@")
	return found && local != "" && !strings.Contains(domain, "@") && strings.Contains(domain, ".")
}

// droppedSecondAddress reports whether the lenient path had to ignore an address in part: with no
// <...>, it keeps only the first of several whitespace-separated tokens containing "@". With
// <...>, an "@" elsewhere is part of the display name (for example "a@b.com" <c@d.com>).
func droppedSecondAddress(part string) bool {
	if strings.Contains(part, "<") {
		return false
	}
	n := 0
	for tok := range strings.FieldsSeq(part) {
		if strings.Contains(tok, "@") {
			n++
		}
	}
	return n > 1
}

// ParseAddressList parses a To/Cc/Bcc value into normalized addresses. It uses
// net/mail.ParseAddressList and falls back to splitting on "," and ";"; every part goes through
// NormalizeAddress. Invalid parts are dropped and allValid is false, so no entry vanishes
// silently: a fallback part holding two addresses separated only by whitespace also sets
// allValid to false, since only one of them can be kept. Empty parts (for example a trailing
// separator) are ignored.
func ParseAddressList(raw string) (addrs []string, allValid bool) {
	allValid = true
	if list, err := mail.ParseAddressList(raw); err == nil {
		for _, a := range list {
			if addr, ok := NormalizeAddress(a.Address); ok {
				addrs = append(addrs, addr)
			} else {
				allValid = false
			}
		}
		return addrs, allValid
	}
	for _, p := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' }) {
		if strings.TrimSpace(p) == "" {
			continue
		}
		addr, ok := NormalizeAddress(p)
		if ok {
			addrs = append(addrs, addr)
		}
		if !ok || droppedSecondAddress(p) {
			allValid = false
		}
	}
	return addrs, allValid
}

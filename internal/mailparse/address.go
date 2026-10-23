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
// trimmed of spaces, quotes, stray angle brackets and trailing dots, then lowercased. It is valid when it has exactly one
// "@", a non-empty local part and a domain containing a ".". Broken but usable forms such as
// ".ward@enron.com" are kept as written.
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
// "<"), and trailing dots.
func trimAddress(s string) string {
	for {
		t := strings.TrimRight(strings.Trim(s, " \t\"'<>"), ".")
		if t == s {
			return s
		}
		s = t
	}
}

// validAddress implements the ADR 0012 rule (exactly one "@", non-empty local part, domain with
// a "."). It also rejects whitespace and control characters, which net/mail accepts in quoted
// local parts ("a b"@x.com) but which cannot be a usable sender key.
func validAddress(s string) bool {
	if strings.ContainsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return false
	}
	local, domain, found := strings.Cut(s, "@")
	return found && local != "" && !strings.Contains(domain, "@") && strings.Contains(domain, ".")
}

// ParseAddressList parses a To/Cc/Bcc value into normalized addresses. It uses
// net/mail.ParseAddressList and falls back to splitting on ","; every part goes through
// NormalizeAddress. Invalid parts are dropped and allValid is false. Empty parts (for example a
// trailing comma) are ignored.
func ParseAddressList(raw string) (addrs []string, allValid bool) {
	allValid = true
	var parts []string
	if list, err := mail.ParseAddressList(raw); err == nil {
		for _, a := range list {
			parts = append(parts, a.Address)
		}
	} else {
		parts = strings.Split(raw, ",")
	}
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		if addr, ok := NormalizeAddress(p); ok {
			addrs = append(addrs, addr)
		} else {
			allValid = false
		}
	}
	return addrs, allValid
}

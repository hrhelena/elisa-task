package mailparse

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/ianaindex"
)

var errUnknownCharset = errors.New("unknown charset")

// wordDecoder decodes RFC 2047 encoded words. Charsets beyond the stdlib's utf-8, iso-8859-1 and
// us-ascii are looked up in the IANA index (for example windows-1252 or koi8-r).
var wordDecoder = &mime.WordDecoder{CharsetReader: charsetReader}

func charsetReader(charset string, input io.Reader) (io.Reader, error) {
	enc, err := ianaindex.MIME.Encoding(charset)
	if err != nil {
		return nil, fmt.Errorf("charset %q: %w", charset, err)
	}
	if enc == nil {
		return nil, fmt.Errorf("charset %q: %w", charset, errUnknownCharset)
	}
	return enc.NewDecoder().Reader(input), nil
}

// decodeWords decodes RFC 2047 encoded words in s and returns s unchanged on error.
func decodeWords(s string) string {
	if !strings.Contains(s, "=?") {
		return s
	}
	out, err := wordDecoder.DecodeHeader(s)
	if err != nil {
		return s
	}
	return out
}

// sanitizer makes strings safe for Postgres TEXT (ADR 0007) and remembers whether it changed any.
type sanitizer struct {
	replaced bool
}

// clean replaces invalid UTF-8 with U+FFFD and removes NUL bytes.
func (s *sanitizer) clean(v string) string {
	if utf8.ValidString(v) && !strings.ContainsRune(v, 0) {
		return v
	}
	s.replaced = true
	return strings.ReplaceAll(strings.ToValidUTF8(v, "�"), "\x00", "")
}

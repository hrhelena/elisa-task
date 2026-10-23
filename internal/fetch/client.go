package fetch

import (
	"errors"
	"net"
	"net/http"
	"time"
)

// errFreeSpaceUnsupported means free space cannot be measured on this platform.
var errFreeSpaceUnsupported = errors.New("free space check not supported on this platform")

// HTTP client timeouts for the download (ticket #10). There is deliberately
// no overall timeout: the archive is about 423 MB, and a stalled transfer is
// detected by the Fetcher's stall timer instead.
const (
	connectTimeout        = 10 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	responseHeaderTimeout = 60 * time.Second
	keepAlive             = 30 * time.Second
)

// NewHTTPClient returns the HTTP client used for the download: a 10s connect
// timeout and no overall timeout. Keep-alives are off: a fetch is one large
// request, so an idle pooled connection would only linger after it.
func NewHTTPClient() *http.Client {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: keepAlive}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
		DisableKeepAlives:     true,
	}
	return &http.Client{Transport: tr}
}

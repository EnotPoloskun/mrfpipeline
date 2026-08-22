package artifact

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

var errPolicy = dlErr("policy")

// Downloader streams one HTTP/HTTPS body into a generated download leaf.
type Downloader struct {
	ws       *Workspace
	progress *jobs.Progress
	client   *http.Client
	resolve  func(context.Context, string) ([]net.IP, error)
	dial     func(context.Context, string, string) (net.Conn, error)
}

// NewDownloader builds the shared process client with Story 04 transport bounds.
func NewDownloader(ws *Workspace, progress *jobs.Progress) *Downloader {
	return newDownloader(ws, progress, headerTimeout, nil, nil, nil)
}

// Workspace returns the downloader's artifact workspace.
func (d *Downloader) Workspace() *Workspace {
	if d == nil {
		return nil
	}
	return d.ws
}

// NewTestDownloader builds a Downloader with injected resolve and dial.
func NewTestDownloader(
	ws *Workspace,
	progress *jobs.Progress,
	resolve func(context.Context, string) ([]net.IP, error),
	dial func(context.Context, string, string) (net.Conn, error),
) *Downloader {
	return newDownloader(ws, progress, headerTimeout, &tls.Config{InsecureSkipVerify: true}, resolve, dial)
}

func newDownloader(
	ws *Workspace,
	progress *jobs.Progress,
	respHeader time.Duration,
	tlsCfg *tls.Config,
	resolve func(context.Context, string) ([]net.IP, error),
	dial func(context.Context, string, string) (net.Conn, error),
) *Downloader {
	d := &Downloader{ws: ws, progress: progress, resolve: resolve, dial: dial}
	if d.progress == nil {
		d.progress = jobs.NewProgress(nil)
	}
	if d.resolve == nil {
		d.resolve = lookupIPs
	}
	if d.dial == nil {
		nd := &net.Dialer{Timeout: dialTimeout, KeepAlive: keepAlive}
		d.dial = nd.DialContext
	}
	if respHeader <= 0 {
		respHeader = headerTimeout
	}
	tr := &http.Transport{
		Proxy:                  func(*http.Request) (*url.URL, error) { return nil, nil },
		DialContext:            d.secureDial,
		TLSHandshakeTimeout:    tlsHandshakeTimeout,
		ResponseHeaderTimeout:  respHeader,
		ExpectContinueTimeout:  expectContinue,
		IdleConnTimeout:        idleConnTimeout,
		MaxIdleConns:           maxIdleConns,
		MaxIdleConnsPerHost:    maxIdleConnsPerHost,
		MaxConnsPerHost:        maxConnsPerHost,
		MaxResponseHeaderBytes: maxResponseHeader,
		DisableCompression:     true,
		TLSClientConfig:        tlsCfg,
	}
	d.client = &http.Client{
		Transport:     tr,
		CheckRedirect: d.checkRedirect,
	}
	return d
}

func lookupIPs(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{ip}, nil
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	out := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.IP)
	}
	return out, nil
}

func isForbiddenIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsPrivate()
}

func (d *Downloader) secureDial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errPolicy
	}
	ips, err := d.resolve(ctx, host)
	if err != nil || len(ips) == 0 {
		return nil, errPolicy
	}
	for _, ip := range ips {
		if isForbiddenIP(ip) {
			return nil, errPolicy
		}
	}
	chosen := ips[0]
	if isForbiddenIP(chosen) {
		return nil, errPolicy
	}
	return d.dial(ctx, network, net.JoinHostPort(chosen.String(), port))
}

func parsePublicURL(raw string) (*url.URL, error) {
	if raw == "" || !utf8.ValidString(raw) {
		return nil, dlErr("url")
	}
	if strings.IndexByte(raw, 0) >= 0 {
		return nil, dlErr("url")
	}
	for _, r := range raw {
		if r < 0x20 || r == 0x7f {
			return nil, dlErr("url")
		}
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil || !u.IsAbs() {
		return nil, dlErr("url")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, dlErr("url")
	}
	if u.User != nil || u.Host == "" || u.Hostname() == "" {
		return nil, dlErr("url")
	}
	if port := u.Port(); port != "" {
		if _, err := strconv.Atoi(port); err != nil {
			return nil, dlErr("url")
		}
	}
	return u, nil
}

func (d *Downloader) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxRedirects {
		return errPolicy
	}
	if req == nil || req.URL == nil {
		return errPolicy
	}
	if len(via) > 0 {
		prev := via[len(via)-1]
		if prev != nil && prev.URL != nil && prev.URL.Scheme == "https" && req.URL.Scheme == "http" {
			return errPolicy
		}
	}
	if _, err := parsePublicURL(req.URL.String()); err != nil {
		return errPolicy
	}
	return nil
}

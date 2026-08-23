package tocimport

import (
	"net/url"
	"strings"
	"unicode/utf8"
)

func validHTTPSLocation(location string) bool {
	if !utf8.ValidString(location) || !strings.HasPrefix(location, "https://") {
		return false
	}
	for _, r := range location {
		if r < 0x20 || r == 0x7f || r == ' ' {
			return false
		}
	}
	u, err := url.Parse(location)
	return err == nil && u.Scheme == "https" && u.IsAbs() && u.Hostname() != "" && u.User == nil
}

func deriveFilename(location string) (string, bool) {
	if !validHTTPSLocation(location) {
		return "", false
	}
	u, _ := url.Parse(location)
	escaped := u.EscapedPath()
	segment := escaped[strings.LastIndexByte(escaped, '/')+1:]
	if segment == "" {
		return "", false
	}
	decoded, err := url.PathUnescape(segment)
	if err != nil || !utf8.ValidString(decoded) || decoded == "." || decoded == ".." || strings.ContainsAny(decoded, "/\\") || hasBadFilenameByte(decoded) {
		return "", false
	}
	return decoded, true
}

func hasBadFilenameByte(v string) bool {
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

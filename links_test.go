package main

import (
	"net/url"
	"testing"
)

func TestCheckURL(t *testing.T) {
	self, _ := url.Parse("https://sho.rt")
	for raw, ok := range map[string]bool{
		"https://example.com/path?q=1":                      true,
		"HTTP://example.com":                                true,
		"example.com":                                       false,
		"javascript:alert(1)":                               false,
		"ftp://example.com":                                 false,
		"https://user:pw@example.com":                       false,
		"https://SHO.RT/abc1234":                            false,
		"https://example.com/" + string(make([]byte, 2048)): false,
	} {
		if err := checkURL(raw, self); (err == nil) != ok {
			t.Errorf("checkURL(%.40q) = %v, want ok=%v", raw, err, ok)
		}
	}
}

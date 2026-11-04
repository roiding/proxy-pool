package domain

import (
	"net/netip"
	"testing"
)

func TestScratchAddrPolicy(t *testing.T) {
	for _, s := range []string{"100.100.100.200", "0.1.2.3", "198.18.0.1", "240.0.0.1", "255.255.255.255",
		"10.0.0.1", "192.168.1.1", "172.16.0.1", "127.0.0.1", "169.254.1.1", "203.0.113.9",
		"192.0.2.9", "198.51.100.9", "224.0.0.5", "::1", "fe80::1", "fd00::1", "2001:db8::1",
		"::ffff:100.100.100.200"} {
		if AddrAllowed(netip.MustParseAddr(s), false) {
			t.Errorf("%s must be rejected", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "1.1.1.1", "2606:4700::1", "::ffff:8.8.8.8"} {
		if !AddrAllowed(netip.MustParseAddr(s), false) {
			t.Errorf("%s must be allowed", s)
		}
	}
}

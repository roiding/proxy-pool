package domain

import (
	"net/netip"
	"strings"
)

// SelectPinnedAddress selects from one already-resolved DNS snapshot. Prefer
// IPv4 for dual-stack proxy/source compatibility, retaining IPv6-only support.
// It does not resolve again, try alternative DNS, or weaken address policy.
func SelectPinnedAddress(answers []string, allowPrivate, rejectMapped bool) (netip.Addr, bool) {
	var firstV6 netip.Addr
	for _, answer := range answers {
		ip, err := netip.ParseAddr(strings.TrimSpace(answer))
		if err != nil || ip.Zone() != "" || rejectMapped && ip.Is4In6() {
			continue
		}
		ip = ip.Unmap()
		if !AddrAllowed(ip, allowPrivate) {
			continue
		}
		if ip.Is4() {
			return ip, true
		}
		if !firstV6.IsValid() {
			firstV6 = ip
		}
	}
	return firstV6, firstV6.IsValid()
}

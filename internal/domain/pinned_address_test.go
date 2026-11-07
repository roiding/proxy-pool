package domain

import "testing"

func TestPinnedDNSPrefersAllowedV4ButPreservesV6Only(t *testing.T) {
	for _, tc := range []struct {
		answers         []string
		private, mapped bool
		want            string
	}{
		{[]string{"2606:4700::1111", "8.8.8.8"}, false, true, "8.8.8.8"},
		{[]string{"10.0.0.1", "2606:4700::1111"}, false, true, "2606:4700::1111"},
		{[]string{"fe80::1%en0", "bad", "::ffff:8.8.8.8"}, true, true, ""},
		{[]string{"::ffff:8.8.8.8"}, false, false, "8.8.8.8"},
		{[]string{"127.0.0.1"}, false, false, ""},
	} {
		ip, ok := SelectPinnedAddress(tc.answers, tc.private, tc.mapped)
		if ok != (tc.want != "") || ok && ip.String() != tc.want {
			t.Fatalf("snapshot selection: %+v => %s %v", tc, ip, ok)
		}
	}
}

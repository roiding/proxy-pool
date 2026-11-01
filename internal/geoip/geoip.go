// Package geoip resolves proxy exit IPs against an offline MaxMind-format
// database (GeoLite2 City or DB-IP Lite City). Lookups never touch the
// network: the signed judge supplies the exit IP, and geo is derived locally.
package geoip

import (
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/oschwald/maxminddb-golang"
)

type Location struct {
	CountryCode string
	Country     string
	Region      string
	City        string
}

type Resolver interface {
	Lookup(ip netip.Addr) (Location, bool)
}

type MMDB struct {
	reader *maxminddb.Reader
}

func OpenMMDB(path string) (*MMDB, error) {
	reader, err := maxminddb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open geoip database: %w", err)
	}
	return &MMDB{reader: reader}, nil
}

func (m *MMDB) Close() error {
	if m == nil || m.reader == nil {
		return nil
	}
	return m.reader.Close()
}

type record struct {
	Country struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"country"`
	RegisteredCountry struct {
		ISOCode string            `maxminddb:"iso_code"`
		Names   map[string]string `maxminddb:"names"`
	} `maxminddb:"registered_country"`
	Subdivisions []struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"subdivisions"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
}

func (m *MMDB) Lookup(ip netip.Addr) (Location, bool) {
	if m == nil || m.reader == nil || !ip.IsValid() {
		return Location{}, false
	}
	var rec record
	if err := m.reader.Lookup(net.IP(ip.AsSlice()), &rec); err != nil {
		return Location{}, false
	}
	loc := Location{
		CountryCode: rec.Country.ISOCode,
		Country:     pickName(rec.Country.Names),
	}
	if loc.CountryCode == "" {
		loc.CountryCode = rec.RegisteredCountry.ISOCode
	}
	if loc.Country == "" {
		loc.Country = pickName(rec.RegisteredCountry.Names)
	}
	if n := len(rec.Subdivisions); n > 0 {
		loc.Region = pickName(rec.Subdivisions[n-1].Names)
	}
	loc.City = pickName(rec.City.Names)
	return loc, loc.CountryCode != "" || loc.Country != "" || loc.Region != "" || loc.City != ""
}

func pickName(names map[string]string) string {
	for _, lang := range []string{"zh-CN", "zh", "en"} {
		if v := strings.TrimSpace(names[lang]); v != "" {
			return v
		}
	}
	for _, v := range names {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

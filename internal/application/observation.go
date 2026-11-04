package application

import (
	"context"
	"net/netip"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/geoip"
	"proxypool/internal/storage/sqlite"
)

// recordObservation stores the signed exit IP and its offline geo lookup.
// Geo failures never affect delivery: the probe evidence is already complete.
func recordObservation(ctx context.Context, store *sqlite.Store, geo geoip.Resolver,
	resourceID string, ev domain.Evidence, now time.Time) *domain.Observation {
	if ev.ObservedExitIP == "" {
		return nil
	}
	obs := domain.Observation{ExitIP: ev.ObservedExitIP, ObservedAt: now.UTC()}
	if geo != nil {
		if ip, err := netip.ParseAddr(ev.ObservedExitIP); err == nil {
			if loc, ok := geo.Lookup(ip); ok {
				obs.CountryCode = loc.CountryCode
				obs.Country = loc.Country
				obs.Region = loc.Region
				obs.City = loc.City
			}
		}
	}
	_ = store.UpsertObservation(ctx, resourceID, obs)
	return &obs
}

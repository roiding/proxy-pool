// Package staticdl preserves the static-list download API over the source
// adapters' shared DNS, redirect, TLS, deadline and decoded-byte boundary.
package staticdl

import (
	"context"
	"net"
	"proxypool/internal/integrations/sourcehttp"
	"time"
)

const (
	MaxBodyBytes   = sourcehttp.MaxBodyBytes
	MaxRedirects   = sourcehttp.MaxRedirects
	DefaultTimeout = sourcehttp.DefaultTimeout
)

type Options struct {
	Timeout      time.Duration
	MaxBytes     int64
	MaxRedirects int
	AllowPrivate bool
}
type Conditional = sourcehttp.Conditional
type Result = sourcehttp.Result
type Error = sourcehttp.Error

func Download(ctx context.Context, rawURL string, cond Conditional, opts Options) (Result, error) {
	return sourcehttp.Download(ctx, rawURL, cond, sourcehttp.Options{Timeout: opts.Timeout, MaxBytes: opts.MaxBytes, MaxRedirects: opts.MaxRedirects, AllowPrivate: opts.AllowPrivate, ResolveHost: resolveHost})
}

// Historical package-local fixture seam; policy is still enforced on answers.
var resolveHost = func(ctx context.Context, host string) ([]string, error) {
	return (&net.Resolver{PreferGo: true}).LookupHost(ctx, host)
}

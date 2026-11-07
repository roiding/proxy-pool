// Command sitecheck performs opt-in, bounded channel acceptance. A single
// -site fetches/parses only by default; -probe N explicitly enables proxy
// validation. -all must be explicit, and SCDN requires a separate -scdn run.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"proxypool/internal/application"
	"proxypool/internal/domain"
	"proxypool/internal/integrations/scdn"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/integrations/sourcehttp"
	"proxypool/internal/validation"
)

type probePhase struct {
	Requested       bool           `json:"requested"`
	Mode            string         `json:"mode,omitempty"`
	Target          string         `json:"target,omitempty"`
	Sample          int            `json:"sample"`
	Attempted       int            `json:"attempted"`
	Pass            int            `json:"pass"`
	Fail            int            `json:"fail"`
	FailKinds       map[string]int `json:"fail_kinds,omitempty"`
	FailReasons     map[string]int `json:"fail_reasons,omitempty"`
	Skipped         string         `json:"skipped,omitempty"`
	AllSamplePassed bool           `json:"all_sample_passed"`
}

type checkResult struct {
	SiteID          string           `json:"site_id"`
	Name            string           `json:"name"`
	State           sites.SiteState  `json:"catalog_state"`
	Protocol        domain.Transport `json:"protocol"`
	ParserVersion   string           `json:"parser_version"`
	FetchOK         bool             `json:"fetch_ok"`
	Entries         int              `json:"entries"`
	Fetched         int              `json:"fetched"`
	Rejected        int              `json:"rejected"`
	PagesDone       int              `json:"pages_done"`
	PagesTotal      int              `json:"pages_total"`
	Complete        bool             `json:"complete"`
	Sampled         bool             `json:"sampled"`
	SourceUpdatedAt *time.Time       `json:"source_updated_at,omitempty"`
	Error           string           `json:"error,omitempty"`
	Probe           probePhase       `json:"probe"`
	Verdict         string           `json:"verdict"`
	Millis          int64            `json:"millis"`
}

type report struct {
	CheckedAt time.Time     `json:"checked_at"`
	Seed      int64         `json:"sample_seed"`
	Sites     []checkResult `json:"sites"`
	SCDN      *scdnReport   `json:"scdn,omitempty"`
	Notes     []string      `json:"notes"`
	AllPassed bool          `json:"all_requested_checks_passed"`
}

type scdnReport struct {
	OK        bool              `json:"ok"`
	Requested int               `json:"requested"`
	Fetched   int               `json:"fetched"`
	Errors    []scdn.EntryError `json:"errors,omitempty"`
	Error     string            `json:"error,omitempty"`
	Note      string            `json:"note"`
	Probe     probePhase        `json:"probe"`
}

func main() {
	one := flag.String("site", "", "check only this site_id")
	all := flag.Bool("all", false, "explicitly check one default variant per admitted public supplier (not the default)")
	listOnly := flag.Bool("list", false, "list admitted supplier channels (one row per brand) without network I/O")
	recheckPaused := flag.Bool("recheck-paused", false, "diagnostically recheck a selected paused endpoint; never changes its catalog state")
	pages := flag.Int("max-pages", 1, "maximum pages per site (1..20)")
	workers := flag.Int("workers", 2, "concurrent source downloads for explicit -all (1..4)")
	timeout := flag.Int("timeout", 30, "whole per-channel fetch+probe deadline in seconds (1..120)")
	asJSON := flag.Bool("json", false, "print a sanitized JSON report")
	seed := flag.Int64("seed", 1, "reproducible shuffled sample seed after canonical deduplication")
	probeCount := flag.Int("probe", 0, "explicitly validate up to N unique proxies (0 = fetch/parse only, maximum 20)")
	probeMode := flag.String("probe-mode", "basic", "validation mode: basic or tls")
	probeTarget := flag.String("probe-target", "", "target host:port for TLS validation")
	judgeURL := flag.String("judge", "", "self-hosted judge URL for basic validation")
	judgeSecret := flag.String("judge-secret", "", "plain-HTTP judge secret (prefer PROXY_POOL_JUDGE_SECRET environment variable)")
	probeTimeout := flag.Int("probe-timeout", 6, "per-probe total timeout seconds (1..30)")
	scdnRun := flag.Bool("scdn", false, "explicitly run only one SCDN diagnostic (consumes vendor quota)")
	scdnEndpoint := flag.String("scdn-endpoint", scdn.DefaultEndpoint, "SCDN endpoint; no credentials or query are permitted")
	scdnProtocol := flag.String("scdn-protocol", "http", "SCDN protocol: http, https, socks5")
	scdnCount := flag.Int("scdn-count", 3, "SCDN fetch count (1..20)")
	scdnCountry := flag.String("scdn-country", "", "optional SCDN two-letter country code")
	flag.Parse()
	invalid := func(text string) { fmt.Fprintln(os.Stderr, text); os.Exit(2) }
	if flag.NArg() != 0 {
		invalid("unexpected positional arguments")
	}
	if *listOnly {
		if *asJSON {
			svc := new(application.IngestService)
			_ = json.NewEncoder(os.Stdout).Encode(svc.ChannelTemplates())
		} else {
			svc := new(application.IngestService)
			for _, channel := range svc.ChannelTemplates() {
				variants := []string{}
				for _, variant := range channel.Variants {
					variants = append(variants, variant.ID)
				}
				fmt.Printf("%-18s %-18s %s\n", channel.ID, channel.Name, strings.Join(variants, ", "))
			}
			fmt.Println("SCDN: use an explicit separate -scdn run; Webshare/account acceptance uses the admin secret/refresh/prewarm APIs.")
		}
		return
	}
	if *pages < 1 || *pages > sites.MaxPages || *workers < 1 || *workers > 4 || *timeout < 1 || *timeout > 120 || *probeCount < 0 || *probeCount > 20 || *probeTimeout < 1 || *probeTimeout > 30 || *scdnCount < 1 || *scdnCount > scdn.MaxCount {
		invalid("limits exceeded: pages 1..20, workers 1..4, timeout 1..120, probe 0..20, probe-timeout 1..30, scdn-count 1..20")
	}
	if (*one == "" && !*all && !*scdnRun) || (*one != "" && *all) || (*scdnRun && (*one != "" || *all)) {
		invalid("choose exactly one -site ID or -scdn; broad public checks require explicit -all (use -list to inspect without I/O)")
	}
	if *recheckPaused && *one == "" {
		invalid("-recheck-paused requires one selected -site")
	}
	if *probeCount > 0 {
		if _, derr := validation.NormalizeOptions(*probeMode, *probeTarget); derr != nil {
			invalid(derr.Message)
		}
		if *probeMode == "basic" && strings.TrimSpace(*judgeURL) == "" {
			invalid("basic probes require -judge; download/TCP success is not proxy liveness")
		}
	}
	if *judgeSecret == "" {
		*judgeSecret = os.Getenv("PROXY_POOL_JUDGE_SECRET")
	}
	list := []sites.Site{}
	if *one != "" {
		s, ok := sites.ByID(*one)
		if !ok {
			invalid(fmt.Sprintf("unknown site %q; use -list", *one))
		}
		if *recheckPaused && !diagnosticRecheckAllowed(s) {
			invalid("-recheck-paused requires a paused endpoint with an implemented parser and confirmed protocol; unknown schemas/protocols cannot be forced")
		}
		list = []sites.Site{s}
	} else if *all {
		list = sites.DefaultSites()
	}
	client := sourcehttp.NewHTTPClient(sourcehttp.Options{Timeout: time.Duration(*timeout) * time.Second, MaxBytes: sites.MaxPageBytes})
	results := make([]checkResult, len(list))
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	for i, original := range list {
		wg.Add(1)
		go func(i int, original sites.Site) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			site := original
			if *recheckPaused && diagnosticRecheckAllowed(original) {
				site.State = sites.SiteActive
			}
			results[i] = checkSite(site, client, *pages, *timeout, *probeCount, *probeMode, *probeTarget, *judgeURL, *judgeSecret, *probeTimeout, *seed)
			results[i].State = original.State
		}(i, original)
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].SiteID < results[j].SiteID })
	rep := report{CheckedAt: time.Now().UTC(), Seed: *seed, Sites: results, AllPassed: true, Notes: []string{
		"fetch_only verifies fetching/parsing only, never proxy liveness",
		"sample_has_pass means at least one unique sampled proxy passed; counts describe the bounded sample, not the whole channel",
		"direct controlled source downloads: no implicit system/environment proxy; TLS certificates stay verified",
		"no candidates, credentials, raw vendor response bodies, or business HTTP payloads are included in this report",
	}}
	if *recheckPaused {
		rep.Notes = append(rep.Notes, "paused endpoint recheck is diagnostic only; no source/catalog state is enabled or changed")
	}
	for _, r := range results {
		if !requestedCheckPassed(r) {
			rep.AllPassed = false
		}
	}
	if *scdnRun {
		rep.SCDN = runSCDN(*scdnEndpoint, *scdnProtocol, *scdnCount, *scdnCountry, *probeCount, *probeMode, *probeTarget, *judgeURL, *judgeSecret, *probeTimeout, *timeout, *seed)
		if !rep.SCDN.OK {
			rep.AllPassed = false
		}
	} else {
		rep.Notes = append(rep.Notes, "SCDN not called: every extraction requires an explicit separate -scdn invocation")
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		printTable(rep)
	}
	if !rep.AllPassed {
		os.Exit(1)
	}
}

func diagnosticRecheckAllowed(site sites.Site) bool {
	return site.State == sites.SitePaused &&
		(site.Protocol == domain.TransportHTTP || site.Protocol == domain.TransportSOCKS5) &&
		site.ParserVersion != "" && !strings.HasPrefix(site.ParserVersion, "awaiting")
}

func checkSite(site sites.Site, client *http.Client, pages, timeoutSec, probeCount int, probeMode, probeTarget, judgeURL, judgeSecret string, probeTimeoutSec int, seeds ...int64) checkResult {
	seed := int64(1)
	if len(seeds) > 0 {
		seed = seeds[0]
	}
	r := checkResult{SiteID: site.ID, Name: site.Name, State: site.State, Protocol: site.Protocol, ParserVersion: site.ParserVersion}
	if !site.State.Runnable() {
		r.Verdict = "not_run"
		r.Error = site.Note
		r.Probe.Skipped = "catalog entry is not runnable"
		return r
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()
	start := time.Now()
	out, err := sites.FetchSite(ctx, client, site, pages, false)
	if err != nil {
		r.Error = err.Error()
		r.Verdict = "fetch_failed"
		r.Millis = time.Since(start).Milliseconds()
		return r
	}
	r.FetchOK = true
	r.Entries = len(sampleEntries(out.Entries, len(out.Entries), seed))
	r.Fetched = out.Fetched
	r.Rejected = out.RejectedCount
	r.PagesDone = out.PagesDone
	r.PagesTotal = out.PagesTotal
	r.Complete = out.Complete
	r.Sampled = out.Truncated
	r.SourceUpdatedAt = out.SourceUpdatedAt
	if probeCount <= 0 {
		r.Probe.Skipped = "probe not requested: candidates are unvalidated"
		r.Verdict = "fetch_only"
	} else if r.Entries == 0 {
		r.Probe.Requested = true
		r.Probe.Skipped = "source returned an explicit empty list"
		r.Verdict = "no_proxy_pass"
	} else {
		r.Probe = probeCandidatesInContext(ctx, out.Entries, probeCount, probeMode, probeTarget, judgeURL, judgeSecret, probeTimeoutSec, seed)
		r.Verdict = probeVerdict(r.Probe)
	}
	r.Millis = time.Since(start).Milliseconds()
	return r
}

func requestedCheckPassed(r checkResult) bool {
	return r.FetchOK && (r.Verdict == "fetch_only" || r.Verdict == "sample_has_pass" || r.Verdict == "sample_all_pass")
}
func probeVerdict(p probePhase) string {
	if !p.Requested || p.Attempted == 0 || p.Pass == 0 {
		return "no_proxy_pass"
	}
	if p.AllSamplePassed {
		return "sample_all_pass"
	}
	return "sample_has_pass"
}

// Sort by canonical connection identity before seeded shuffling, so reordered
// provider pages and duplicate rows cannot silently change the sample.
func sampleEntries(entries []sites.Entry, count int, seed int64) []sites.Entry {
	out := make([]sites.Entry, 0, len(entries))
	seen := map[string]bool{}
	for _, e := range entries {
		fp := domain.ConnectionFingerprint("sitecheck-sample", e.Conn.Transport, e.Conn.Host, e.Conn.Port, e.Conn.Username, e.Conn.Password)
		if !seen[fp] {
			seen[fp] = true
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Conn, out[j].Conn
		return domain.ConnectionFingerprint("sitecheck-sample", a.Transport, a.Host, a.Port, a.Username, a.Password) < domain.ConnectionFingerprint("sitecheck-sample", b.Transport, b.Host, b.Port, b.Username, b.Password)
	})
	rand.New(rand.NewSource(seed)).Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	if count < 0 {
		count = 0
	}
	if count < len(out) {
		out = out[:count]
	}
	return out
}

func probeCandidates(entries []sites.Entry, probeCount int, mode, target, judgeURL, judgeSecret string, timeoutSec int) probePhase {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return probeCandidatesInContext(ctx, entries, probeCount, mode, target, judgeURL, judgeSecret, timeoutSec, 1)
}

func probeCandidatesInContext(ctx context.Context, entries []sites.Entry, probeCount int, mode, target, judgeURL, judgeSecret string, timeoutSec int, seed int64) probePhase {
	phase := probePhase{Requested: true, Mode: mode, Target: target, FailKinds: map[string]int{}, FailReasons: map[string]int{}}
	scope, derr := validation.NormalizeOptions(mode, target)
	if derr != nil {
		phase.Skipped = derr.Message
		return phase
	}
	if probeCount > 20 {
		probeCount = 20
	}
	sample := sampleEntries(entries, probeCount, seed)
	phase.Sample = len(sample)
	limits := validation.ProbeLimits{DialTimeout: 3 * time.Second, ProbeTotal: time.Duration(timeoutSec) * time.Second, AllowPrivate: false}
	if mode == "basic" {
		limits.JudgeSecret = judgeSecret
	}
	opts := validation.ProbeOptions{JudgeURL: judgeURL, Limits: limits}
	for _, e := range sample {
		if ctx.Err() != nil {
			phase.Skipped = "whole-channel time budget ended"
			break
		}
		probeCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
		_, fail := validation.Probe(probeCtx, scope, e.Conn, opts, "sitecheck")
		cancel()
		if ctx.Err() != nil {
			phase.Skipped = "whole-channel time budget ended; canceled attempt is not proxy failure evidence"
			break
		}
		phase.Attempted++
		if fail == nil {
			phase.Pass++
		} else {
			phase.Fail++
			phase.FailKinds[fail.Kind]++
			phase.FailReasons[safeProbeFailureReason(fail)]++
		}
	}
	phase.AllSamplePassed = phase.Sample > 0 && phase.Attempted == phase.Sample && phase.Pass == phase.Sample
	if phase.Attempted == 0 && phase.Skipped == "" {
		phase.Skipped = "no parsed entries to probe"
	}
	return phase
}

func runSCDN(endpoint, protocol string, count int, country string, probeCount int, probeMode, probeTarget, judgeURL, judgeSecret string, probeTimeout, timeoutSec int, seeds ...int64) *scdnReport {
	seed := int64(1)
	if len(seeds) > 0 {
		seed = seeds[0]
	}
	rep := &scdnReport{Requested: count, Note: "explicit bounded extraction; unused candidates die with this diagnostic; no inventory or liveness claim"}
	cfg := scdn.Config{Protocol: protocol, CountryCode: country, FetchCount: count}
	if err := cfg.Validate(); err != nil {
		rep.Error = err.Error()
		return rep
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()
	client := sourcehttp.NewHTTPClient(sourcehttp.Options{MaxBytes: scdn.MaxResponseBytes})
	batch, err := scdn.Fetch(ctx, client, endpoint, cfg, false)
	if err != nil {
		rep.Error = err.Error()
		return rep
	}
	rep.Fetched = len(batch.Entries)
	rep.Errors = batch.Errors
	rep.OK = true
	if probeCount > 0 {
		entries := make([]sites.Entry, 0, len(batch.Entries))
		for _, e := range batch.Entries {
			entries = append(entries, sites.Entry{Conn: domain.Connection{Transport: e.Transport, Host: e.Host, Port: e.Port}})
		}
		rep.Probe = probeCandidatesInContext(ctx, entries, probeCount, probeMode, probeTarget, judgeURL, judgeSecret, probeTimeout, seed)
		rep.OK = rep.Probe.Pass > 0
	} else {
		rep.Probe.Skipped = "probe not requested: SCDN candidates are unvalidated"
	}
	return rep
}

func printTable(rep report) {
	for _, r := range rep.Sites {
		fmt.Printf("%-18s %-27s state=%-8s fetch=%v unique=%4d rejected=%3d pages=%d/%d probe=%d/%d pass %7dms %s\n", r.Verdict, r.SiteID, r.State, r.FetchOK, r.Entries, r.Rejected, r.PagesDone, r.PagesTotal, r.Probe.Pass, r.Probe.Attempted, r.Millis, firstNonEmpty(r.Error, r.Probe.Skipped))
	}
	if s := rep.SCDN; s != nil {
		fmt.Printf("scdn requested=%d fetched=%d check_ok=%v probe=%d/%d %s\n", s.Requested, s.Fetched, s.OK, s.Probe.Pass, s.Probe.Attempted, firstNonEmpty(s.Error, s.Note))
	}
	for _, n := range rep.Notes {
		fmt.Println("note:", n)
	}
}
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

var connectStatusPattern = regexp.MustCompile(`HTTP ([0-9]{3})`)

// Diagnostic aggregates only fixed reason labels. Never include raw dial,
// credential-bearing URL, vendor text, IP address or certificate contents.
func safeProbeFailureReason(fail *validation.Failure) string {
	detail := strings.ToLower(fail.Detail)
	if fail.Kind == validation.FailAuth {
		return "proxy_authentication"
	}
	if strings.Contains(detail, "connection refused") {
		return "connection_refused"
	}
	if strings.Contains(detail, "timeout") || strings.Contains(detail, "deadline exceeded") {
		return "timeout"
	}
	if strings.Contains(detail, "network is unreachable") || strings.Contains(detail, "no route to host") {
		return "network_unreachable"
	}
	if strings.Contains(detail, "proxy connect failed for ") {
		if match := connectStatusPattern.FindStringSubmatch(fail.Detail); match != nil {
			return "connect_http_" + match[1]
		}
	}
	if strings.Contains(detail, "certificate") {
		return "tls_certificate_rejected"
	}
	if strings.Contains(detail, "tls handshake") {
		return "target_tls_handshake"
	}
	if strings.Contains(detail, "resolve target") || strings.Contains(detail, "resolve proxy") {
		return "dns_failure"
	}
	if strings.Contains(detail, "read connect response") || strings.Contains(detail, "malformed") {
		return "invalid_proxy_protocol"
	}
	if strings.Contains(detail, "reset by peer") || strings.Contains(detail, "eof") {
		return "connection_closed"
	}
	return "other_" + strings.ToLower(fail.Kind)
}

// Command sitecheck checks every site_fetch catalog entry (or a -site filter)
// and, when explicitly requested, probes a bounded sample of the parsed
// candidates through the real validation path. Fetch/parse success never
// counts as proxy liveness: a channel only "passes" when the requested probe
// phase passes, and channels needing credentials are reported as pending.
// The SCDN on-demand API is only called with the explicit -scdn flag because
// every call consumes vendor quota.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"proxypool/internal/domain"
	"proxypool/internal/integrations/scdn"
	"proxypool/internal/integrations/sites"
	"proxypool/internal/validation"
)

type probePhase struct {
	Requested bool           `json:"requested"`
	Mode      string         `json:"mode,omitempty"`
	Sample    int            `json:"sample"`
	Attempted int            `json:"attempted"`
	Pass      int            `json:"pass"`
	Fail      int            `json:"fail"`
	FailKinds map[string]int `json:"fail_kinds,omitempty"`
	Skipped   string         `json:"skipped,omitempty"`
}

type checkResult struct {
	SiteID    string     `json:"site_id"`
	Name      string     `json:"name"`
	FetchOK   bool       `json:"fetch_ok"`
	Entries   int        `json:"entries"`
	Rejected  int        `json:"rejected"`
	PagesDone int        `json:"pages_done"`
	Error     string     `json:"error,omitempty"`
	Probe     probePhase `json:"probe"`
	Verdict   string     `json:"verdict"`
	Millis    int64      `json:"millis"`
}

type report struct {
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
	one := flag.String("site", "", "only check this site_id")
	pages := flag.Int("max-pages", 1, "max pages per site")
	workers := flag.Int("workers", 4, "concurrent checks")
	timeout := flag.Int("timeout", 30, "per-site fetch timeout seconds")
	asJSON := flag.Bool("json", false, "print JSON instead of a table")
	probeCount := flag.Int("probe", 0, "probe up to N parsed candidates per site (0 = fetch/parse only)")
	probeMode := flag.String("probe-mode", "basic", "probe validation mode: basic or tls")
	probeTarget := flag.String("probe-target", "", "target host:port for -probe-mode tls")
	judgeURL := flag.String("judge", "", "judge base URL required for basic probes")
	judgeSecret := flag.String("judge-secret", "", "judge HMAC secret for plain-HTTP judges")
	probeTimeout := flag.Int("probe-timeout", 10, "per-probe total timeout seconds")
	scdnRun := flag.Bool("scdn", false, "explicitly run one SCDN on-demand diagnostic (consumes vendor quota)")
	scdnEndpoint := flag.String("scdn-endpoint", scdn.DefaultEndpoint, "SCDN API endpoint")
	scdnProtocol := flag.String("scdn-protocol", "http", "SCDN protocol filter: http, https, socks5")
	scdnCount := flag.Int("scdn-count", scdn.MaxCount, "SCDN fetch_count (1..20)")
	scdnCountry := flag.String("scdn-country", "", "SCDN country_code filter")
	flag.Parse()

	if *probeCount > 0 && *probeMode == "basic" && strings.TrimSpace(*judgeURL) == "" {
		fmt.Fprintln(os.Stderr, "basic probes require -judge (self-hosted judge URL); refusing to fake liveness")
		os.Exit(2)
	}
	if *probeCount > 0 && *probeMode == "tls" && strings.TrimSpace(*probeTarget) == "" {
		fmt.Fprintln(os.Stderr, "tls probes require -probe-target host:port")
		os.Exit(2)
	}

	list := sites.Catalog
	if *one != "" {
		s, ok := sites.ByID(*one)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown site %q\n", *one)
			os.Exit(2)
		}
		list = []sites.Site{s}
	}

	client := &http.Client{Timeout: time.Duration(*timeout) * time.Second}
	results := make([]checkResult, len(list))
	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	for i, site := range list {
		wg.Add(1)
		go func(i int, site sites.Site) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = checkSite(site, client, *pages, *timeout,
				*probeCount, *probeMode, *probeTarget, *judgeURL, *judgeSecret, *probeTimeout)
		}(i, site)
	}
	wg.Wait()
	sort.Slice(results, func(a, b int) bool { return results[a].SiteID < results[b].SiteID })

	rep := report{Sites: results, AllPassed: true}
	rep.Notes = append(rep.Notes,
		"fetched entry counts describe parsing, not proxy liveness",
		"probe phase samples a bounded subset; it is not a guarantee for unprobed entries")
	if *scdnRun {
		rep.SCDN = runSCDN(*scdnEndpoint, *scdnProtocol, *scdnCount, *scdnCountry,
			*probeCount, *probeMode, *probeTarget, *judgeURL, *judgeSecret,
			*probeTimeout)
		if *probeCount > 0 && rep.SCDN.Probe.Pass == 0 {
			rep.SCDN.OK = false
		}
		if !rep.SCDN.OK {
			rep.AllPassed = false
		}
	} else {
		rep.Notes = append(rep.Notes,
			"SCDN on-demand API not called: pass -scdn explicitly (each call consumes vendor quota)")
	}
	for _, r := range results {
		if r.Verdict != "pass" {
			rep.AllPassed = false
		}
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

func checkSite(site sites.Site, client *http.Client, pages, timeoutSec, probeCount int,
	probeMode, probeTarget, judgeURL, judgeSecret string, probeTimeoutSec int) checkResult {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()
	start := time.Now()
	r := checkResult{SiteID: site.ID, Name: site.Name}
	out, err := sites.FetchSite(ctx, client, site, pages, false)
	r.Millis = time.Since(start).Milliseconds()
	if err != nil {
		r.Error = err.Error()
		r.Verdict = "fail"
		return r
	}
	r.FetchOK = true
	r.Entries = len(out.Entries)
	r.Rejected = len(out.Rejected)
	r.PagesDone = out.PagesDone
	if r.Entries == 0 {
		r.Error = "parsed 0 entries"
		r.Verdict = "fail"
		return r
	}
	if probeCount <= 0 {
		r.Probe.Skipped = "probe not requested: entries are unvalidated"
		r.Verdict = "fetch_only"
		return r
	}
	r.Probe = probeCandidates(out.Entries, probeCount, probeMode, probeTarget,
		judgeURL, judgeSecret, probeTimeoutSec)
	r.Verdict = probeVerdict(r.Probe)
	return r
}

func probeVerdict(p probePhase) string {
	if !p.Requested || p.Skipped != "" && p.Attempted == 0 {
		return "fail"
	}
	if p.Attempted == 0 {
		return "fail"
	}
	if p.Pass > 0 {
		return "pass"
	}
	return "fail"
}

// probeCandidates runs the production validation path against a bounded
// sample and reports attributed failure classes separately from passes.
func probeCandidates(entries []sites.Entry, probeCount int, mode, target,
	judgeURL, judgeSecret string, timeoutSec int) probePhase {
	phase := probePhase{Requested: true, Mode: mode, Sample: probeCount,
		FailKinds: map[string]int{}}
	scope, derr := validation.NormalizeOptions(mode, target)
	if derr != nil {
		phase.Skipped = derr.Message
		return phase
	}
	limits := validation.ProbeLimits{
		DialTimeout:  3 * time.Second,
		ProbeTotal:   time.Duration(timeoutSec) * time.Second,
		AllowPrivate: false,
	}
	if mode == validation.ModeBasic.String() {
		limits.JudgeSecret = judgeSecret
	}
	opts := validation.ProbeOptions{JudgeURL: judgeURL, Limits: limits}
	for _, e := range entries {
		if phase.Attempted >= probeCount {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
		_, fail := validation.Probe(ctx, scope, e.Conn, opts, "sitecheck")
		cancel()
		phase.Attempted++
		if fail == nil {
			phase.Pass++
			continue
		}
		phase.Fail++
		phase.FailKinds[fail.Kind]++
	}
	if phase.Attempted == 0 {
		phase.Skipped = "no parsed entries to probe"
	} else if phase.Pass == 0 {
		phase.Skipped = "no candidate passed validation"
	}
	return phase
}

func runSCDN(endpoint, protocol string, count int, country string, probeCount int,
	probeMode, probeTarget, judgeURL, judgeSecret string, probeTimeout int) *scdnReport {
	rep := &scdnReport{Requested: count,
		Note: "explicit diagnostic call; on-demand candidates never enter the pool"}
	cfg := scdn.Config{Protocol: protocol, CountryCode: country, FetchCount: count}
	if err := cfg.Validate(); err != nil {
		rep.Error = err.Error()
		return rep
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	batch, ferr := scdn.Fetch(ctx, http.DefaultClient, endpoint, cfg, false)
	if ferr != nil {
		rep.Error = ferr.Error()
		return rep
	}
	rep.Fetched = len(batch.Entries)
	rep.Errors = batch.Errors
	rep.OK = true
	if probeCount > 0 {
		entries := make([]sites.Entry, 0, len(batch.Entries))
		for _, e := range batch.Entries {
			entries = append(entries, sites.Entry{Conn: domain.Connection{
				Transport: e.Transport, Host: e.Host, Port: e.Port}})
		}
		rep.Probe = probeCandidates(entries, probeCount, probeMode, probeTarget,
			judgeURL, judgeSecret, probeTimeout)
		rep.Note += "; bounded sample was probe-validated"
	} else {
		rep.Probe.Skipped = "probe not requested: SCDN entries are unvalidated"
		rep.Note += "; fetched/parsed only, not live-verified"
	}
	return rep
}

func printTable(rep report) {
	for _, r := range resultsOnly(rep) {
		status := r.Verdict
		fmt.Printf("%-10s %-26s fetch=%v entries=%4d rejected=%3d pages=%d probe=%d/%d pass %7dms %s\n",
			status, r.SiteID, r.FetchOK, r.Entries, r.Rejected, r.PagesDone,
			r.Probe.Pass, r.Probe.Attempted, r.Millis, firstNonEmpty(r.Error, r.Probe.Skipped))
	}
	if rep.SCDN != nil {
		status := "fail"
		if rep.SCDN.OK {
			status = "ok"
		}
		fmt.Printf("%-10s %-26s requested=%d fetched=%d probe=%d/%d %s\n",
			status, "scdn-on-demand", rep.SCDN.Requested, rep.SCDN.Fetched,
			rep.SCDN.Probe.Pass, rep.SCDN.Probe.Attempted,
			firstNonEmpty(rep.SCDN.Error, rep.SCDN.Note))
	}
	for _, n := range rep.Notes {
		fmt.Printf("note: %s\n", n)
	}
}

func resultsOnly(rep report) []checkResult { return rep.Sites }

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

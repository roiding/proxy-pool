// Command sitecheck fetches every site_fetch catalog entry (or a -site
// filter) through the production parsers and prints per-channel results, so
// source drift is visible before channels are created. It is a standalone
// tool: no database, safe to run on a laptop or the server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"proxypool/internal/integrations/sites"
)

type checkResult struct {
	SiteID    string `json:"site_id"`
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	Entries   int    `json:"entries"`
	Rejected  int    `json:"rejected"`
	PagesDone int    `json:"pages_done"`
	Error     string `json:"error,omitempty"`
	Millis    int64  `json:"millis"`
}

func main() {
	one := flag.String("site", "", "only check this site_id")
	pages := flag.Int("max-pages", 1, "max pages per site")
	workers := flag.Int("workers", 4, "concurrent checks")
	timeout := flag.Int("timeout", 30, "per-site timeout seconds")
	asJSON := flag.Bool("json", false, "print JSON instead of a table")
	flag.Parse()

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
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
			defer cancel()
			start := time.Now()
			out, err := sites.FetchSite(ctx, client, site, *pages, false)
			r := checkResult{SiteID: site.ID, Name: site.Name, Millis: time.Since(start).Milliseconds()}
			if err != nil {
				r.Error = err.Error()
			} else {
				r.OK = len(out.Entries) > 0
				r.Entries = len(out.Entries)
				r.Rejected = len(out.Rejected)
				r.PagesDone = out.PagesDone
				if !r.OK {
					r.Error = "parsed 0 entries"
				}
			}
			results[i] = r
		}(i, site)
	}
	wg.Wait()
	sort.Slice(results, func(a, b int) bool { return results[a].SiteID < results[b].SiteID })

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
	} else {
		okCount := 0
		for _, r := range results {
			status := "FAIL"
			if r.OK {
				status = "ok  "
				okCount++
			}
			fmt.Printf("%s %-26s %4d entries %3d rejected %d pages %6dms %s\n",
				status, r.SiteID, r.Entries, r.Rejected, r.PagesDone, r.Millis, r.Error)
		}
		fmt.Printf("\n%d/%d channels returned at least one entry\n", okCount, len(results))
	}
	for _, r := range results {
		if !r.OK {
			os.Exit(1)
		}
	}
}

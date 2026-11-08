package main

import (
	"context"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/geoip"
	"proxypool/internal/maintenance"
	"proxypool/internal/storage/sqlite"
	"proxypool/internal/usage"
)

// version is injected at build time via -ldflags "-X main.version=...".
// The vcs.* variables come from the build's VCS stamping when available.
var (
	version = "dev"
	vcsRef  string
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildVersion())
		return
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	for _, path := range []string{cfg.LockPath, cfg.DatabasePath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			log.Error("create data dir failed", "path", path, "error", err)
			os.Exit(1)
		}
	}

	release, err := acquireSingletonLock(cfg.LockPath)
	if err != nil {
		log.Error("singleton lock failed", "error", err)
		os.Exit(1)
	}
	defer release()

	db, err := sqlite.Open(cfg.DatabasePath, cfg.SQLiteCacheKiB)
	if err != nil {
		log.Error("open sqlite failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	store := sqlite.New(db)
	if _, err := store.MigrateWithOptions(context.Background(), sqlite.MigrateOptions{
		IdentitySecret:    cfg.IdentitySecret,
		LegacyMaxCooldown: cfg.D(cfg.MaxDownstreamCooldownMS),
	}); err != nil {
		log.Error("migrate failed", "error", err)
		os.Exit(1)
	}
	if err := store.KeepCustomSupply(context.Background()); err != nil {
		log.Error("configure upload lifetime failed", "error", err)
		os.Exit(1)
	}
	if err := store.BootstrapSourceBlacklist(context.Background(), time.Now()); err != nil {
		log.Error("initialize source blacklist failed", "error", err)
		os.Exit(1)
	}

	rootCAs, err := buildRootCAs(cfg.JudgeCAPem)
	if err != nil {
		log.Error("judge CA config invalid", "error", err)
		os.Exit(1)
	}
	log.Info("judge endpoint", "path", "/v1/judge", "signed", cfg.JudgeSecret != "",
		"trust_forwarded", cfg.JudgeTrustForwarded)
	ingest := application.NewIngestService(store, cfg.IdentitySecret, cfg.AllowPrivateAddresses)
	if err := ingest.ConfigureSourceSecrets(cfg.SecretEncryptionKey); err != nil {
		log.Error("source secret encryption config failed", "error", err)
		os.Exit(1)
	}
	if err := ingest.ConfigureStaticCache(application.DefaultStaticCacheTTL,
		cfg.StaticCacheMaxEntries, cfg.StaticCacheMaxBytes); err != nil {
		log.Error("static cache config failed", "error", err)
		os.Exit(1)
	}
	if err := ingest.ReconcileEnabledChannels(context.Background()); err != nil {
		log.Warn("fixed channel reconciliation failed", "error", err)
	}
	keys := application.NewKeyService(store, cfg.IdentitySecret)
	var geoResolver geoip.Resolver
	if cfg.GeoIPDBPath != "" {
		mmdb, err := geoip.OpenMMDB(cfg.GeoIPDBPath)
		if err != nil {
			log.Error("geoip database invalid", "path", cfg.GeoIPDBPath, "error", err)
			os.Exit(1)
		}
		defer mmdb.Close()
		geoResolver = mmdb
		log.Info("geoip database loaded", "path", cfg.GeoIPDBPath)
	}
	leaseSvc := application.NewLeaseService(store, application.CooldownConfig{
		Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter,
	})
	probeSlots := make(chan struct{}, cfg.ProbeSlots)
	acquire := application.NewAcquireService(store, cfg, cfg.JudgeURL, rootCAs, probeSlots, geoResolver)
	acquire.Refiller = ingest
	nightly := application.NewNightlyValidator(store, cfg, cfg.JudgeURL, rootCAs,
		application.CooldownConfig{Base: cfg.CooldownBase(), Max: cfg.CooldownMax(), Jitter: cfg.CooldownJitter},
		probeSlots, geoResolver)
	prewarm := application.NewPrewarmService(store, cfg, cfg.JudgeURL, rootCAs, probeSlots, geoResolver)
	prewarm.StaticSupply = ingest
	poolMaintenance := application.NewPoolMaintenanceService(store, prewarm)
	if cfg.PoolMaintenanceIntervalMS > 0 {
		poolMaintenance.TargetMetBackoff = time.Duration(cfg.PoolMaintenanceIntervalMS) * time.Millisecond
	}
	refreshSvc := application.NewSourceRefreshService(store, ingest, cfg)
	// Traffic accounting owns its own tables on the same database handle;
	// its scheduler never touches the acquire path.
	usageSvc, err := usage.New(db, usage.Options{
		MasterKey:        cfg.SecretEncryptionKey,
		BarkURL:          cfg.BarkURL,
		BarkEnabled:      false,
		HuaweiAK:         cfg.HuaweiAccessKeyID,
		HuaweiSK:         cfg.HuaweiSecretAccessKey,
		HuaweiRegion:     cfg.HuaweiRegion,
		HuaweiResourceID: cfg.HuaweiResourceID,
		HuaweiPackageID:  cfg.HuaweiPackageID,
		MonthlyTotalGB:   cfg.CloudUsageMonthlyTotalGB,
		ThresholdPercent: int64(cfg.CloudUsageThresholdPercent),
		CheckTime:        cfg.CloudUsageCheckTime,
		Timezone:         cfg.CloudUsageTimezone,
		BarkTimeout:      cfg.BarkTimeout(),
		BarkRetries:      cfg.BarkMaxRetries,
		CloudTimeout:     cfg.CloudUsageTimeout(),
	})
	if err != nil {
		log.Error("usage service init failed", "error", err)
		os.Exit(1)
	}

	// Server timeouts must exceed the largest acquire budget plus margin:
	// a long-running acquire is write-bound, and a 30s cap would cut off
	// legitimate 120s requests mid-response.
	serverTimeout := cfg.MaxAcquireDeadline() + 10*time.Second
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	srv := newHTTPServer(cfg,
		api.NewServer(cfg, log, acquire, ingest, leaseSvc, store, nightly, prewarm, keys,
			api.WithUsage(usageSvc), api.WithVersion(buildVersion()), api.WithSourceRefresh(refreshSvc)).Handler(),
		runCtx, serverTimeout)

	var background sync.WaitGroup
	background.Add(3)
	go func() { defer background.Done(); refreshSvc.Run(runCtx, log) }()
	go func() {
		defer background.Done()
		maintenance.Run(runCtx, store, cfg.SweepInterval(), cfg.AttemptRetention(),
			cfg.NightlyRetireFailures, log)
	}()
	if cfg.NightlyValidationTime != "" {
		background.Add(1)
		go func() {
			defer background.Done()
			nightly.Run(runCtx, cfg.NightlyValidationTime, log)
		}()
	}
	if _, err := prewarm.RecoverInterrupted(runCtx); err != nil {
		log.Warn("prewarm job recovery failed", "error", err)
	}
	background.Add(1)
	go func() {
		defer background.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if n, err := prewarm.ReapStaleJobs(runCtx); err != nil {
					log.Warn("prewarm stale job sweep failed", "error", err)
				} else if n > 0 {
					log.Warn("prewarm stale jobs reaped", "count", n)
				}
			}
		}
	}()
	background.Add(1)
	go func() {
		defer background.Done()
		poolMaintenance.Run(runCtx, log)
	}()
	go func() {
		defer background.Done()
		usageSvc.Run(runCtx, log)
	}()

	httpErr := make(chan error, 1)
	go func() {
		log.Info("proxy-pool listening", "version", buildVersion(),
			"addr", cfg.ListenAddr, "db", cfg.DatabasePath)
		httpErr <- srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signalNotify(sig)
	select {
	case err := <-httpErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	case <-sig:
		log.Info("shutting down")
		// Cancel background scheduling immediately, then drain HTTP and wait
		// for in-flight jobs in parallel under one bounded deadline. Waiting
		// for HTTP first would leave an expired context for job finalization.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		stop()
		shutdownErr := make(chan struct {
			name string
			err  error
		}, 4)
		go func() {
			shutdownErr <- struct {
				name string
				err  error
			}{"http", srv.Shutdown(shutdownCtx)}
		}()
		go func() {
			shutdownErr <- struct {
				name string
				err  error
			}{"prewarm", prewarm.Shutdown(shutdownCtx)}
		}()
		go func() {
			shutdownErr <- struct {
				name string
				err  error
			}{"validation", nightly.Shutdown(shutdownCtx)}
		}()
		go func() {
			shutdownErr <- struct {
				name string
				err  error
			}{"source_refresh", refreshSvc.Shutdown(shutdownCtx)}
		}()
		for i := 0; i < 4; i++ {
			if result := <-shutdownErr; result.err != nil {
				log.Warn("shutdown component did not finish", "component", result.name, "error", result.err)
			}
		}
		joinBackground(shutdownCtx, &background, log)
	}
}

// joinBackground keeps the process alive until cancelable scheduling loops
// have returned. NightlyValidator.Shutdown only owns manual async passes, so
// its synchronous scheduled Run needs this join as well.
func joinBackground(ctx context.Context, background *sync.WaitGroup, log *slog.Logger) {
	done := make(chan struct{})
	go func() {
		background.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		log.Warn("shutdown deadline passed before background joins")
	}
}

// newHTTPServer binds the listener configuration. Request contexts derive
// from the run context: stopping the run cancels in-flight requests (an
// acquire with a deliberately slow judge included) so shutdown drains them
// instead of waiting out their full deadline with a detached context.
func newHTTPServer(cfg config.Config, handler http.Handler, baseCtx context.Context,
	timeout time.Duration) *http.Server {
	return &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: handler,
		BaseContext: func(net.Listener) context.Context {
			return baseCtx
		},
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout,
		IdleTimeout:       60 * time.Second,
	}
}

func buildVersion() string {
	if vcsRef != "" {
		return version + " (" + vcsRef + ")"
	}
	return version
}

func buildRootCAs(pem string) (*x509.CertPool, error) {
	if pem == "" {
		return nil, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(pem)) {
		return nil, fmt.Errorf("judge_ca_pem contains no usable certificates")
	}
	return pool, nil
}

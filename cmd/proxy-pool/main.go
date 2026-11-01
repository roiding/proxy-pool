package main

import (
	"context"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"proxypool/internal/api"
	"proxypool/internal/application"
	"proxypool/internal/config"
	"proxypool/internal/geoip"
	"proxypool/internal/maintenance"
	"proxypool/internal/storage/sqlite"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("proxy-pool v0.3.0-alpha")
		return
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	release, err := acquireSingletonLock(cfg.LockPath)
	if err != nil {
		log.Error("singleton lock failed", "error", err)
		os.Exit(1)
	}
	defer release()

	if err := os.MkdirAll(filepath.Dir(cfg.DatabasePath), 0o750); err != nil {
		log.Error("create data dir failed", "error", err)
		os.Exit(1)
	}
	db, err := sqlite.Open(cfg.DatabasePath, cfg.SQLiteCacheKiB)
	if err != nil {
		log.Error("open sqlite failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	store := sqlite.New(db)
	if err := store.Migrate(context.Background()); err != nil {
		log.Error("migrate failed", "error", err)
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

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewServer(cfg, log, acquire, ingest, leaseSvc, store, nightly, prewarm, keys).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go maintenance.Run(runCtx, store, cfg.SweepInterval(), cfg.AttemptRetention(),
		cfg.NightlyRetireFailures, log)
	if cfg.NightlyValidationTime != "" {
		go nightly.Run(runCtx, cfg.NightlyValidationTime, log)
	}

	httpErr := make(chan error, 1)
	go func() {
		log.Info("proxy-pool listening", "addr", cfg.ListenAddr, "db", cfg.DatabasePath)
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		stop()
	}
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

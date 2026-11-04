package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"proxypool/internal/config"
)

// A valid request that is still running when shutdown begins must observe
// the run context cancellation through its request context. Before the
// BaseContext link, a request in flight kept a detached context and shutdown
// waited out its full deadline.
func TestShutdownCancelsInFlightRequest(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	handlerStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		select {
		case <-r.Context().Done():
			close(requestCanceled)
		case <-time.After(30 * time.Second):
		}
	})
	srv := newHTTPServer(config.Config{ListenAddr: "127.0.0.1:0"}, mux, runCtx, time.Minute)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(listener) }()

	client := &http.Client{Timeout: 30 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+"/slow", nil)
		if err != nil {
			t.Error(err)
			return
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		// The transport error (context canceled) is the expected outcome;
		// only a full 30s hang would prove the shutdown was not linked.
	}()

	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	shutdownStart := time.Now()
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown did not drain the in-flight request: %v", err)
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("in-flight request never observed the shutdown")
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serve returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve goroutine never returned after shutdown")
	}
	if elapsed := time.Since(shutdownStart); elapsed > 5*time.Second {
		t.Fatalf("shutdown waited too long for the in-flight request: %s", elapsed)
	}
	<-done
}

func TestJoinBackgroundWaitsForScheduledLoop(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	var background sync.WaitGroup
	started := make(chan struct{})
	finished := make(chan struct{})
	background.Add(1)
	go func() {
		defer background.Done()
		defer close(finished)
		close(started)
		<-runCtx.Done()
		time.Sleep(20 * time.Millisecond)
	}()
	<-started

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stop()
	joinBackground(shutdownCtx, &background, slog.New(slog.NewTextHandler(io.Discard, nil)))
	select {
	case <-finished:
	default:
		t.Fatal("joinBackground returned before the canceled scheduled loop finished")
	}
}

func TestJoinBackgroundHonorsDeadline(t *testing.T) {
	var background sync.WaitGroup
	background.Add(1)
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		background.Wait()
	})
	go func() {
		defer background.Done()
		<-release
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	joinBackground(shutdownCtx, &background, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if shutdownCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("joinBackground did not return on the shared deadline: %v", shutdownCtx.Err())
	}
}

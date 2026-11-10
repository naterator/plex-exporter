package plex

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestOperationHealthPreservesLastSuccess(t *testing.T) {
	server := &Server{ID: "test-id", health: newHealthState()}

	if server.Ready() {
		t.Fatal("server should not be ready before a session poll")
	}

	server.recordOperation("sessions", nil)
	first := server.operationSnapshot("sessions")
	if !first.success || first.lastSuccess.IsZero() {
		t.Fatalf("successful operation snapshot = %+v", first)
	}

	server.recordOperation("sessions", errors.New("temporary Plex failure"))
	second := server.operationSnapshot("sessions")
	if second.success {
		t.Fatal("failed operation should clear the current success bit")
	}
	if second.lastSuccess != first.lastSuccess {
		t.Fatalf("last success changed after failure: got %v want %v", second.lastSuccess, first.lastSuccess)
	}
	if second.errorCount != 1 {
		t.Fatalf("error count = %d, want 1", second.errorCount)
	}
	if server.Ready() {
		t.Fatal("server should not be ready after the latest session poll failed")
	}

	server.recordOperation("sessions", nil)
	if !server.Ready() {
		t.Fatal("server should be ready after a fresh successful session poll")
	}
	if !server.Healthy() {
		t.Fatal("server should be healthy while it is open")
	}
}

func TestHealthMetricsHaveBoundedOperationLabels(t *testing.T) {
	server := &Server{ID: "test-id", health: newHealthState()}
	server.recordOperation("sessions", nil)
	server.recordOperation("not-a-real-operation", errors.New("ignored label"))
	server.setWebsocketConnected(true)

	registry := prometheus.NewRegistry()
	if err := registry.Register(server); err != nil {
		t.Fatalf("register server: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	found := make(map[string]bool)
	for _, family := range families {
		found[family.GetName()] = true
	}
	for _, name := range []string{
		"plex_operation_success",
		"plex_operation_last_success_timestamp_seconds",
		"plex_exporter_errors_total",
		"plex_websocket_connected",
	} {
		if !found[name] {
			t.Fatalf("health metric %q was not collected", name)
		}
	}

	for _, family := range families {
		if family.GetName() != "plex_operation_success" {
			continue
		}
		if got := len(family.GetMetric()); got != len(healthOperations) {
			t.Fatalf("operation success series = %d, want %d", got, len(healthOperations))
		}
	}
}

func TestUnsupportedOptionalOperationDoesNotCountAsExporterError(t *testing.T) {
	server := &Server{health: newHealthState()}
	server.recordOperation("resources", nil)
	server.recordUnsupportedOperation("resources")

	status := server.operationSnapshot("resources")
	if status.success {
		t.Fatal("unsupported operation should not report success")
	}
	if status.errorCount != 0 {
		t.Fatalf("unsupported operation error count = %d, want 0", status.errorCount)
	}
	if status.lastSuccess.IsZero() {
		t.Fatal("unsupported operation should preserve the last success timestamp")
	}
}

func TestOptionalStatistics404DoesNotIncrementExporterErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL, "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	server := &Server{Client: client, health: newHealthState()}
	if err := server.refreshResources(); err != nil {
		t.Fatalf("refreshResources: %v", err)
	}
	if err := server.refreshBandwidth(); err != nil {
		t.Fatalf("refreshBandwidth: %v", err)
	}

	for _, operation := range []string{"resources", "bandwidth"} {
		status := server.operationSnapshot(operation)
		if status.success {
			t.Errorf("%s should not report success for an unsupported endpoint", operation)
		}
		if status.errorCount != 0 {
			t.Errorf("%s error count = %d, want 0", operation, status.errorCount)
		}
		if !status.lastSuccess.IsZero() {
			t.Errorf("%s unexpectedly recorded a success timestamp", operation)
		}
	}
}

func TestNewServerDegradedStartupRecovers(t *testing.T) {
	oldDelay := startupFullRefreshDelaySeconds
	oldJitter := startupFullRefreshJitterMax
	startupFullRefreshDelaySeconds = 3600
	startupFullRefreshJitterMax = 0
	defer func() {
		startupFullRefreshDelaySeconds = oldDelay
		startupFullRefreshJitterMax = oldJitter
	}()

	var recovered atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/media/providers":
			if !recovered.Load() {
				http.Error(w, "Plex is still starting", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"MediaContainer":{"friendlyName":"Recovered Plex","machineIdentifier":"recovered-id","version":"1.0","MediaProvider":[]}}`))
		case "/":
			_, _ = w.Write([]byte(`{"MediaContainer":{"version":"1.0","platform":"test","platformVersion":"1"}}`))
		case "/status/sessions":
			_, _ = w.Write([]byte(`{"MediaContainer":{"Metadata":[]}}`))
		case "/statistics/resources", "/statistics/bandwidth":
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	server, err := NewServer(ts.URL, "token")
	if err != nil {
		t.Fatalf("NewServer should start degraded on HTTP 503: %v", err)
	}
	defer server.Close()
	if !server.Healthy() {
		t.Fatal("degraded server should remain healthy")
	}
	if server.Ready() {
		t.Fatal("degraded server should not be ready before Plex identity is recovered")
	}
	if _, id := server.identity(); id != "" {
		t.Fatalf("degraded server unexpectedly has Plex identity %q", id)
	}

	recovered.Store(true)
	server.refreshMu.Lock()
	server.nextLibraryMetadataRefresh = time.Time{}
	refreshErr := server.refreshLight()
	server.refreshMu.Unlock()
	if refreshErr != nil {
		t.Fatalf("recovered metadata refresh: %v", refreshErr)
	}
	if _, id := server.identity(); id != "recovered-id" {
		t.Fatalf("recovered Plex identity = %q, want recovered-id", id)
	}

	server.mtx.RLock()
	store := server.eventSessions
	server.mtx.RUnlock()
	if store == nil {
		t.Fatal("degraded startup did not initialize the session store")
	}
	pollErr := server.pollSessions(context.Background(), store, defaultSessionStaleTimeout)
	server.recordOperation("sessions", pollErr)
	if pollErr != nil {
		t.Fatalf("recovery session poll: %v", pollErr)
	}
	if !server.Ready() {
		t.Fatal("server should become ready after identity and a successful session poll")
	}

	server.Close()
	if server.Healthy() {
		t.Fatal("closed server should not remain healthy")
	}
	select {
	case <-server.fullRefreshDone:
	default:
		t.Fatal("deferred refresh worker did not acknowledge shutdown")
	}
	select {
	case <-store.done:
	default:
		t.Fatal("session store did not acknowledge shutdown")
	}
}

func TestStartupMetadataRetryBackoffIsBounded(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Plex is still starting", http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL, "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	server := &Server{
		Client:                         client,
		LibraryMetadataRefreshInterval: 15 * time.Minute,
		health:                         newHealthState(),
	}

	attemptAt := time.Now()
	wants := []time.Duration{
		30 * time.Second,
		60 * time.Second,
		120 * time.Second,
		240 * time.Second,
		5 * time.Minute,
		5 * time.Minute,
	}
	for attempt, want := range wants {
		server.refreshMu.Lock()
		err := server.refreshLibraryMetadataIfDue(attemptAt)
		next := server.nextLibraryMetadataRefresh
		server.refreshMu.Unlock()
		if err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", attempt+1)
		}
		if got := next.Sub(attemptAt); got != want {
			t.Fatalf("attempt %d retry delay = %v, want %v", attempt+1, got, want)
		}
		attemptAt = next
	}
}

func TestTransientPlexErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "service unavailable", err: &HTTPStatusError{StatusCode: 503}, want: true},
		{name: "rate limited", err: &HTTPStatusError{StatusCode: 429}, want: true},
		{name: "connection refused", err: &url.Error{Op: "Get", URL: "http://plex", Err: syscall.ECONNREFUSED}, want: true},
		{name: "DNS not found", err: &net.DNSError{Name: "plex", Err: "no such host", IsNotFound: true}, want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "malformed JSON", err: &json.SyntaxError{Offset: 1}, want: true},
		{name: "unauthorized", err: &HTTPStatusError{StatusCode: 401}, want: false},
		{name: "not found", err: ErrNotFound, want: false},
		{name: "unknown certificate authority", err: x509.UnknownAuthorityError{Cert: &x509.Certificate{}}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTransientPlexError(test.err); got != test.want {
				t.Fatalf("isTransientPlexError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestReadinessExpiresStaleSessionPoll(t *testing.T) {
	server := &Server{ID: "test-id", health: newHealthState()}
	server.recordOperation("sessions", nil)

	// Use the operation snapshot directly so this test does not need to wait
	// for the production two-minute readiness window.
	server.health.mtx.Lock()
	status := server.health.operations["sessions"]
	status.lastSuccess = time.Now().Add(-defaultReadinessFreshness - time.Second)
	server.health.operations["sessions"] = status
	server.health.mtx.Unlock()

	if server.Ready() {
		t.Fatal("server should not be ready when its session poll is stale")
	}
}

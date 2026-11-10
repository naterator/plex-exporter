package plex

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Operation names are deliberately fixed. They are part of the diagnostics
// metric contract, and a fixed set prevents a bad Plex response from creating
// unbounded label cardinality.
var healthOperations = [...]string{"server", "library", "resources", "bandwidth", "sessions"}

const (
	defaultReadinessFreshness = 2 * time.Minute
	startupMetadataRetry      = 30 * time.Second
	maxReadinessSeconds       = int64((1<<63 - 1) / int64(time.Second))
)

type operationHealth struct {
	success     bool
	lastSuccess time.Time
	errorCount  uint64
	lastFailure time.Time
}

type healthState struct {
	mtx                sync.RWMutex
	operations         map[string]operationHealth
	websocketConnected bool
}

func newHealthState() healthState {
	operations := make(map[string]operationHealth, len(healthOperations))
	for _, operation := range healthOperations {
		operations[operation] = operationHealth{}
	}
	return healthState{operations: operations}
}

func (s *Server) ensureHealthState() {
	s.health.mtx.Lock()
	defer s.health.mtx.Unlock()
	if s.health.operations != nil {
		return
	}
	s.health.operations = make(map[string]operationHealth, len(healthOperations))
	for _, operation := range healthOperations {
		s.health.operations[operation] = operationHealth{}
	}
}

func normalizeHealthOperation(operation string) string {
	for _, known := range healthOperations {
		if operation == known {
			return operation
		}
	}
	return ""
}

// recordOperation records the result of one bounded Plex operation. A failed
// request clears only the current success bit; its previous success timestamp
// remains available so callers can distinguish a transient failure from data
// that has never been observed.
func (s *Server) recordOperation(operation string, err error) {
	operation = normalizeHealthOperation(operation)
	if operation == "" {
		return
	}
	s.ensureHealthState()
	s.health.mtx.Lock()
	defer s.health.mtx.Unlock()
	status := s.health.operations[operation]
	if err == nil {
		status.success = true
		status.lastSuccess = time.Now()
	} else {
		status.success = false
		status.errorCount++
		status.lastFailure = time.Now()
	}
	s.health.operations[operation] = status
}

// recordUnsupportedOperation records that an optional Plex endpoint was
// reached but is not available on this server (for example, Plex Pass-only
// statistics endpoints returning 404). It is not an exporter error, so the
// operation remains unsuccessful while its last-success timestamp and error
// counter are preserved.
func (s *Server) recordUnsupportedOperation(operation string) {
	operation = normalizeHealthOperation(operation)
	if operation == "" {
		return
	}
	s.ensureHealthState()
	s.health.mtx.Lock()
	defer s.health.mtx.Unlock()
	status := s.health.operations[operation]
	status.success = false
	s.health.operations[operation] = status
}

func (s *Server) setWebsocketConnected(connected bool) {
	s.ensureHealthState()
	s.health.mtx.Lock()
	s.health.websocketConnected = connected
	s.health.mtx.Unlock()
}

func (s *Server) operationSnapshot(operation string) operationHealth {
	operation = normalizeHealthOperation(operation)
	if operation == "" {
		return operationHealth{}
	}
	s.ensureHealthState()
	s.health.mtx.RLock()
	status := s.health.operations[operation]
	s.health.mtx.RUnlock()
	return status
}

// Healthy reports whether the exporter process can serve diagnostics. Plex
// reachability belongs to Ready and the operation metrics; a process can stay
// alive while Plex is restarting or undergoing maintenance.
func (s *Server) Healthy() bool {
	if s == nil {
		return false
	}
	s.mtx.RLock()
	closed := s.closed
	s.mtx.RUnlock()
	return !closed
}

// Ready reports whether the exporter has identified Plex and has received a
// recent /status/sessions response. WebSocket availability is intentionally
// excluded because polling remains a valid source of playback state.
func (s *Server) Ready() bool {
	if s == nil || !s.Healthy() {
		return false
	}
	_, serverID := s.identity()
	if serverID == "" {
		return false
	}
	sessions := s.operationSnapshot("sessions")
	if !sessions.success || sessions.lastSuccess.IsZero() {
		return false
	}
	return time.Since(sessions.lastSuccess) <= readinessFreshness()
}

func readinessFreshness() time.Duration {
	if value := os.Getenv("SESSION_READINESS_TIMEOUT_SECONDS"); value != "" {
		if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds > 0 && seconds <= maxReadinessSeconds {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultReadinessFreshness
}

func isTransientPlexError(err error) bool {
	if err == nil {
		return false
	}
	// Preserve clear operator-actionable TLS failures. A certificate trust or
	// hostname problem will not be repaired by retrying, and returning it from
	// NewServer makes the configuration mistake visible immediately.
	var certVerificationErr *tls.CertificateVerificationError
	if errors.As(err, &certVerificationErr) {
		return false
	}
	var unknownAuthorityErr x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthorityErr) {
		return false
	}
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return false
	}
	var invalidCertificateErr x509.CertificateInvalidError
	if errors.As(err, &invalidCertificateErr) {
		return false
	}
	var rootsErr x509.SystemRootsError
	if errors.As(err, &rootsErr) {
		return false
	}

	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode == 408 || statusErr.StatusCode == 429 || statusErr.StatusCode >= 500
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return isTransientPlexError(urlErr.Err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		// A proxy or Plex restart can return a truncated/non-JSON body with a
		// successful HTTP status. Keep the exporter available and let the next
		// bounded retry establish whether the response has recovered.
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsNotFound || dnsErr.IsTimeout || dnsErr.IsTemporary
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

var (
	healthOperationSuccessDesc = prometheus.NewDesc(
		"plex_operation_success",
		"Whether the most recent operation succeeded (1) or failed (0)",
		[]string{"operation"}, nil)
	healthOperationLastSuccessDesc = prometheus.NewDesc(
		"plex_operation_last_success_timestamp_seconds",
		"Unix timestamp of the most recent successful operation; zero if none has succeeded",
		[]string{"operation"}, nil)
	healthExporterErrorsDesc = prometheus.NewDesc(
		"plex_exporter_errors_total",
		"Number of failed Plex operations by bounded operation type",
		[]string{"operation"}, nil)
	healthWebsocketConnectedDesc = prometheus.NewDesc(
		"plex_websocket_connected",
		"Whether the Plex notification WebSocket is currently connected",
		nil, nil)
)

func describeHealth(ch chan<- *prometheus.Desc) {
	ch <- healthOperationSuccessDesc
	ch <- healthOperationLastSuccessDesc
	ch <- healthExporterErrorsDesc
	ch <- healthWebsocketConnectedDesc
}

func (s *Server) collectHealth(ch chan<- prometheus.Metric) {
	s.ensureHealthState()
	s.health.mtx.RLock()
	statuses := make(map[string]operationHealth, len(healthOperations))
	for _, operation := range healthOperations {
		statuses[operation] = s.health.operations[operation]
	}
	websocketConnected := s.health.websocketConnected
	s.health.mtx.RUnlock()

	for _, operation := range healthOperations {
		status := statuses[operation]
		success := 0.0
		if status.success {
			success = 1
		}
		lastSuccess := float64(0)
		if !status.lastSuccess.IsZero() {
			lastSuccess = float64(status.lastSuccess.UnixNano()) / float64(time.Second)
		}
		ch <- prometheus.MustNewConstMetric(healthOperationSuccessDesc, prometheus.GaugeValue, success, operation)
		ch <- prometheus.MustNewConstMetric(healthOperationLastSuccessDesc, prometheus.GaugeValue, lastSuccess, operation)
		ch <- prometheus.MustNewConstMetric(healthExporterErrorsDesc, prometheus.CounterValue, float64(status.errorCount), operation)
	}
	connected := 0.0
	if websocketConnected {
		connected = 1
	}
	ch <- prometheus.MustNewConstMetric(healthWebsocketConnectedDesc, prometheus.GaugeValue, connected)
}

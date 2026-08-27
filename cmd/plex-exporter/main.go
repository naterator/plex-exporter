package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/naterator/plex-exporter/pkg/log"
	"github.com/naterator/plex-exporter/pkg/metrics"
	"github.com/naterator/plex-exporter/pkg/plex"
)

const (
	MetricsServerAddr         = ":9000"
	listenerRetryInitialDelay = time.Second
	listenerRetryMaxDelay     = 30 * time.Second
)

type plexEventListener interface {
	Listen(context.Context, log.Logger) error
}

var (
	// Build metadata is populated by the Makefile and container build. Defaults
	// keep direct `go build` invocations useful outside a Git checkout.
	Version  = "dev"
	Revision = "unknown"
	Branch   = "unknown"

	// logger is intentionally not initialized at package level to avoid timing issues.
	// In containerized environments, environment variables (especially from .env files)
	// may not be available during package initialization, but are guaranteed to be
	// available when main() executes. We initialize this in main() to ensure
	// LOG_LEVEL and other env vars are properly read for logger configuration.
	logger log.Logger
)

// createLogger creates the appropriate logger based on environment
func createLogger() log.Logger {
	// Use development logging when explicitly requested (better for local dev)
	if os.Getenv("LOG_FORMAT") == "console" || os.Getenv("ENVIRONMENT") == "development" {
		return log.NewDevelopmentLogger()
	}
	// Default to production JSON logger (better for containers and log aggregation)
	return log.NewProductionLogger()
}

// maskToken returns a masked representation of the token suitable for logs.
// It preserves the last 4 characters when possible and replaces the rest with '*'.
func maskToken(t string) string {
	if t == "" {
		return "(unset)"
	}
	if len(t) <= 4 {
		return "****"
	}
	// keep last 4 characters visible
	return strings.Repeat("*", len(t)-4) + t[len(t)-4:]
}

func main() {
	// CRITICAL: Initialize logger first to ensure environment variables are fully loaded.
	// This fixes a timing issue where package-level logger initialization occurs before
	// container environment variables (including .env files) are available, causing
	// LOG_LEVEL=debug and similar settings to be ignored.
	logger = createLogger()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	serverAddress := os.Getenv("PLEX_SERVER")
	if serverAddress == "" {
		logger.Error("PLEX_SERVER environment variable must be specified")
		cancel()
		os.Exit(1)
	}

	plexToken := os.Getenv("PLEX_TOKEN")
	if plexToken == "" {
		logger.Error("PLEX_TOKEN environment variable must be specified")
		cancel()
		os.Exit(1)
	}

	// Support TZ environment variable: if set, attempt to load and apply it
	tzEnv := os.Getenv("TZ")
	tzResolved := "(system)"
	if tzEnv != "" {
		if loc, err := time.LoadLocation(tzEnv); err != nil {
			logger.Warn("invalid TZ, using system default", zap.String("TZ", tzEnv), zap.Error(err))
			tzResolved = "invalid: " + tzEnv
		} else {
			time.Local = loc
			tzResolved = loc.String()
		}
	}

	// Log key environment variables at startup (mask sensitive values)
	libRefresh := os.Getenv("LIBRARY_REFRESH_INTERVAL")
	if libRefresh == "" {
		libRefresh = "15 (minutes, default; 0 = disable caching)"
	} else {
		libRefresh += " (minutes; 0 = disable caching)"
	}
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	skipTLS := os.Getenv("SKIP_TLS_VERIFICATION")
	if skipTLS == "" {
		skipTLS = "false"
	}

	logger.Info("starting exporter",
		zap.String("version", Version),
		zap.String("revision", Revision),
		zap.String("branch", Branch),
		zap.String("PLEX_SERVER", serverAddress),
		zap.String("PLEX_TOKEN", maskToken(plexToken)),
		zap.String("LIBRARY_REFRESH_INTERVAL", libRefresh),
		zap.String("LOG_LEVEL", logLevel),
		zap.String("SKIP_TLS_VERIFICATION", skipTLS),
		zap.String("TZ", tzResolved),
	)

	server, err := plex.NewServer(serverAddress, plexToken)
	if err != nil {
		logger.Error("cannot initialize connection to plex server", zap.Error(err))
		os.Exit(1)
	}

	metrics.Register(server)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsServer := http.Server{
		Addr:         MetricsServerAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	metricsErrCh := make(chan error, 1)
	go func() {
		logger.Info("starting metrics server on " + MetricsServerAddr)
		metricsErrCh <- metricsServer.ListenAndServe()
	}()

	plexErrCh := make(chan error, 1)
	go func() {
		plexErrCh <- listenWithRetry(ctx, server, logger, listenerRetryInitialDelay, listenerRetryMaxDelay)
	}()

	exitCode := 0
	select {
	case listenErr := <-metricsErrCh:
		if !errors.Is(listenErr, http.ErrServerClosed) {
			logger.Error("cannot start metrics server", zap.Error(listenErr))
			exitCode = 1
		}
	case listenErr := <-plexErrCh:
		if listenErr != nil && ctx.Err() == nil {
			logger.Error("cannot listen to plex server events", zap.Error(listenErr))
			exitCode = 1
		}
	case <-ctx.Done():
	}
	cancel()
	server.Close()

	logger.Debug("shutting down metrics server")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("cannot gracefully shutdown metrics server", zap.Error(err))
	}

	// ensure shutdown cancel is called before exiting so any resources are
	// released; avoid relying on deferred calls that won't run after os.Exit
	shutdownCancel()
	os.Exit(exitCode)
}

func listenWithRetry(
	ctx context.Context,
	server plexEventListener,
	logger log.Logger,
	initialDelay, maxDelay time.Duration,
) error {
	if initialDelay <= 0 {
		initialDelay = time.Second
	}
	if maxDelay < initialDelay {
		maxDelay = initialDelay
	}
	delay := initialDelay

	for {
		startedAt := time.Now()
		err := server.Listen(ctx, logger)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, plex.ErrServerClosed) || errors.Is(err, plex.ErrAlreadyListening) {
			return err
		}
		if err == nil {
			err = plex.ErrListenerClosed
		}

		// A connection that remained healthy for at least the maximum backoff
		// window should not inherit the penalty from much older failures.
		if time.Since(startedAt) >= maxDelay {
			delay = initialDelay
		}
		logger.Warn("Plex event listener disconnected; retrying",
			zap.Error(err),
			zap.Duration("retryIn", delay))

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil
		case <-timer.C:
		}

		if delay < maxDelay {
			if delay > maxDelay/2 {
				delay = maxDelay
			} else {
				delay *= 2
			}
		}
	}
}

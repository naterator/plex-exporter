package plex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	plexapi "github.com/timothystewart6/go-plex-client"
)

const (
	defaultSessionPollInterval = 5 * time.Second
	defaultSessionStaleTimeout = 5 * time.Minute
	defaultMaxTrackedSessions  = 256
)

var sessionPollingStarted sync.Map

// sessionPollContainer is deliberately a pointer in the response envelope.
// Plex returning {} or null is malformed and must not be interpreted as an
// empty, authoritative session list that stops every tracked session.
type sessionPollContainer struct {
	Metadata []json.RawMessage `json:"Metadata"`
}

type sessionPollResponse struct {
	MediaContainer *sessionPollContainer `json:"MediaContainer"`
}

type sessionPollSnapshot struct {
	metadata   []plexapi.Metadata
	transcodes []transcodeHardwareObservation
}

func (s *Server) startSessionPolling() {
	if s == nil || s.Client == nil {
		return
	}
	if _, loaded := sessionPollingStarted.LoadOrStore(s, struct{}{}); loaded {
		return
	}

	s.mtx.Lock()
	store := s.eventSessions
	if store == nil {
		store = NewSessions(context.Background(), s)
		s.eventSessions = store
	}
	if s.refreshStop == nil {
		s.refreshStop = make(chan struct{})
	}
	stop := s.refreshStop
	s.mtx.Unlock()

	interval := sessionPollIntervalFromEnv()
	staleAfter := sessionStaleTimeoutFromEnv()
	s.refreshWG.Add(1)
	go func() {
		defer s.refreshWG.Done()
		defer sessionPollingStarted.Delete(s)
		pollCtx, cancelPoll := context.WithCancel(context.Background())
		defer cancelPoll()
		stopWatcherDone := make(chan struct{})
		if stop != nil {
			go func() {
				select {
				case <-stop:
					cancelPoll()
				case <-stopWatcherDone:
				}
			}()
		}
		defer close(stopWatcherDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		poll := func() {
			err := s.pollSessions(pollCtx, store, staleAfter)
			recordServerOperation(s, "sessions", err)
		}
		select {
		case <-stop:
			return
		default:
		}
		poll()
		for {
			select {
			case <-ticker.C:
				poll()
			case <-stop:
				return
			}
		}
	}()
}

func (s *Server) pollSessions(ctx context.Context, store *sessions, staleAfter time.Duration) error {
	if s == nil || s.Client == nil || store == nil {
		return errors.New("session polling is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	observedAt := time.Now()
	snapshot, err := s.Client.getSessionsContextSnapshot(ctx)
	if err != nil {
		store.markStale(time.Now(), staleAfter)
		return fmt.Errorf("fetch current sessions: %w", err)
	}
	store.syncCurrent(snapshot.metadata, observedAt, snapshot.transcodes)
	return nil
}

func (c *Client) getSessionsContext(ctx context.Context) ([]plexapi.Metadata, error) {
	snapshot, err := c.getSessionsContextSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.metadata, nil
}

func (c *Client) getSessionsContextSnapshot(ctx context.Context) (sessionPollSnapshot, error) {
	if c == nil || c.URL == nil {
		return sessionPollSnapshot{}, errors.New("plex client is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := c.NewRequest(http.MethodGet, "/status/sessions")
	if err != nil {
		return sessionPollSnapshot{}, err
	}
	req = req.WithContext(ctx)
	var response sessionPollResponse
	// Use the client's configured transport, timeout, and TLS settings while
	// retaining the caller's cancellation context.
	if err := c.Do(req, &response); err != nil {
		return sessionPollSnapshot{}, err
	}
	if response.MediaContainer == nil {
		return sessionPollSnapshot{}, errors.New("plex session response is missing MediaContainer")
	}

	snapshot := sessionPollSnapshot{
		metadata:   make([]plexapi.Metadata, 0, len(response.MediaContainer.Metadata)),
		transcodes: make([]transcodeHardwareObservation, 0),
	}
	for _, raw := range response.MediaContainer.Metadata {
		parsed, err := parseSessionPollMetadata(raw)
		if err != nil {
			return sessionPollSnapshot{}, err
		}
		snapshot.metadata = append(snapshot.metadata, parsed.Metadata)
		snapshot.transcodes = append(snapshot.transcodes, parsed.Observations...)
	}
	return snapshot, nil
}

func sessionPollIntervalFromEnv() time.Duration {
	return durationSecondsFromEnv("SESSION_POLL_INTERVAL_SECONDS", defaultSessionPollInterval)
}

func sessionStaleTimeoutFromEnv() time.Duration {
	return durationSecondsFromEnv("SESSION_STALE_TIMEOUT_SECONDS", defaultSessionStaleTimeout)
}

func maxTrackedSessionsFromEnv() int {
	value := strings.TrimSpace(os.Getenv("SESSION_MAX_TRACKED_SESSIONS"))
	if value == "" {
		return defaultMaxTrackedSessions
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultMaxTrackedSessions
	}
	return parsed
}

func durationSecondsFromEnv(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 || seconds > int64((time.Duration(1<<63-1))/time.Second) {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

// The health implementation lives with Server, but these wrappers keep the
// session package nil-safe for lightweight tests and older embedders.
type operationRecorder interface {
	recordOperation(string, error)
}

type websocketStateRecorder interface {
	setWebsocketConnected(bool)
}

func recordServerOperation(server *Server, operation string, err error) {
	if server == nil {
		return
	}
	if recorder, ok := any(server).(operationRecorder); ok {
		recorder.recordOperation(operation, err)
	}
}

func recordWebsocketState(server *Server, connected bool) {
	if server == nil {
		return
	}
	if recorder, ok := any(server).(websocketStateRecorder); ok {
		recorder.setWebsocketConnected(connected)
	}
}

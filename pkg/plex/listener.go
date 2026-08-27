package plex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/timothystewart6/go-plex-client"
	"go.uber.org/zap"

	"github.com/naterator/plex-exporter/pkg/log"
)

var (
	ErrAlreadyListening = errors.New("already listening")
	ErrListenerClosed   = errors.New("plex websocket listener closed unexpectedly")
	ErrServerClosed     = errors.New("plex server is closed")
)

// Retry/backoff tuning variables. These are exported so callers (for
// example `cmd/plex-exporter/main.go` or tests) can adjust them
// at runtime if you observe races with your Plex server.
var (
	// SessionLookupMaxRetries controls how many times we retry fetching the
	// current sessions when a notification refers to a session not present
	// in the initial listing.
	SessionLookupMaxRetries = 3

	// SessionLookupBaseDelay is the base delay for session lookup retries; the
	// delay doubles each attempt (exponential backoff).
	SessionLookupBaseDelay = 100 * time.Millisecond

	// MetadataMaxRetries controls how many times we retry fetching metadata
	// for a rating key when it's not immediately available.
	MetadataMaxRetries = 3

	// MetadataBaseDelay is the base delay for metadata fetch retries.
	MetadataBaseDelay = 100 * time.Millisecond
)

// newPlex is a variable so tests can replace it with a fake constructor.
// Keep the wrapper with the old two-argument signature so tests that assign
// a func(base, token string) (*ttPlex.Plex, error) remain compatible.
var newPlex = func(base, token string) (*plex.Plex, error) { return plex.New(base, token) }

type plexClient interface {
	GetSessions() (plex.CurrentSessions, error)
	GetMetadata(string) (plex.MediaMetadata, error)
	GetTranscodeSessions() (plex.TranscodeSessionsResponse, error)
}

// Ensure the concrete plex.Plex from the client dependency implements the
// minimal interface we require. This provides a helpful compile-time check
// and documents the intent: production code uses *plex.Plex while tests may
// provide fakes that implement the same methods.
var _ plexClient = (*plex.Plex)(nil)

type plexListener struct {
	server         *Server
	conn           plexClient
	activeSessions *sessions
	log            log.Logger
}

func (s *Server) Listen(ctx context.Context, logger log.Logger) error {
	if ctx == nil {
		return errors.New("listener context is nil")
	}
	if logger == nil {
		logger = packageLogger()
	}
	s.mtx.Lock()
	if s.closed {
		s.mtx.Unlock()
		return ErrServerClosed
	}
	if s.listener != nil {
		s.mtx.Unlock()
		return ErrAlreadyListening
	}

	conn, err := newPlex(s.URL.String(), s.Token)
	if err != nil {
		s.mtx.Unlock()
		return fmt.Errorf("failed to connect to %s: %w", s.URL.String(), err)
	}
	// Keep session and metadata calls made by the websocket client on the same
	// timeout configured for the exporter's other Plex API requests.
	if s.Client != nil && s.Client.httpClient.Timeout > 0 {
		conn.HTTPClient.Timeout = s.Client.httpClient.Timeout
	}
	eventSessions := s.eventSessions
	if eventSessions == nil {
		eventSessions = NewSessions(ctx, s)
		s.eventSessions = eventSessions
	}
	listenCtx, cancelListener := context.WithCancel(ctx)

	listener := &plexListener{
		server:         s,
		conn:           conn,
		activeSessions: eventSessions,
		log:            logger,
	}
	s.listener = listener
	s.listenerCancel = cancelListener

	s.mtx.Unlock()
	defer func() {
		cancelListener()
		s.mtx.Lock()
		if s.listener == listener {
			s.listener = nil
			s.listenerCancel = nil
		}
		s.mtx.Unlock()
	}()

	// forward context completion to timothystewart6/go-plex-client
	ctrlC := make(chan os.Signal, 1)
	bridgeDone := make(chan struct{})
	defer close(bridgeDone)
	go func() {
		select {
		case <-listenCtx.Done():
			close(ctrlC)
		case <-bridgeDone:
		}
	}()

	doneChan := make(chan error, 1)
	var closeOnce sync.Once
	var errSent bool
	var mutex sync.Mutex

	onError := func(err error) {
		mutex.Lock()
		defer mutex.Unlock()

		// If we've already sent an error or closed, ignore subsequent calls
		if errSent {
			return
		}
		if listenCtx.Err() != nil {
			errSent = true
			closeOnce.Do(func() { close(doneChan) })
			return
		}

		var closeErr *websocket.CloseError
		if errors.As(err, &closeErr) {
			if closeErr.Code == websocket.CloseNormalClosure {
				if ctx.Err() == nil {
					doneChan <- ErrListenerClosed
				}
				errSent = true
				closeOnce.Do(func() { close(doneChan) })
				return
			}
		}
		logger.Error("error in websocket processing", zap.Error(err))

		// Try to send error, but don't panic if channel is closed
		select {
		case doneChan <- err:
			errSent = true
		default:
			// Channel already closed or full, ignore
		}
		closeOnce.Do(func() { close(doneChan) })
	}

	events := plex.NewNotificationEvents()
	events.OnPlaying(listener.onPlayingHandler)
	events.OnTimeline(listener.onTimelineHandler)
	// register transcode update handler to record transcode type
	events.OnTranscodeUpdate(listener.onTranscodeUpdateHandler)

	// The dependency manages one websocket connection. Listen returns when it
	// closes so the process-level retry loop can reconnect with backoff.
	conn.SubscribeToNotifications(events, ctrlC, onError)
	select { // SubscribeToNotifications doesn't return error directly, so we read one from channel without blocking.
	case err, ok := <-doneChan:
		return listenerResult(listenCtx, err, ok)
	default:
		// noop
	}
	if err := listener.syncCurrentSessions(); err != nil {
		logger.Warn("could not seed active sessions after websocket connection", zap.Error(err))
	}

	serverName, serverID := s.identity()
	logger.Info("Successfully connected", zap.String("machineID", serverID), zap.String("server", serverName))

	listenerErr, channelOpen := <-doneChan
	return listenerResult(listenCtx, listenerErr, channelOpen)
}

func (l *plexListener) syncCurrentSessions() error {
	if l.conn == nil || l.activeSessions == nil {
		return errors.New("plex listener is not initialized")
	}
	observedAt := time.Now()
	current, err := l.conn.GetSessions()
	if err != nil {
		return fmt.Errorf("fetch current sessions: %w", err)
	}
	l.activeSessions.syncCurrent(current.MediaContainer.Metadata, observedAt)
	return nil
}

func listenerResult(ctx context.Context, err error, channelOpen bool) error {
	if ctx.Err() != nil {
		return nil
	}
	if !channelOpen || err == nil {
		return ErrListenerClosed
	}
	return err
}

func getSessionByID(sessions plex.CurrentSessions, sessionID string) *plex.Metadata {
	for _, session := range sessions.MediaContainer.Metadata {
		if sessionID == session.SessionKey {
			return &session
		}
	}
	return nil
}

func (l *plexListener) onPlayingHandler(c plex.NotificationContainer) {
	err := l.onPlaying(c)
	if err != nil {
		// Extract simple fields for structured logging so the logfmt encoder
		// doesn't attempt to serialize complex nested structs.
		var sessionKeys []string
		var ratingKeys []string
		var states []string
		for _, n := range c.PlaySessionStateNotification {
			sessionKeys = append(sessionKeys, n.SessionKey)
			ratingKeys = append(ratingKeys, n.RatingKey)
			states = append(states, n.State)
		}

		l.log.Error("error handling OnPlaying event",
			zap.String("sessionKeys", strings.Join(sessionKeys, ",")),
			zap.String("ratingKeys", strings.Join(ratingKeys, ",")),
			zap.String("states", strings.Join(states, ",")),
			zap.Error(err),
		)
	}
}

// onTimelineHandler logs concise timeline entries at debug level. Plex can
// emit these frequently during playback, so info-level logging would create a
// sustained stream of otherwise unactionable output.
func (l *plexListener) onTimelineHandler(c plex.NotificationContainer) {
	// Log a summary of timeline entries without passing complex structs to the
	// log encoder. Include identifier, itemID, title, sectionID, and state.
	if len(c.TimelineEntry) == 0 {
		return
	}

	var summaries []string
	for _, te := range c.TimelineEntry {
		summaries = append(summaries, fmt.Sprintf("id=%s item=%d title=%s section=%d state=%d", te.Identifier, te.ItemID, te.Title, te.SectionID, te.State))
	}

	l.log.Debug("timeline entries",
		zap.Int("count", len(c.TimelineEntry)),
		zap.String("entries", strings.Join(summaries, " | ")))
}

// onTranscodeUpdateHandler receives TranscodeSession updates and logs a concise
// transcode type (audio/video/both). We avoid sending nested structs to the
// logger and only emit primitive fields.
//
// Note: example keys and fixture data shown in tests and examples are
// randomized/sanitized values that resemble real Plex identifiers but do not
// contain customer-identifying information. Tests and documentation intentionally
// use synthetic keys that are similar in shape to real data to exercise
// matching logic without exposing production identifiers.
// nolint:gocyclo // complexity tolerated for now; can be refactored later
func (l *plexListener) onTranscodeUpdateHandler(c plex.NotificationContainer) {
	if len(c.TranscodeSession) == 0 {
		return
	}

	for _, ts := range c.TranscodeSession {
		ts.Key = strings.TrimSpace(ts.Key)
		if ts.Key == "" {
			l.log.Warn("ignoring transcode update without a session key")
			continue
		}
		kind := transcodeKind(ts)

		subtitle, hasSubtitleAction := subtitleAction(ts)

		l.log.Debug("transcode session update",
			zap.String("sessionKey", ts.Key),
			zap.String("type", kind),
			zap.String("subtitle", subtitle))

		if l.activeSessions == nil {
			continue
		}

		matched := l.activeSessions.TrySetTranscodeType(ts.Key, kind)
		subMatched := !hasSubtitleAction
		if hasSubtitleAction {
			subMatched = l.activeSessions.TrySetSubtitleAction(ts.Key, subtitle)
		}
		if matched && subMatched {
			continue
		}

		// refresh sessions and retry
		if l.conn != nil {
			if sessions, err := l.conn.GetSessions(); err == nil {
				for i := range sessions.MediaContainer.Metadata {
					sess := &sessions.MediaContainer.Metadata[i]
					l.activeSessions.Update(sess.SessionKey, statePlaying, sess, sess)
					state := sessionState(strings.ToLower(strings.TrimSpace(sess.Player.State)))
					if state == statePaused || state == stateBuffering {
						l.activeSessions.Update(sess.SessionKey, state, nil, nil)
					}
				}
				matched = l.activeSessions.TrySetTranscodeType(ts.Key, kind)
				if hasSubtitleAction {
					subMatched = l.activeSessions.TrySetSubtitleAction(ts.Key, subtitle)
				}
				if matched && subMatched {
					continue
				}
			}
		}

		// diagnostics and transcode sessions API fallback
		if l.conn != nil {
			// Build known sessions list from our in-memory store so logs include
			// both the map key and the inner SessionKey used in the metadata.
			if l.activeSessions != nil {
				// Build a concise known-sessions summary for diagnostics. Note that
				// tests and README examples intentionally populate session keys,
				// names, and user titles with randomized/sanitized values
				// (they mimic real-world shapes like numeric session keys or
				// UUID-like transcode IDs) so logs and test output don't leak
				// identifying information while keeping matching behavior
				// realistic.
				var ssum []string
				l.activeSessions.mtx.Lock()
				// Determine a stable ordering of sessions for deterministic logs
				var keys []string
				for k := range l.activeSessions.sessions {
					keys = append(keys, k)
				}
				l.activeSessions.mtx.Unlock()

				// sort keys outside the activeSessions lock to minimize lock hold time
				if len(keys) > 1 {
					sort.Strings(keys)
				}

				l.activeSessions.mtx.Lock()
				for _, k := range keys {
					ss := l.activeSessions.sessions[k]
					// Skip orphaned sessions (those with no session key and no meaningful data) from diagnostic output
					if ss.session.SessionKey == "" && len(ss.session.Media) == 0 {
						continue
					}
					user := ss.session.User.Title
					player := ss.session.Player.Product
					inner := ss.session.SessionKey
					ssum = append(ssum, fmt.Sprintf("mapKey=%s sessionKey=%s user=%s player=%s", k, inner, user, player))
				}
				l.activeSessions.mtx.Unlock()

				// Only log warning if there are actual active sessions to show
				if len(ssum) > 0 {
					l.log.Warn("transcode update did not match any active session",
						zap.String("tsKey", ts.Key),
						zap.String("detectedKind", kind),
						zap.String("knownSessions", strings.Join(ssum, "; ")))
				} else {
					l.log.Debug("transcode update for session not currently active",
						zap.String("tsKey", ts.Key),
						zap.String("detectedKind", kind))
				}
			} else {
				l.log.Warn("transcode update did not match and activeSessions is nil",
					zap.String("tsKey", ts.Key),
					zap.String("detectedKind", kind))
			}

			if tcs, err := l.conn.GetTranscodeSessions(); err == nil {
				var tsum []string
				for _, t := range tcs.Children {
					tsum = append(tsum, fmt.Sprintf("k=%s video=%s audio=%s decision=%s", t.Key, t.VideoCodec, t.AudioCodec, t.VideoDecision))
				}
				l.log.Warn("active transcode sessions",
					zap.String("list", strings.Join(tsum, "; ")))

				for _, t := range tcs.Children {
					t.Key = strings.TrimSpace(t.Key)
					if t.Key == "" {
						continue
					}
					match := false
					if sameTranscodeID(t.Key, ts.Key) || strings.Contains(ts.Key, t.Key) || strings.Contains(t.Key, ts.Key) {
						match = true
					}
					if !match {
						continue
					}

					subFromAPI, hasSubtitleAction := subtitleActionFromFields(t.SubtitleDecision, t.Container)
					if !hasSubtitleAction {
						subFromAPI = ""
					}

					apiKind := ""
					vDecisionAPI := strings.ToLower(strings.TrimSpace(t.VideoDecision))
					aDecisionAPI := strings.ToLower(strings.TrimSpace(t.AudioDecision))
					if vDecisionAPI == "transcode" {
						if aDecisionAPI == "transcode" {
							apiKind = "both"
						} else {
							apiKind = "video"
						}
					} else if aDecisionAPI == "transcode" {
						apiKind = "audio"
					}

					if subFromAPI != "" {
						l.log.Info("derived subtitle action from transcode sessions API",
							zap.String("tsKey", t.Key),
							zap.String("subtitle", subFromAPI))
					}

					// apply to sessions (prefer TrySet, but create if necessary)
					applied := false
					if subFromAPI != "" {
						if l.activeSessions.TrySetSubtitleAction(ts.Key, subFromAPI) {
							subMatched = true
							applied = true
						} else {
							l.activeSessions.SetSubtitleAction(ts.Key, subFromAPI)
							subMatched = true
							applied = true
						}
					}
					if apiKind != "" {
						if l.activeSessions.TrySetTranscodeType(ts.Key, apiKind) {
							matched = true
							applied = true
						} else {
							l.activeSessions.SetTranscodeType(ts.Key, apiKind)
							matched = true
							applied = true
						}
					}
					if applied {
						break
					}
				}
			}
		}

		if !matched {
			l.activeSessions.SetTranscodeType(ts.Key, kind)
			if !hasSubtitleAction {
				l.activeSessions.SetSubtitleAction(ts.Key, "none")
			}
		}
		if hasSubtitleAction && !subMatched {
			l.activeSessions.SetSubtitleAction(ts.Key, subtitle)
		}
	}
}

func (l *plexListener) onPlaying(c plex.NotificationContainer) error {
	if len(c.PlaySessionStateNotification) == 0 {
		return nil
	}
	if l.activeSessions == nil {
		return errors.New("active session store is not initialized")
	}

	// Stop notifications must not depend on /status/sessions. Plex often
	// removes the session before emitting the final event, and a transient API
	// failure here would otherwise leave the exporter counting it as playing.
	activeNotifications := make([]plex.PlaySessionStateNotification, 0, len(c.PlaySessionStateNotification))
	for _, notification := range c.PlaySessionStateNotification {
		sessionKey := strings.TrimSpace(notification.SessionKey)
		if sessionKey == "" {
			l.log.Warn("ignoring playback notification without a session key",
				zap.String("state", notification.State),
				zap.String("ratingKey", notification.RatingKey))
			continue
		}
		notification.SessionKey = sessionKey
		notification.State = strings.ToLower(strings.TrimSpace(notification.State))
		if sessionState(notification.State) == stateStopped {
			l.activeSessions.Update(sessionKey, stateStopped, nil, nil)
			continue
		}
		activeNotifications = append(activeNotifications, notification)
	}
	if len(activeNotifications) == 0 {
		return nil
	}
	if l.conn == nil {
		return errors.New("plex client is not initialized")
	}

	sessions, err := l.conn.GetSessions()
	if err != nil {
		return fmt.Errorf("error fetching sessions: %w", err)
	}

	for i, n := range activeNotifications {

		// Try to resolve the session with exponential backoff. Notifications
		// can arrive slightly before the session list is updated; retry a few
		// times before giving up.
		session := getSessionByID(sessions, n.SessionKey)
		if session == nil {
			for attempt := 1; attempt <= SessionLookupMaxRetries && session == nil; attempt++ {
				// shift a duration to compute exponential backoff without int->uint conversion
				backoff := SessionLookupBaseDelay * (time.Duration(1) << (attempt - 1))
				time.Sleep(backoff)
				if s2, err := l.conn.GetSessions(); err == nil {
					session = getSessionByID(s2, n.SessionKey)
				} else {
					l.log.Debug("retrying GetSessions failed",
						zap.Int("attempt", attempt),
						zap.Error(err))
				}
			}
		}

		if session == nil {
			l.log.Warn("session not found for notification after retries, skipping",
				zap.String("SessionKey", n.SessionKey),
				zap.String("RatingKey", n.RatingKey),
				zap.String("state", n.State))
			continue
		}

		// Fetch metadata with retries in case the server hasn't populated it yet.
		var metadata plex.MediaMetadata
		var metaErr error
		for attempt := 1; attempt <= MetadataMaxRetries; attempt++ {
			metadata, metaErr = l.conn.GetMetadata(n.RatingKey)
			if metaErr == nil && len(metadata.MediaContainer.Metadata) > 0 {
				break
			}
			// if this was the last attempt break and we'll log below
			if attempt < MetadataMaxRetries {
				backoff := MetadataBaseDelay * (time.Duration(1) << (attempt - 1))
				time.Sleep(backoff)
			}
		}

		if metaErr != nil {
			l.log.Error("error fetching metadata for notification after retries, skipping",
				zap.String("RatingKey", n.RatingKey),
				zap.Error(metaErr))
			continue
		}

		if len(metadata.MediaContainer.Metadata) == 0 {
			l.log.Warn("metadata response empty after retries, skipping",
				zap.String("RatingKey", n.RatingKey))
			continue
		}

		// Log only the first notification in the batch to keep logs concise
		// and structured. Always update sessions for every notification.
		if i == 0 {
			batchCount := len(activeNotifications)
			l.log.Info("Received PlaySessionStateNotification",
				zap.String("SessionKey", n.SessionKey),
				zap.String("userName", session.User.Title),
				zap.String("userID", session.User.ID),
				zap.String("state", n.State),
				zap.String("mediaTitle", metadata.MediaContainer.Metadata[0].Title),
				zap.String("mediaID", metadata.MediaContainer.Metadata[0].RatingKey),
				zap.Duration("timestamp", time.Duration(time.Millisecond)*time.Duration(n.ViewOffset)),
				zap.Int("batchCount", batchCount),
			)
		}

		l.activeSessions.Update(n.SessionKey, sessionState(n.State), session, &metadata.MediaContainer.Metadata[0])
		// Update the transcode mapping after the session exists so an earlier
		// out-of-order transcode event can transfer its cached attributes.
		l.activeSessions.SetSessionTranscodeMapping(n.SessionKey, n.TranscodeSession)
	}

	return nil
}

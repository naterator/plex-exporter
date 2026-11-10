package plex

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timothystewart6/go-plex-client"

	"github.com/naterator/plex-exporter/pkg/metrics"
)

type sessionState string

const (
	statePlaying   sessionState = "playing"
	stateStopped   sessionState = "stopped"
	statePaused    sessionState = "paused"
	stateBuffering sessionState = "buffering"
	stateUnknown   sessionState = "unknown"

	mediaTypeEpisode = "episode"

	// How long metrics for sessions are kept after the last update.
	// This is used to prune prometheus metrics and keep cardinality
	// down.
	sessionTimeout = time.Minute
)

var trackedSessionStates = [...]sessionState{
	statePlaying,
	statePaused,
	stateBuffering,
	stateStopped,
	stateUnknown,
}

type session struct {
	session    plex.Metadata
	media      plex.Metadata
	state      sessionState
	lastUpdate time.Time
	// confirmedAt records the last successful observation from either the
	// session endpoint or websocket. It is intentionally separate from
	// lastUpdate so a failed poll can age a session into unknown without
	// advancing playback time.
	confirmedAt    time.Time
	playStarted    time.Time
	prevPlayedTime time.Duration
	// Persisted library labels resolved when the media was first seen.
	// Keeping these prevents metric label churn if server library lookup
	// temporarily fails later.
	resolvedLibraryName string
	resolvedLibraryID   string
	resolvedLibraryType string
	// persisted transcode type for this session, set when transcode events
	// are received via the websocket. Helps keep metrics stable and avoid
	// trying to infer transcode type at collection time.
	transcodeType string
	// persisted subtitle action for this session (burn|copy|none)
	subtitleAction string
	// Hardware transcode evidence is populated only when Plex explicitly
	// reports separate decoder/encoder outcomes. A request flag alone is not
	// enough to claim hardware was actually used.
	transcodeHWRequested bool
	transcodeHWDecode    string
	transcodeHWEncode    string
	transcodeHWKnown     bool
}

type sessions struct {
	mtx      sync.Mutex
	sessions map[string]session
	// sessionTranscodeMap maps sessionKey to current transcodeSession ID
	// This is populated from PlaySessionStateNotification.transcodeSession
	sessionTranscodeMap            map[string]string
	server                         *Server
	totalEstimatedTransmittedKBits float64
	maxTrackedSessions             int
	stop                           chan struct{}
	done                           chan struct{}
	closeOnce                      sync.Once
}

func NewSessions(ctx context.Context, server *Server) *sessions {
	if ctx == nil {
		ctx = context.Background()
	}
	s := &sessions{
		sessions:            map[string]session{},
		sessionTranscodeMap: map[string]string{},
		server:              server,
		maxTrackedSessions:  maxTrackedSessionsFromEnv(),
		stop:                make(chan struct{}),
		done:                make(chan struct{}),
	}

	ticker := time.NewTicker(time.Minute)
	go func() {
		defer ticker.Stop()
		defer close(s.done)
		for {
			select {
			case <-ticker.C:
				s.pruneOldSessions()
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			}
		}
	}()

	return s
}

func (s *sessions) Close() {
	if s == nil || s.stop == nil {
		return
	}
	s.closeOnce.Do(func() { close(s.stop) })
	if s.done != nil {
		<-s.done
	}
}

func (s *sessions) pruneOldSessions() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for k, v := range s.sessions {
		// Remove terminal and stale sessions after the retention interval. The
		// final unknown scrape remains visible long enough for dashboards, but
		// cannot permanently consume one of the bounded tracker slots.
		if (v.state == stateStopped || v.state == stateUnknown) && time.Since(v.lastUpdate) > sessionTimeout {
			delete(s.sessions, k)
			delete(s.sessionTranscodeMap, k)
			continue
		}

		// Remove orphaned transcode sessions (sessions with no session key and no meaningful data) more aggressively
		// These are created when transcode notifications arrive for sessions that don't exist
		// Remove them if they're older than 10 seconds to allow for brief race conditions
		if v.session.SessionKey == "" && len(v.session.Media) == 0 &&
			(v.lastUpdate.IsZero() || time.Since(v.lastUpdate) > 10*time.Second) {
			delete(s.sessions, k)
			continue
		}
	}
}

// SetSessionTranscodeMapping establishes the mapping between a sessionKey and its transcodeSession ID.
// This is called when PlaySessionStateNotification events are received.
func (s *sessions) SetSessionTranscodeMapping(sessionKey, transcodeSessionID string) {
	sessionKey = strings.TrimSpace(sessionKey)
	transcodeSessionID = strings.TrimSpace(transcodeSessionID)
	if sessionKey == "" {
		return
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.sessionTranscodeMap == nil {
		s.sessionTranscodeMap = make(map[string]string)
	}
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}

	if transcodeSessionID != "" {
		if !s.canTrackMappingLocked(sessionKey) {
			return
		}
		s.sessionTranscodeMap[sessionKey] = transcodeSessionID
		// A transcode update can arrive before the play-session notification.
		// Transfer attributes from the temporary orphan once the real session
		// exists, then remove the orphan so it cannot linger until pruning.
		target, targetExists := s.sessions[sessionKey]
		if targetExists {
			for key, candidate := range s.sessions {
				if key == sessionKey || candidate.session.SessionKey != "" || len(candidate.session.Media) != 0 {
					continue
				}
				if !sameTranscodeID(key, transcodeSessionID) {
					continue
				}
				if candidate.transcodeType != "" {
					target.transcodeType = candidate.transcodeType
				}
				if candidate.subtitleAction != "" {
					target.subtitleAction = candidate.subtitleAction
				}
				if candidate.transcodeHWKnown {
					target.transcodeHWRequested = candidate.transcodeHWRequested
					target.transcodeHWDecode = candidate.transcodeHWDecode
					target.transcodeHWEncode = candidate.transcodeHWEncode
					target.transcodeHWKnown = true
				}
				delete(s.sessions, key)
			}
			s.sessions[sessionKey] = target
		}
	} else {
		// If transcodeSessionID is empty, remove the mapping (direct play)
		delete(s.sessionTranscodeMap, sessionKey)
		if current, ok := s.sessions[sessionKey]; ok {
			current.transcodeType = ""
			current.subtitleAction = ""
			current.transcodeHWRequested = false
			current.transcodeHWDecode = ""
			current.transcodeHWEncode = ""
			current.transcodeHWKnown = false
			s.sessions[sessionKey] = current
		}
	}
}

// SetTranscodeType sets the transcode type for a given session id.
// It's safe to call concurrently with other session updates.
func (s *sessions) SetTranscodeType(sessionID, ttype string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || (strings.HasPrefix(sessionID, "/transcode/sessions/") && extractTranscodeSessionID(sessionID) == "") {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}

	// Fast-path: exact map key match
	if ss, ok := s.sessions[sessionID]; ok {
		ss.transcodeType = ttype
		s.sessions[sessionID] = ss
		return
	}

	// Try to find a session whose inner Metadata.SessionKey equals the
	// provided sessionID (some websocket notifications use slightly
	// different keys). If found, set the transcode type on that session.
	for k, ss := range s.sessions {
		if ss.session.SessionKey == sessionID {
			ss.transcodeType = ttype
			s.sessions[k] = ss
			return
		}
	}

	// Heuristic: attempt substring matches in either direction. This
	// handles cases where the notification key contains extra prefixes
	// or suffixes compared to the session key.
	for k, ss := range s.sessions {
		if ss.session.SessionKey != "" && (strings.Contains(sessionID, ss.session.SessionKey) || strings.Contains(ss.session.SessionKey, sessionID)) {
			ss.transcodeType = ttype
			s.sessions[k] = ss
			return
		}
	}

	// Fallback: create/set an entry under the provided key so tests that
	// expect this behavior continue to pass.
	if !s.canTrackLocked(sessionID) {
		return
	}
	ss := s.sessions[sessionID]
	ss.transcodeType = ttype
	s.sessions[sessionID] = ss
}

// SetTranscodeHardware records the only hardware signal exposed by the
// websocket model. It intentionally does not create an orphan session: a
// hardware update without a matching playback session is not useful to
// dashboards and must not bypass the session cardinality bound.
func (s *sessions) SetTranscodeHardware(sessionID string, requested bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.sessions == nil {
		return
	}
	if key, ok := s.matchSessionKeyLocked(sessionID); ok {
		ss := s.sessions[key]
		ss.transcodeHWRequested = requested
		s.sessions[key] = ss
	}
}

func (s *sessions) SetTranscodeHardwareEvidence(sessionID, decode, encode string) {
	sessionID = strings.TrimSpace(sessionID)
	decode = strings.ToLower(strings.TrimSpace(decode))
	encode = strings.ToLower(strings.TrimSpace(encode))
	if sessionID == "" || decode == "" || encode == "" ||
		!validTranscodeHardwareOutcome(decode) || !validTranscodeHardwareOutcome(encode) {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if key, ok := s.matchSessionKeyLocked(sessionID); ok {
		ss := s.sessions[key]
		ss.transcodeHWDecode = decode
		ss.transcodeHWEncode = encode
		ss.transcodeHWKnown = true
		s.sessions[key] = ss
	}
}

func validTranscodeHardwareOutcome(value string) bool {
	return value == "hardware" || value == "software" || value == "unknown"
}

// SetSubtitleAction sets the subtitle action for a given session id.
func (s *sessions) SetSubtitleAction(sessionID, action string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || (strings.HasPrefix(sessionID, "/transcode/sessions/") && extractTranscodeSessionID(sessionID) == "") {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}

	if ss, ok := s.sessions[sessionID]; ok {
		ss.subtitleAction = action
		s.sessions[sessionID] = ss
		return
	}

	for k, ss := range s.sessions {
		if ss.session.SessionKey == sessionID {
			ss.subtitleAction = action
			s.sessions[k] = ss
			return
		}
	}

	for k, ss := range s.sessions {
		if ss.session.SessionKey != "" && (strings.Contains(sessionID, ss.session.SessionKey) || strings.Contains(ss.session.SessionKey, sessionID)) {
			ss.subtitleAction = action
			s.sessions[k] = ss
			return
		}
	}

	if !s.canTrackLocked(sessionID) {
		return
	}
	ss := s.sessions[sessionID]
	ss.subtitleAction = action
	s.sessions[sessionID] = ss
}

// TrySetSubtitleAction behaves like SetSubtitleAction but returns true if a
// session was found and updated.
func (s *sessions) TrySetSubtitleAction(sessionID, action string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// If sessionID is a full transcode session path, extract just the session ID part
	originalSessionID := sessionID
	if strings.HasPrefix(sessionID, "/transcode/sessions/") {
		sessionID = extractTranscodeSessionID(sessionID)
		if sessionID == "" {
			return false
		}
	}

	if ss, ok := s.sessions[sessionID]; ok {
		ss.subtitleAction = action
		s.sessions[sessionID] = ss
		return true
	}

	for k, ss := range s.sessions {
		if ss.session.SessionKey == sessionID {
			ss.subtitleAction = action
			s.sessions[k] = ss
			return true
		}
	}

	// Check if any session has this transcode session ID mapped via PlaySessionStateNotification
	for sessionKey, mappedTranscodeID := range s.sessionTranscodeMap {
		if sameTranscodeID(mappedTranscodeID, originalSessionID) {
			if ss, ok := s.sessions[sessionKey]; ok {
				ss.subtitleAction = action
				s.sessions[sessionKey] = ss
				return true
			}
		}
	}

	for k, ss := range s.sessions {
		if ss.session.SessionKey != "" && (strings.Contains(sessionID, ss.session.SessionKey) || strings.Contains(ss.session.SessionKey, sessionID)) {
			ss.subtitleAction = action
			s.sessions[k] = ss
			return true
		}
	}

	// fallback: no match
	return false
}

// TrySetTranscodeType behaves like SetTranscodeType but returns true if a
// session was found and updated. This allows callers to detect no-match and
// emit additional debug information without duplicating matching logic.
// nolint:gocyclo // complexity acknowledged; refactor later if needed
func (s *sessions) TrySetTranscodeType(sessionID, ttype string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// If sessionID is a full transcode session path, extract just the session ID part
	originalSessionID := sessionID
	if strings.HasPrefix(sessionID, "/transcode/sessions/") {
		sessionID = extractTranscodeSessionID(sessionID)
		if sessionID == "" {
			return false
		}
	}

	// Helper: return last path segment for loose/suffix matching
	lastSeg := func(p string) string {
		if p == "" {
			return ""
		}
		if strings.Contains(p, "/") {
			parts := strings.Split(p, "/")
			return parts[len(parts)-1]
		}
		return p
	}

	if ss, ok := s.sessions[sessionID]; ok {
		ss.transcodeType = ttype
		s.sessions[sessionID] = ss
		return true
	}

	// Exact match against stored map key
	for k, ss := range s.sessions {
		if k == sessionID {
			ss.transcodeType = ttype
			s.sessions[k] = ss
			return true
		}
	}

	// Exact match against inner SessionKey
	for k, ss := range s.sessions {
		if ss.session.SessionKey == sessionID {
			ss.transcodeType = ttype
			s.sessions[k] = ss
			return true
		}
	}

	// Suffix/last-segment tolerant matching: compare last segments both ways
	for k, ss := range s.sessions {
		if ss.session.SessionKey != "" {
			if lastSeg(sessionID) == lastSeg(ss.session.SessionKey) {
				ss.transcodeType = ttype
				s.sessions[k] = ss
				return true
			}
			if strings.Contains(sessionID, ss.session.SessionKey) || strings.Contains(ss.session.SessionKey, sessionID) {
				ss.transcodeType = ttype
				s.sessions[k] = ss
				return true
			}
		}
	}

	// Check if any session has this transcode session ID mapped via PlaySessionStateNotification
	for sessionKey, mappedTranscodeID := range s.sessionTranscodeMap {
		if sameTranscodeID(mappedTranscodeID, originalSessionID) {
			if ss, ok := s.sessions[sessionKey]; ok {
				ss.transcodeType = ttype
				s.sessions[sessionKey] = ss
				return true
			}
		}
	}

	// Check if the transcode session matches any of the transcode sessions embedded in active session media
	for k, ss := range s.sessions {
		// Skip sessions without proper session data (orphaned transcode entries)
		if ss.session.SessionKey == "" || ss.session.User.Title == "" {
			continue
		}

		if len(ss.session.Media) > 0 {
			for _, media := range ss.session.Media {
				for _, part := range media.Part {
					if part.Key != "" && strings.Contains(part.Key, "/transcode/sessions/") {
						partTranscodeID := extractTranscodeSessionID(part.Key)
						if partTranscodeID == sessionID || partTranscodeID == extractTranscodeSessionID(originalSessionID) {
							ss.transcodeType = ttype
							s.sessions[k] = ss
							return true
						}
					}
				}
			}
		}
	}

	// Enhanced transcode session matching: check if the websocket sessionID
	// matches any transcode session keys embedded in the session data.
	// This handles cases where websocket sends keys like "abc123" or "/transcode/sessions/abc123" but
	// session data contains transcode session paths like "/transcode/sessions/abc123".
	// Prefer play-session transcode keys found in media parts: treat these as authoritative
	for k, ss := range s.sessions {
		if len(ss.session.Media) == 0 {
			continue
		}
		for _, media := range ss.session.Media {
			for _, part := range media.Part {
				if part.Key == "" {
					continue
				}

				// If the media part contains a transcode sessions path, prefer that mapping
				if strings.Contains(part.Key, "/transcode/sessions/") {
					partID := extractTranscodeSessionID(part.Key)
					if partID != "" {
						if partID == sessionID || partID == originalSessionID || lastSeg(partID) == lastSeg(sessionID) {
							ss.transcodeType = ttype
							s.sessions[k] = ss
							return true
						}
					}
				}

				// Direct match against original full path
				if originalSessionID != sessionID && part.Key == originalSessionID {
					ss.transcodeType = ttype
					s.sessions[k] = ss
					return true
				}
			}
		}
	}

	// No existing session matched.
	// Heuristic fallback: apply to any session that currently has a
	// part decision of "transcode". This handles cases where the
	// websocket transcode key doesn't map directly to our session map
	// keys but the server is clearly performing a transcode for a
	// session we already track.
	//
	// Enhanced heuristic: try multiple strategies in order of preference

	// Strategy 1: Apply to any single transcoding session if there's only one
	transcodingSessions := []string{}
	for k, ss := range s.sessions {
		// Skip orphaned transcode sessions (those with no session key and no meaningful data)
		if ss.session.SessionKey == "" && len(ss.session.Media) == 0 {
			continue
		}

		if len(ss.session.Media) > 0 && len(ss.session.Media[0].Part) > 0 {
			if ss.session.Media[0].Part[0].Decision == "transcode" {
				transcodingSessions = append(transcodingSessions, k)
			}
		}
	}

	// If there's exactly one transcoding session, apply the update to it
	if len(transcodingSessions) == 1 {
		k := transcodingSessions[0]
		ss := s.sessions[k]
		ss.transcodeType = ttype
		s.sessions[k] = ss
		return true
	}

	// Strategy 2: Apply to most recent session if we have multiple transcoding sessions
	if len(transcodingSessions) > 1 {
		var mostRecentKey string
		var mostRecentTime time.Time

		for _, k := range transcodingSessions {
			ss := s.sessions[k]
			if ss.playStarted.After(mostRecentTime) {
				mostRecentTime = ss.playStarted
				mostRecentKey = k
			}
		}

		if mostRecentKey != "" {
			ss := s.sessions[mostRecentKey]
			ss.transcodeType = ttype
			s.sessions[mostRecentKey] = ss
			return true
		}
	}

	return false
}

// extractTranscodeSessionID extracts the session ID from a transcode session path.
// For example, "/transcode/sessions/abc123" returns "abc123".
func extractTranscodeSessionID(path string) string {
	const transcodePrefix = "/transcode/sessions/"
	if strings.Contains(path, transcodePrefix) {
		// Find the position after the prefix
		if idx := strings.Index(path, transcodePrefix); idx >= 0 {
			sessionPart := path[idx+len(transcodePrefix):]
			// Return everything up to the next slash or end of string
			if slashIdx := strings.Index(sessionPart, "/"); slashIdx >= 0 {
				return sessionPart[:slashIdx]
			}
			return sessionPart
		}
	}
	return ""
}

func sameTranscodeID(left, right string) bool {
	normalize := func(value string) string {
		value = strings.TrimSpace(value)
		if extracted := extractTranscodeSessionID(value); extracted != "" {
			return extracted
		}
		return value
	}
	left = normalize(left)
	right = normalize(right)
	return left != "" && left == right
}

func (s *sessions) canTrackLocked(sessionID string) bool {
	if s.sessions == nil {
		return false
	}
	if _, exists := s.sessions[sessionID]; exists {
		return true
	}
	max := s.maxTrackedSessions
	if max <= 0 {
		max = defaultMaxTrackedSessions
	}
	if len(s.sessions) < max {
		return true
	}
	// Prefer evicting records that can no longer contribute current-session
	// metrics. This keeps a stale client from occupying the entire bounded
	// store while a new playback session is trying to arrive.
	var candidate string
	var candidateAt time.Time
	for key, current := range s.sessions {
		if current.state != stateStopped && current.state != stateUnknown &&
			(current.session.SessionKey != "" || len(current.session.Media) != 0) {
			continue
		}
		if candidate == "" || current.lastUpdate.Before(candidateAt) {
			candidate = key
			candidateAt = current.lastUpdate
		}
	}
	if candidate == "" {
		return false
	}
	delete(s.sessions, candidate)
	delete(s.sessionTranscodeMap, candidate)
	return true
}

// canTrackMappingLocked keeps the transcode lookup table bounded even when a
// notification arrives for a session that the bounded session store rejected.
// Existing mappings remain updateable, and mappings for already-evicted
// sessions are reclaimed before refusing a new one.
func (s *sessions) canTrackMappingLocked(sessionKey string) bool {
	if _, exists := s.sessionTranscodeMap[sessionKey]; exists {
		return true
	}
	max := s.maxTrackedSessions
	if max <= 0 {
		max = defaultMaxTrackedSessions
	}
	if len(s.sessionTranscodeMap) < max {
		return true
	}
	for key := range s.sessionTranscodeMap {
		if _, exists := s.sessions[key]; !exists {
			delete(s.sessionTranscodeMap, key)
			return true
		}
	}
	return false
}

// matchSessionKeyLocked performs the inexpensive forms of the transcode
// lookup used by the hardware evidence path. The full transcode-type matcher
// remains deliberately more permissive because it also handles old Plex
// websocket payload variants.
func (s *sessions) matchSessionKeyLocked(sessionID string) (string, bool) {
	if _, ok := s.sessions[sessionID]; ok {
		return sessionID, true
	}
	for key, current := range s.sessions {
		if current.session.SessionKey == sessionID || sameTranscodeID(key, sessionID) ||
			sameTranscodeID(current.session.SessionKey, sessionID) {
			return key, true
		}
	}
	for key, mapped := range s.sessionTranscodeMap {
		if sameTranscodeID(mapped, sessionID) {
			if _, ok := s.sessions[key]; ok {
				return key, true
			}
		}
	}
	for key, current := range s.sessions {
		for _, media := range current.session.Media {
			for _, part := range media.Part {
				if sameTranscodeID(part.Key, sessionID) {
					return key, true
				}
			}
		}
	}
	return "", false
}

func (s *sessions) Update(sessionID string, newState sessionState, newSession *plex.Metadata, media *plex.Metadata) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	if newState == stateStopped && newSession == nil && media == nil {
		s.mtx.Lock()
		_, exists := s.sessions[sessionID]
		s.mtx.Unlock()
		if !exists {
			return
		}
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}
	s.updateLocked(sessionID, newState, newSession, media, time.Now())
}

// syncCurrent atomically reconciles the in-memory store with a session-list
// response obtained after observedAt. Websocket events processed while the
// request was in flight have a newer lastUpdate and therefore win over the
// potentially stale HTTP snapshot. The optional rawEvidence batch is applied
// under the same lock only for records whose snapshot was accepted.
func (s *sessions) syncCurrent(current []plex.Metadata, observedAt time.Time, rawEvidence ...[]transcodeHardwareObservation) {
	active := make(map[string]struct{}, len(current))
	applied := make(map[string]struct{}, len(current))
	now := time.Now()

	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}
	for i := range current {
		metadata := &current[i]
		sessionID := strings.TrimSpace(metadata.SessionKey)
		if sessionID == "" {
			continue
		}
		active[sessionID] = struct{}{}
		existing, exists := s.sessions[sessionID]
		if exists && !existing.lastUpdate.Before(observedAt) {
			continue
		}
		// /status/sessions can carry a sparse session snapshot while the
		// metadata fetched for a notification contains the full library item.
		// Preserve the richer cached item rather than replacing it on every
		// successful poll.
		media := metadata
		if exists && existing.media.RatingKey != "" && existing.media.RatingKey == metadata.RatingKey &&
			metadataRichness(*metadata) < metadataRichness(existing.media) {
			media = nil
		}
		state := sessionState(strings.ToLower(strings.TrimSpace(metadata.Player.State)))
		switch state {
		case statePaused, stateBuffering, stateStopped:
		case statePlaying:
		case "":
			// /status/sessions is itself an active-session endpoint. Older Plex
			// versions omit Player.State for those records, so treat that
			// endpoint membership as the available playing evidence. Explicit
			// unrecognized states still become stateUnknown below.
			state = statePlaying
		default:
			state = stateUnknown
		}
		s.updateLocked(sessionID, state, metadata, media, now)
		applied[sessionID] = struct{}{}
	}

	for sessionID, existing := range s.sessions {
		if _, ok := active[sessionID]; ok {
			continue
		}
		if existing.state == stateStopped {
			continue
		}
		if existing.session.SessionKey == "" && len(existing.session.Media) == 0 {
			continue
		}
		if !existing.lastUpdate.Before(observedAt) {
			continue
		}
		s.updateLocked(sessionID, stateStopped, nil, nil, now)
	}
	if len(rawEvidence) > 0 {
		s.applyTranscodeHardwareObservationsLocked(rawEvidence[0], applied)
	}
}

func metadataRichness(metadata plex.Metadata) int {
	richness := 0
	if metadata.RatingKey != "" {
		richness++
	}
	if metadata.Title != "" {
		richness++
	}
	if metadata.Type != "" {
		richness++
	}
	if metadata.Summary != "" {
		richness++
	}
	if metadata.LibrarySectionID.Int64() != 0 {
		richness++
	}
	if len(metadata.Media) > 0 {
		richness += 2
		if len(metadata.Media[0].Part) > 0 {
			richness++
		}
	}
	return richness
}

// markStale moves active sessions to an explicit unknown state only after the
// configured grace period. It does not update confirmedAt and therefore never
// makes a failed poll look like playback progress.
func (s *sessions) markStale(now time.Time, staleAfter time.Duration) {
	if s == nil || staleAfter <= 0 {
		return
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for key, current := range s.sessions {
		if current.state == stateStopped || current.state == stateUnknown {
			continue
		}
		confirmed := current.confirmedAt
		if confirmed.IsZero() {
			confirmed = current.lastUpdate
		}
		if confirmed.IsZero() || now.Sub(confirmed) < staleAfter {
			continue
		}
		// Freeze playback at the last confirmed observation. The wall-clock
		// timestamp still advances so this record is not reconsidered on every
		// failed request, but no time after confirmed is banked.
		if current.state == statePlaying && !current.playStarted.IsZero() {
			if playedFor := confirmed.Sub(current.playStarted); playedFor > 0 {
				current.prevPlayedTime += playedFor
				if bitrate := sessionBitrate(current); bitrate > 0 {
					s.totalEstimatedTransmittedKBits += playedFor.Seconds() * float64(bitrate)
				}
			}
		}
		current.state = stateUnknown
		current.lastUpdate = now
		s.sessions[key] = current
	}
}

// cachedMetadata returns the most recent metadata for the session when it is
// for the same rating key. A rating key check prevents one session's previous
// title from being reused after Plex switches the client to another item.
func (s *sessions) cachedMetadata(sessionKey, ratingKey string) (plex.Metadata, bool) {
	if s == nil {
		return plex.Metadata{}, false
	}
	sessionKey = strings.TrimSpace(sessionKey)
	ratingKey = strings.TrimSpace(ratingKey)
	if sessionKey == "" || ratingKey == "" {
		return plex.Metadata{}, false
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	current, ok := s.sessions[sessionKey]
	if !ok {
		return plex.Metadata{}, false
	}
	if current.media.RatingKey == ratingKey {
		return current.media, true
	}
	if current.session.RatingKey == ratingKey && (len(current.session.Media) > 0 || current.session.Title != "") {
		return current.session, true
	}
	return plex.Metadata{}, false
}

func (s *sessions) updateLocked(sessionID string, newState sessionState, newSession *plex.Metadata, media *plex.Metadata, now time.Time) {
	switch newState {
	case statePlaying, statePaused, stateBuffering, stateStopped, stateUnknown:
	default:
		newState = stateUnknown
	}
	if s.sessions == nil {
		s.sessions = make(map[string]session)
	}
	if !s.canTrackLocked(sessionID) {
		return
	}

	ss := s.sessions[sessionID]
	previousRatingKey := strings.TrimSpace(ss.media.RatingKey)
	if previousRatingKey == "" {
		previousRatingKey = strings.TrimSpace(ss.session.RatingKey)
	}
	incomingRatingKey := ""
	if newSession != nil {
		incomingRatingKey = strings.TrimSpace(newSession.RatingKey)
	}
	if incomingRatingKey == "" && media != nil {
		incomingRatingKey = strings.TrimSpace(media.RatingKey)
	}
	if previousRatingKey != "" && incomingRatingKey != "" && previousRatingKey != incomingRatingKey {
		// A client can keep one session key while moving to another item. Do
		// not carry library or transcode labels from the previous item into the
		// new one; playback time remains session-scoped and is intentionally
		// preserved.
		ss.resolvedLibraryName = ""
		ss.resolvedLibraryID = ""
		ss.resolvedLibraryType = ""
		ss.transcodeType = ""
		ss.subtitleAction = ""
		ss.transcodeHWRequested = false
		ss.transcodeHWDecode = ""
		ss.transcodeHWEncode = ""
		ss.transcodeHWKnown = false
		delete(s.sessionTranscodeMap, sessionID)
	}

	if newSession != nil {
		ss.session = *newSession
	}

	if media != nil {
		ss.media = *media
		// Attempt to resolve the library for this media and persist the
		// labels so later Collect() can emit stable labels even if the
		// server's library list is temporarily unavailable.
		if ss.resolvedLibraryName == "" && s.server != nil {
			if media.LibrarySectionID.Int64() != 0 {
				lib := s.server.Library(strconv.FormatInt(media.LibrarySectionID.Int64(), 10))
				if lib != nil {
					ss.resolvedLibraryName = lib.Name
					ss.resolvedLibraryID = lib.ID
					ss.resolvedLibraryType = lib.Type
				}
			}
		}
	}
	// A session can first be observed while paused or buffering (for example
	// during startup or after reconnect). Give it a stable start timestamp so
	// it is exported immediately, even though elapsed playing time is unknown.
	if ss.playStarted.IsZero() && (newState == statePaused || newState == stateBuffering) {
		ss.playStarted = now
	}

	if ss.state == statePlaying && newState != statePlaying && !ss.playStarted.IsZero() {
		// If the session was playing but now is not, then flatten
		// the play time into the total.
		playedFor := now.Sub(ss.playStarted)
		if playedFor > 0 {
			ss.prevPlayedTime += playedFor
			if bitrate := sessionBitrate(ss); bitrate > 0 {
				s.totalEstimatedTransmittedKBits += playedFor.Seconds() * float64(bitrate)
			}
		}
	}

	if ss.state != statePlaying && newState == statePlaying {
		// Started playing
		ss.playStarted = now
	}

	ss.state = newState
	ss.lastUpdate = now
	if newState != stateUnknown {
		ss.confirmedAt = now
	}
	s.sessions[sessionID] = ss
}

func (s *sessions) extrapolatedTransmittedBytes() float64 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	total := s.totalEstimatedTransmittedKBits

	for _, ss := range s.sessions {
		if ss.state == statePlaying && !ss.playStarted.IsZero() {
			// Be defensive: session.Media may be empty in some edge cases
			// (for example, when a websocket event arrived before metadata was
			// fully populated). Only use the bitrate when available.
			if len(ss.session.Media) > 0 {
				total += time.Since(ss.playStarted).Seconds() * float64(ss.session.Media[0].Bitrate)
			}
		}
	}

	return total * 128.0 // Kbits -> Bytes, 1024 / 8
}

func (s *sessions) Describe(ch chan<- *prometheus.Desc) {
	ch <- metrics.MetricPlayCountDesc
	ch <- metrics.MetricPlaySecondsTotalDesc

	ch <- metrics.MetricEstimatedTransmittedBytesTotal
	for _, desc := range metrics.SessionDescs() {
		ch <- desc
	}
}

func (s *sessions) Collect(ch chan<- prometheus.Metric) {
	s.mtx.Lock()
	type sessionSnapshot struct {
		id      string
		session session
	}
	sessionSnapshots := make([]sessionSnapshot, 0, len(s.sessions))
	for id, current := range s.sessions {
		sessionSnapshots = append(sessionSnapshots, sessionSnapshot{id: id, session: current})
	}
	estimatedKBits := s.totalEstimatedTransmittedKBits
	s.mtx.Unlock()

	serverName, serverID := "", ""
	if s.server != nil {
		serverName, serverID = s.server.identity()
	}
	now := time.Now()
	stateCounts := make(map[sessionState]float64, len(trackedSessionStates))
	for _, state := range trackedSessionStates {
		stateCounts[state] = 0
	}

	for _, snapshot := range sessionSnapshots {
		id := snapshot.id
		session := snapshot.session

		// Skip orphaned sessions (those with no session key and no meaningful data)
		// These can occur when transcode notifications arrive for sessions that don't exist
		if session.session.SessionKey == "" && len(session.session.Media) == 0 && session.media.RatingKey == "" {
			continue
		}

		collectCurrentSessionMetrics(
			ch, id, session, serverName, serverID, now, &estimatedKBits, stateCounts,
		)

		if session.playStarted.IsZero() {
			continue
		}

		title, season, episode := labels(session.media)

		// Prefer persisted resolved labels if available; fall back to
		// live lookup and then to 'unknown' if necessary.
		var libraryName, libraryID, libraryType string
		if session.resolvedLibraryName != "" {
			libraryName = session.resolvedLibraryName
			libraryID = session.resolvedLibraryID
			libraryType = session.resolvedLibraryType
		} else {
			var library *Library
			if s.server != nil {
				library = s.server.Library(strconv.FormatInt(session.media.LibrarySectionID.Int64(), 10))
			}
			if library == nil {
				libraryName = "unknown"
				libraryID = "0"
				libraryType = "unknown"
			} else {
				libraryName = library.Name
				libraryID = library.ID
				libraryType = library.Type
			}
		}

		// Defensive extraction of nested fields that may be missing when
		// websocket notifications arrive before full metadata is available.
		streamType := "unknown"
		sessionRes := ""
		fileRes := ""
		bitrate := "0"
		device := session.session.Player.Device
		product := session.session.Player.Product
		user := session.session.User.Title

		if len(session.session.Media) > 0 {
			if len(session.session.Media[0].Part) > 0 {
				if session.session.Media[0].Part[0].Decision != "" {
					streamType = session.session.Media[0].Part[0].Decision
				}
			}
			if session.session.Media[0].VideoResolution != "" {
				sessionRes = session.session.Media[0].VideoResolution
			}
			if session.session.Media[0].Bitrate != 0 {
				bitrate = strconv.Itoa(session.session.Media[0].Bitrate)
			}
		}
		if len(session.media.Media) > 0 {
			if session.media.Media[0].VideoResolution != "" {
				fileRes = session.media.Media[0].VideoResolution
			}
		}

		ch <- metrics.Play(
			1.0,
			"plex",
			serverName,
			serverID,
			libraryType,
			libraryName,
			libraryID,
			session.media.Type,
			title,
			season,
			episode,
			streamType, // stream type
			sessionRes, // stream res
			fileRes,    // file res
			bitrate,    // bitrate
			device,     // device
			product,    // device type
			user,
			id,
			session.transcodeType,
			session.subtitleAction,
		)

		totalPlayTime := session.prevPlayedTime
		if session.state == statePlaying {
			if elapsed := confirmedPlayingElapsed(session, now); elapsed > 0 {
				totalPlayTime += elapsed
			}
		}

		ch <- metrics.PlayDuration(
			float64(totalPlayTime.Seconds()),
			"plex",
			serverName,
			serverID,
			libraryType,
			libraryName,
			libraryID,
			session.media.Type,
			title,
			season,
			episode,
			streamType, // stream type
			sessionRes, // stream res
			fileRes,    // file res
			bitrate,    // bitrate
			device,     // device
			product,    // device type
			user,
			id,
			session.transcodeType,
			session.subtitleAction,
		)
	}

	for _, state := range trackedSessionStates {
		ch <- prometheus.MustNewConstMetric(
			metrics.MetricSessionsDesc,
			prometheus.GaugeValue,
			stateCounts[state],
			"plex", serverName, serverID, string(state),
		)
	}

	ch <- prometheus.MustNewConstMetric(metrics.MetricEstimatedTransmittedBytesTotal, prometheus.CounterValue, estimatedKBits*128.0, "plex", serverName, serverID)
}

// collectCurrentSessionMetrics emits the bounded-label metrics for one
// retained session and updates the aggregate state counts and estimated
// traffic total used by the caller. Keeping this separate from the legacy
// media-label path keeps the collector readable without changing either
// metric contract.
func collectCurrentSessionMetrics(
	ch chan<- prometheus.Metric,
	id string,
	current session,
	serverName, serverID string,
	now time.Time,
	estimatedKBits *float64,
	stateCounts map[sessionState]float64,
) {
	currentState := current.state
	if _, known := stateCounts[currentState]; !known {
		currentState = stateUnknown
	}
	stateCounts[currentState]++
	sessionID := strings.TrimSpace(current.session.SessionKey)
	if sessionID == "" {
		sessionID = id
	}
	user := current.session.User.Title
	active := 0.0
	if isActiveSessionState(currentState) {
		active = 1
	}
	ch <- prometheus.MustNewConstMetric(
		metrics.MetricSessionActiveDesc,
		prometheus.GaugeValue,
		active,
		"plex", serverName, serverID, sessionID, user,
	)
	ch <- prometheus.MustNewConstMetric(
		metrics.MetricSessionStateDesc,
		prometheus.GaugeValue,
		1,
		"plex", serverName, serverID, sessionID, user, string(currentState),
	)

	// This counter has only stable identity labels. It is emitted even when
	// its value is zero so a newly observed paused/stopped session is useful
	// to recording rules immediately.
	totalPlayTime := current.prevPlayedTime
	if currentState == statePlaying && !current.playStarted.IsZero() {
		if elapsed := confirmedPlayingElapsed(current, now); elapsed > 0 {
			totalPlayTime += elapsed
			if bitrate := sessionBitrate(current); bitrate > 0 {
				*estimatedKBits += elapsed.Seconds() * float64(bitrate)
			}
		}
	}
	ch <- prometheus.MustNewConstMetric(
		metrics.MetricSessionPlaySecondsTotalDesc,
		prometheus.CounterValue,
		float64(totalPlayTime.Seconds()),
		"plex", serverName, serverID, sessionID, user,
	)

	location := sessionLocation(current.session)
	if bitrate := sessionBitrate(current); bitrate > 0 {
		value := float64(bitrate)
		if !isActiveSessionState(currentState) {
			value = 0
		}
		ch <- prometheus.MustNewConstMetric(
			metrics.MetricSessionBitrateKbpsDesc,
			prometheus.GaugeValue,
			value,
			"plex", serverName, serverID, sessionID, user, location,
		)
	}
	if bandwidth := current.session.Session.Bandwidth; bandwidth > 0 {
		value := float64(bandwidth)
		if !isActiveSessionState(currentState) {
			value = 0
		}
		ch <- prometheus.MustNewConstMetric(
			metrics.MetricSessionBandwidthKbpsDesc,
			prometheus.GaugeValue,
			value,
			"plex", serverName, serverID, sessionID, user, location,
		)
	}
	if current.transcodeHWKnown && sessionHasVideoTranscode(current) {
		value := 0.0
		if isActiveSessionState(currentState) {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(
			metrics.MetricSessionVideoTranscodeDesc,
			prometheus.GaugeValue,
			value,
			"plex", serverName, serverID, sessionID, user, current.transcodeHWDecode, current.transcodeHWEncode,
		)
	}
}

// confirmedPlayingElapsed intentionally stops at confirmedAt. A scrape may
// happen while Plex is unreachable, but a counter must not advance during that
// unconfirmed interval and then decrease when the stale timeout is reached.
func confirmedPlayingElapsed(current session, now time.Time) time.Duration {
	if current.state != statePlaying || current.playStarted.IsZero() {
		return 0
	}
	end := now
	if !current.confirmedAt.IsZero() && current.confirmedAt.Before(end) {
		end = current.confirmedAt
	}
	if end.Before(current.playStarted) {
		return 0
	}
	return end.Sub(current.playStarted)
}

func sessionBitrate(current session) int {
	if len(current.session.Media) > 0 && current.session.Media[0].Bitrate > 0 {
		return current.session.Media[0].Bitrate
	}
	if len(current.media.Media) > 0 && current.media.Media[0].Bitrate > 0 {
		return current.media.Media[0].Bitrate
	}
	return 0
}

func isActiveSessionState(state sessionState) bool {
	return state == statePlaying || state == statePaused || state == stateBuffering
}

func sessionLocation(metadata plex.Metadata) string {
	if metadata.Player.Local {
		return "lan"
	}
	switch strings.ToLower(strings.TrimSpace(metadata.Session.Location)) {
	case "lan", "local":
		return "lan"
	case "wan", "remote":
		return "wan"
	}
	if metadata.Player.Address != "" || metadata.Player.RemotePublicAddress != "" {
		return "wan"
	}
	return "unknown"
}

func sessionHasVideoTranscode(current session) bool {
	typeName := strings.ToLower(strings.TrimSpace(current.transcodeType))
	if typeName == "video" || typeName == "both" || typeName == "hw" || typeName == "hardware" {
		return true
	}
	for _, media := range current.session.Media {
		for _, part := range media.Part {
			if strings.EqualFold(strings.TrimSpace(part.Decision), "transcode") {
				// An audio-only transcode is represented by an audio transcode
				// type when websocket data is available. Without that signal,
				// use the presence of video metadata as conservative evidence.
				return media.VideoCodec != "" || media.VideoResolution != ""
			}
		}
	}
	return false
}

func labels(m plex.Metadata) (title, season, episodeTitle string) {
	if m.Type == mediaTypeEpisode {
		return m.GrandparentTitle, m.ParentTitle, m.Title
	}
	return m.Title, "", ""
}

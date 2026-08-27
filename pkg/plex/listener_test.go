package plex

// NOTE: Test fixtures in this file use randomized/sanitized identifiers
// (session keys, transcode keys, machine identifiers, user/player names).
// These values mimic the shape of real Plex data so matching logic is
// exercised without containing production-identifying information.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	ttPlex "github.com/timothystewart6/go-plex-client"

	"github.com/naterator/plex-exporter/pkg/log"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// fakePlexClient implements the minimal plexClient interface for tests.
type fakePlexClient struct {
	sessions ttPlex.CurrentSessions
	sessErr  error
}

func (f *fakePlexClient) GetSessions() (ttPlex.CurrentSessions, error) { return f.sessions, f.sessErr }
func (f *fakePlexClient) GetMetadata(s string) (ttPlex.MediaMetadata, error) {
	return ttPlex.MediaMetadata{}, errors.New("not implemented")
}
func (f *fakePlexClient) GetTranscodeSessions() (ttPlex.TranscodeSessionsResponse, error) {
	return ttPlex.TranscodeSessionsResponse{}, errors.New("not implemented")
}

// fakePlex implements minimal methods used by plexListener for richer tests.
type fakePlex struct {
	sessions     ttPlex.CurrentSessions
	sessErr      error
	metadataMap  map[string]ttPlex.Metadata // Map of ratingKey -> metadata
	metaErr      error
	transcodeErr error
	// Optional custom transcode sessions response for tests
	transcodeResp ttPlex.TranscodeSessionsResponse
}

func (f *fakePlex) GetSessions() (ttPlex.CurrentSessions, error) {
	if f.sessErr != nil {
		return ttPlex.CurrentSessions{}, f.sessErr
	}
	if f.sessions.MediaContainer.Metadata != nil {
		return f.sessions, nil
	}
	// Default sessions for compatibility
	s := ttPlex.CurrentSessions{}
	s.MediaContainer.Metadata = []ttPlex.Metadata{{SessionKey: "sess1", User: ttPlex.User{Title: "user1", ID: "u1"}}, {SessionKey: "sess2", User: ttPlex.User{Title: "user2", ID: "u2"}}}
	return s, nil
}

func (f *fakePlex) GetMetadata(ratingKey string) (ttPlex.MediaMetadata, error) {
	if f.metaErr != nil {
		return ttPlex.MediaMetadata{}, f.metaErr
	}

	// Use the metadata map to return specific metadata for rating keys
	if f.metadataMap != nil {
		if metadata, exists := f.metadataMap[ratingKey]; exists {
			mm := ttPlex.MediaMetadata{}
			mm.MediaContainer.Metadata = []ttPlex.Metadata{metadata}
			return mm, nil
		}
		// If metadataMap is explicitly set but rating key not found, return empty response
		mm := ttPlex.MediaMetadata{}
		mm.MediaContainer.Metadata = []ttPlex.Metadata{}
		return mm, nil
	}

	// Default metadata for compatibility (only when metadataMap is nil)
	mm := ttPlex.MediaMetadata{}
	mm.MediaContainer.Metadata = []ttPlex.Metadata{{RatingKey: ratingKey, Title: "Episode 1"}}
	return mm, nil
}

func (f *fakePlex) GetTranscodeSessions() (ttPlex.TranscodeSessionsResponse, error) {
	if f.transcodeErr != nil {
		return ttPlex.TranscodeSessionsResponse{}, f.transcodeErr
	}
	// If a test provided a custom response, return it
	if f.transcodeResp.Children != nil {
		return f.transcodeResp, nil
	}
	// Default response used by older tests
	t := ttPlex.TranscodeSessionsResponse{}
	child := struct {
		ElementType      string  `json:"_elementType"`
		AudioChannels    int     `json:"audioChannels"`
		AudioCodec       string  `json:"audioCodec"`
		AudioDecision    string  `json:"audioDecision"`
		SubtitleDecision string  `json:"subtitleDecision"`
		Container        string  `json:"container"`
		Context          string  `json:"context"`
		Duration         int     `json:"duration"`
		Height           int     `json:"height"`
		Key              string  `json:"key"`
		Progress         float64 `json:"progress"`
		Protocol         string  `json:"protocol"`
		Remaining        int     `json:"remaining"`
		Speed            float64 `json:"speed"`
		Throttled        bool    `json:"throttled"`
		VideoCodec       string  `json:"videoCodec"`
		VideoDecision    string  `json:"videoDecision"`
		Width            int     `json:"width"`
	}{
		Key:              "ts1",
		VideoDecision:    "transcode",
		AudioDecision:    "transcode",
		SubtitleDecision: "burn",
		Container:        "mp4",
	}
	t.Children = []struct {
		ElementType      string  `json:"_elementType"`
		AudioChannels    int     `json:"audioChannels"`
		AudioCodec       string  `json:"audioCodec"`
		AudioDecision    string  `json:"audioDecision"`
		SubtitleDecision string  `json:"subtitleDecision"`
		Container        string  `json:"container"`
		Context          string  `json:"context"`
		Duration         int     `json:"duration"`
		Height           int     `json:"height"`
		Key              string  `json:"key"`
		Progress         float64 `json:"progress"`
		Protocol         string  `json:"protocol"`
		Remaining        int     `json:"remaining"`
		Speed            float64 `json:"speed"`
		Throttled        bool    `json:"throttled"`
		VideoCodec       string  `json:"videoCodec"`
		VideoDecision    string  `json:"videoDecision"`
		Width            int     `json:"width"`
	}{child}
	return t, nil
}

func TestListen_AlreadyListeningAndNewPlexError(t *testing.T) {
	// Already listening
	s := &Server{}
	s.listener = &plexListener{}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := s.Listen(ctx, log.NewNopLogger()); err != ErrAlreadyListening {
		t.Fatalf("expected ErrAlreadyListening, got %v", err)
	}

	// newPlex error path
	old := newPlex
	newPlex = func(base, token string) (*ttPlex.Plex, error) { return nil, errors.New("connect fail") }
	defer func() { newPlex = old }()

	s2 := &Server{URL: &url.URL{Scheme: "http", Host: "localhost:32400"}, Token: "test"}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	if err := s2.Listen(ctx2, log.NewNopLogger()); err == nil {
		t.Fatalf("expected Listen to return error when newPlex fails")
	}
}

func TestListenConnectsSeedsSessionsAndShutsDown(t *testing.T) {
	connected := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{}
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/:/websockets/notifications":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			connected <- struct{}{}
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		case "/status/sessions":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer":{"Metadata":[{"sessionKey":"existing-session","ratingKey":"movie-1","title":"Existing Movie","type":"movie","Player":{"state":"paused"}}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer testServer.Close()

	client, err := NewClient(testServer.URL, "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	server := &Server{
		URL:    client.URL,
		Token:  client.Token,
		Client: client,
		Name:   "listener-test",
		ID:     "listener-test-id",
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- server.Listen(ctx, log.NewNopLogger()) }()

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("websocket listener did not connect")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		server.mtx.RLock()
		store := server.eventSessions
		server.mtx.RUnlock()
		seeded := false
		if store != nil {
			store.mtx.Lock()
			_, seeded = store.sessions["existing-session"]
			store.mtx.Unlock()
		}
		if seeded {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("existing Plex session was not seeded")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Listen returned during cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Listen did not stop after cancellation")
	}
	server.mtx.RLock()
	listener := server.listener
	server.mtx.RUnlock()
	if listener != nil {
		t.Fatal("listener state was not cleared after shutdown")
	}
	server.Close()
}

func TestOnPlayingHandler_GetSessionsError(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// Create listener with fake client that returns error for GetSessions
	l := &plexListener{
		conn:           &fakePlexClient{sessErr: errors.New("fetch sessions failed")},
		activeSessions: NewSessions(context.Background(), s),
		log:            logger,
	}

	// Build a NotificationContainer with a PlaySessionStateNotification
	nc := ttPlex.NotificationContainer{PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{SessionKey: "s1", RatingKey: "r1", State: "playing"}}}

	// Call handler; it should not panic and should log the error properly.
	l.onPlayingHandler(nc)

	// Verify error is logged with proper structure
	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("error handling OnPlaying event")) {
		t.Fatalf("expected error log message, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"sessionKeys": "s1"`)) {
		t.Fatalf("expected sessionKeys in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"ratingKeys": "r1"`)) {
		t.Fatalf("expected ratingKeys in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"states": "playing"`)) {
		t.Fatalf("expected states in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte("fetch sessions failed")) {
		t.Fatalf("expected error message in log, got: %s", out)
	}
}

func TestSyncCurrentSessionsSeedsExistingPlayback(t *testing.T) {
	server := &Server{
		Name: "srv",
		ID:   "id",
		libraries: []*Library{{
			ID:   "1",
			Name: "Movies",
			Type: "movie",
		}},
	}
	current := ttPlex.CurrentSessions{}
	current.MediaContainer.Metadata = []ttPlex.Metadata{
		{
			SessionKey:       "already-playing",
			RatingKey:        "movie-1",
			LibrarySectionID: 1,
			Title:            "Existing Movie",
			Type:             "movie",
			User:             ttPlex.User{Title: "viewer"},
		},
		{
			SessionKey: "already-paused",
			RatingKey:  "movie-2",
			Title:      "Paused Movie",
			Type:       "movie",
			Player:     ttPlex.Player{State: "paused"},
		},
		{SessionKey: ""},
	}
	store := &sessions{
		sessions:            make(map[string]session),
		sessionTranscodeMap: make(map[string]string),
		server:              server,
	}
	listener := &plexListener{
		server:         server,
		conn:           &fakePlex{sessions: current},
		activeSessions: store,
		log:            log.NewNopLogger(),
	}

	if err := listener.syncCurrentSessions(); err != nil {
		t.Fatalf("syncCurrentSessions: %v", err)
	}
	store.mtx.Lock()
	defer store.mtx.Unlock()
	got, exists := store.sessions["already-playing"]
	if !exists || got.state != statePlaying || got.media.RatingKey != "movie-1" || got.playStarted.IsZero() {
		t.Fatalf("existing playback was not seeded: %+v", got)
	}
	if paused := store.sessions["already-paused"]; paused.state != statePaused || paused.playStarted.IsZero() {
		t.Fatalf("paused playback was not seeded correctly: %+v", paused)
	}
	if len(store.sessions) != 2 {
		t.Fatalf("session without a key was not ignored: %+v", store.sessions)
	}
}

func TestOnTranscodeUpdateHandler_Empty(t *testing.T) {
	l := &plexListener{activeSessions: NewSessions(context.Background(), &Server{}), log: log.NewNopLogger()}
	l.onTranscodeUpdateHandler(ttPlex.NotificationContainer{})
}

func TestOnTranscodeUpdateHandlerSetsSessionType(t *testing.T) {
	tests := []struct {
		name string
		ts   ttPlex.TranscodeSession
		want string
	}{
		{name: "video", ts: ttPlex.TranscodeSession{Key: "s1", SourceVideoCodec: "h264", VideoCodec: "hevc"}, want: "video"},
		{name: "audio", ts: ttPlex.TranscodeSession{Key: "s2", SourceAudioCodec: "aac", AudioCodec: "mp3"}, want: "audio"},
		{name: "both", ts: ttPlex.TranscodeSession{Key: "s3", SourceVideoCodec: "h264", VideoCodec: "vp9", SourceAudioCodec: "aac", AudioCodec: "ac3"}, want: "both"},
		{name: "unknown", ts: ttPlex.TranscodeSession{Key: "s4"}, want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// prepare sessions store and listener
			sessStore := &sessions{sessions: map[string]session{}, server: &Server{Name: "srv", ID: "id"}}
			l := &plexListener{activeSessions: sessStore, log: log.NewNopLogger()}

			// call handler
			c := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{tt.ts}}
			l.onTranscodeUpdateHandler(c)

			ss, ok := sessStore.sessions[tt.ts.Key]
			if !ok {
				t.Fatalf("expected session %q to exist", tt.ts.Key)
			}
			if ss.transcodeType != tt.want {
				t.Fatalf("session transcodeType = %q, want %q", ss.transcodeType, tt.want)
			}
			// subtitle_action defaults to "none" for these test cases
			if ss.subtitleAction != "none" {
				t.Fatalf("expected subtitleAction=none, got %q", ss.subtitleAction)
			}
		})
	}
}

func TestTranscodeSessionsAPIOverridesApply(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	sessStore := &sessions{sessions: map[string]session{}, server: s}
	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: sessStore,
		log:            log.NewNopLogger(),
	}

	// Simulate a websocket transcode update with a key that doesn't match
	// active sessions so the listener will consult GetTranscodeSessions().
	c := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "ts1"}}}
	l.onTranscodeUpdateHandler(c)

	ss, ok := sessStore.sessions["ts1"]
	if !ok {
		t.Fatalf("expected session ts1 to exist after API fallback")
	}
	if ss.transcodeType != "both" {
		t.Fatalf("expected transcodeType=both, got %q", ss.transcodeType)
	}
	if ss.subtitleAction != "burn" {
		t.Fatalf("expected subtitleAction=burn, got %q", ss.subtitleAction)
	}
}

func TestOnPlayingUpdatesSessions(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            log.NewNopLogger(),
	}

	// craft a notification container with one playing notification
	c := ttPlex.NotificationContainer{}
	c.PlaySessionStateNotification = []ttPlex.PlaySessionStateNotification{{
		SessionKey: "sess1",
		RatingKey:  "rk1",
		State:      "playing",
		ViewOffset: 1000,
	}}

	if err := l.onPlaying(c); err != nil {
		t.Fatalf("onPlaying error: %v", err)
	}

	// ensure session was added
	if _, ok := l.activeSessions.sessions["sess1"]; !ok {
		t.Fatalf("expected session sess1 to be present")
	}

	// ensure metadata was attached
	ss := l.activeSessions.sessions["sess1"]
	if ss.media.RatingKey != "rk1" && ss.session.SessionKey != "sess1" {
		t.Fatalf("unexpected session contents: %+v", ss)
	}

	// simulate stop + old timestamp and prune
	ss.state = stateStopped
	ss.lastUpdate = time.Now().Add(-2 * sessionTimeout)
	l.activeSessions.sessions["sess1"] = ss
	l.activeSessions.pruneOldSessions()
	if _, ok := l.activeSessions.sessions["sess1"]; ok {
		t.Fatalf("expected session sess1 to be pruned")
	}
}

func TestOnPlayingLogsFirstNotification(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	// capture logs in a buffer
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            logger,
	}

	// craft a notification container with two playing notifications
	c := ttPlex.NotificationContainer{}
	c.PlaySessionStateNotification = []ttPlex.PlaySessionStateNotification{{
		SessionKey: "sess1",
		RatingKey:  "rk1",
		State:      "playing",
		ViewOffset: 1000,
	}, {
		SessionKey: "sess2",
		RatingKey:  "rk2",
		State:      "playing",
		ViewOffset: 2000,
	}}

	if err := l.onPlaying(c); err != nil {
		t.Fatalf("onPlaying error: %v", err)
	}

	out := buf.String()
	if out == "" {
		t.Fatalf("expected log output, got empty string")
	}

	// Ensure the first session key and batchCount=2 are present in the log
	if !bytes.Contains([]byte(out), []byte(`"SessionKey": "sess1"`)) {
		t.Fatalf("expected SessionKey=sess1 in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"batchCount": 2`)) {
		t.Fatalf("expected batchCount=2 in log, got: %s", out)
	}
}

func TestOnTimelineLogsEntries(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            logger,
	}

	// craft a notification container with two timeline entries
	c := ttPlex.NotificationContainer{}
	c.TimelineEntry = []ttPlex.TimelineEntry{{
		Identifier: "id1",
		ItemID:     123,
		Title:      "Test1",
		SectionID:  15,
		State:      2,
	}, {
		Identifier: "id2",
		ItemID:     124,
		Title:      "Test2",
		SectionID:  15,
		State:      3,
	}}

	// our handler expects the dependency's NotificationContainer type used by
	// the websocket client; convert and call the handler.
	l.onTimelineHandler(c)

	out := buf.String()
	if out == "" {
		t.Fatalf("expected log output, got empty string")
	}

	if !bytes.Contains([]byte(out), []byte("timeline entries")) {
		t.Fatalf("expected timeline entries in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte("id=id1")) {
		t.Fatalf("expected id=id1 in log, got: %s", out)
	}
}

// TestOnPlayingHandlerSuccess tests the success path of onPlayingHandler.
func TestOnPlayingHandlerSuccess(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            logger,
	}

	// Create a notification container with playing notifications
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "sess1",
			RatingKey:  "rk1",
			State:      "playing",
			ViewOffset: 1000,
		}},
	}

	// Call onPlayingHandler - should succeed without errors
	l.onPlayingHandler(nc)

	// Should not log any errors
	out := buf.String()
	if bytes.Contains([]byte(out), []byte("error handling OnPlaying event")) {
		t.Fatalf("expected no error logs, but got: %s", out)
	}

	// Session should be added
	if _, ok := l.activeSessions.sessions["sess1"]; !ok {
		t.Fatalf("expected session sess1 to be present")
	}
}

// Reproduce the runtime scenario where transcode updates arrive as
// "/transcode/sessions/<uuid>" and map to a play session through its media
// part key without producing a false unmatched-session warning.
func TestOnTranscodeUpdate_UnmatchedThenMatched(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// activeSessions with two numeric map keys: 350 and 351
	sessStore := &sessions{sessions: map[string]session{}, server: s}

	// session 350 references transcode id 41ee... in a media part
	m350 := ttPlex.Metadata{SessionKey: "350", User: ttPlex.User{Title: "userA"}, Player: ttPlex.Player{Product: "Plex Web"}, Media: []ttPlex.Media{{Part: []ttPlex.Part{{Key: "/transcode/sessions/fake1234-5678-9abc-def0-123456789abc/file.m3u8"}}}}}
	// session 351 references transcode id 0yqi... in a media part
	m351 := ttPlex.Metadata{SessionKey: "351", User: ttPlex.User{Title: "userB"}, Player: ttPlex.Player{Product: "Plex for Vizio"}, Media: []ttPlex.Media{{Part: []ttPlex.Part{{Key: "/transcode/sessions/faketsession1a2b3c4d5e6f/segment.m3u8"}}}}}

	sessStore.mtx.Lock()
	sessStore.sessions["350"] = session{session: m350, state: statePlaying, playStarted: time.Now()}
	sessStore.sessions["351"] = session{session: m351, state: statePlaying, playStarted: time.Now()}
	sessStore.mtx.Unlock()

	l := &plexListener{server: s, conn: &fakePlex{}, activeSessions: sessStore, log: logger}

	// Send a transcode update that uses the full path for 41ee... (both audio+video)
	c1 := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "/transcode/sessions/fake1234-5678-9abc-def0-123456789abc", VideoDecision: "transcode", AudioDecision: "transcode"}}}
	l.onTranscodeUpdateHandler(c1)

	out := buf.String()
	if bytes.Contains(buf.Bytes(), []byte("transcode update did not match any active session")) {
		t.Fatalf("matched transcode update was logged as unmatched: %s", out)
	}

	// Clear buffer and send a raw transcode key (no prefix) for 0yqi... which should match session 351
	buf.Reset()
	c2 := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "faketsession1a2b3c4d5e6f", VideoDecision: "transcode", AudioDecision: "transcode"}}}
	l.onTranscodeUpdateHandler(c2)

	out2 := buf.String()
	if !bytes.Contains([]byte(out2), []byte("transcode session update")) {
		t.Fatalf("expected transcode session update log for raw key, got: %s", out2)
	}

	// Verify the transcode type was set on both sessions via TrySetTranscodeType
	sessStore.mtx.Lock()
	ts350 := sessStore.sessions["350"].transcodeType
	ts351 := sessStore.sessions["351"].transcodeType
	sessStore.mtx.Unlock()

	if ts350 != "both" {
		t.Fatalf("expected session 350 transcodeType=both, got %q", ts350)
	}
	if ts351 != "both" {
		t.Fatalf("expected session 351 transcodeType=both, got %q", ts351)
	}
}

// Check both raw key and full-path forms map to the same session when parts contain the transcode ID
func TestOnTranscodeUpdate_RawAndFullKeyForms(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	sessStore := &sessions{sessions: map[string]session{}, server: s}
	// Session references transcode id 'faketsession6f5e4d3c2b1a'
	m := ttPlex.Metadata{SessionKey: "99", User: ttPlex.User{Title: "u"}, Player: ttPlex.Player{Product: "p"}, Media: []ttPlex.Media{{Part: []ttPlex.Part{{Key: "/transcode/sessions/faketsession6f5e4d3c2b1a"}}}}}

	sessStore.mtx.Lock()
	sessStore.sessions["99"] = session{session: m, state: statePlaying, playStarted: time.Now()}
	sessStore.mtx.Unlock()

	l := &plexListener{server: s, conn: &fakePlex{}, activeSessions: sessStore, log: logger}

	// raw key
	buf.Reset()
	cRaw := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "faketsession6f5e4d3c2b1a", VideoDecision: "transcode", AudioDecision: "transcode"}}}
	l.onTranscodeUpdateHandler(cRaw)
	if !bytes.Contains(buf.Bytes(), []byte("transcode session update")) {
		t.Fatalf("expected transcode session update for raw key, got: %s", buf.String())
	}

	sessStore.mtx.Lock()
	ss := sessStore.sessions["99"]
	if ss.transcodeType != "both" {
		sessStore.mtx.Unlock()
		t.Fatalf("expected transcodeType=both for session 99 after raw key, got %q", ss.transcodeType)
	}
	// reset for full path
	ss.transcodeType = ""
	sessStore.sessions["99"] = ss
	sessStore.mtx.Unlock()

	// full path
	buf.Reset()
	cFull := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "/transcode/sessions/faketsession6f5e4d3c2b1a", VideoDecision: "transcode", AudioDecision: "transcode"}}}
	l.onTranscodeUpdateHandler(cFull)
	if !bytes.Contains(buf.Bytes(), []byte("transcode session update")) {
		t.Fatalf("expected transcode session update for full path, got: %s", buf.String())
	}

	sessStore.mtx.Lock()
	if sessStore.sessions["99"].transcodeType != "both" {
		t.Fatalf("expected transcodeType=both for session 99 after full path, got %q", sessStore.sessions["99"].transcodeType)
	}
	sessStore.mtx.Unlock()
}

// TestOnPlayingHandlerError tests the error handling path of onPlayingHandler.
func TestOnPlayingHandlerError(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// Create a fake client that returns an error for GetSessions
	l := &plexListener{
		server:         s,
		conn:           &fakePlex{sessErr: errors.New("sessions fetch failed")},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            logger,
	}

	// Create a notification container with playing notifications
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "sess1",
			RatingKey:  "rk1",
			State:      "playing",
			ViewOffset: 1000,
		}, {
			SessionKey: "sess2",
			RatingKey:  "rk2",
			State:      "paused",
			ViewOffset: 2000,
		}},
	}

	// Call onPlayingHandler - should handle error gracefully
	l.onPlayingHandler(nc)

	// Should log error details
	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("error handling OnPlaying event")) {
		t.Fatalf("expected error log message, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"sessionKeys": "sess1,sess2"`)) {
		t.Fatalf("expected sessionKeys in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"ratingKeys": "rk1,rk2"`)) {
		t.Fatalf("expected ratingKeys in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"states": "playing,paused"`)) {
		t.Fatalf("expected states in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte("sessions fetch failed")) {
		t.Fatalf("expected error message in log, got: %s", out)
	}
}

// TestOnPlayingHandlerEmptyNotifications tests onPlayingHandler with empty notifications.
func TestOnPlayingHandlerEmptyNotifications(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            logger,
	}

	// Create empty notification container
	nc := ttPlex.NotificationContainer{}

	// Call onPlayingHandler - should succeed without issues
	l.onPlayingHandler(nc)

	// Should not log any errors
	out := buf.String()
	if bytes.Contains([]byte(out), []byte("error handling OnPlaying event")) {
		t.Fatalf("expected no error logs, but got: %s", out)
	}
}

// TestOnPlayingHandlerMultipleErrors tests error path with multiple notifications.
func TestOnPlayingHandlerMultipleErrors(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// Create a fake client that returns an error
	l := &plexListener{
		server:         s,
		conn:           &fakePlex{sessErr: errors.New("network error")},
		activeSessions: &sessions{sessions: map[string]session{}, server: s},
		log:            logger,
	}

	// Create notification with multiple sessions in different states
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "session_abc",
			RatingKey:  "rating_123",
			State:      "buffering",
			ViewOffset: 5000,
		}, {
			SessionKey: "session_def",
			RatingKey:  "rating_456",
			State:      "stopped",
			ViewOffset: 0,
		}, {
			SessionKey: "session_ghi",
			RatingKey:  "rating_789",
			State:      "playing",
			ViewOffset: 15000,
		}},
	}

	// Call onPlayingHandler
	l.onPlayingHandler(nc)

	// Verify error logging includes all session details
	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("error handling OnPlaying event")) {
		t.Fatalf("expected error log message, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"sessionKeys": "session_abc,session_def,session_ghi"`)) {
		t.Fatalf("expected all sessionKeys in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"ratingKeys": "rating_123,rating_456,rating_789"`)) {
		t.Fatalf("expected all ratingKeys in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"states": "buffering,stopped,playing"`)) {
		t.Fatalf("expected all states in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte("network error")) {
		t.Fatalf("expected network error in log, got: %s", out)
	}
}

// TestOnPlayingSessionNotFoundAfterRetries tests the case where a session is not found after retries.
func TestOnPlayingSessionNotFoundAfterRetries(t *testing.T) {
	// Save original values and restore after test
	origMaxRetries := SessionLookupMaxRetries
	origBaseDelay := SessionLookupBaseDelay
	defer func() {
		SessionLookupMaxRetries = origMaxRetries
		SessionLookupBaseDelay = origBaseDelay
	}()

	// Set low values for faster test
	SessionLookupMaxRetries = 1
	SessionLookupBaseDelay = 1 * time.Millisecond

	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// Create a fake that returns sessions but without the session we're looking for
	sessions := ttPlex.CurrentSessions{}
	sessions.MediaContainer.Metadata = []ttPlex.Metadata{{SessionKey: "different_session", User: ttPlex.User{Title: "user1", ID: "u1"}}}

	l := &plexListener{
		server:         s,
		conn:           &fakePlex{sessions: sessions},
		activeSessions: NewSessions(context.Background(), s),
		log:            logger,
	}

	// Create notification for a session that doesn't exist
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "missing_session",
			RatingKey:  "rk1",
			State:      "playing",
			ViewOffset: 1000,
		}},
	}

	// This should log a warning about session not found after retries
	if err := l.onPlaying(nc); err != nil {
		t.Fatalf("onPlaying should not return error, got: %v", err)
	}

	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("session not found for notification after retries, skipping")) {
		t.Fatalf("expected session not found warning, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"SessionKey": "missing_session"`)) {
		t.Fatalf("expected missing session key in log, got: %s", out)
	}
}

// TestOnPlayingMetadataErrorAfterRetries tests metadata fetch failure after retries.
func TestOnPlayingMetadataErrorAfterRetries(t *testing.T) {
	// Save original values and restore after test
	origMaxRetries := MetadataMaxRetries
	origBaseDelay := MetadataBaseDelay
	defer func() {
		MetadataMaxRetries = origMaxRetries
		MetadataBaseDelay = origBaseDelay
	}()

	// Set low values for faster test
	MetadataMaxRetries = 1
	MetadataBaseDelay = 1 * time.Millisecond

	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	l := &plexListener{
		server:         s,
		conn:           &fakePlex{metaErr: errors.New("metadata fetch failed")},
		activeSessions: NewSessions(context.Background(), s),
		log:            logger,
	}

	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "sess1",
			RatingKey:  "rk1",
			State:      "playing",
			ViewOffset: 1000,
		}},
	}

	// This should log an error about metadata fetch failure
	if err := l.onPlaying(nc); err != nil {
		t.Fatalf("onPlaying should not return error, got: %v", err)
	}

	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("error fetching metadata for notification after retries, skipping")) {
		t.Fatalf("expected metadata error log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"RatingKey": "rk1"`)) {
		t.Fatalf("expected rating key in log, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte("metadata fetch failed")) {
		t.Fatalf("expected metadata error message in log, got: %s", out)
	}
}

// TestOnPlayingEmptyMetadataAfterRetries tests empty metadata response after retries.
func TestOnPlayingEmptyMetadataAfterRetries(t *testing.T) {
	// Save original values and restore after test
	origMaxRetries := MetadataMaxRetries
	origBaseDelay := MetadataBaseDelay
	defer func() {
		MetadataMaxRetries = origMaxRetries
		MetadataBaseDelay = origBaseDelay
	}()

	// Set low values for faster test
	MetadataMaxRetries = 1
	MetadataBaseDelay = 1 * time.Millisecond

	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// Create metadata with empty metadata array - we'll use metadataMap that returns no results
	l := &plexListener{
		server:         s,
		conn:           &fakePlex{metadataMap: map[string]ttPlex.Metadata{}}, // Empty map means no metadata found
		activeSessions: NewSessions(context.Background(), s),
		log:            logger,
	}

	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "sess1",
			RatingKey:  "rk1",
			State:      "playing",
			ViewOffset: 1000,
		}},
	}

	// This should log a warning about empty metadata response
	if err := l.onPlaying(nc); err != nil {
		t.Fatalf("onPlaying should not return error, got: %v", err)
	}

	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("metadata response empty after retries, skipping")) {
		t.Fatalf("expected empty metadata warning, got: %s", out)
	}
	if !bytes.Contains([]byte(out), []byte(`"RatingKey": "rk1"`)) {
		t.Fatalf("expected rating key in log, got: %s", out)
	}
}

// TestOnPlayingStoppedSession tests handling of stopped session state.
func TestOnPlayingStoppedSession(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	sessStore := NewSessions(context.Background(), s)
	l := &plexListener{
		server:         s,
		conn:           &fakePlex{},
		activeSessions: sessStore,
		log:            log.NewNopLogger(),
	}

	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "sess1",
			RatingKey:  "rk1",
			State:      "stopped",
			ViewOffset: 1000,
		}},
	}

	if err := l.onPlaying(nc); err != nil {
		t.Fatalf("onPlaying should not return error, got: %v", err)
	}

	// Session should be updated with stopped state
	// We can't directly access the sessions map, but we can test the behavior indirectly
	// by ensuring no error occurred and the function completed successfully
}

func TestOnPlayingProcessesStopWhenSessionLookupFails(t *testing.T) {
	server := &Server{Name: "srv", ID: "id"}
	store := NewSessions(context.Background(), server)
	metadata := ttPlex.Metadata{
		SessionKey: "stopping-session",
		Media:      []ttPlex.Media{{Bitrate: 1000}},
	}
	store.Update("stopping-session", statePlaying, &metadata, &ttPlex.Metadata{RatingKey: "movie-1"})

	listener := &plexListener{
		server:         server,
		conn:           &fakePlex{sessErr: errors.New("sessions endpoint unavailable")},
		activeSessions: store,
		log:            log.NewNopLogger(),
	}
	notification := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{{
			SessionKey: "stopping-session",
			State:      "STOPPED",
		}},
	}

	if err := listener.onPlaying(notification); err != nil {
		t.Fatalf("stopped notification unexpectedly required session lookup: %v", err)
	}
	store.mtx.Lock()
	stopped := store.sessions["stopping-session"]
	store.mtx.Unlock()
	if stopped.state != stateStopped || stopped.prevPlayedTime <= 0 {
		t.Fatalf("stopped session was not finalized: %+v", stopped)
	}
}

func TestOnTranscodeUpdateIgnoresEmptySessionKey(t *testing.T) {
	server := &Server{Name: "srv", ID: "id"}
	store := NewSessions(context.Background(), server)
	metadata := ttPlex.Metadata{SessionKey: "real-session"}
	store.Update("real-session", statePlaying, &metadata, &ttPlex.Metadata{RatingKey: "movie-1"})
	listener := &plexListener{activeSessions: store, log: log.NewNopLogger()}

	listener.onTranscodeUpdateHandler(ttPlex.NotificationContainer{
		TranscodeSession: []ttPlex.TranscodeSession{{Key: " ", VideoDecision: "transcode"}},
	})

	store.mtx.Lock()
	defer store.mtx.Unlock()
	if len(store.sessions) != 1 || store.sessions["real-session"].transcodeType != "" {
		t.Fatalf("empty transcode key mutated session store: %+v", store.sessions)
	}
}

func TestListenerResultDistinguishesShutdownFromUnexpectedClose(t *testing.T) {
	if err := listenerResult(context.Background(), nil, false); !errors.Is(err, ErrListenerClosed) {
		t.Fatalf("unexpected close result = %v, want ErrListenerClosed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := listenerResult(ctx, errors.New("closed connection"), true); err != nil {
		t.Fatalf("shutdown listener result = %v, want nil", err)
	}
}

// TestSetTranscodeType tests all paths of the SetTranscodeType function.
func TestSetTranscodeType(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	sessStore := NewSessions(context.Background(), s)

	// Test 1: Fast path - exact key match
	sessStore.Update("sess1", statePlaying, &ttPlex.Metadata{SessionKey: "sess1"}, &ttPlex.Metadata{RatingKey: "rk1"})
	sessStore.SetTranscodeType("sess1", "video")

	if session, ok := sessStore.sessions["sess1"]; !ok || session.transcodeType != "video" {
		t.Fatalf("expected sess1 to have transcodeType=video")
	}

	// Test 2: Inner SessionKey match
	sessStore.Update("outer_key", statePlaying, &ttPlex.Metadata{SessionKey: "inner_session"}, &ttPlex.Metadata{RatingKey: "rk2"})
	sessStore.SetTranscodeType("inner_session", "audio")

	if session := sessStore.sessions["outer_key"]; session.transcodeType != "audio" {
		t.Fatalf("expected outer_key session to have transcodeType=audio, got %s", session.transcodeType)
	}

	// Test 3: Substring match (sessionID contains session key)
	sessStore.Update("short", statePlaying, &ttPlex.Metadata{SessionKey: "short"}, &ttPlex.Metadata{RatingKey: "rk3"})
	sessStore.SetTranscodeType("prefix_short_suffix", "both")

	if session := sessStore.sessions["short"]; session.transcodeType != "both" {
		t.Fatalf("expected short session to have transcodeType=both, got %s", session.transcodeType)
	}

	// Test 4: Substring match (session key contains sessionID)
	sessStore.Update("long_session_key", statePlaying, &ttPlex.Metadata{SessionKey: "long_session_key"}, &ttPlex.Metadata{RatingKey: "rk4"})

	// Clear the sessions map except for the one we want to test
	sessStore.mtx.Lock()
	temp := sessStore.sessions["long_session_key"]
	sessStore.sessions = map[string]session{"long_session_key": temp}
	sessStore.mtx.Unlock()

	sessStore.SetTranscodeType("session", "unknown")

	sessStore.mtx.Lock()
	session := sessStore.sessions["long_session_key"]
	sessStore.mtx.Unlock()

	if session.transcodeType != "unknown" {
		t.Fatalf("expected long_session_key to have transcodeType=unknown, got %s", session.transcodeType)
	}

	// Test 5: Fallback - create new entry
	sessStore.SetTranscodeType("new_session", "fallback")

	if session, ok := sessStore.sessions["new_session"]; !ok || session.transcodeType != "fallback" {
		t.Fatalf("expected new_session to be created with transcodeType=fallback")
	}
}

// TestSetSubtitleAction tests all paths of the SetSubtitleAction function.
func TestSetSubtitleAction(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	sessStore := NewSessions(context.Background(), s)

	// Test 1: Fast path - exact key match
	sessStore.Update("sess1", statePlaying, &ttPlex.Metadata{SessionKey: "sess1"}, &ttPlex.Metadata{RatingKey: "rk1"})
	sessStore.SetSubtitleAction("sess1", "burn")

	if session, ok := sessStore.sessions["sess1"]; !ok || session.subtitleAction != "burn" {
		t.Fatalf("expected sess1 to have subtitleAction=burn")
	}

	// Test 2: Inner SessionKey match
	sessStore.Update("outer_key", statePlaying, &ttPlex.Metadata{SessionKey: "inner_session"}, &ttPlex.Metadata{RatingKey: "rk2"})
	sessStore.SetSubtitleAction("inner_session", "copy")

	if session := sessStore.sessions["outer_key"]; session.subtitleAction != "copy" {
		t.Fatalf("expected outer_key session to have subtitleAction=copy, got %s", session.subtitleAction)
	}

	// Test 3: Substring match
	sessStore.Update("substring_test", statePlaying, &ttPlex.Metadata{SessionKey: "substring_test"}, &ttPlex.Metadata{RatingKey: "rk3"})
	sessStore.SetSubtitleAction("substring", "none")

	if session := sessStore.sessions["substring_test"]; session.subtitleAction != "none" {
		t.Fatalf("expected substring_test session to have subtitleAction=none, got %s", session.subtitleAction)
	}

	// Test 4: Fallback - create new entry
	sessStore.SetSubtitleAction("new_subtitle_session", "extract")

	if session, ok := sessStore.sessions["new_subtitle_session"]; !ok || session.subtitleAction != "extract" {
		t.Fatalf("expected new_subtitle_session to be created with subtitleAction=extract")
	}
}

// TestTrySetTranscodeType tests all paths of the TrySetTranscodeType function.
func TestTrySetTranscodeType(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	sessStore := NewSessions(context.Background(), s)

	// Test 1: Fast path - exact key match
	sessStore.Update("sess1", statePlaying, &ttPlex.Metadata{SessionKey: "sess1"}, &ttPlex.Metadata{RatingKey: "rk1"})
	if !sessStore.TrySetTranscodeType("sess1", "video") {
		t.Fatalf("expected TrySetTranscodeType to return true for exact match")
	}

	// Test 2: Inner SessionKey match
	sessStore.Update("outer_key", statePlaying, &ttPlex.Metadata{SessionKey: "inner_session"}, &ttPlex.Metadata{RatingKey: "rk2"})
	if !sessStore.TrySetTranscodeType("inner_session", "audio") {
		t.Fatalf("expected TrySetTranscodeType to return true for inner session match")
	}

	// Test 3: Substring match
	sessStore.Update("substring_test", statePlaying, &ttPlex.Metadata{SessionKey: "substring_test"}, &ttPlex.Metadata{RatingKey: "rk3"})
	if !sessStore.TrySetTranscodeType("substring", "both") {
		t.Fatalf("expected TrySetTranscodeType to return true for substring match")
	}

	// Test 4: Heuristic fallback - session with transcode decision
	metadata := &ttPlex.Metadata{
		SessionKey: "transcode_session",
		Media: []ttPlex.Media{{
			Part: []ttPlex.Part{{Decision: "transcode"}},
		}},
	}
	sessStore.Update("transcode_key", statePlaying, metadata, &ttPlex.Metadata{RatingKey: "rk4"})
	if !sessStore.TrySetTranscodeType("nonexistent_key", "heuristic") {
		t.Fatalf("expected TrySetTranscodeType to return true for heuristic fallback")
	}

	// Verify the transcode type was set on the session with transcode decision
	if session := sessStore.sessions["transcode_key"]; session.transcodeType != "heuristic" {
		t.Fatalf("expected transcode_key session to have transcodeType=heuristic, got %s", session.transcodeType)
	}

	// Test 5: No match found
	sessStore.mtx.Lock()
	sessStore.sessions = map[string]session{} // Clear all sessions
	sessStore.mtx.Unlock()

	if sessStore.TrySetTranscodeType("completely_nonexistent", "should_fail") {
		t.Fatalf("expected TrySetTranscodeType to return false when no match found")
	}
}

// TestTrySetSubtitleAction tests all paths of the TrySetSubtitleAction function.
func TestTrySetSubtitleAction(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	sessStore := NewSessions(context.Background(), s)

	// Test 1: Fast path - exact key match
	sessStore.Update("sess1", statePlaying, &ttPlex.Metadata{SessionKey: "sess1"}, &ttPlex.Metadata{RatingKey: "rk1"})
	if !sessStore.TrySetSubtitleAction("sess1", "burn") {
		t.Fatalf("expected TrySetSubtitleAction to return true for exact match")
	}

	// Test 2: Inner SessionKey match
	sessStore.Update("outer_key", statePlaying, &ttPlex.Metadata{SessionKey: "inner_session"}, &ttPlex.Metadata{RatingKey: "rk2"})
	if !sessStore.TrySetSubtitleAction("inner_session", "copy") {
		t.Fatalf("expected TrySetSubtitleAction to return true for inner session match")
	}

	// Test 3: Substring match
	sessStore.Update("substring_test", statePlaying, &ttPlex.Metadata{SessionKey: "substring_test"}, &ttPlex.Metadata{RatingKey: "rk3"})
	if !sessStore.TrySetSubtitleAction("substring", "none") {
		t.Fatalf("expected TrySetSubtitleAction to return true for substring match")
	}

	// Test 4: No match found
	if sessStore.TrySetSubtitleAction("completely_nonexistent", "should_fail") {
		t.Fatalf("expected TrySetSubtitleAction to return false when no match found")
	}
}

// TestOnPlayingHandlerSessionTranscodeMapping tests that the onPlayingHandler correctly captures session transcode mappings
func TestOnPlayingHandlerSessionTranscodeMapping(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	var buf bytes.Buffer
	logger := log.NewTestLogger(&buf)

	// Create fake client with sessions that match the notification
	fakeSessions := ttPlex.CurrentSessions{}
	fakeSessions.MediaContainer.Metadata = []ttPlex.Metadata{
		{
			SessionKey: "350",
			User:       ttPlex.User{Title: "user1", ID: "1"},
			Player:     ttPlex.Player{Product: "Plex Web"},
			Title:      "Test Movie",
			RatingKey:  "123", // Important: must match the notification RatingKey
		},
		{
			SessionKey: "352",
			User:       ttPlex.User{Title: "user2", ID: "2"},
			Player:     ttPlex.Player{Product: "Plex for TV"},
			Title:      "Test Show",
			RatingKey:  "456", // Important: must match the notification RatingKey
		},
	}

	// Create fake metadata responses
	metadataMap := map[string]ttPlex.Metadata{
		"123": {RatingKey: "123", Title: "Test Movie"},
		"456": {RatingKey: "456", Title: "Test Show"},
	}

	fakePlex := &fakePlex{
		sessions:    fakeSessions,
		metadataMap: metadataMap,
	}

	activeSessions := NewSessions(context.Background(), s)

	l := &plexListener{
		conn:           fakePlex,
		activeSessions: activeSessions,
		log:            logger,
	}

	// Build PlaySessionStateNotifications with TranscodeSession data like real Plex sends
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{
			{
				SessionKey:       "350",
				RatingKey:        "123",
				State:            "playing",
				TranscodeSession: "abc123-transcode-session",
			},
			{
				SessionKey:       "352",
				RatingKey:        "456",
				State:            "playing",
				TranscodeSession: "xyz789-transcode-session",
			},
		},
	}

	// Call the handler (which calls onPlaying internally)
	l.onPlayingHandler(nc)

	// Verify that session transcode mappings were established
	activeSessions.mtx.Lock()
	mapping1, exists1 := activeSessions.sessionTranscodeMap["350"]
	mapping2, exists2 := activeSessions.sessionTranscodeMap["352"]
	activeSessions.mtx.Unlock()

	if !exists1 || mapping1 != "abc123-transcode-session" {
		t.Fatalf("expected session 350 to be mapped to 'abc123-transcode-session', got exists=%v mapping=%q", exists1, mapping1)
	}
	if !exists2 || mapping2 != "xyz789-transcode-session" {
		t.Fatalf("expected session 352 to be mapped to 'xyz789-transcode-session', got exists=%v mapping=%q", exists2, mapping2)
	}

	// Test that transcode matching now works via the mapping
	if !activeSessions.TrySetTranscodeType("/transcode/sessions/abc123-transcode-session", "video") {
		t.Fatal("TrySetTranscodeType should work with established mapping for session 350")
	}
	if !activeSessions.TrySetTranscodeType("/transcode/sessions/xyz789-transcode-session", "audio") {
		t.Fatal("TrySetTranscodeType should work with established mapping for session 352")
	}

	// Verify the transcode types were set correctly
	activeSessions.mtx.Lock()
	if activeSessions.sessions["350"].transcodeType != "video" {
		t.Fatalf("expected session 350 transcodeType=video, got %q", activeSessions.sessions["350"].transcodeType)
	}
	if activeSessions.sessions["352"].transcodeType != "audio" {
		t.Fatalf("expected session 352 transcodeType=audio, got %q", activeSessions.sessions["352"].transcodeType)
	}
	activeSessions.mtx.Unlock()
}

// TestOnPlayingHandlerEmptyTranscodeSession tests handling of empty TranscodeSession fields
func TestOnPlayingHandlerEmptyTranscodeSession(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	fakeSessions := ttPlex.CurrentSessions{}
	fakeSessions.MediaContainer.Metadata = []ttPlex.Metadata{
		{SessionKey: "350", User: ttPlex.User{Title: "user1"}, RatingKey: "123"},
	}

	fakeMetadata := map[string]ttPlex.Metadata{
		"123": {RatingKey: "123", Title: "Test Movie"},
	}

	fakePlex := &fakePlex{
		sessions:    fakeSessions,
		metadataMap: fakeMetadata,
	}

	activeSessions := NewSessions(context.Background(), s)

	l := &plexListener{
		conn:           fakePlex,
		activeSessions: activeSessions,
		log:            log.NewNopLogger(),
	}

	// PlaySessionStateNotification with empty TranscodeSession (direct play scenario)
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{
			{
				SessionKey:       "350",
				RatingKey:        "123",
				State:            "playing",
				TranscodeSession: "", // Empty - direct play
			},
		},
	}

	l.onPlayingHandler(nc)

	// Verify that no mapping was created for empty transcode session
	activeSessions.mtx.Lock()
	_, exists := activeSessions.sessionTranscodeMap["350"]
	activeSessions.mtx.Unlock()

	if exists {
		t.Fatal("No mapping should be created for empty TranscodeSession")
	}
}

// TestOnPlayingHandlerMappingOverwrite tests that mappings can be overwritten when sessions change
func TestOnPlayingHandlerMappingOverwrite(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	fakeSessions := ttPlex.CurrentSessions{}
	fakeSessions.MediaContainer.Metadata = []ttPlex.Metadata{
		{SessionKey: "350", User: ttPlex.User{Title: "user1"}, RatingKey: "123"},
	}

	fakeMetadata := map[string]ttPlex.Metadata{
		"123": {RatingKey: "123", Title: "Test Movie"},
	}

	fakePlex := &fakePlex{
		sessions:    fakeSessions,
		metadataMap: fakeMetadata,
	}

	activeSessions := NewSessions(context.Background(), s)

	l := &plexListener{
		conn:           fakePlex,
		activeSessions: activeSessions,
		log:            log.NewNopLogger(),
	}

	// First notification with initial transcode session
	nc1 := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{
			{
				SessionKey:       "350",
				RatingKey:        "123",
				State:            "playing",
				TranscodeSession: "initial-transcode-id",
			},
		},
	}

	l.onPlayingHandler(nc1)

	// Verify initial mapping
	activeSessions.mtx.Lock()
	mapping, exists := activeSessions.sessionTranscodeMap["350"]
	activeSessions.mtx.Unlock()
	if !exists || mapping != "initial-transcode-id" {
		t.Fatalf("expected initial mapping to 'initial-transcode-id', got exists=%v mapping=%q", exists, mapping)
	}

	// Second notification with different transcode session (simulates quality change)
	nc2 := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{
			{
				SessionKey:       "350",
				RatingKey:        "123",
				State:            "playing",
				TranscodeSession: "updated-transcode-id",
			},
		},
	}

	l.onPlayingHandler(nc2)

	// Verify mapping was updated
	activeSessions.mtx.Lock()
	updatedMapping, exists := activeSessions.sessionTranscodeMap["350"]
	activeSessions.mtx.Unlock()
	if !exists || updatedMapping != "updated-transcode-id" {
		t.Fatalf("expected updated mapping to 'updated-transcode-id', got exists=%v mapping=%q", exists, updatedMapping)
	}

	// Verify old mapping no longer works
	if activeSessions.TrySetTranscodeType("/transcode/sessions/initial-transcode-id", "video") {
		t.Fatal("Old transcode mapping should no longer work")
	}

	// Verify new mapping works
	if !activeSessions.TrySetTranscodeType("/transcode/sessions/updated-transcode-id", "video") {
		t.Fatal("New transcode mapping should work")
	}
}

// TestOnPlayingHandlerMultipleSessionsNotification tests handling multiple sessions in one notification
func TestOnPlayingHandlerMultipleSessionsNotification(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	fakeSessions := ttPlex.CurrentSessions{}
	fakeSessions.MediaContainer.Metadata = []ttPlex.Metadata{
		{SessionKey: "100", User: ttPlex.User{Title: "user1"}, RatingKey: "1001"},
		{SessionKey: "200", User: ttPlex.User{Title: "user2"}, RatingKey: "2001"},
		{SessionKey: "300", User: ttPlex.User{Title: "user3"}, RatingKey: "3001"},
	}

	fakeMetadata := map[string]ttPlex.Metadata{
		"1001": {RatingKey: "1001", Title: "Movie 1"},
		"2001": {RatingKey: "2001", Title: "Movie 2"},
		"3001": {RatingKey: "3001", Title: "Movie 3"},
	}

	fakePlex := &fakePlex{
		sessions:    fakeSessions,
		metadataMap: fakeMetadata,
	}

	activeSessions := NewSessions(context.Background(), s)

	l := &plexListener{
		conn:           fakePlex,
		activeSessions: activeSessions,
		log:            log.NewNopLogger(),
	}

	// Notification with multiple sessions having different transcode states
	nc := ttPlex.NotificationContainer{
		PlaySessionStateNotification: []ttPlex.PlaySessionStateNotification{
			{
				SessionKey:       "100",
				RatingKey:        "1001",
				State:            "playing",
				TranscodeSession: "ts-100-abc",
			},
			{
				SessionKey:       "200",
				RatingKey:        "2001",
				State:            "playing",
				TranscodeSession: "", // Direct play
			},
			{
				SessionKey:       "300",
				RatingKey:        "3001",
				State:            "playing",
				TranscodeSession: "ts-300-xyz",
			},
		},
	}

	l.onPlayingHandler(nc)

	// Verify mappings for transcode sessions
	activeSessions.mtx.Lock()
	mapping100, exists100 := activeSessions.sessionTranscodeMap["100"]
	_, exists200 := activeSessions.sessionTranscodeMap["200"]
	mapping300, exists300 := activeSessions.sessionTranscodeMap["300"]
	activeSessions.mtx.Unlock()

	if !exists100 || mapping100 != "ts-100-abc" {
		t.Fatalf("expected session 100 mapping to 'ts-100-abc', got exists=%v mapping=%q", exists100, mapping100)
	}
	if exists200 {
		t.Fatal("session 200 should not have mapping (direct play)")
	}
	if !exists300 || mapping300 != "ts-300-xyz" {
		t.Fatalf("expected session 300 mapping to 'ts-300-xyz', got exists=%v mapping=%q", exists300, mapping300)
	}

	// Test that only transcoding sessions work with mapping
	if !activeSessions.TrySetTranscodeType("/transcode/sessions/ts-100-abc", "video") {
		t.Fatal("session 100 should match via mapping")
	}
	if activeSessions.TrySetTranscodeType("/transcode/sessions/nonexistent", "audio") {
		t.Fatal("session 200 (direct play) should not match any transcode path")
	}
	if !activeSessions.TrySetTranscodeType("/transcode/sessions/ts-300-xyz", "both") {
		t.Fatal("session 300 should match via mapping")
	}
}

// Test handler when activeSessions is nil — should not panic and should be no-op.
func TestOnTranscodeUpdateHandlerNilActiveSessions(t *testing.T) {
	l := &plexListener{activeSessions: nil, log: log.NewNopLogger()}
	c := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "noactive", VideoCodec: "hevc", SourceVideoCodec: "h264"}}}
	l.onTranscodeUpdateHandler(c) // must not panic
}

// Test that handler creates/sets a session transcodeType even when session didn't exist.
func TestOnTranscodeUpdateHandlerCreatesSession(t *testing.T) {
	sessStore := &sessions{sessions: map[string]session{}, server: &Server{Name: "srv", ID: "id"}}
	l := &plexListener{activeSessions: sessStore, log: log.NewNopLogger()}

	ts := ttPlex.TranscodeSession{Key: "s-create", SourceAudioCodec: "aac", AudioCodec: "mp3"}
	c := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{ts}}
	l.onTranscodeUpdateHandler(c)

	ss, ok := sessStore.sessions[ts.Key]
	if !ok {
		t.Fatalf("expected session %q to exist", ts.Key)
	}
	if ss.transcodeType != "audio" {
		t.Fatalf("expected transcodeType=audio, got %q", ss.transcodeType)
	}
	if ss.subtitleAction != "none" {
		t.Fatalf("expected subtitleAction=none, got %q", ss.subtitleAction)
	}
}

// Test that subtitleDecision values "transcode" and "transcoding" are
// interpreted as a distinct "transcode" subtitleAction.
func TestOnTranscodeUpdateHandlerSubtitleTranscodePreserved(t *testing.T) {
	sessStore := &sessions{sessions: map[string]session{}, server: &Server{Name: "srv", ID: "id"}}
	l := &plexListener{activeSessions: sessStore, log: log.NewNopLogger()}

	ts1 := ttPlex.TranscodeSession{Key: "s-transcode", SubtitleDecision: "transcode"}
	ts2 := ttPlex.TranscodeSession{Key: "s-transcoding", SubtitleDecision: "transcoding"}

	c := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{ts1, ts2}}
	l.onTranscodeUpdateHandler(c)

	for _, k := range []string{"s-transcode", "s-transcoding"} {
		ss, ok := sessStore.sessions[k]
		if !ok {
			t.Fatalf("expected session %q to exist", k)
		}
		if ss.subtitleAction != "transcode" {
			t.Fatalf("expected subtitleAction=transcode for session %q, got %q", k, ss.subtitleAction)
		}
	}
}

// Concurrent updates should not cause races; final value should be one of expected.
func TestOnTranscodeUpdateHandlerConcurrent(t *testing.T) {
	sessStore := &sessions{sessions: map[string]session{}, server: &Server{Name: "srv", ID: "id"}}
	l := &plexListener{activeSessions: sessStore, log: log.NewNopLogger()}

	var wg sync.WaitGroup
	kinds := []ttPlex.TranscodeSession{
		{Key: "concurrent", SourceVideoCodec: "h264", VideoCodec: "vp9"},
		{Key: "concurrent", SourceAudioCodec: "aac", AudioCodec: "mp3"},
		{Key: "concurrent", SourceVideoCodec: "h264", VideoCodec: "hevc", SourceAudioCodec: "aac", AudioCodec: "ac3"},
		{Key: "concurrent"},
	}

	for i := 0; i < 50; i++ {
		for _, k := range kinds {
			wg.Add(1)
			go func(ts ttPlex.TranscodeSession) {
				defer wg.Done()
				c := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{ts}}
				l.onTranscodeUpdateHandler(c)
			}(k)
		}
	}
	wg.Wait()

	ss, ok := sessStore.sessions["concurrent"]
	if !ok {
		t.Fatalf("expected session 'concurrent' to exist")
	}
	switch ss.transcodeType {
	case "audio", "video", "both", "unknown":
		// ok
	default:
		t.Fatalf("unexpected transcodeType %q", ss.transcodeType)
	}
	// subtitleAction should be one of these (or none)
	switch ss.subtitleAction {
	case "none", "copy", "burn", "transcode":
		// ok
	default:
		t.Fatalf("unexpected subtitleAction %q", ss.subtitleAction)
	}
}

// Integration-style test: ensure when websocket transcode update doesn't match
// an active session the listener consults the transcode-sessions API and
// applies the derived subtitle action and transcode kind to the session.
func TestTranscodeSessionsAPIFallbackPreservesTranscodeAction(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}

	// fake plex that returns no sessions from GetSessions but returns a
	// transcode sessions response containing a matching child
	fake := &fakePlex{}
	fake.transcodeErr = nil
	fake.sessions = ttPlex.CurrentSessions{} // empty so websocket key won't match

	// Build a TranscodeSessionsResponse child via the dependency type
	tc := ttPlex.TranscodeSessionsResponse{}
	tc.Children = []struct {
		ElementType      string  `json:"_elementType"`
		AudioChannels    int     `json:"audioChannels"`
		AudioCodec       string  `json:"audioCodec"`
		AudioDecision    string  `json:"audioDecision"`
		SubtitleDecision string  `json:"subtitleDecision"`
		Container        string  `json:"container"`
		Context          string  `json:"context"`
		Duration         int     `json:"duration"`
		Height           int     `json:"height"`
		Key              string  `json:"key"`
		Progress         float64 `json:"progress"`
		Protocol         string  `json:"protocol"`
		Remaining        int     `json:"remaining"`
		Speed            float64 `json:"speed"`
		Throttled        bool    `json:"throttled"`
		VideoCodec       string  `json:"videoCodec"`
		VideoDecision    string  `json:"videoDecision"`
		Width            int     `json:"width"`
	}{{
		Key:              "api-ts-1",
		VideoDecision:    "transcode",
		AudioDecision:    "transcode",
		SubtitleDecision: "transcode",
		Container:        "mp4",
	}}

	fake.transcodeErr = nil
	// Return our constructed response from the fake
	fake.transcodeResp = tc

	sessStore := NewSessions(context.Background(), s)
	l := &plexListener{server: s, conn: fake, activeSessions: sessStore, log: log.NewNopLogger()}

	// send a transcode update that doesn't match active sessions (key will be "api-ts-1")
	nc := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: "api-ts-1"}}}
	l.onTranscodeUpdateHandler(nc)

	// After handler, session should exist under key "api-ts-1" with transcodeType "both" and subtitleAction "transcode"
	ss, ok := sessStore.sessions["api-ts-1"]
	if !ok {
		t.Fatalf("expected session api-ts-1 to exist after API fallback")
	}
	if ss.transcodeType != "both" {
		t.Fatalf("expected transcodeType=both, got %q", ss.transcodeType)
	}
	if ss.subtitleAction != "transcode" {
		t.Fatalf("expected subtitleAction=transcode, got %q", ss.subtitleAction)
	}
}

func TestTranscodeSessionsAPIFallbackIgnoresEmptyKeys(t *testing.T) {
	response := ttPlex.TranscodeSessionsResponse{}
	if err := json.Unmarshal([]byte(`{"_children":[{"key":"","audioDecision":"transcode"}]}`), &response); err != nil {
		t.Fatalf("decode transcode response: %v", err)
	}

	store := NewSessions(context.Background(), &Server{})
	listener := &plexListener{
		conn: &fakePlex{
			sessions:      ttPlex.CurrentSessions{},
			transcodeResp: response,
		},
		activeSessions: store,
		log:            log.NewNopLogger(),
	}
	listener.onTranscodeUpdateHandler(ttPlex.NotificationContainer{
		TranscodeSession: []ttPlex.TranscodeSession{{
			Key:              "real-session",
			SourceVideoCodec: "h264",
			VideoCodec:       "hevc",
		}},
	})

	store.mtx.Lock()
	got := store.sessions["real-session"].transcodeType
	store.mtx.Unlock()
	if got != "video" {
		t.Fatalf("transcode type = %q, want websocket-derived video", got)
	}
}

// End-to-end test: simulate a transcode update indicating subtitleDecision="transcode"
// and assert the sessions collector emits a metric labeled subtitle_action="transcode".
func TestTranscodeUpdateEndToEndSubtitleBurn(t *testing.T) {
	s := &Server{Name: "srv", ID: "id"}
	// create sessions store and populate a session that will be associated with a transcode id
	sessStore := NewSessions(context.Background(), s)

	// Create a metadata entry that references a transcode session id in the media part
	transcodeID := "abc123-transcode-id"
	meta := ttPlex.Metadata{SessionKey: "sess-x", User: ttPlex.User{Title: "u"}, Player: ttPlex.Player{Product: "p"}, Media: []ttPlex.Media{{Part: []ttPlex.Part{{Key: "/transcode/sessions/" + transcodeID}}}}}

	// Insert session into store
	sessStore.mtx.Lock()
	sessStore.sessions["sess-x"] = session{session: meta, media: meta, state: statePlaying, playStarted: time.Now()}
	sessStore.mtx.Unlock()

	// Map the play session key to the transcode session ID so TrySetSubtitleAction can match
	sessStore.SetSessionTranscodeMapping("sess-x", transcodeID)

	// Create listener wired to our sessions store and a fake plex client
	l := &plexListener{server: s, conn: &fakePlex{}, activeSessions: sessStore, log: log.NewNopLogger()}

	// Simulate receiving a transcode update where SubtitleDecision is reported as "transcode"
	nc := ttPlex.NotificationContainer{TranscodeSession: []ttPlex.TranscodeSession{{Key: transcodeID, VideoDecision: "transcode", AudioDecision: "transcode", SubtitleDecision: "transcode"}}}
	l.onTranscodeUpdateHandler(nc)

	// Now collect metrics and assert that one of the emitted metrics contains subtitle_action="transcode"
	ch := make(chan prometheus.Metric, 32)
	sessStore.Collect(ch)
	close(ch)

	found := false
	for m := range ch {
		dto := &io_prometheus_client.Metric{}
		// convert to DTO to inspect labels
		if err := m.Write(dto); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		for _, lp := range dto.GetLabel() {
			if lp.GetName() == "subtitle_action" && lp.GetValue() == "transcode" {
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		t.Fatalf("expected to find a metric with subtitle_action=\"burn\" but none was emitted")
	}
}

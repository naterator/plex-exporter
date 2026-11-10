package plex

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ttPlex "github.com/timothystewart6/go-plex-client"

	"github.com/naterator/plex-exporter/pkg/metrics"
)

func TestGetSessionsContextRejectsMissingContainer(t *testing.T) {
	for _, body := range []string{"{}", "null"} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()

			client, err := NewClient(server.URL, "token")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.getSessionsContext(context.Background()); err == nil {
				t.Fatalf("malformed %s response was accepted", body)
			}
		})
	}
}

func TestGetSessionsContextAcceptsEmptyContainer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaContainer":{"size":0,"Metadata":[]}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := client.getSessionsContext(context.Background())
	if err != nil {
		t.Fatalf("empty session response failed: %v", err)
	}
	if len(metadata) != 0 {
		t.Fatalf("empty session response returned %d records", len(metadata))
	}
}

func TestPollSessionsAppliesRawTranscodeHardwareEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status/sessions" {
			t.Fatalf("request path = %q, want /status/sessions", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaContainer":{"Metadata":[{
			"sessionKey":"session-raw",
			"User":{"title":"viewer"},
			"Player":{"state":"playing"},
			"Media":[{"videoCodec":"h264","Part":[{"decision":"transcode"}]}],
			"TranscodeSession":{
				"key":"/transcode/sessions/raw-1",
				"videoDecision":"transcode",
				"transcodeHwDecoding":"vaapi",
				"transcodeHwEncoding":""
			}
		},{
			"sessionKey":"session-partial",
			"Player":{"state":"playing"},
			"Media":[{"videoCodec":"h264","Part":[{"decision":"transcode"}]}],
			"TranscodeSession":{
				"key":"/transcode/sessions/partial-1",
				"videoDecision":"transcode",
				"transcodeHwEncoding":"qsv"
			}
		}]}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	plexServer := &Server{URL: client.URL, Token: client.Token, Client: client}
	store := &sessions{
		sessions:            map[string]session{},
		sessionTranscodeMap: map[string]string{},
		server:              plexServer,
		maxTrackedSessions:  defaultMaxTrackedSessions,
	}
	store.Update("session-raw", statePlaying, &ttPlex.Metadata{SessionKey: "session-raw"}, nil)
	store.Update("session-partial", statePlaying, &ttPlex.Metadata{SessionKey: "session-partial"}, nil)

	if err := plexServer.pollSessions(context.Background(), store, defaultSessionStaleTimeout); err != nil {
		t.Fatalf("pollSessions: %v", err)
	}
	current := store.sessions["session-raw"]
	if current.transcodeHWDecode != "hardware" || current.transcodeHWEncode != "software" ||
		!current.transcodeHWKnown {
		t.Fatalf("raw hardware evidence = %+v", current)
	}
	partial := store.sessions["session-partial"]
	if partial.transcodeHWDecode != "unknown" || partial.transcodeHWEncode != "hardware" ||
		!partial.transcodeHWKnown {
		t.Fatalf("partial hardware evidence = %+v", partial)
	}

	seen := map[string]float64{}
	registry := prometheus.NewRegistry()
	if err := registry.Register(store); err != nil {
		t.Fatalf("register session collector: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather session collector: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "plex_session_video_transcode" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			seen[labels["session"]+":"+labels["decode"]+":"+labels["encode"]] = metric.GetGauge().GetValue()
		}
	}
	if seen["session-raw:hardware:software"] != 1 {
		t.Fatalf("software-outcome video metric = %v, want 1", seen["session-raw:hardware:software"])
	}
	if seen["session-partial:unknown:hardware"] != 1 {
		t.Fatalf("partial-outcome video metric = %v, want 1", seen["session-partial:unknown:hardware"])
	}
}

func TestPollSessionsDoesNotOverwriteNewerTranscodeEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"MediaContainer":{"Metadata":[{
			"sessionKey":"session-newer",
			"Player":{"state":"playing"},
			"TranscodeSession":{"videoDecision":"transcode","transcodeHwDecoding":"vaapi","transcodeHwEncoding":"nvenc"}
		}]}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	plexServer := &Server{URL: client.URL, Token: client.Token, Client: client}
	observedEvent := time.Now().Add(time.Second)
	store := &sessions{
		sessions: map[string]session{
			"session-newer": {
				session:           ttPlex.Metadata{SessionKey: "session-newer"},
				state:             statePlaying,
				lastUpdate:        observedEvent,
				transcodeHWDecode: "software",
				transcodeHWEncode: "unknown",
				transcodeHWKnown:  true,
			},
		},
		sessionTranscodeMap: map[string]string{},
		server:              plexServer,
	}

	if err := plexServer.pollSessions(context.Background(), store, defaultSessionStaleTimeout); err != nil {
		t.Fatalf("pollSessions: %v", err)
	}
	current := store.sessions["session-newer"]
	if current.transcodeHWDecode != "software" || current.transcodeHWEncode != "unknown" {
		t.Fatalf("newer event was overwritten by poll = %+v", current)
	}
}

func TestMarkStaleFreezesAtLastConfirmedObservation(t *testing.T) {
	confirmed := time.Now().Add(-10 * time.Second)
	started := confirmed.Add(-20 * time.Second)
	store := &sessions{
		sessions: map[string]session{
			"session-1": {
				session:     ttPlex.Metadata{SessionKey: "session-1", Media: []ttPlex.Media{{Bitrate: 1000}}},
				state:       statePlaying,
				playStarted: started,
				confirmedAt: confirmed,
				lastUpdate:  confirmed,
			},
		},
		sessionTranscodeMap: map[string]string{},
	}
	store.markStale(time.Now(), 5*time.Second)

	current := store.sessions["session-1"]
	if current.state != stateUnknown {
		t.Fatalf("stale session state = %q, want unknown", current.state)
	}
	if current.prevPlayedTime < 19*time.Second || current.prevPlayedTime > 21*time.Second {
		t.Fatalf("frozen play time = %s, want about 20s", current.prevPlayedTime)
	}
}

func TestSessionTrackerEvictsStaleRecordAtBound(t *testing.T) {
	store := &sessions{
		sessions: map[string]session{
			"old": {state: stateUnknown, lastUpdate: time.Now().Add(-time.Minute)},
		},
		sessionTranscodeMap: map[string]string{},
		maxTrackedSessions:  1,
	}
	store.Update("new", statePlaying, &ttPlex.Metadata{SessionKey: "new"}, nil)
	if _, ok := store.sessions["old"]; ok {
		t.Fatal("stale session was not evicted at the configured bound")
	}
	if _, ok := store.sessions["new"]; !ok {
		t.Fatal("new session was rejected despite an evictable stale record")
	}
}

func TestSessionPlayCounterDoesNotDecreaseAcrossStaleResume(t *testing.T) {
	now := time.Now()
	metadata := ttPlex.Metadata{
		SessionKey: "session-counter",
		User:       ttPlex.User{Title: "viewer"},
		Media:      []ttPlex.Media{{Bitrate: 1000}},
	}
	store := &sessions{
		sessions: map[string]session{
			"session-counter": {
				session:     metadata,
				media:       metadata,
				state:       statePlaying,
				playStarted: now.Add(-20 * time.Second),
				confirmedAt: now.Add(-5 * time.Second),
				lastUpdate:  now.Add(-5 * time.Second),
			},
		},
		sessionTranscodeMap: map[string]string{},
		server:              &Server{Name: "server", ID: "id"},
	}

	collect := func() float64 {
		channel := make(chan prometheus.Metric, 64)
		store.Collect(channel)
		close(channel)
		for metric := range channel {
			if metric.Desc().String() != metrics.MetricSessionPlaySecondsTotalDesc.String() {
				continue
			}
			value, _, err := readMetric(metric)
			if err != nil {
				t.Fatalf("read stable session counter: %v", err)
			}
			return value
		}
		t.Fatal("stable session counter was not collected")
		return 0
	}

	first := collect()
	store.markStale(time.Now(), time.Second)
	unknown := collect()
	if unknown < first {
		t.Fatalf("session counter decreased at stale transition: before=%v after=%v", first, unknown)
	}

	store.mtx.Lock()
	resumed := store.sessions["session-counter"]
	resumed.state = statePlaying
	resumed.playStarted = time.Now().Add(-2 * time.Second)
	resumed.confirmedAt = time.Now().Add(-time.Second)
	resumed.lastUpdate = resumed.confirmedAt
	store.sessions["session-counter"] = resumed
	store.mtx.Unlock()
	if resumedValue := collect(); resumedValue < unknown {
		t.Fatalf("session counter decreased after resume: before=%v after=%v", unknown, resumedValue)
	}
}

func TestInactiveSessionGaugesAreZero(t *testing.T) {
	for _, state := range []sessionState{stateStopped, stateUnknown} {
		t.Run(string(state), func(t *testing.T) {
			metadata := ttPlex.Metadata{
				SessionKey: "inactive-" + string(state),
				User:       ttPlex.User{Title: "viewer"},
				Player:     ttPlex.Player{Local: true},
				Session:    ttPlex.Session{Bandwidth: 2000, Location: "lan"},
				Media: []ttPlex.Media{{
					Bitrate:    1000,
					VideoCodec: "h264",
					Part:       []ttPlex.Part{{Decision: "transcode"}},
				}},
			}
			store := &sessions{
				sessions: map[string]session{
					metadata.SessionKey: {
						session:              metadata,
						media:                metadata,
						state:                state,
						transcodeType:        "video",
						transcodeHWDecode:    "hardware",
						transcodeHWEncode:    "hardware",
						transcodeHWKnown:     true,
						transcodeHWRequested: true,
					},
				},
				sessionTranscodeMap: map[string]string{},
				server:              &Server{Name: "server", ID: "id"},
			}
			channel := make(chan prometheus.Metric, 64)
			store.Collect(channel)
			close(channel)
			seen := map[string]float64{}
			for metric := range channel {
				value, labels, err := readMetric(metric)
				if err != nil {
					t.Fatalf("read session metric: %v", err)
				}
				if labels["session"] != metadata.SessionKey {
					continue
				}
				seen[metric.Desc().String()] = value
			}
			for _, descriptor := range []*prometheus.Desc{
				metrics.MetricSessionActiveDesc,
				metrics.MetricSessionBitrateKbpsDesc,
				metrics.MetricSessionBandwidthKbpsDesc,
				metrics.MetricSessionVideoTranscodeDesc,
			} {
				value, ok := seen[descriptor.String()]
				if !ok {
					t.Fatalf("missing inactive metric %s", descriptor)
				}
				if value != 0 {
					t.Fatalf("inactive metric %s = %v, want 0", descriptor, value)
				}
			}
		})
	}
}

func TestHardwareRequestWithoutOutcomeIsNotHardwareEvidence(t *testing.T) {
	metadata := ttPlex.Metadata{
		SessionKey: "requested-only",
		User:       ttPlex.User{Title: "viewer"},
		Media:      []ttPlex.Media{{VideoCodec: "h264", Part: []ttPlex.Part{{Decision: "transcode"}}}},
	}
	store := &sessions{
		sessions: map[string]session{
			metadata.SessionKey: {session: metadata, media: metadata, state: statePlaying, transcodeType: "video"},
		},
		sessionTranscodeMap: map[string]string{},
		server:              &Server{Name: "server", ID: "id"},
	}
	store.SetTranscodeHardware(metadata.SessionKey, true)
	channel := make(chan prometheus.Metric, 64)
	store.Collect(channel)
	close(channel)
	for metric := range channel {
		if metric.Desc().String() == metrics.MetricSessionVideoTranscodeDesc.String() {
			t.Fatal("hardware-requested session emitted hardware evidence without decoder/encoder outcome")
		}
	}
}

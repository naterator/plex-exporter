package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type probeStatus struct{ healthy, ready bool }

func (s probeStatus) Healthy() bool { return s.healthy }
func (s probeStatus) Ready() bool   { return s.ready }

func TestHealthProbesSeparateProcessAndPlexAvailability(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       probeStatus
		live, ready int
	}{
		{"starting or Plex unavailable", probeStatus{true, false}, 200, 503},
		{"ready", probeStatus{true, true}, 200, 200},
		{"closed", probeStatus{false, false}, 503, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := newMetricsMux(tc.state)
			for path, want := range map[string]int{"/healthz": tc.live, "/readyz": tc.ready} {
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
				if w.Code != want {
					t.Fatalf("%s status %d, want %d", path, w.Code, want)
				}
				if w.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("probe response can be cached")
				}
			}
		})
	}
}

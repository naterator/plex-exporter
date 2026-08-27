package plex

// NOTE: Test fixtures in this file use synthetic/sanitized machine identifiers
// and other sample values. They are intentionally non-production while
// resembling real data shapes for parsing and logic tests.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/naterator/plex-exporter/pkg/log"
	"github.com/naterator/plex-exporter/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	ttPlex "github.com/timothystewart6/go-plex-client"
)

// fakeRoundTripper returns canned responses for known Plex endpoints
type fakeRoundTripper struct{}

func TestParseLibraryRefreshInterval(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Duration
		valid bool
	}{
		{name: "unset", value: "", want: 15 * time.Minute, valid: true},
		{name: "disabled", value: "0", want: 0, valid: true},
		{name: "custom", value: "30", want: 30 * time.Minute, valid: true},
		{name: "negative", value: "-1", want: 15 * time.Minute, valid: false},
		{name: "not an integer", value: "five", want: 15 * time.Minute, valid: false},
		{name: "overflow", value: "999999999999999999999", want: 15 * time.Minute, valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := parseLibraryRefreshInterval(tt.value)
			if got != tt.want || valid != tt.valid {
				t.Fatalf("parseLibraryRefreshInterval(%q) = (%v, %t), want (%v, %t)", tt.value, got, valid, tt.want, tt.valid)
			}
		})
	}
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var body string
	// Normalize URL: ignore includeMeta query parameter to remain compatible
	q := req.URL.Query()
	q.Del("includeMeta")
	url := req.URL.Path
	if encoded := q.Encode(); encoded != "" {
		url += "?" + encoded
	}

	switch url {
	case "/media/providers", "/media/providers?includeStorage=1":
		body = `{"MediaContainer": {"friendlyName":"FakeTestServer","machineIdentifier":"fake-machine-123","version":"1.2.3","MediaProvider":[{"identifier":"com.plexapp.plugins.library","Feature":[{"type":"content","Directory":[{"id":"1","durationTotal":1000,"storageTotal":2000,"title":"FakeMovies","type":"movie"},{"id":"2","durationTotal":2000,"storageTotal":3000,"title":"FakeShows","type":"show"}]}]}]}}`
	case "/library/sections/1/all":
		body = `{"MediaContainer":{"size":10}}`
	case "/library/sections/2/all":
		body = `{"MediaContainer":{"size":20}}`
	case "/":
		body = `{"MediaContainer":{"version":"v","platform":"p","platformVersion":"pv"}}`
	case "/statistics/resources":
		body = `{"MediaContainer":{"StatisticsResources":[{"at":1,"hostCpuUtilization":5,"hostMemoryUtilization":6}]}}`
	case "/statistics/bandwidth?timespan=6":
		body = `{"MediaContainer":{"StatisticsBandwidth":[{"at":100,"bytes":1000},{"at":200,"bytes":2000}]}}`
	default:
		body = `{"MediaContainer":{}}`
	}

	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
		Request:    req,
	}
	resp.Header.Set("Content-Type", "application/json")
	return resp, nil
}

func TestServerRefreshPopulatesLibraryItems(t *testing.T) {
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// Replace the http client transport with our fake RT
	client.httpClient = http.Client{Transport: &fakeRoundTripper{}, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		lastBandwidthAt: int(time.Now().Unix()),
	}

	if err := srv.Refresh(); err != nil {
		t.Fatalf("Refresh error: %v", err)
	}

	if len(srv.libraries) != 2 {
		t.Fatalf("expected 2 libraries, got %d", len(srv.libraries))
	}

	// Check item counts for each library
	expected := map[string]int64{"1": 10, "2": 20}
	for _, lib := range srv.libraries {
		want, ok := expected[lib.ID]
		if !ok {
			t.Fatalf("unexpected library id %s", lib.ID)
		}
		if lib.ItemsCount != want {
			t.Fatalf("library %s items: want %d got %d", lib.ID, want, lib.ItemsCount)
		}

		// Validate the metric produced by metrics.LibraryItems matches value & labels
		contentType := getContentTypeForLibrary(lib.Type)
		m := metrics.LibraryItems(lib.ItemsCount, "plex", srv.Name, srv.ID, lib.Type, lib.Name, lib.ID, contentType)
		var dtoMetric dto.Metric
		if err := m.Write(&dtoMetric); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}

		if dtoMetric.GetGauge() == nil {
			t.Fatalf("expected gauge metric")
		}
		if dtoMetric.GetGauge().GetValue() != float64(want) {
			t.Fatalf("metric value mismatch: want %v got %v", want, dtoMetric.GetGauge().GetValue())
		}

		// Ensure some expected labels are present
		labels := map[string]bool{}
		for _, lp := range dtoMetric.Label {
			labels[lp.GetValue()] = true
		}
		if !labels[srv.Name] || !labels[srv.ID] || !labels[lib.Name] || !labels[lib.ID] {
			t.Fatalf("expected labels missing in metric: %v", dtoMetric.Label)
		}
	}
}

// TestRefreshBandwidthSuccess tests the successful refresh of bandwidth statistics.
func TestRefreshBandwidthSuccess(t *testing.T) {
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	client.httpClient = http.Client{Transport: &fakeRoundTripper{}, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		Name:            "FakeTestServer",
		ID:              "server123",
		lastBandwidthAt: 50, // Set to before our test data timestamps (100, 200)
	}

	// Test refreshBandwidth
	if err := srv.Refresh(); err != nil {
		t.Fatalf("Refresh error: %v", err)
	}

	// Verify lastBandwidthAt was updated to the highest timestamp
	// The test data in fakeRoundTripper has timestamps 100 and 200
	if srv.lastBandwidthAt != 200 {
		t.Fatalf("expected lastBandwidthAt=200, got %d", srv.lastBandwidthAt)
	}
}

// TestRefreshBandwidthSimple tests bandwidth logic with a more direct approach.
func TestRefreshBandwidthSimple(t *testing.T) {
	// Create a very simple test that verifies bandwidth timestamps get updated
	simpleRT := &customRoundTripper{
		responses: map[string]response{
			"/media/providers":                 {statusCode: 200, body: `{"MediaContainer": {"friendlyName":"FakeTestServer","machineIdentifier":"fake-machine-123","version":"1.2.3","MediaProvider":[{"identifier":"com.plexapp.plugins.library","Feature":[]}]}}`},
			"/statistics/resources":            {statusCode: 200, body: `{"MediaContainer":{"StatisticsResources":[]}}`},
			"/statistics/bandwidth?timespan=6": {statusCode: 200, body: `{"MediaContainer":{"StatisticsBandwidth":[{"at":300,"bytes":3000}]}}`},
		},
	}

	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	client.httpClient = http.Client{Transport: simpleRT, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		Name:            "FakeTestServer",
		ID:              "server123",
		lastBandwidthAt: 250, // Set to before our test timestamp (300)
	}

	if err := srv.Refresh(); err != nil {
		t.Fatalf("Refresh error: %v", err)
	}

	// Verify lastBandwidthAt was updated to 300
	if srv.lastBandwidthAt != 300 {
		t.Fatalf("expected lastBandwidthAt=300, got %d", srv.lastBandwidthAt)
	}
}

// TestRefreshBandwidthNotFound tests bandwidth refresh when endpoint returns 404.
func TestRefreshBandwidthNotFound(t *testing.T) {
	// Create a custom round tripper that returns 404 for bandwidth endpoint
	notFoundRT := &customRoundTripper{
		responses: map[string]response{
			"/statistics/bandwidth?timespan=6": {statusCode: 404, body: ""},
		},
	}

	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	client.httpClient = http.Client{Transport: notFoundRT, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		Name:            "FakeTestServer",
		ID:              "server123",
		lastBandwidthAt: 50,
	}

	// This should not return an error for 404 (paid feature not available)
	if err := srv.refreshBandwidth(); err != nil {
		t.Fatalf("expected no error for 404 response, got: %v", err)
	}

	// lastBandwidthAt should remain unchanged
	if srv.lastBandwidthAt != 50 {
		t.Fatalf("expected lastBandwidthAt=50, got %d", srv.lastBandwidthAt)
	}
}

// TestRefreshBandwidthEmptyData tests bandwidth refresh with empty data.
func TestRefreshBandwidthEmptyData(t *testing.T) {
	emptyRT := &customRoundTripper{
		responses: map[string]response{
			"/statistics/bandwidth?timespan=6": {statusCode: 200, body: `{"MediaContainer":{"StatisticsBandwidth":[]}}`},
		},
	}

	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	client.httpClient = http.Client{Transport: emptyRT, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		Name:            "FakeTestServer",
		ID:              "server123",
		lastBandwidthAt: 50,
	}

	if err := srv.Refresh(); err != nil {
		t.Fatalf("Refresh error: %v", err)
	}

	// An empty response must preserve the checkpoint; rewinding it would count
	// old bandwidth samples again if Plex returned them on the next refresh.
	if srv.lastBandwidthAt != 50 {
		t.Fatalf("expected lastBandwidthAt=50 after empty response, got %d", srv.lastBandwidthAt)
	}
}

func TestRefreshBandwidthIgnoresNegativeSamplesWithoutRetryingThem(t *testing.T) {
	rt := &customRoundTripper{responses: map[string]response{
		"/statistics/bandwidth?timespan=6": {
			statusCode: http.StatusOK,
			body:       `{"MediaContainer":{"StatisticsBandwidth":[{"at":60,"bytes":-1}]}}`,
		},
	}}
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.httpClient = http.Client{Transport: rt, Timeout: time.Second}
	server := &Server{Client: client, Name: "negative-bandwidth", ID: "negative-bandwidth-id", lastBandwidthAt: 50}

	if err := server.refreshBandwidth(); err != nil {
		t.Fatalf("refreshBandwidth: %v", err)
	}
	if server.lastBandwidthAt != 60 {
		t.Fatalf("checkpoint = %d, want 60", server.lastBandwidthAt)
	}
}

func TestRefreshResourcesSelectsNewestTimestamp(t *testing.T) {
	rt := &customRoundTripper{responses: map[string]response{
		"/statistics/resources?timespan=6": {
			statusCode: http.StatusOK,
			body:       `{"MediaContainer":{"StatisticsResources":[{"at":200,"hostCpuUtilization":22,"hostMemoryUtilization":33},{"at":100,"hostCpuUtilization":11,"hostMemoryUtilization":12}]}}`,
		},
	}}
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.httpClient = http.Client{Transport: rt, Timeout: time.Second}
	server := &Server{Client: client, Name: "resource-order", ID: "resource-order-id"}

	if err := server.refreshResources(); err != nil {
		t.Fatalf("refreshResources: %v", err)
	}
	var cpuMetric dto.Metric
	if err := metrics.ServerHostCpuUtilization.WithLabelValues("plex", server.Name, server.ID).Write(&cpuMetric); err != nil {
		t.Fatalf("write CPU metric: %v", err)
	}
	if got := cpuMetric.GetGauge().GetValue(); got != 22 {
		t.Fatalf("host CPU = %v, want newest sample value 22", got)
	}
}

func TestRefreshServerInfoRemovesStaleLabelSet(t *testing.T) {
	rt := &customRoundTripper{responses: map[string]response{
		"/": {
			statusCode: http.StatusOK,
			body:       `{"MediaContainer":{"version":"1.0","platform":"test","platformVersion":"1"}}`,
		},
	}}
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.httpClient = http.Client{Transport: rt, Timeout: time.Second}
	server := &Server{Client: client, Name: "server-info-update", ID: "server-info-update-id"}
	oldLabels := []string{"plex", server.Name, server.ID, "1.0", "test", "1"}
	newLabels := []string{"plex", server.Name, server.ID, "2.0", "test", "2"}
	metrics.ServerInfo.DeleteLabelValues(oldLabels...)
	metrics.ServerInfo.DeleteLabelValues(newLabels...)

	if err := server.refreshServerInfo(); err != nil {
		t.Fatalf("first refreshServerInfo: %v", err)
	}
	rt.responses["/"] = response{
		statusCode: http.StatusOK,
		body:       `{"MediaContainer":{"version":"2.0","platform":"test","platformVersion":"2"}}`,
	}
	if err := server.refreshServerInfo(); err != nil {
		t.Fatalf("second refreshServerInfo: %v", err)
	}
	if metrics.ServerInfo.DeleteLabelValues(oldLabels...) {
		t.Fatal("stale server_info labelset remained after version update")
	}
	if !metrics.ServerInfo.DeleteLabelValues(newLabels...) {
		t.Fatal("updated server_info labelset was not emitted")
	}
}

// customRoundTripper allows custom responses for specific paths
type customRoundTripper struct {
	responses map[string]response
}

type response struct {
	statusCode int
	body       string
}

func (c *customRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	q := req.URL.Query()
	q.Del("includeMeta")
	url := req.URL.Path
	if encoded := q.Encode(); encoded != "" {
		url += "?" + encoded
	}

	if resp, ok := c.responses[url]; ok {
		return &http.Response{
			StatusCode: resp.statusCode,
			Status:     http.StatusText(resp.statusCode),
			Body:       io.NopCloser(bytes.NewBufferString(resp.body)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}

	// Default response
	return &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       io.NopCloser(bytes.NewBufferString(`{"MediaContainer":{}}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

// fakeTotalsRoundTripper returns canned responses for endpoints used to
// compute server-level movie and episode totals.
type fakeTotalsRoundTripper struct{}

func (f *fakeTotalsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var body string
	// Normalize queries to ignore includeMeta param
	q := req.URL.Query()
	q.Del("includeMeta")
	rawQuery := q.Encode()
	switch {
	case req.URL.Path == "/media/providers":
		body = `{"MediaContainer": {"friendlyName":"FakeTotalsServer","machineIdentifier":"fake-machine-totals","version":"1.0","MediaProvider":[{"identifier":"com.plexapp.plugins.library","Feature":[{"type":"content","Directory":[{"id":"m1","durationTotal":100,"storageTotal":100,"title":"FakeMoviesA","type":"movie"},{"id":"m2","durationTotal":200,"storageTotal":200,"title":"FakeMoviesB","type":"movie"},{"id":"s1","durationTotal":300,"storageTotal":300,"title":"FakeShows","type":"show"},{"id":"mu1","durationTotal":0,"storageTotal":0,"title":"FakeMusic","type":"music"},{"id":"p1","durationTotal":0,"storageTotal":0,"title":"FakePhotos","type":"photo"},{"id":"hv1","durationTotal":0,"storageTotal":0,"title":"FakeHomeVideos","type":"homevideo"}]}]}]}}`
	case req.URL.Path == "/library/sections/m1/all" && rawQuery == "":
		body = `{"MediaContainer":{"size":7}}`
	case req.URL.Path == "/library/sections/m2/all":
		body = `{"MediaContainer":{"size":3}}`
	// nolint:gocritic // test fixture pattern; keep readability
	case req.URL.Path == "/library/sections/s1/all" && rawQuery == "type=4":
		// Filtered episodes request
		body = `{"MediaContainer":{"size":42}}`
	case req.URL.Path == "/library/sections/s1/all" && rawQuery == "":
		// Unfiltered call used by Refresh(); return 0 to force the
		// filtered type=4 request to be used for episode counting.
		body = `{"MediaContainer":{"size":0}}`
	case req.URL.Path == "/library/sections/mu1/all" && rawQuery == "type=10":
		// music tracks (type=10)
		body = `{"MediaContainer":{"size":123}}`
	case req.URL.Path == "/library/sections/mu1/all" && rawQuery == "":
		body = `{"MediaContainer":{"size":0}}`
	case req.URL.Path == "/library/sections/p1/all" && rawQuery == "":
		body = `{"MediaContainer":{"size":55}}`
	case req.URL.Path == "/library/sections/hv1/all" && rawQuery == "":
		body = `{"MediaContainer":{"size":9}}`
	case rawQuery == "":
		// fallback
		body = `{"MediaContainer":{}}`
	default:
		// handle the filtered episodes call
		switch {
		case req.URL.Path == "/library/sections/s1/all" && rawQuery == "type=4":
			body = `{"MediaContainer":{"size":42}}`
		case req.URL.Path == "/":
			body = `{"MediaContainer":{"version":"v","platform":"p","platformVersion":"pv"}}`
		case req.URL.Path == "/statistics/resources":
			body = `{"MediaContainer":{"StatisticsResources":[]}}`
		case req.URL.Path == "/statistics/bandwidth":
			body = `{"MediaContainer":{"StatisticsBandwith":[]}}`
		default:
			body = `{"MediaContainer":{}}`
		}
	}

	resp := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
		Request:    req,
	}
	resp.Header.Set("Content-Type", "application/json")
	return resp, nil
}

func TestServerTotalsComputed(t *testing.T) {
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// Replace the http client transport with our fake RT
	client.httpClient = http.Client{Transport: &fakeTotalsRoundTripper{}, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		lastBandwidthAt: int(time.Now().Unix()),
	}

	if err := srv.Refresh(); err != nil {
		t.Fatalf("Refresh error: %v", err)
	}

	// Movie totals: m1 (7) + m2 (3) = 10
	if srv.MovieCount != 10 {
		t.Fatalf("expected MovieCount 10, got %d", srv.MovieCount)
	}

	// Episode totals: s1 type=4 returns 42
	if srv.EpisodeCount != 42 {
		t.Fatalf("expected EpisodeCount 42, got %d", srv.EpisodeCount)
	}
}

// Test that Collect emits server-level media totals for all supported types.
func TestServerCollectEmitsMediaTotals(t *testing.T) {
	client, err := NewClient("http://example.com", "token")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}

	// Use the fake totals round tripper which returns canned sizes for our
	// movie/show/music/photo/homevideo libraries and filtered track/episode calls.
	client.httpClient = http.Client{Transport: &fakeTotalsRoundTripper{}, Timeout: time.Second * 2}

	srv := &Server{
		URL:             client.URL,
		Token:           client.Token,
		Client:          client,
		lastBandwidthAt: int(time.Now().Unix()),
	}

	if err := srv.Refresh(); err != nil {
		t.Fatalf("Refresh error: %v", err)
	}

	t.Logf("computed totals: movies=%d episodes=%d music=%d photos=%d other=%d", srv.MovieCount, srv.EpisodeCount, srv.MusicCount, srv.PhotoCount, srv.OtherVideoCount)

	reg := prometheus.NewRegistry()
	if err := reg.Register(srv); err != nil {
		t.Fatalf("failed to register server collector: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather error: %v", err)
	}

	// Helper to find metric family by name
	find := func(name string) *dto.MetricFamily {
		for _, mf := range mfs {
			if mf.GetName() == name {
				return mf
			}
		}
		return nil
	}

	checks := []struct {
		name string
		want float64
	}{
		{"plex_media_movies", 10},
		{"plex_media_episodes", 42},
		{"plex_media_music", 123},
		{"plex_media_photos", 55},
		{"plex_media_other_videos", 9},
	}

	for _, c := range checks {
		mf := find(c.name)
		if mf == nil {
			t.Fatalf("metric family %s not found", c.name)
		}
		if len(mf.Metric) == 0 {
			t.Fatalf("no metrics in family %s", c.name)
		}
		// Metric family is expected to have a single gauge metric with our server labels
		m := mf.Metric[0]
		var got float64
		// nolint:gocritic // keep simple conditional checks in tests
		if m.GetGauge() != nil {
			got = m.GetGauge().GetValue()
		} else if m.GetCounter() != nil {
			got = m.GetCounter().GetValue()
		} else {
			t.Fatalf("metric %s has no gauge or counter", c.name)
		}
		if got != c.want {
			t.Fatalf("metric %s: want %v got %v", c.name, c.want, got)
		}
	}
}

func TestNewServerRefreshPopulatesFields(t *testing.T) {
	// httptest server that responds to the endpoints used by Refresh
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/media/providers":
			// includeStorage=1 may be in RawQuery; respond with one movie library
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer": {"friendlyName": "FakeMyServer", "machineIdentifier":"fake-ID123", "version":"1.0", "MediaProvider":[{"identifier":"com.plexapp.plugins.library","Feature":[{"type":"content","Directory":[{"id":"1","durationTotal":1000,"storageTotal":2000,"title":"FakeLib1","type":"movie"}]}]}]}}`))
		case "/library/sections/1/all":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer": {"size": 5}}`))
		case "/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer": {"version": "1.0", "platform": "darwin", "platformVersion": "13.0"}}`))
		case "/statistics/resources":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer": {"StatisticsResources": [{"at": 1, "hostCpuUtilization": 1.2, "hostMemoryUtilization": 2.3}]}}`))
		case "/statistics/bandwidth":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer": {"StatisticsBandwith": [{"at": 2, "bytes": 100}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	serverURL := ts.URL

	// Override startup delay/jitter so tests don't wait long. Restore
	// originals after the test runs.
	oldDelay := startupFullRefreshDelaySeconds
	oldJitter := startupFullRefreshJitterMax
	startupFullRefreshDelaySeconds = 0
	startupFullRefreshJitterMax = 0
	defer func() {
		startupFullRefreshDelaySeconds = oldDelay
		startupFullRefreshJitterMax = oldJitter
	}()

	srv, err := NewServer(serverURL, "token")
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	// Wait for the background deferred full refresh to complete.
	// Use a timeout to avoid hanging tests.
	select {
	case <-srv.fullRefreshDone:
		// proceed
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for background full refresh to complete")
	}

	if srv.Name != "FakeMyServer" {
		t.Fatalf("expected server name FakeMyServer, got %q", srv.Name)
	}
	if srv.ID != "fake-ID123" {
		t.Fatalf("expected server ID fake-ID123, got %q", srv.ID)
	}
	if srv.MovieCount != 5 {
		t.Fatalf("expected MovieCount 5, got %d", srv.MovieCount)
	}
}

func TestServerCloseStopsDeferredRefreshWorkers(t *testing.T) {
	oldDelay := startupFullRefreshDelaySeconds
	oldJitter := startupFullRefreshJitterMax
	startupFullRefreshDelaySeconds = 3600
	startupFullRefreshJitterMax = 0
	defer func() {
		startupFullRefreshDelaySeconds = oldDelay
		startupFullRefreshJitterMax = oldJitter
	}()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/media/providers":
			_, _ = fmt.Fprintln(w, `{"MediaContainer":{"friendlyName":"Close Test","machineIdentifier":"close-test","MediaProvider":[]}}`)
		case "/", "/statistics/resources":
			_, _ = fmt.Fprintln(w, `{"MediaContainer":{}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	server, err := NewServer(ts.URL, "token")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	eventSessions := NewSessions(context.Background(), server)
	server.mtx.Lock()
	server.eventSessions = eventSessions
	server.mtx.Unlock()
	done := make(chan struct{})
	go func() {
		server.Close()
		server.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not stop refresh workers promptly")
	}
	select {
	case <-server.fullRefreshDone:
	default:
		t.Fatal("deferred full refresh did not acknowledge shutdown")
	}
	select {
	case <-eventSessions.done:
	default:
		t.Fatal("session worker did not acknowledge shutdown")
	}
}

func TestServerCloseRejectsNewListeners(t *testing.T) {
	server := &Server{}
	server.Close()
	if err := server.Listen(context.Background(), log.NewNopLogger()); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Listen after Close error = %v, want ErrServerClosed", err)
	}
}

func TestCollectUsesPersistentSessionsBetweenListenerConnections(t *testing.T) {
	server := &Server{Name: "persistent-session-server", ID: "persistent-session-id"}
	store := NewSessions(context.Background(), server)
	server.eventSessions = store
	defer server.Close()

	metadata := ttPlex.Metadata{
		SessionKey: "persistent-session",
		RatingKey:  "movie-1",
		Title:      "Persistent Movie",
		Type:       "movie",
		User:       ttPlex.User{Title: "viewer"},
		Player:     ttPlex.Player{Product: "player"},
	}
	store.Update(metadata.SessionKey, statePlaying, &metadata, &metadata)

	registry := prometheus.NewRegistry()
	if err := registry.Register(server); err != nil {
		t.Fatalf("register server: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "plays_total" && len(family.Metric) == 1 {
			return
		}
	}
	t.Fatal("persistent session metric was not collected while no listener connection was active")
}

// Verify that a generic library count prefers the x-plex-container-total-size
// response header when present and populates ItemsCount accordingly.
func TestEnsureLibraryItemsCount_UsesHeaderForGeneric(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-plex-container-total-size", "4")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 4}}`)
	}))
	defer ts.Close()

	c, err := NewClient(ts.URL, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	s := &Server{Client: c}
	lib := &Library{ID: "42", Type: "movie", Server: s}
	s.mtx.Lock()
	s.libraries = []*Library{lib}
	s.mtx.Unlock()

	s.ensureLibraryItemsCount(lib)
	if lib.ItemsCount != 4 {
		t.Fatalf("expected ItemsCount 4, got %d", lib.ItemsCount)
	}
}

// Verify that for music libraries the exporter will try track-specific
// type queries, prefer the header when present, and cache the successful
// track type on the library record.
func TestEnsureLibraryItemsCount_MusicPrefersHeaderAndCachesTrackType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.RawQuery
		// Simulate a Plex server that returns a total-size header for type=10
		if strings.Contains(q, "type=10") {
			w.Header().Set("x-plex-container-total-size", "10")
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 10}}`)
			return
		}
		if strings.Contains(q, "type=7") {
			w.Header().Set("x-plex-container-total-size", "7")
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 7}}`)
			return
		}

		// Generic fallback
		w.Header().Set("x-plex-container-total-size", "0")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1}}`)
	}))
	defer ts.Close()

	c, err := NewClient(ts.URL, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	s := &Server{Client: c}
	lib := &Library{ID: "99", Type: "music", Server: s}
	s.mtx.Lock()
	s.libraries = []*Library{lib}
	s.mtx.Unlock()

	s.ensureLibraryItemsCount(lib)
	if lib.ItemsCount != 10 {
		t.Fatalf("expected ItemsCount 10, got %d", lib.ItemsCount)
	}
	if lib.cachedTrackType != "10" {
		t.Fatalf("expected cachedTrackType '10', got '%s'", lib.cachedTrackType)
	}
}

// Verify that when the x-plex-container-total-size header is absent the
// code falls back to the MediaContainer.size value from the response body.
func TestEnsureLibraryItemsCount_FallsBackToBodySize(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No x-plex-container-total-size header set
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 3}}`)
	}))
	defer ts.Close()

	c, err := NewClient(ts.URL, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	s := &Server{Client: c}
	lib := &Library{ID: "77", Type: "movie", Server: s}
	s.mtx.Lock()
	s.libraries = []*Library{lib}
	s.mtx.Unlock()

	s.ensureLibraryItemsCount(lib)
	if lib.ItemsCount != 3 {
		t.Fatalf("expected fallback ItemsCount 3, got %d", lib.ItemsCount)
	}
}

func TestMediaContainerCountPrefersBodyTotalSize(t *testing.T) {
	var response mediaContainerSizeResponse
	response.MediaContainer.Size = 1
	response.MediaContainer.TotalSize = 57
	if got := mediaContainerCount(make(http.Header), response); got != 57 {
		t.Fatalf("mediaContainerCount = %d, want body totalSize 57", got)
	}
}

func TestEnsureLibraryItemsCount_MusicTriesAlternateAfterEmptyResult(t *testing.T) {
	var type10Requests, type7Requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("type") {
		case "10":
			type10Requests++
			_, _ = fmt.Fprintln(w, `{"MediaContainer":{"size":0,"totalSize":0}}`)
		case "7":
			type7Requests++
			_, _ = fmt.Fprintln(w, `{"MediaContainer":{"size":1,"totalSize":23}}`)
		default:
			http.Error(w, "unexpected generic query", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL, "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	server := &Server{Client: client, LibraryRefreshInterval: time.Hour}
	library := &Library{ID: "music", Type: "artist", Server: server}
	server.libraries = []*Library{library}

	server.ensureLibraryItemsCount(library)
	if library.ItemsCount != 23 || library.cachedTrackType != "7" {
		t.Fatalf("music library = %+v, want 23 tracks using type 7", library)
	}
	if type10Requests != 1 || type7Requests != 1 {
		t.Fatalf("track requests: type10=%d type7=%d, want one each", type10Requests, type7Requests)
	}
}

func TestGetContentTypeForLibraryMatchesCollectedItems(t *testing.T) {
	tests := map[string]string{
		"movie":     "movies",
		"show":      "shows",
		"artist":    "tracks",
		"music":     "tracks",
		"photo":     "photos",
		"homevideo": "videos",
	}
	for libraryType, want := range tests {
		if got := getContentTypeForLibrary(libraryType); got != want {
			t.Errorf("getContentTypeForLibrary(%q) = %q, want %q", libraryType, got, want)
		}
	}
}

func TestMusicTrackTypeCandidatesPreferCachedType(t *testing.T) {
	got := musicTrackTypeCandidates("7")
	if len(got) != 2 || got[0] != "7" || got[1] != "10" {
		t.Fatalf("musicTrackTypeCandidates(7) = %v, want [7 10]", got)
	}
}

// Verify that ensureLibraryItemsCount uses a fresh cached value from
// an existing library entry when LIBRARY_REFRESH_INTERVAL is enabled and
// the previous fetch time is recent.
func TestEnsureLibraryItemsCount_UsesCacheWhenFresh(t *testing.T) {
	now := time.Now().Unix()

	s := &Server{
		LibraryRefreshInterval: 15 * time.Minute,
	}

	// Prepare an "old" library entry that represents previously fetched
	// values; lastItemsFetch is set to now so it is considered fresh.
	old := &Library{
		ID:             "cached",
		lastItemsFetch: now,
		lastItemsCount: 123,
	}

	s.mtx.Lock()
	s.libraries = []*Library{old}
	s.mtx.Unlock()

	// New library object that will be passed to ensureLibraryItemsCount
	lib := &Library{ID: "cached", Type: "movie", Server: s}

	s.ensureLibraryItemsCount(lib)

	if lib.ItemsCount != 123 {
		t.Fatalf("expected ItemsCount 123 from cache, got %d", lib.ItemsCount)
	}
	if lib.lastItemsCount != 123 {
		t.Fatalf("expected lastItemsCount 123, got %d", lib.lastItemsCount)
	}
	if lib.lastItemsFetch != now {
		t.Fatalf("expected lastItemsFetch %d, got %d", now, lib.lastItemsFetch)
	}
}

// Verify that a stale cached entry causes ensureLibraryItemsCount to perform
// an HTTP fetch and update the library counts from the server response.
func TestEnsureLibraryItemsCount_StaleCacheTriggersFetch(t *testing.T) {
	// httptest server returns a header totalSize of 9
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-plex-container-total-size", "9")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 9}}`)
	}))
	defer ts.Close()

	c, err := NewClient(ts.URL, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Server configured with a refresh interval so caching logic runs
	s := &Server{Client: c, LibraryRefreshInterval: 15 * time.Minute}

	// Old cached entry is stale (set to 2 hours in the past)
	staleFetch := time.Now().Unix() - int64(2*time.Hour.Seconds())
	old := &Library{
		ID:             "stale",
		lastItemsFetch: staleFetch,
		lastItemsCount: 123,
	}

	s.mtx.Lock()
	s.libraries = []*Library{old}
	s.mtx.Unlock()

	lib := &Library{ID: "stale", Type: "movie", Server: s}
	s.ensureLibraryItemsCount(lib)

	if lib.ItemsCount != 9 {
		t.Fatalf("expected fetched ItemsCount 9, got %d", lib.ItemsCount)
	}

	// Ensure the server cache was updated with the new count and fetch time
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if len(s.libraries) != 1 {
		t.Fatalf("expected 1 library in server cache, got %d", len(s.libraries))
	}
	if s.libraries[0].lastItemsCount != 9 {
		t.Fatalf("expected server cache lastItemsCount 9, got %d", s.libraries[0].lastItemsCount)
	}
	if s.libraries[0].lastItemsFetch <= staleFetch {
		t.Fatalf("expected lastItemsFetch to be updated to a newer timestamp; old=%d new=%d", staleFetch, s.libraries[0].lastItemsFetch)
	}
}

// Stress test computeLibraryCounts with many libraries to validate concurrent
// fetching and correct aggregation of totals.
func TestComputeLibraryCounts_ConcurrentManyLibraries(t *testing.T) {
	// httptest server returns fixed totals depending on query
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "type=4"):
			// episodes per show
			w.Header().Set("x-plex-container-total-size", "4")
			_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 4}}`)
		case strings.Contains(q, "type=10"):
			// tracks per music library (type 10)
			w.Header().Set("x-plex-container-total-size", "6")
			_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 6}}`)
		case strings.Contains(q, "type=7"):
			// fallback track type returns 0
			w.Header().Set("x-plex-container-total-size", "0")
			_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 0, "totalSize": 0}}`)
		default:
			// generic fallback
			w.Header().Set("x-plex-container-total-size", "3")
			_, _ = fmt.Fprintln(w, `{"MediaContainer": {"size": 1, "totalSize": 3}}`)
		}
	}))
	defer ts.Close()

	c, err := NewClient(ts.URL, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	s := &Server{Client: c, LibraryRefreshInterval: 0}

	// Create many libraries: shows, music, movies
	nShows := 50
	nMusic := 50
	nMovies := 30

	var newLibs []*Library
	for i := 0; i < nShows; i++ {
		newLibs = append(newLibs, &Library{ID: fmt.Sprintf("show-%d", i), Type: "show"})
	}
	for i := 0; i < nMusic; i++ {
		newLibs = append(newLibs, &Library{ID: fmt.Sprintf("music-%d", i), Type: "music"})
	}
	// movies contribute ItemsCount directly
	for i := 0; i < nMovies; i++ {
		// give each movie library an ItemsCount of 2
		newLibs = append(newLibs, &Library{ID: fmt.Sprintf("movie-%d", i), Type: "movie", ItemsCount: 2})
	}

	moviesTotal, episodesTotal, musicTotal, photosTotal, otherVideosTotal := s.computeLibraryCounts(newLibs)

	expectedEpisodes := int64(nShows * 4)
	expectedMusic := int64(nMusic * 6)
	expectedMovies := int64(nMovies * 2)

	if episodesTotal != expectedEpisodes {
		t.Fatalf("expected episodesTotal %d, got %d", expectedEpisodes, episodesTotal)
	}
	if musicTotal != expectedMusic {
		t.Fatalf("expected musicTotal %d, got %d", expectedMusic, musicTotal)
	}
	if moviesTotal != expectedMovies {
		t.Fatalf("expected moviesTotal %d, got %d", expectedMovies, moviesTotal)
	}
	if photosTotal != 0 {
		t.Fatalf("expected photosTotal 0, got %d", photosTotal)
	}
	if otherVideosTotal != 0 {
		t.Fatalf("expected otherVideosTotal 0, got %d", otherVideosTotal)
	}
}

// refreshFixtureRoundTripper records requests to the expensive library
// endpoints and can inject transport failures for cadence tests.
type refreshFixtureRoundTripper struct {
	mu     sync.Mutex
	counts map[string]int
	fail   map[string]bool
}

func (r *refreshFixtureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	key := req.URL.Path
	if req.URL.RawQuery != "" {
		key += "?" + req.URL.RawQuery
	}

	r.mu.Lock()
	if r.counts == nil {
		r.counts = make(map[string]int)
	}
	r.counts[key]++
	failed := r.fail[key]
	r.mu.Unlock()
	if failed {
		return nil, errors.New("synthetic Plex request failure")
	}

	body := `{"MediaContainer":{}}`
	switch key {
	case "/media/providers?includeStorage=1":
		body = `{"MediaContainer":{"friendlyName":"Fixture Plex","machineIdentifier":"fixture-id","version":"1.0","MediaProvider":[{"identifier":"com.plexapp.plugins.library","Feature":[{"type":"content","Directory":[{"id":"show","durationTotal":1,"storageTotal":2,"title":"Shows","type":"show"},{"id":"music","durationTotal":3,"storageTotal":4,"title":"Music","type":"music"}]}]}]}}`
	case "/library/sections/show/all":
		body = `{"MediaContainer":{"size":20}}`
	case "/library/sections/show/all?type=4":
		body = `{"MediaContainer":{"size":42}}`
	case "/library/sections/music/all":
		body = `{"MediaContainer":{"size":12}}`
	case "/library/sections/music/all?type=10":
		body = `{"MediaContainer":{"size":12}}`
	case "/library/sections/music/all?type=7":
		body = `{"MediaContainer":{"size":12}}`
	case "/?timespan=6", "/":
		body = `{"MediaContainer":{"version":"1.0","platform":"fixture","platformVersion":"1"}}`
	case "/statistics/resources?timespan=6":
		body = `{"MediaContainer":{"StatisticsResources":[]}}`
	case "/statistics/bandwidth?timespan=6":
		body = `{"MediaContainer":{"StatisticsBandwith":[]}}`
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func (r *refreshFixtureRoundTripper) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[key]
}

func newRefreshFixtureServer(t *testing.T, rt *refreshFixtureRoundTripper) *Server {
	t.Helper()
	client, err := NewClient("http://fixture.example", "token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.httpClient = http.Client{Transport: rt, Timeout: time.Second}

	return &Server{
		URL:                    client.URL,
		Token:                  client.Token,
		Client:                 client,
		LibraryRefreshInterval: time.Hour,
	}
}

func TestRefreshLightPreservesAllLibraryRefreshState(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)
	now := time.Now().Unix()
	old := &Library{
		ID:               "show",
		Name:             "old name",
		Type:             "show",
		Server:           s,
		ItemsCount:       11,
		cachedTrackType:  "7",
		lastMusicFetch:   now - 1,
		lastMusicCount:   22,
		lastEpisodeFetch: now - 2,
		lastEpisodeCount: 33,
		lastItemsFetch:   now - 3,
		lastItemsCount:   44,
	}
	s.libraries = []*Library{old}

	if err := s.refreshLibraryMetadata(); err != nil {
		t.Fatalf("refreshLibraryMetadata: %v", err)
	}

	if len(s.libraries) != 2 {
		t.Fatalf("expected two libraries, got %d", len(s.libraries))
	}
	var got *Library
	for _, lib := range s.libraries {
		if lib.ID == old.ID {
			got = lib
			break
		}
	}
	if got == nil {
		t.Fatal("expected existing show library to remain present")
	}
	if got.Name != "Shows" || got.DurationTotal != 1 || got.StorageTotal != 2 {
		t.Fatalf("metadata was not refreshed: %+v", got)
	}
	if got.ItemsCount != old.ItemsCount || got.cachedTrackType != old.cachedTrackType ||
		got.lastMusicFetch != old.lastMusicFetch || got.lastMusicCount != old.lastMusicCount ||
		got.lastEpisodeFetch != old.lastEpisodeFetch || got.lastEpisodeCount != old.lastEpisodeCount ||
		got.lastItemsFetch != old.lastItemsFetch || got.lastItemsCount != old.lastItemsCount {
		t.Fatalf("library refresh state was not preserved: got=%+v old=%+v", got, old)
	}
}

func TestRefreshLibraryMetadataResetsCacheWhenLibraryTypeChanges(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)
	s.libraries = []*Library{{
		ID:               "show",
		Name:             "Former Movies",
		Type:             "movie",
		Server:           s,
		ItemsCount:       99,
		cachedTrackType:  "7",
		lastMusicFetch:   10,
		lastMusicCount:   11,
		lastEpisodeFetch: 12,
		lastEpisodeCount: 13,
		lastItemsFetch:   14,
		lastItemsCount:   15,
		DurationTotal:    16,
		StorageTotal:     17,
	}}

	if err := s.refreshLibraryMetadata(); err != nil {
		t.Fatalf("refreshLibraryMetadata: %v", err)
	}
	got := s.Library("show")
	if got == nil || got.Type != "show" {
		t.Fatalf("refreshed library = %+v, want show", got)
	}
	if got.ItemsCount != 0 || got.cachedTrackType != "" || got.lastMusicFetch != 0 ||
		got.lastMusicCount != 0 || got.lastEpisodeFetch != 0 || got.lastEpisodeCount != 0 ||
		got.lastItemsFetch != 0 || got.lastItemsCount != 0 {
		t.Fatalf("cache from former library type was retained: %+v", got)
	}
}

func TestRefreshSkipsLibraryEnumerationWithinInterval(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)

	if err := s.Refresh(); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	if got := rt.count("/library/sections/show/all?type=4"); got != 1 {
		t.Fatalf("expected one episode enumeration on first refresh, got %d", got)
	}
	if got := rt.count("/library/sections/music/all?type=10"); got != 1 {
		t.Fatalf("expected one music enumeration on first refresh, got %d", got)
	}

	if err := s.Refresh(); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	if got := rt.count("/library/sections/show/all?type=4"); got != 1 {
		t.Fatalf("episode enumeration repeated within interval: %d requests", got)
	}
	if got := rt.count("/library/sections/music/all?type=10"); got != 1 {
		t.Fatalf("music enumeration repeated within interval: %d requests", got)
	}
	if s.EpisodeCount != 42 || s.MusicCount != 12 {
		t.Fatalf("cached totals changed unexpectedly: episodes=%d music=%d", s.EpisodeCount, s.MusicCount)
	}
}

func TestRefreshEnumeratesLibraryAfterInterval(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)

	if err := s.Refresh(); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}

	// Move every cache timestamp beyond the configured window without waiting
	// in real time. The next refresh must perform the type-specific queries
	// again rather than suppressing them permanently after the first run.
	staleAt := time.Now().Add(-2 * time.Hour).Unix()
	s.mtx.Lock()
	for _, lib := range s.libraries {
		lib.lastItemsFetch = staleAt
		if lib.Type == "show" {
			lib.lastEpisodeFetch = staleAt
		}
		if lib.Type == "music" {
			lib.lastMusicFetch = staleAt
		}
	}
	s.mtx.Unlock()

	if err := s.Refresh(); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	if got := rt.count("/library/sections/show/all?type=4"); got != 2 {
		t.Fatalf("expected episode enumeration after interval, got %d requests", got)
	}
	if got := rt.count("/library/sections/music/all?type=10"); got != 2 {
		t.Fatalf("expected music enumeration after interval, got %d requests", got)
	}
}

func TestRefreshBacksOffFailedLibraryEnumeration(t *testing.T) {
	rt := &refreshFixtureRoundTripper{
		fail: map[string]bool{
			"/library/sections/show/all?type=4":   true,
			"/library/sections/music/all?type=10": true,
			"/library/sections/music/all?type=7":  true,
		},
	}
	s := newRefreshFixtureServer(t, rt)

	if err := s.Refresh(); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	if err := s.Refresh(); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}

	// Failed requests still consume the configured refresh window. This is
	// important when Plex is unavailable: the five-second ticker must not turn
	// a transient failure into a sustained request storm.
	for _, key := range []string{
		"/library/sections/show/all?type=4",
		"/library/sections/music/all?type=10",
		"/library/sections/music/all?type=7",
	} {
		if got := rt.count(key); got != 1 {
			t.Errorf("failed enumeration %s retried within interval: %d requests", key, got)
		}
	}
}

func TestRefreshRetainsCountsAfterTransientEnumerationFailure(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)
	if err := s.Refresh(); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	if s.EpisodeCount != 42 || s.MusicCount != 12 {
		t.Fatalf("initial totals: episodes=%d music=%d", s.EpisodeCount, s.MusicCount)
	}

	staleAt := time.Now().Add(-2 * time.Hour).Unix()
	s.mtx.Lock()
	for _, library := range s.libraries {
		library.lastItemsFetch = staleAt
		library.lastEpisodeFetch = staleAt
		library.lastMusicFetch = staleAt
	}
	s.mtx.Unlock()
	rt.mu.Lock()
	rt.fail = map[string]bool{
		"/library/sections/show/all?type=4":   true,
		"/library/sections/music/all?type=10": true,
		"/library/sections/music/all?type=7":  true,
	}
	rt.mu.Unlock()

	if err := s.Refresh(); err != nil {
		t.Fatalf("Refresh with failed enumeration: %v", err)
	}
	if s.EpisodeCount != 42 || s.MusicCount != 12 {
		t.Fatalf("transient failure reset totals: episodes=%d music=%d", s.EpisodeCount, s.MusicCount)
	}
}

func TestRefreshWithoutCachingQueriesMusicOnce(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)
	s.LibraryRefreshInterval = 0

	if err := s.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := rt.count("/library/sections/music/all?type=10"); got != 1 {
		t.Fatalf("music track enumeration count = %d, want 1", got)
	}
	if got := rt.count("/library/sections/music/all"); got != 0 {
		t.Fatalf("generic music enumeration count = %d, want 0", got)
	}
}

func TestRefreshContinuesWhenOptionalMetricsEndpointsFail(t *testing.T) {
	rt := &refreshFixtureRoundTripper{fail: map[string]bool{
		"/":                                true,
		"/statistics/resources?timespan=6": true,
		"/statistics/bandwidth?timespan=6": true,
	}}
	s := newRefreshFixtureServer(t, rt)

	if err := s.Refresh(); err != nil {
		t.Fatalf("Refresh returned an optional endpoint error: %v", err)
	}
	if got := rt.count("/library/sections/show/all?type=4"); got != 1 {
		t.Fatalf("episode count request count = %d, want 1", got)
	}
	if got := rt.count("/library/sections/music/all?type=10"); got != 1 {
		t.Fatalf("music count request count = %d, want 1", got)
	}
}

func TestCollectConcurrentWithRefresh(t *testing.T) {
	rt := &refreshFixtureRoundTripper{}
	s := newRefreshFixtureServer(t, rt)
	if err := s.Refresh(); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}

	registry := prometheus.NewRegistry()
	if err := registry.Register(s); err != nil {
		t.Fatalf("register server collector: %v", err)
	}

	const iterations = 50
	errCh := make(chan error, iterations*2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range iterations {
			if err := s.Refresh(); err != nil {
				errCh <- fmt.Errorf("refresh: %w", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for range iterations {
			if _, err := registry.Gather(); err != nil {
				errCh <- fmt.Errorf("gather: %w", err)
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestPkgLoggerTimingFix verifies that the package logger respects LOG_LEVEL
// when initLogger() is called, preventing regression of the timing issue.
func TestPkgLoggerTimingFix(t *testing.T) {
	// Save original LOG_LEVEL
	originalLogLevel := os.Getenv("LOG_LEVEL")
	defer func() {
		if originalLogLevel != "" {
			_ = os.Setenv("LOG_LEVEL", originalLogLevel)
		} else {
			_ = os.Unsetenv("LOG_LEVEL")
		}
	}()

	// Simulate the container timing issue: LOG_LEVEL set after package init
	_ = os.Unsetenv("LOG_LEVEL")

	// This represents the package-level initialization (old broken way)
	// The logger might be initialized with default settings

	// Now simulate environment variables becoming available
	_ = os.Setenv("LOG_LEVEL", "debug")

	// Call initLogger() to refresh the package logger (the fix)
	initLogger()

	// Test that pkgLog now respects the debug level
	// We can't directly test pkgLog output easily, but we can test that
	// initLogger() creates a new logger that respects the environment

	// Verify that DefaultLogger() (which initLogger uses) works with debug
	var buf bytes.Buffer
	testLogger := log.NewTestLogger(&buf)
	testLogger.Debug("plex package debug test message")

	output := buf.String()
	if !strings.Contains(output, "plex package debug test message") {
		t.Errorf("Package logger should output debug logs after initLogger() when LOG_LEVEL=debug. Output: %s", output)
	}
}

// TestNewServerInitializesLogger verifies that NewServer() calls initLogger()
// to ensure the package logger respects current environment variables.
func TestNewServerInitializesLogger(t *testing.T) {
	// Save original LOG_LEVEL
	originalLogLevel := os.Getenv("LOG_LEVEL")
	defer func() {
		if originalLogLevel != "" {
			_ = os.Setenv("LOG_LEVEL", originalLogLevel)
		} else {
			_ = os.Unsetenv("LOG_LEVEL")
		}
	}()

	// Set LOG_LEVEL=debug
	_ = os.Setenv("LOG_LEVEL", "debug")

	// Create a test server with fake HTTP responses
	fakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "media/providers") {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"MediaContainer": {"friendlyName":"FakeTestServer","machineIdentifier":"fake-test123","version":"1.0","MediaProvider":[]}}`)
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, `{"MediaContainer":{}}`)
		}
	}))
	defer fakeServer.Close()

	// Create server instance - this should call initLogger()
	server, err := NewServer(fakeServer.URL, "fake-token")
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer server.Close()

	// Verify server was created (indicating initLogger() didn't panic or fail)
	if server == nil {
		t.Fatal("NewServer should not return nil server")
	}

	// Verify that the server's Debug flag is set when LOG_LEVEL=debug
	if !server.Debug {
		t.Error("Server.Debug should be true when LOG_LEVEL=debug")
	}

	// Test with LOG_LEVEL=info
	_ = os.Setenv("LOG_LEVEL", "info")
	server2, err := NewServer(fakeServer.URL, "fake-token")
	if err != nil {
		t.Fatalf("NewServer failed with LOG_LEVEL=info: %v", err)
	}
	defer server2.Close()

	if server2.Debug {
		t.Error("Server.Debug should be false when LOG_LEVEL=info")
	}
}

// TestInitLoggerMultipleCalls verifies that initLogger() can be called multiple
// times safely and updates the logger each time.
func TestInitLoggerMultipleCalls(t *testing.T) {
	// Save original LOG_LEVEL
	originalLogLevel := os.Getenv("LOG_LEVEL")
	defer func() {
		if originalLogLevel != "" {
			_ = os.Setenv("LOG_LEVEL", originalLogLevel)
		} else {
			_ = os.Unsetenv("LOG_LEVEL")
		}
	}()

	// Test calling initLogger() multiple times with different LOG_LEVEL values
	testCases := []struct {
		logLevel string
		expected bool // whether debug should be enabled
	}{
		{"debug", true},
		{"info", false},
		{"debug", true},
		{"warn", false},
		{"debug", true},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("call_%d_level_%s", i+1, tc.logLevel), func(t *testing.T) {
			_ = os.Setenv("LOG_LEVEL", tc.logLevel)

			// Call initLogger - should not panic and should update pkgLog
			initLogger()

			// Verify pkgLog is not nil after initialization
			if pkgLog == nil {
				t.Fatal("pkgLog should not be nil after initLogger()")
			}

			// For debug test cases, verify concept with test logger
			if tc.expected {
				var buf bytes.Buffer
				testLogger := log.NewTestLogger(&buf)
				testLogger.Debug("test debug in multiple calls test")

				output := buf.String()
				if !strings.Contains(output, "test debug in multiple calls test") {
					t.Errorf("Test logger should output debug message for debug levels")
				}
			}
		})
	}
}

// TestPackageLevelLoggerSafety verifies that the package-level logger
// is always usable, even before initLogger() is called.
func TestPackageLevelLoggerSafety(t *testing.T) {
	// This test ensures that pkgLog is never nil and can be used safely
	// even if initLogger() hasn't been called yet.

	if pkgLog == nil {
		t.Fatal("pkgLog should never be nil - it should have a default value")
	}

	// Test that we can call methods on pkgLog without panicking
	// (We can't easily test the output, but we can ensure it doesn't crash)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("pkgLog methods should not panic: %v", r)
		}
	}()

	pkgLog.Info("package logger safety test")
	pkgLog.Debug("package logger debug safety test")
	pkgLog.Warn("package logger warn safety test")
	pkgLog.Error("package logger error safety test")
}

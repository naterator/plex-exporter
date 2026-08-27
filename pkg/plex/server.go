package plex

import (
	"crypto/rand"
	"errors"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/naterator/plex-exporter/pkg/log"
	"github.com/naterator/plex-exporter/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Plex API type parameter mappings:
// 1=movie, 2=show, 3=season, 4=episode, 5=artist, 6=album, 7=track, 8=photoalbum, 9=photo, 10=track
// Note: Some Plex servers use type=10 for tracks instead of type=7

const (
	defaultLibraryRefreshInterval = 15 * time.Minute
	maxLibraryRefreshMinutes      = int64((1<<63 - 1) / int64(time.Minute))
)

// getContentTypeForLibrary returns a descriptive label for what type of items
// are counted in each library type based on what the Plex API returns
func getContentTypeForLibrary(libraryType string) string {
	switch libraryType {
	case "movie":
		return "movies"
	case "show":
		return "shows"
	case "artist", "music":
		return "tracks"
	case "photo":
		return "photos"
	case "homevideo":
		return "videos"
	default:
		return "items"
	}
}

type Server struct {
	ID      string
	Name    string
	Version string

	Token string
	URL   *url.URL

	Client *Client

	listener       *plexListener
	listenerCancel func()
	eventSessions  *sessions
	closed         bool

	mtx       sync.RWMutex
	refreshMu sync.Mutex
	libraries []*Library

	lastBandwidthAt int
	MovieCount      int64
	EpisodeCount    int64
	MusicCount      int64
	PhotoCount      int64
	OtherVideoCount int64
	serverInfo      [6]string
	hasServerInfo   bool
	// LibraryRefreshInterval controls how often we re-query expensive per-library
	// counts (music tracks, episodes). If zero, caching is disabled.
	LibraryRefreshInterval time.Duration
	// Debug enables verbose debug logging when true.
	Debug bool

	// fullRefreshDone is closed once the initial background full refresh
	// (deferred at startup) completes. Tests may wait on this to observe
	// when the heavy startup work has finished.
	fullRefreshDone chan struct{}
	refreshStop     chan struct{}
	refreshWG       sync.WaitGroup
	closeOnce       sync.Once
}

// pkg-level logger used for structured logs within this package. Tests and
// callers may still pass their own logger to listeners; this logger is a
// sensible default for package-level messages.
//
// IMPORTANT: This logger gets refreshed in NewServer() via initLogger() to ensure
// environment variables like LOG_LEVEL are properly respected, fixing a timing
// issue where package initialization occurs before env vars are available.
var (
	pkgLogMu sync.RWMutex
	pkgLog   log.Logger = log.DefaultLogger()
)

// initLogger initializes or updates the package logger based on current environment.
// This ensures LOG_LEVEL and other environment variables are respected even when
// the package is initialized before environment variables are fully loaded
// (common in containerized environments).
func initLogger() {
	logger := log.DefaultLogger()
	pkgLogMu.Lock()
	pkgLog = logger
	pkgLogMu.Unlock()
}

func packageLogger() log.Logger {
	pkgLogMu.RLock()
	logger := pkgLog
	pkgLogMu.RUnlock()
	if logger == nil {
		return log.NewNopLogger()
	}
	return logger
}

// debugf logs only when server debug is enabled.  (Previously used for
// verbose package-level debugging; removed to satisfy linter when unused.)

// Controls for startup deferred full refresh. Defaults are production
// values; tests in this package may override them to avoid long sleeps.
var (
	startupFullRefreshDelaySeconds = 15
	startupFullRefreshJitterMax    = 5
)

type StatisticsBandwidth struct {
	At    int   `json:"at"`
	Lan   bool  `json:"lan"`
	Bytes int64 `json:"bytes"`
}

type StatisticsResources struct {
	At             int     `json:"at"`
	HostCpuUtil    float64 `json:"hostCpuUtilization"`
	HostMemUtil    float64 `json:"hostMemoryUtilization"`
	ProcessCpuUtil float64 `json:"processCpuUtilization"`
	ProcessMemUtil float64 `json:"processMemoryUtilization"`
}

type mediaContainerSizeResponse struct {
	MediaContainer struct {
		Size      int64 `json:"size"`
		TotalSize int64 `json:"totalSize"`
	} `json:"MediaContainer"`
}

func mediaContainerCount(headers http.Header, response mediaContainerSizeResponse) int64 {
	if value := headers.Get("x-plex-container-total-size"); value != "" {
		if count, err := strconv.ParseInt(value, 10, 64); err == nil && count >= 0 {
			return count
		}
	}
	if response.MediaContainer.TotalSize > 0 {
		return response.MediaContainer.TotalSize
	}
	if response.MediaContainer.Size > 0 {
		return response.MediaContainer.Size
	}
	return 0
}

func musicTrackTypeCandidates(preferred string) []string {
	candidates := make([]string, 0, 2)
	if preferred == "7" || preferred == "10" {
		candidates = append(candidates, preferred)
	}
	for _, trackType := range []string{"10", "7"} {
		if trackType != preferred {
			candidates = append(candidates, trackType)
		}
	}
	return candidates
}

func NewServer(serverURL, token string) (*Server, error) {
	client, err := NewClient(serverURL, token)
	if err != nil {
		return nil, err
	}

	server := &Server{
		URL:   client.URL,
		Token: client.Token,

		Client:          client,
		lastBandwidthAt: int(time.Now().Unix()),
	}

	// CRITICAL: Initialize logger based on current environment variables.
	// This must be called here (not at package level) to ensure environment
	// variables like LOG_LEVEL are available and properly respected. This fixes
	// a timing issue where LOG_LEVEL=debug was ignored in containerized environments.
	initLogger()

	// LIBRARY_REFRESH_INTERVAL is an integer number of minutes. Zero is the
	// documented opt-out from caching; missing or invalid values use the safe
	// default rather than accidentally enabling five-second library scans.
	rawLibraryRefreshInterval := os.Getenv("LIBRARY_REFRESH_INTERVAL")
	interval, validInterval := parseLibraryRefreshInterval(rawLibraryRefreshInterval)
	server.LibraryRefreshInterval = interval
	if rawLibraryRefreshInterval != "" && !validInterval {
		packageLogger().Warn("invalid LIBRARY_REFRESH_INTERVAL",
			zap.String("value", rawLibraryRefreshInterval),
			zap.String("note", "expected a non-negative integer number of minutes; falling back to 15 minutes"))
	}

	// Configure debug behavior via LOG_LEVEL (debug|info|warn|error). If
	// LOG_LEVEL=="debug" enable server debug features.
	if v := os.Getenv("LOG_LEVEL"); strings.ToLower(v) == "debug" {
		server.Debug = true
	}

	// Log effective interval; if 0 then caching is disabled
	if server.LibraryRefreshInterval == 0 {
		packageLogger().Info("Library refresh interval disabled; caching is off")
	} else {
		packageLogger().Info("Library refresh interval set",
			zap.Int("minutes", int(server.LibraryRefreshInterval.Minutes())))
	}

	// Perform a fast, lightweight refresh at startup to populate basic
	// server and library metadata without running expensive per-library
	// item counts. Defer the expensive full refresh to run in the
	// background after a short delay to reduce startup memory/CPU spikes.
	if err := server.RefreshLight(); err != nil {
		return nil, err
	}

	// Channel closed when the initial background full refresh completes or is
	// canceled during shutdown.
	server.fullRefreshDone = make(chan struct{})
	server.refreshStop = make(chan struct{})

	// Schedule the full refresh after 15s + small jitter.
	server.refreshWG.Add(1)
	go func() {
		defer server.refreshWG.Done()
		delaySec := startupFullRefreshDelaySeconds
		// Use crypto/rand for jitter to satisfy gosec's recommendation
		jitter := 0
		if startupFullRefreshJitterMax > 0 {
			if n, err := rand.Int(rand.Reader, big.NewInt(int64(startupFullRefreshJitterMax))); err == nil {
				jitter = int(n.Int64())
			}
		}
		timer := time.NewTimer(time.Duration(delaySec+jitter) * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-server.refreshStop:
			close(server.fullRefreshDone)
			return
		}

		packageLogger().Info("Starting background full library refresh")
		if err := server.Refresh(); err != nil {
			packageLogger().Error("background full refresh failed", zap.Error(err))
		} else {
			packageLogger().Info("background full library refresh completed")
		}
		close(server.fullRefreshDone)
	}()

	ticker := time.NewTicker(time.Second * 5)
	server.refreshWG.Add(1)
	go func() {
		defer server.refreshWG.Done()
		defer ticker.Stop()
		for {
			select {
			case <-server.refreshStop:
				return
			case <-ticker.C:
			}
			select {
			case <-server.refreshStop:
				return
			default:
			}

			// Before the first full refresh completes, only run the
			// lightweight refresh to avoid heavy work on the hot path. After
			// that, Refresh keeps lightweight metrics current while library
			// enumeration remains gated by LibraryRefreshInterval.
			select {
			case <-server.fullRefreshDone:
				if err := server.Refresh(); err != nil {
					packageLogger().Debug("periodic full refresh failed", zap.Error(err))
				}
			default:
				if err := server.RefreshLight(); err != nil {
					packageLogger().Debug("periodic light refresh failed", zap.Error(err))
				}
			}
		}
	}()

	return server, nil
}

// Close stops the server's listener, session-pruning worker, and periodic
// refresh workers. It is safe to call more than once. In-flight Plex requests
// are allowed to finish and each remains bounded by the configured timeout.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.mtx.Lock()
		s.closed = true
		cancelListener := s.listenerCancel
		eventSessions := s.eventSessions
		s.mtx.Unlock()

		if cancelListener != nil {
			cancelListener()
		}
		if s.refreshStop != nil {
			close(s.refreshStop)
		}
		if eventSessions != nil {
			eventSessions.Close()
		}
	})
	s.refreshWG.Wait()
}

func parseLibraryRefreshInterval(value string) (time.Duration, bool) {
	if value == "" {
		return defaultLibraryRefreshInterval, true
	}

	minutes, err := strconv.ParseInt(value, 10, 64)
	if err != nil || minutes < 0 || minutes > maxLibraryRefreshMinutes {
		return defaultLibraryRefreshInterval, false
	}

	return time.Duration(minutes) * time.Minute, true
}

func (s *Server) Refresh() error {
	// Serialize refreshes. The startup refresh and the five-second ticker can
	// overlap when Plex takes longer than one tick to answer. In addition to
	// wasting work, overlapping refreshes would otherwise race while replacing
	// the library slice and its cached counts.
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	// Refresh metadata and the inexpensive server metrics on every tick. The
	// expensive per-library enumeration below is independently rate limited so
	// that server/session metrics stay responsive without repeatedly scanning
	// large libraries.
	if err := s.refreshLibraryMetadata(); err != nil {
		return err
	}
	if err := s.refreshServerInfo(); err != nil {
		packageLogger().Debug("server info refresh failed", zap.Error(err))
	}
	if err := s.refreshResources(); err != nil {
		packageLogger().Debug("resource statistics refresh failed", zap.Error(err))
	}

	// Work on value copies. Refresh helpers update the authoritative library
	// records under mtx, while collectors can safely snapshot those records at
	// the same time without observing partially updated cache fields.
	s.mtx.RLock()
	libs := make([]*Library, 0, len(s.libraries))
	for _, lib := range s.libraries {
		if lib == nil {
			continue
		}
		cloned := *lib
		libs = append(libs, &cloned)
	}
	s.mtx.RUnlock()

	var moviesTotal, episodesTotal, musicTotal, photosTotal, otherVideosTotal int64
	if s.libraryRefreshNeeded(libs, time.Now()) {
		// Ensure each library has a current ItemsCount. Music counts are
		// populated by computeLibraryCounts because the type-specific track
		// query is also the server-level music total; querying it here as well
		// would double the I/O whenever caching is disabled.
		for _, lib := range libs {
			if lib.Type != "music" && lib.Type != "artist" {
				s.ensureLibraryItemsCount(lib)
			}
		}

		moviesTotal, episodesTotal, musicTotal, photosTotal, otherVideosTotal = s.computeLibraryCounts(libs)
	} else {
		// The metadata refresh recreates the library records. Recompute totals
		// from the preserved cache without starting the expensive enumeration
		// goroutines on every five-second tick.
		moviesTotal, episodesTotal, musicTotal, photosTotal, otherVideosTotal = cachedLibraryCounts(libs)
	}

	// Update server state under lock with computed totals
	s.mtx.Lock()
	s.MovieCount = moviesTotal
	s.EpisodeCount = episodesTotal
	s.MusicCount = musicTotal
	s.PhotoCount = photosTotal
	s.OtherVideoCount = otherVideosTotal
	s.mtx.Unlock()

	if err := s.refreshBandwidth(); err != nil {
		packageLogger().Debug("bandwidth statistics refresh failed", zap.Error(err))
	}

	return nil
}

// RefreshLight updates server metadata and inexpensive metrics without
// enumerating every library item. It is used during startup while the first
// full refresh is deferred, and remains useful to callers that only need the
// current server/library inventory.
func (s *Server) RefreshLight() error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	return s.refreshLight()
}

// refreshLight is the lock-free implementation shared by Refresh and
// RefreshLight. The caller must hold refreshMu when using it.
func (s *Server) refreshLight() error {
	if err := s.refreshLibraryMetadata(); err != nil {
		return err
	}

	// These endpoints are best effort for the light refresh. Refresh performs
	// the same calls with normal error propagation because it is the full
	// metrics refresh path.
	_ = s.refreshServerInfo()
	_ = s.refreshResources()

	return nil
}

// ensureLibraryItemsCount populates lib.ItemsCount using cached values when
// available otherwise querying the Plex server with a minimal payload.
//
//nolint:gocyclo // Endpoint fallbacks and cache/backoff updates share one state transition.
func (s *Server) ensureLibraryItemsCount(lib *Library) {
	usedCache := false
	now := time.Now()
	if s.LibraryRefreshInterval > 0 {
		interval := s.LibraryRefreshInterval
		s.mtx.RLock()
		for _, old := range s.libraries {
			if old.ID == lib.ID {
				if cacheTimestampFresh(old.lastItemsFetch, now, interval) {
					lib.ItemsCount = old.lastItemsCount
					lib.lastItemsCount = old.lastItemsCount
					lib.lastItemsFetch = old.lastItemsFetch
					usedCache = true
				}
				break
			}
		}
		s.mtx.RUnlock()
	}

	if usedCache {
		return
	}
	attemptAt := now.Unix()
	headers := map[string]string{
		"X-Plex-Container-Start": "0",
		"X-Plex-Container-Size":  "1",
	}

	if lib.Type == "music" || lib.Type == "artist" {
		var firstSuccessfulType string
		var firstSuccessfulCount int64
		for _, trackType := range musicTrackTypeCandidates(lib.cachedTrackType) {
			var sizeOnly mediaContainerSizeResponse
			path := "/library/sections/" + lib.ID + "/all?type=" + trackType
			hdrs, err := s.Client.GetWithHeadersReturnHeaders(path, &sizeOnly, headers)
			if err != nil {
				continue
			}

			count := mediaContainerCount(hdrs, sizeOnly)
			if firstSuccessfulType == "" {
				firstSuccessfulType = trackType
				firstSuccessfulCount = count
			}
			// Some Plex versions return a successful empty response for an
			// unsupported track type. Try the alternate type before accepting
			// zero, while still preserving zero for a genuinely empty library.
			if count > 0 {
				firstSuccessfulType = trackType
				firstSuccessfulCount = count
				break
			}
		}
		if firstSuccessfulType != "" {
			lib.ItemsCount = firstSuccessfulCount
			lib.cachedTrackType = firstSuccessfulType
			if s.LibraryRefreshInterval > 0 {
				lib.lastMusicFetch = attemptAt
				lib.lastMusicCount = firstSuccessfulCount
				lib.lastItemsFetch = attemptAt
				lib.lastItemsCount = firstSuccessfulCount
			}
			s.mtx.Lock()
			for _, current := range s.libraries {
				if current.ID == lib.ID {
					current.ItemsCount = firstSuccessfulCount
					current.cachedTrackType = firstSuccessfulType
					if s.LibraryRefreshInterval > 0 {
						current.lastMusicFetch = attemptAt
						current.lastMusicCount = firstSuccessfulCount
						current.lastItemsFetch = attemptAt
						current.lastItemsCount = firstSuccessfulCount
					}
					break
				}
			}
			s.mtx.Unlock()
			return
		}

		// Record the failed type-specific attempt before falling back to the
		// generic endpoint. This timestamp provides the retry backoff even if
		// both track-type variants are unavailable.
		if s.LibraryRefreshInterval > 0 {
			lib.lastMusicFetch = attemptAt
			s.mtx.Lock()
			for _, l := range s.libraries {
				if l.ID == lib.ID {
					l.lastMusicFetch = attemptAt
					break
				}
			}
			s.mtx.Unlock()
		}
	}

	// fallback generic items count (request page size 1 and prefer header)
	path := "/library/sections/" + lib.ID + "/all"
	var sizeOnly mediaContainerSizeResponse
	if hdrs, err := s.Client.GetWithHeadersReturnHeaders(path, &sizeOnly, headers); err == nil {
		lib.ItemsCount = mediaContainerCount(hdrs, sizeOnly)
		if s.LibraryRefreshInterval > 0 {
			lib.lastItemsCount = lib.ItemsCount
			lib.lastItemsFetch = attemptAt
		}
		s.mtx.Lock()
		for _, l := range s.libraries {
			if l.ID == lib.ID {
				l.ItemsCount = lib.ItemsCount
				if s.LibraryRefreshInterval > 0 {
					l.lastItemsCount = lib.ItemsCount
					l.lastItemsFetch = attemptAt
				}
				break
			}
		}
		s.mtx.Unlock()
	} else if s.LibraryRefreshInterval > 0 {
		// Failed requests are cached as attempts for the interval. Retain the
		// previous successful value, if any, while preventing a five-second
		// retry storm against a struggling Plex server.
		lib.lastItemsFetch = attemptAt
		s.mtx.Lock()
		for _, l := range s.libraries {
			if l.ID == lib.ID {
				l.lastItemsFetch = attemptAt
				break
			}
		}
		s.mtx.Unlock()
	}
}

// refreshLibraryMetadata performs a minimal refresh that populates server
// metadata and the basic library list (ID, name, type, duration, storage)
// without performing expensive per-library counts like episode or track
// totals. The caller must hold refreshMu when invoking this method.
func (s *Server) refreshLibraryMetadata() error {
	container := struct {
		MediaContainer struct {
			FriendlyName      string `json:"friendlyName"`
			MachineIdentifier string `json:"machineIdentifier"`
			Version           string `json:"version"`
			MediaProviders    []struct {
				Identifier string `json:"identifier"`
				Features   []struct {
					Type        string `json:"type"`
					Directories []struct {
						Identifier    string `json:"id"`
						DurationTotal int64  `json:"durationTotal"`
						StorageTotal  int64  `json:"storageTotal"`
						Title         string `json:"title"`
						Type          string `json:"type"`
					} `json:"Directory"`
				} `json:"Feature"`
			} `json:"MediaProvider"`
		} `json:"MediaContainer"`
	}{}

	if err := s.Client.Get("/media/providers?includeStorage=1", &container); err != nil {
		return err
	}

	// Take a value snapshot of the previous records before constructing the new
	// inventory. RefreshLight intentionally replaces the inventory so removed
	// libraries disappear promptly, but every cache field must follow a library
	// that still exists. Copying the whole value also keeps future cache fields
	// from being accidentally dropped when the metadata shape changes.
	s.mtx.RLock()
	previousLibraries := make(map[string]Library, len(s.libraries))
	for _, old := range s.libraries {
		if old != nil {
			previousLibraries[old.ID] = *old
		}
	}
	s.mtx.RUnlock()

	var newLibraries []*Library
	for _, provider := range container.MediaContainer.MediaProviders {
		if provider.Identifier != "com.plexapp.plugins.library" {
			continue
		}
		for _, feature := range provider.Features {
			if feature.Type != "content" {
				continue
			}
			for _, directory := range feature.Directories {
				if !isLibraryDirectoryType(directory.Type) {
					continue
				}
				lib := &Library{
					ID:            directory.Identifier,
					Name:          directory.Title,
					Type:          directory.Type,
					DurationTotal: directory.DurationTotal,
					StorageTotal:  directory.StorageTotal,
					Server:        s,
				}
				if old, ok := previousLibraries[lib.ID]; ok && old.Type == lib.Type {
					// Retain all state that is not supplied by
					// /media/providers, including successful counts and the
					// timestamps that rate-limit failed attempts as well.
					cached := old
					cached.ID = lib.ID
					cached.Name = lib.Name
					cached.Type = lib.Type
					cached.Server = s
					cached.DurationTotal = lib.DurationTotal
					cached.StorageTotal = lib.StorageTotal
					lib = &cached
				}

				newLibraries = append(newLibraries, lib)
			}
		}
	}

	// Update server metadata and libraries atomically.
	s.mtx.Lock()
	s.ID = container.MediaContainer.MachineIdentifier
	s.Name = container.MediaContainer.FriendlyName
	s.Version = container.MediaContainer.Version
	s.libraries = newLibraries
	s.mtx.Unlock()

	return nil
}

// libraryRefreshNeeded reports whether at least one library's item or
// type-specific count is outside the configured refresh window. Timestamps
// represent the last query attempt (not just successful queries), which keeps
// a temporarily unavailable Plex endpoint from being hammered every ticker
// interval.
func (s *Server) libraryRefreshNeeded(libraries []*Library, now time.Time) bool {
	if s.LibraryRefreshInterval <= 0 {
		// A zero interval is the documented opt-out from caching.
		return true
	}

	for _, lib := range libraries {
		if lib == nil || !cacheTimestampFresh(lib.lastItemsFetch, now, s.LibraryRefreshInterval) {
			return true
		}

		switch lib.Type {
		case "show":
			if !cacheTimestampFresh(lib.lastEpisodeFetch, now, s.LibraryRefreshInterval) {
				return true
			}
		case "music", "artist":
			if !cacheTimestampFresh(lib.lastMusicFetch, now, s.LibraryRefreshInterval) {
				return true
			}
		}
	}

	return false
}

func cacheTimestampFresh(timestamp int64, now time.Time, interval time.Duration) bool {
	if timestamp == 0 {
		return false
	}

	return now.Sub(time.Unix(timestamp, 0)) < interval
}

// cachedLibraryCounts calculates totals without issuing any Plex requests.
// It is used on the frequent lightweight refresh path while the per-library
// cache timestamps are still within the configured interval.
func cachedLibraryCounts(libraries []*Library) (moviesTotal, episodesTotal, musicTotal, photosTotal, otherVideosTotal int64) {
	for _, lib := range libraries {
		if lib == nil {
			continue
		}

		switch lib.Type {
		case "movie":
			moviesTotal += lib.ItemsCount
		case "show":
			episodesTotal += lib.lastEpisodeCount
		case "music", "artist":
			if lib.lastMusicFetch != 0 {
				musicTotal += lib.lastMusicCount
			} else {
				musicTotal += lib.ItemsCount
			}
		case "photo":
			photosTotal += lib.ItemsCount
		case "homevideo":
			otherVideosTotal += lib.ItemsCount
		}
	}

	return
}

// computeLibraryCounts performs the expensive per-library counting (episodes,
// music tracks). It mirrors the logic previously embedded in Refresh() and is
// intended to be invoked in background or during full refreshes.
// nolint:gocyclo // function remains complex; consider refactor in a follow-up
func (s *Server) computeLibraryCounts(newLibraries []*Library) (moviesTotal, episodesTotal, musicTotal, photosTotal, otherVideosTotal int64) {
	// Tally straightforward items first
	for _, lib := range newLibraries {
		switch lib.Type {
		case "movie":
			moviesTotal += lib.ItemsCount
		case "music", "artist":
			musicTotal += lib.ItemsCount
		case "photo":
			photosTotal += lib.ItemsCount
		case "homevideo":
			otherVideosTotal += lib.ItemsCount
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)

	// Reset musicTotal since we'll compute exact track counts
	musicTotal = 0

	for _, lib := range newLibraries {
		if lib.Type == "show" {
			wg.Add(1)
			sem <- struct{}{}
			go func(sectionID string, previousCount int64) {
				defer wg.Done()
				defer func() { <-sem }()
				// no large results buffer required; we only need size

				useCached := false
				count := previousCount
				interval := s.LibraryRefreshInterval
				if interval > 0 {
					s.mtx.RLock()
					for _, l := range s.libraries {
						if l.ID == sectionID {
							count = l.lastEpisodeCount
							if cacheTimestampFresh(l.lastEpisodeFetch, time.Now(), interval) {
								useCached = true
							}
							break
						}
					}
					s.mtx.RUnlock()
				}

				if !useCached {
					path := "/library/sections/" + sectionID + "/all?type=4"
					var sizeOnly mediaContainerSizeResponse
					headers := map[string]string{
						"X-Plex-Container-Start": "0",
						"X-Plex-Container-Size":  "1",
					}
					attemptAt := time.Now().Unix()
					if hdrs, err := s.Client.GetWithHeadersReturnHeaders(path, &sizeOnly, headers); err == nil {
						count = mediaContainerCount(hdrs, sizeOnly)
						s.mtx.Lock()
						for _, l := range s.libraries {
							if l.ID == sectionID {
								l.lastEpisodeCount = count
								if interval > 0 {
									l.lastEpisodeFetch = attemptAt
								}
								break
							}
						}
						s.mtx.Unlock()
					} else {
						packageLogger().Error("Error fetching episodes for section",
							zap.String("section", sectionID),
							zap.Error(err))
						if interval > 0 {
							s.mtx.Lock()
							for _, l := range s.libraries {
								if l.ID == sectionID {
									l.lastEpisodeFetch = attemptAt
									break
								}
							}
							s.mtx.Unlock()
						}
					}
				}

				mu.Lock()
				episodesTotal += count
				mu.Unlock()
			}(lib.ID, lib.lastEpisodeCount)
		}

		if lib.Type == "music" || lib.Type == "artist" {
			wg.Add(1)
			sem <- struct{}{}
			go func(sectionID string, previousCount int64, preferredType string) {
				defer wg.Done()
				defer func() { <-sem }()
				trackCount := previousCount

				interval := s.LibraryRefreshInterval
				useCached := false
				s.mtx.RLock()
				for _, current := range s.libraries {
					if current.ID == sectionID {
						trackCount = current.lastMusicCount
						if current.cachedTrackType != "" {
							preferredType = current.cachedTrackType
						}
						if interval > 0 && cacheTimestampFresh(current.lastMusicFetch, time.Now(), interval) {
							useCached = true
						}
						break
					}
				}
				s.mtx.RUnlock()

				if !useCached {
					attemptAt := time.Now().Unix()
					var successfulType string
					var successfulCount int64
					for _, trackType := range musicTrackTypeCandidates(preferredType) {
						var sizeOnly mediaContainerSizeResponse
						path := "/library/sections/" + sectionID + "/all?type=" + trackType
						headers := map[string]string{
							"X-Plex-Container-Start": "0",
							"X-Plex-Container-Size":  "1",
						}
						if hdrs, err := s.Client.GetWithHeadersReturnHeaders(path, &sizeOnly, headers); err == nil {
							count := mediaContainerCount(hdrs, sizeOnly)
							if successfulType == "" {
								successfulType = trackType
								successfulCount = count
							}
							if count > 0 {
								successfulType = trackType
								successfulCount = count
								break
							}
						} else {
							packageLogger().Error("Error fetching music tracks",
								zap.String("type", trackType),
								zap.String("section", sectionID),
								zap.Error(err))
						}
					}

					if successfulType != "" {
						trackCount = successfulCount
					}
					s.mtx.Lock()
					for _, current := range s.libraries {
						if current.ID == sectionID {
							if successfulType != "" {
								current.ItemsCount = successfulCount
								current.lastItemsCount = successfulCount
								current.lastMusicCount = successfulCount
								current.cachedTrackType = successfulType
							}
							if interval > 0 {
								current.lastMusicFetch = attemptAt
								current.lastItemsFetch = attemptAt
							}
							break
						}
					}
					s.mtx.Unlock()
				}

				mu.Lock()
				musicTotal += trackCount
				mu.Unlock()
			}(lib.ID, lib.lastMusicCount, lib.cachedTrackType)
		}
	}

	wg.Wait()
	close(sem)

	return
}

func (s *Server) refreshServerInfo() error {
	resp := struct {
		MediaContainer struct {
			Version         string `json:"version"`
			Platform        string `json:"platform"`
			PlatformVersion string `json:"platformVersion"`
		} `json:"MediaContainer"`
	}{}
	err := s.Client.Get("/", &resp)

	if err != nil {
		return err
	}

	s.mtx.Lock()
	labels := [6]string{
		"plex",
		s.Name,
		s.ID,
		resp.MediaContainer.Version,
		resp.MediaContainer.Platform,
		resp.MediaContainer.PlatformVersion,
	}
	if s.hasServerInfo && s.serverInfo != labels {
		metrics.ServerInfo.DeleteLabelValues(s.serverInfo[:]...)
	}
	metrics.ServerInfo.WithLabelValues(labels[:]...).Set(1.0)
	s.serverInfo = labels
	s.hasServerInfo = true
	s.mtx.Unlock()

	return nil
}

func (s *Server) refreshResources() error {
	resources := struct {
		MediaContainer struct {
			StatisticsResources []StatisticsResources `json:"StatisticsResources"`
		} `json:"MediaContainer"`
	}{}
	err := s.Client.Get("/statistics/resources?timespan=6", &resources)

	// This is a paid feature and API may not be available
	if errors.Is(err, ErrNotFound) {
		return nil
	}

	if err != nil {
		return err
	}

	if len(resources.MediaContainer.StatisticsResources) > 0 {
		stats := resources.MediaContainer.StatisticsResources[0]
		for _, candidate := range resources.MediaContainer.StatisticsResources[1:] {
			if candidate.At >= stats.At {
				stats = candidate
			}
		}
		serverName, serverID := s.identity()

		metrics.ServerHostCpuUtilization.WithLabelValues("plex", serverName, serverID).Set(stats.HostCpuUtil)
		metrics.ServerHostMemUtilization.WithLabelValues("plex", serverName, serverID).Set(stats.HostMemUtil)
	}

	return nil
}

func (s *Server) refreshBandwidth() error {
	bandwidth := struct {
		MediaContainer struct {
			StatisticsBandwith []StatisticsBandwidth `json:"StatisticsBandwidth"`
		} `json:"MediaContainer"`
	}{}
	err := s.Client.Get("/statistics/bandwidth?timespan=6", &bandwidth)

	// This is a paid feature and API may not be available
	if errors.Is(err, ErrNotFound) {
		return nil
	}

	if err != nil {
		return err
	}

	// Record updates newer than our last sync.  We also keep track of
	// the highest timestamp see and use that as our last sync time.
	// Sort by timestamp to ensure they are processed in order
	updates := bandwidth.MediaContainer.StatisticsBandwith

	sort.Slice(updates, func(i, j int) bool {
		return updates[i].At < updates[j].At
	})

	s.mtx.RLock()
	lastBandwidthAt := s.lastBandwidthAt
	serverName := s.Name
	serverID := s.ID
	s.mtx.RUnlock()

	// Start at the existing checkpoint. Empty responses and responses that
	// contain only old samples must not rewind it and cause old traffic to be
	// counted again on a later refresh.
	highest := lastBandwidthAt
	for _, u := range updates {
		if u.At <= lastBandwidthAt {
			continue
		}
		if u.At > highest {
			highest = u.At
		}
		if u.Bytes < 0 {
			packageLogger().Warn("ignoring negative Plex bandwidth sample",
				zap.Int("at", u.At),
				zap.Int64("bytes", u.Bytes))
			continue
		}
		metrics.MetricTransmittedBytesTotal.WithLabelValues("plex", serverName, serverID).Add(float64(u.Bytes))
	}

	s.mtx.Lock()
	if highest > s.lastBandwidthAt {
		s.lastBandwidthAt = highest
	}
	s.mtx.Unlock()

	return nil
}

func (s *Server) Library(id string) *Library {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for _, library := range s.libraries {
		if library != nil && library.ID == id {
			copy := *library
			return &copy
		}
	}
	return nil
}

func (s *Server) identity() (name, id string) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.Name, s.ID
}

func (s *Server) Describe(ch chan<- *prometheus.Desc) {
	ch <- metrics.MetricsLibraryDurationTotalDesc
	ch <- metrics.MetricsLibraryStorageTotalDesc
	ch <- metrics.MetricsLibraryItemsDesc
	ch <- metrics.MetricsMediaMoviesDesc
	ch <- metrics.MetricsMediaEpisodesDesc
	ch <- metrics.MetricsMediaMusicDesc
	ch <- metrics.MetricsMediaPhotosDesc
	ch <- metrics.MetricsMediaOtherVideosDesc
	ch <- metrics.MetricPlayCountDesc
	ch <- metrics.MetricPlaySecondsTotalDesc
	ch <- metrics.MetricEstimatedTransmittedBytesTotal
}

func (s *Server) Collect(ch chan<- prometheus.Metric) {
	s.mtx.RLock()
	libraries := make([]Library, 0, len(s.libraries))
	for _, library := range s.libraries {
		if library != nil {
			libraries = append(libraries, *library)
		}
	}
	serverName := s.Name
	serverID := s.ID
	movieCount := s.MovieCount
	episodeCount := s.EpisodeCount
	musicCount := s.MusicCount
	photoCount := s.PhotoCount
	otherVideoCount := s.OtherVideoCount
	eventSessions := s.eventSessions
	if eventSessions == nil && s.listener != nil {
		eventSessions = s.listener.activeSessions
	}
	s.mtx.RUnlock()

	for _, library := range libraries {
		ch <- metrics.LibraryDuration(library.DurationTotal,
			"plex",
			serverName,
			serverID,
			library.Type,
			library.Name,
			library.ID,
		)
		ch <- metrics.LibraryStorage(library.StorageTotal,
			"plex",
			serverName,
			serverID,
			library.Type,
			library.Name,
			library.ID,
		)

		// Determine what type of content is being counted based on library type
		contentType := getContentTypeForLibrary(library.Type)

		ch <- metrics.LibraryItems(library.ItemsCount,
			"plex",
			serverName,
			serverID,
			library.Type,
			library.Name,
			library.ID,
			contentType,
		)
	}

	// Emit server-level media totals
	ch <- metrics.MediaMovies(movieCount, "plex", serverName, serverID)
	ch <- metrics.MediaEpisodes(episodeCount, "plex", serverName, serverID)
	ch <- metrics.MediaMusic(musicCount, "plex", serverName, serverID)
	ch <- metrics.MediaPhotos(photoCount, "plex", serverName, serverID)
	ch <- metrics.MediaOtherVideos(otherVideoCount, "plex", serverName, serverID)

	if eventSessions != nil {
		eventSessions.Collect(ch)
	}
}

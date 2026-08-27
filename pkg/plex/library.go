package plex

type Library struct {
	Name string
	ID   string
	Type string

	Server *Server

	DurationTotal int64
	StorageTotal  int64
	ItemsCount    int64
	// cachedTrackType records which "type" parameter worked for this
	// music library ("7" or "10"). Empty if unknown.
	cachedTrackType string
	// lastMusicFetch stores unix seconds of the last track-count query attempt
	// for this library. Failed attempts are recorded too, so a transient Plex
	// error cannot trigger another expensive request on every five-second tick.
	lastMusicFetch int64
	// lastMusicCount stores the last successful exact track count we fetched.
	// If non-zero we can reuse it when skipping a fetch.
	lastMusicCount int64
	// lastEpisodeFetch stores unix seconds of the last episode-count query
	// attempt for this library (shows). Failed attempts are recorded too, so a
	// transient Plex error cannot trigger another expensive request on every
	// five-second tick.
	lastEpisodeFetch int64
	// lastEpisodeCount stores the last successful exact episode count.
	lastEpisodeCount int64
	// lastItemsFetch and lastItemsCount cache the unfiltered
	// /library/sections/<id>/all response so we can avoid re-querying library
	// item counts too frequently. lastItemsFetch is also updated for failed
	// attempts to provide the same retry backoff as type-specific counts.
	lastItemsFetch int64
	lastItemsCount int64
}

func isLibraryDirectoryType(directoryType string) bool {
	switch directoryType {
	case
		"movie",
		"show",
		"artist",
		"music",
		"photo",
		"homevideo":
		return true
	}
	return false
}

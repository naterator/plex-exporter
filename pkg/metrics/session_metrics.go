package metrics

import "github.com/prometheus/client_golang/prometheus"

// Session metrics deliberately use a small, stable label set.  The legacy
// play metrics retain their detailed media labels, while these metrics are
// intended for current-session dashboards and long-lived recording rules.
var (
	MetricSessionActiveDesc = prometheus.NewDesc(
		"plex_session_active",
		"Whether a tracked Plex session is currently active",
		append(append([]string(nil), serverLabels...), "session", "user"),
		nil,
	)

	MetricSessionStateDesc = prometheus.NewDesc(
		"plex_session_state",
		"Current state of a tracked Plex session; the current state is one",
		append(append([]string(nil), serverLabels...), "session", "user", "state"),
		nil,
	)

	MetricSessionsDesc = prometheus.NewDesc(
		"plex_sessions",
		"Number of tracked Plex sessions by state",
		append(append([]string(nil), serverLabels...), "state"),
		nil,
	)

	MetricSessionPlaySecondsTotalDesc = prometheus.NewDesc(
		"plex_session_play_seconds_total",
		"Observed playback time for a Plex session in seconds",
		append(append([]string(nil), serverLabels...), "session", "user"),
		nil,
	)

	MetricSessionBitrateKbpsDesc = prometheus.NewDesc(
		"plex_session_bitrate_kbps",
		"Source bitrate reported for a Plex session in kilobits per second",
		append(append([]string(nil), serverLabels...), "session", "user", "location"),
		nil,
	)

	MetricSessionBandwidthKbpsDesc = prometheus.NewDesc(
		"plex_session_bandwidth_kbps",
		"Bandwidth reported for a Plex session in kilobits per second",
		append(append([]string(nil), serverLabels...), "session", "user", "location"),
		nil,
	)

	MetricSessionVideoTranscodeDesc = prometheus.NewDesc(
		"plex_session_video_transcode",
		"Whether a session is actively transcoding video; decoder and encoder labels report observed execution",
		append(append([]string(nil), serverLabels...), "session", "user", "decode", "encode"),
		nil,
	)
)

// SessionDescs returns every descriptor emitted by the session collector.
// Server.Describe uses this helper so the collector's descriptor contract is
// kept in one place as session metrics evolve.
func SessionDescs() []*prometheus.Desc {
	return []*prometheus.Desc{
		MetricSessionActiveDesc,
		MetricSessionStateDesc,
		MetricSessionsDesc,
		MetricSessionPlaySecondsTotalDesc,
		MetricSessionBitrateKbpsDesc,
		MetricSessionBandwidthKbpsDesc,
		MetricSessionVideoTranscodeDesc,
	}
}

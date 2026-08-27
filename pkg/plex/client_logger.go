package plex

import (
	ttPlex "github.com/timothystewart6/go-plex-client"
	"go.uber.org/zap"

	"github.com/naterator/plex-exporter/pkg/log"
)

const unknownWebsocketEventMessage = "unknown websocket event name"

// goPlexLogger routes dependency logs through the exporter's configured
// logger. Plex emits several valid notification types that go-plex-client does
// not currently handle, so its generic unknown-event warning is diagnostic
// noise rather than an actionable warning for the exporter.
type goPlexLogger struct {
	log.Logger
}

var _ ttPlex.Logger = goPlexLogger{}

func (l goPlexLogger) Warn(msg string, fields ...zap.Field) {
	if msg == unknownWebsocketEventMessage {
		l.Debug(msg, fields...)
		return
	}

	l.Logger.Warn(msg, fields...)
}

// ConfigureClientLogger routes go-plex-client logs through logger and
// downgrades unsupported WebSocket event notifications to debug. Passing nil
// restores the dependency's default logger.
func ConfigureClientLogger(logger log.Logger) {
	if logger == nil {
		ttPlex.SetLogger(nil)
		return
	}

	ttPlex.SetLogger(goPlexLogger{Logger: logger})
}

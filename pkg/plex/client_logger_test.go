package plex

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/naterator/plex-exporter/pkg/log"
)

func bufferedLogger(buffer *bytes.Buffer, level zapcore.Level) log.Logger {
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(encoder, zapcore.AddSync(buffer), level)
	return log.NewLogger(zap.New(core))
}

func TestGoPlexLoggerDowngradesUnknownWebsocketEvents(t *testing.T) {
	for _, event := range []string{"progress", "status", "account"} {
		t.Run(event, func(t *testing.T) {
			var infoOutput bytes.Buffer
			infoLogger := goPlexLogger{Logger: bufferedLogger(&infoOutput, zapcore.InfoLevel)}
			infoLogger.Warn(unknownWebsocketEventMessage, zap.String("event", event))
			if infoOutput.Len() != 0 {
				t.Fatalf("unknown event was logged at the default info level: %s", infoOutput.String())
			}

			var debugOutput bytes.Buffer
			debugLogger := goPlexLogger{Logger: bufferedLogger(&debugOutput, zapcore.DebugLevel)}
			debugLogger.Warn(unknownWebsocketEventMessage, zap.String("event", event))

			var entry struct {
				Level string `json:"level"`
				Msg   string `json:"msg"`
				Event string `json:"event"`
			}
			if err := json.Unmarshal(bytes.TrimSpace(debugOutput.Bytes()), &entry); err != nil {
				t.Fatalf("decode debug log: %v", err)
			}
			if entry.Level != "debug" || entry.Msg != unknownWebsocketEventMessage || entry.Event != event {
				t.Fatalf("unexpected debug log entry: %+v", entry)
			}
		})
	}
}

func TestGoPlexLoggerPreservesOtherWarnings(t *testing.T) {
	var output bytes.Buffer
	logger := goPlexLogger{Logger: bufferedLogger(&output, zapcore.InfoLevel)}
	logger.Warn("failed to unmarshal websocket message", zap.String("error", "invalid JSON"))

	var entry struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &entry); err != nil {
		t.Fatalf("decode warning log: %v", err)
	}
	if entry.Level != "warn" || entry.Msg != "failed to unmarshal websocket message" {
		t.Fatalf("unexpected warning log entry: %+v", entry)
	}
}

package plex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	plexapi "github.com/timothystewart6/go-plex-client"
)

// transcodeHardwareObservation is the small part of a /status/sessions
// record that the dependency's Metadata type does not model. Plex embeds a
// TranscodeSession object in a playing video entry, while the pinned client
// only models the websocket variant of that object. Keep the wire parsing
// here so the rest of the exporter can continue using the dependency types.
type transcodeHardwareObservation struct {
	sessionID          string
	transcodeSessionID string
	videoDecision      string
	decode             string
	encode             string
	decodePresent      bool
	encodePresent      bool
}

// rawSessionMetadata is intentionally limited to the fields needed to join
// the raw transcode details back to the typed session metadata. The Plex JSON
// response uses capitalized child element names in this part of the payload.
type rawSessionMetadata struct {
	SessionKey string `json:"sessionKey"`
	Session    struct {
		ID string `json:"id"`
	} `json:"Session"`
	VideoDecision    string          `json:"videoDecision"`
	TranscodeSession json.RawMessage `json:"TranscodeSession"`
}

type rawTranscodeSession struct {
	Key                 string          `json:"key"`
	VideoDecision       string          `json:"videoDecision"`
	TranscodeHwDecoding json.RawMessage `json:"transcodeHwDecoding"`
	TranscodeHwEncoding json.RawMessage `json:"transcodeHwEncoding"`
}

// parseSessionPollMetadata decodes one Metadata object twice: once into the
// pinned dependency's type and once into the raw fields it does not expose.
// This keeps getSessionsContext compatible with existing callers while making
// the poller able to observe actual transcoder outcomes.
func parseSessionPollMetadata(raw json.RawMessage) (metadata sessionPollMetadata, err error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return sessionPollMetadata{}, errors.New("plex session metadata is null")
	}
	if err := json.Unmarshal(raw, &metadata.Metadata); err != nil {
		return sessionPollMetadata{}, fmt.Errorf("decode Plex session metadata: %w", err)
	}

	var details rawSessionMetadata
	if err := json.Unmarshal(raw, &details); err != nil {
		return sessionPollMetadata{}, fmt.Errorf("decode Plex transcode metadata: %w", err)
	}
	metadata.Observations = parseTranscodeObservations(details)
	return metadata, nil
}

// sessionPollMetadata carries the dependency model and any raw evidence found
// alongside it. It is kept separate from the HTTP response envelope so tests
// can exercise the parser without starting a server.
type sessionPollMetadata struct {
	Metadata     plexapi.Metadata
	Observations []transcodeHardwareObservation
}

// parseTranscodeObservations extracts the optional embedded TranscodeSession.
// Plex has returned both an object and a one-element array across API
// versions, so accept either shape. A missing TranscodeSession is normal for
// direct play and produces no observation.
func parseTranscodeObservations(details rawSessionMetadata) []transcodeHardwareObservation {
	if len(bytes.TrimSpace(details.TranscodeSession)) == 0 ||
		bytes.Equal(bytes.TrimSpace(details.TranscodeSession), []byte("null")) {
		return nil
	}

	var sessions []rawTranscodeSession
	raw := bytes.TrimSpace(details.TranscodeSession)
	if len(raw) > 0 && raw[0] == '[' {
		if err := json.Unmarshal(raw, &sessions); err != nil {
			return nil
		}
	} else {
		var single rawTranscodeSession
		if err := json.Unmarshal(raw, &single); err != nil {
			return nil
		}
		sessions = []rawTranscodeSession{single}
	}

	observations := make([]transcodeHardwareObservation, 0, len(sessions))
	for _, current := range sessions {
		decision := strings.ToLower(strings.TrimSpace(current.VideoDecision))
		if decision == "" {
			decision = strings.ToLower(strings.TrimSpace(details.VideoDecision))
		}
		if decision != "transcode" {
			// Audio-only transcode and direct-stream/copy sessions must not be
			// presented as video-transcode evidence.
			continue
		}

		decode, decodePresent := normalizeHardwareOutcome(current.TranscodeHwDecoding)
		encode, encodePresent := normalizeHardwareOutcome(current.TranscodeHwEncoding)
		sessionID := strings.TrimSpace(details.SessionKey)
		if sessionID == "" {
			sessionID = strings.TrimSpace(details.Session.ID)
		}
		observations = append(observations, transcodeHardwareObservation{
			sessionID:          sessionID,
			transcodeSessionID: strings.TrimSpace(current.Key),
			videoDecision:      decision,
			decode:             decode,
			encode:             encode,
			decodePresent:      decodePresent,
			encodePresent:      encodePresent,
		})
	}
	return observations
}

// normalizeHardwareOutcome distinguishes an omitted field from a field that
// Plex supplied as empty/software. Known engine names are classified as
// hardware, while an unrecognized value remains unknown; the requested flag
// is deliberately not consulted because Plex can fall back to software.
func normalizeHardwareOutcome(raw json.RawMessage) (value string, present bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "unknown", false
	}

	var text string
	if err := json.Unmarshal(trimmed, &text); err == nil {
		return classifyHardwareOutcomeString(text), true
	}

	var boolean bool
	if err := json.Unmarshal(trimmed, &boolean); err == nil {
		if boolean {
			return "hardware", true
		}
		return "software", true
	}

	var number float64
	if err := json.Unmarshal(trimmed, &number); err == nil {
		if number == 0 {
			return "software", true
		}
		return "hardware", true
	}
	return "unknown", false
}

func classifyHardwareOutcomeString(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "", "software", "sw", "none", "false", "0":
		return "software"
	case "true", "1", "vaapi", "nvenc", "nvdec", "qsv", "d3d11va", "videotoolbox", "amf", "cuda", "vulkan", "mf", "mediafoundation":
		return "hardware"
	default:
		// An unrecognized non-empty value was present, but it is not safe to
		// claim hardware from a name the exporter does not understand.
		return "unknown"
	}
}

// applyTranscodeHardwareObservationsLocked applies raw poll evidence while
// syncCurrent still holds the session-store lock. The applied map contains
// only records whose HTTP snapshot was accepted; a newer websocket update is
// therefore never overwritten by the response that was in flight before it.
func (s *sessions) applyTranscodeHardwareObservationsLocked(observations []transcodeHardwareObservation, applied map[string]struct{}) {
	if s == nil {
		return
	}
	if s.sessions == nil {
		return
	}
	for _, observation := range observations {
		if observation.videoDecision != "transcode" {
			continue
		}
		key, ok := s.matchSessionKeyLocked(observation.sessionID)
		if !ok && observation.transcodeSessionID != "" {
			key, ok = s.matchSessionKeyLocked(observation.transcodeSessionID)
		}
		if !ok {
			continue
		}
		current := s.sessions[key]
		if _, ok := applied[key]; !ok {
			if _, ok = applied[current.session.SessionKey]; !ok {
				continue
			}
		}
		current.transcodeHWDecode = observation.decode
		current.transcodeHWEncode = observation.encode
		// The videoDecision itself is authoritative evidence that video is
		// transcoding. Missing outcome fields remain unknown; they must not
		// suppress the active-video metric or be replaced by the request flag.
		current.transcodeHWKnown = true
		s.sessions[key] = current
	}
}

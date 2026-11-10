package plex

import (
	"encoding/json"
	"testing"
)

func TestNormalizeHardwareOutcome(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		value   string
		present bool
	}{
		{name: "vaapi engine", raw: `"vaapi"`, value: "hardware", present: true},
		{name: "nvenc engine", raw: `"nvenc"`, value: "hardware", present: true},
		{name: "qsv engine", raw: `"qsv"`, value: "hardware", present: true},
		{name: "explicit software", raw: `"software"`, value: "software", present: true},
		{name: "present empty", raw: `""`, value: "software", present: true},
		{name: "boolean hardware", raw: `true`, value: "hardware", present: true},
		{name: "numeric software", raw: `0`, value: "software", present: true},
		{name: "unknown engine", raw: `"future-engine"`, value: "unknown", present: true},
		{name: "omitted", raw: "", value: "unknown", present: false},
		{name: "null", raw: "null", value: "unknown", present: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, present := normalizeHardwareOutcome(json.RawMessage(test.raw))
			if value != test.value || present != test.present {
				t.Fatalf("normalizeHardwareOutcome(%q) = (%q, %t), want (%q, %t)", test.raw, value, present, test.value, test.present)
			}
		})
	}
}

func TestParseSessionPollMetadataTranscodeHardware(t *testing.T) {
	raw := json.RawMessage(`{
		"sessionKey":"session-1",
		"Session":{"id":"session-1"},
		"Player":{"state":"playing"},
		"TranscodeSession":{
			"key":"/transcode/sessions/transcode-1",
			"videoDecision":"transcode",
			"transcodeHwRequested":true,
			"transcodeHwDecoding":"vaapi",
			"transcodeHwEncoding":"nvenc"
		}
	}`)

	parsed, err := parseSessionPollMetadata(raw)
	if err != nil {
		t.Fatalf("parseSessionPollMetadata: %v", err)
	}
	if parsed.Metadata.SessionKey != "session-1" {
		t.Fatalf("session key = %q, want session-1", parsed.Metadata.SessionKey)
	}
	if len(parsed.Observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(parsed.Observations))
	}
	observation := parsed.Observations[0]
	if observation.sessionID != "session-1" || observation.transcodeSessionID != "/transcode/sessions/transcode-1" {
		t.Fatalf("observation identity = %+v", observation)
	}
	if observation.decode != "hardware" || observation.encode != "hardware" ||
		!observation.decodePresent || !observation.encodePresent {
		t.Fatalf("hardware observation = %+v", observation)
	}
}

func TestParseSessionPollMetadataDistinguishesSoftwareAndUnknown(t *testing.T) {
	raw := json.RawMessage(`{
		"sessionKey":"session-software",
		"TranscodeSession":{
			"key":"transcode-software",
			"videoDecision":"transcode",
			"transcodeHwDecoding":"",
			"transcodeHwEncoding":"software"
		}
	}`)

	parsed, err := parseSessionPollMetadata(raw)
	if err != nil {
		t.Fatalf("parseSessionPollMetadata: %v", err)
	}
	if len(parsed.Observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(parsed.Observations))
	}
	observation := parsed.Observations[0]
	if observation.decode != "software" || observation.encode != "software" ||
		!observation.decodePresent || !observation.encodePresent {
		t.Fatalf("software observation = %+v", observation)
	}

	raw = json.RawMessage(`{
		"sessionKey":"session-unknown",
		"TranscodeSession":{"videoDecision":"transcode","transcodeHwEncoding":"qsv"}
	}`)
	parsed, err = parseSessionPollMetadata(raw)
	if err != nil {
		t.Fatalf("parseSessionPollMetadata unknown half: %v", err)
	}
	observation = parsed.Observations[0]
	if observation.decode != "unknown" || observation.decodePresent ||
		observation.encode != "hardware" || !observation.encodePresent {
		t.Fatalf("unknown-half observation = %+v", observation)
	}
}

func TestParseSessionPollMetadataIgnoresNonVideoTranscode(t *testing.T) {
	for _, decision := range []string{"copy", "directplay", ""} {
		raw := json.RawMessage(`{"sessionKey":"session-1","TranscodeSession":{"videoDecision":"` + decision + `","transcodeHwEncoding":"nvenc"}}`)
		parsed, err := parseSessionPollMetadata(raw)
		if err != nil {
			t.Fatalf("decision %q: %v", decision, err)
		}
		if len(parsed.Observations) != 0 {
			t.Fatalf("decision %q produced %d observations", decision, len(parsed.Observations))
		}
	}
}

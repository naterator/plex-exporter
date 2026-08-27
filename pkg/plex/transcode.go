package plex

import (
	"strings"

	ttPlex "github.com/timothystewart6/go-plex-client"
)

// transcodeKind returns one of: "video", "audio", "both", or "unknown".
// It inspects source vs. target codec fields in the TranscodeSession reported
// by the Plex websocket notification.
func transcodeKind(ts ttPlex.TranscodeSession) string {
	vSrc := strings.ToLower(strings.TrimSpace(ts.SourceVideoCodec))
	vNew := strings.ToLower(strings.TrimSpace(ts.VideoCodec))
	aSrc := strings.ToLower(strings.TrimSpace(ts.SourceAudioCodec))
	aNew := strings.ToLower(strings.TrimSpace(ts.AudioCodec))
	vDecision := strings.ToLower(strings.TrimSpace(ts.VideoDecision))
	aDecision := strings.ToLower(strings.TrimSpace(ts.AudioDecision))

	// If Plex explicitly reports a decision to transcode video/audio, prefer
	// that signal (this handles cases like subtitle burn-in where the video
	// stream is transcoded even if codec strings may look unchanged).
	hasVideoChange := vDecision == "transcode" || (vNew != "" && vNew != vSrc)
	hasAudioChange := aDecision == "transcode" || (aNew != "" && aNew != aSrc)

	if hasVideoChange {
		if hasAudioChange {
			return "both"
		}
		return "video"
	}
	if hasAudioChange {
		return "audio"
	}

	// No explicit codec change detected.
	// If target codec(s) are present and equal to source, treat as "unknown"
	if (vNew != "" && vNew == vSrc) || (aNew != "" && aNew == aSrc) {
		return "unknown"
	}

	// Heuristic: if only source codecs are present (no target), infer type.
	if vNew == "" && vSrc != "" && aNew == "" && aSrc != "" {
		return "both"
	}
	if vNew == "" && vSrc != "" {
		return "video"
	}
	if aNew == "" && aSrc != "" {
		return "audio"
	}

	return "unknown"
}

// subtitleAction returns a normalized subtitle action and whether the Plex
// payload contained enough subtitle-specific information to determine it. A
// video transcode by itself is not evidence of subtitle burn-in: resolution,
// bitrate, or codec conversion can all require video transcoding without any
// subtitles being active.
func subtitleAction(ts ttPlex.TranscodeSession) (string, bool) {
	return subtitleActionFromFields(ts.SubtitleDecision, ts.Container)
}

func subtitleActionFromFields(decision, container string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(decision)) {
	case "burn", "burn-in":
		return "burn", true
	case "copy", "copying":
		return "copy", true
	case "transcode", "transcoding":
		return "transcode", true
	case "none":
		return "none", true
	}

	if container := strings.ToLower(strings.TrimSpace(container)); strings.Contains(container, "srt") {
		return "copy", true
	}
	return "none", false
}

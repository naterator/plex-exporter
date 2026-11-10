#!/usr/bin/env python3
"""Generate the checked-in Grafana dashboard from the metric contract."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "grafana" / "plex-exporter.json"
DATASOURCE = {"type": "prometheus", "uid": "${datasource}"}
# Job and instance are single-select variables. Let the Prometheus data
# source escape label strings; generic regex formatting emits backslashes
# that are not valid escapes inside a quoted PromQL string.
BASE = 'job="$job",instance="$instance",server_type="plex"'
TARGET = 'job="$job",instance="$instance"'


def target(expr: str, legend: str = "", instant: bool = False, ref: str = "A") -> dict:
    return {
        "datasource": DATASOURCE,
        "editorMode": "code",
        "expr": expr,
        "format": "time_series",
        "instant": instant,
        "legendFormat": legend,
        "range": not instant,
        "refId": ref,
        "interval": "$scrape_interval",
    }


def field(unit: str = "short", maximum: float | None = None, thresholds=None) -> dict:
    defaults = {
        "unit": unit,
        "noValue": "No samples",
        "color": {"mode": "palette-classic"},
    }
    if maximum is not None:
        defaults["max"] = maximum
    if thresholds is not None:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
    return {"defaults": defaults, "overrides": []}


def stat_options() -> dict:
    return {
        "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
        "orientation": "auto",
        "textMode": "auto",
        "colorMode": "value",
        "graphMode": "none",
        "justifyMode": "auto",
    }


def timeseries_options() -> dict:
    return {
        "legend": {"displayMode": "list", "placement": "bottom", "calcs": ["lastNotNull"]},
        "tooltip": {"mode": "multi", "sort": "desc"},
    }


def panel(pid: int, title: str, kind: str, x: int, y: int, width: int, height: int,
          description: str = "", targets=None, unit: str = "short", maximum=None, thresholds=None) -> dict:
    out = {
        "id": pid,
        "title": title,
        "type": kind,
        "gridPos": {"x": x, "y": y, "w": width, "h": height},
        "description": description,
    }
    if kind == "row":
        out.update({"collapsed": False, "panels": []})
        return out
    if kind == "text":
        out["options"] = {"mode": "markdown", "content": description}
        return out
    out.update({"datasource": DATASOURCE, "targets": targets or [], "fieldConfig": field(unit, maximum, thresholds)})
    if kind == "stat":
        out["options"] = stat_options()
    elif kind == "timeseries":
        out["options"] = timeseries_options()
        out["fieldConfig"]["defaults"]["custom"] = {
            "drawStyle": "line",
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "fillOpacity": 8,
            "showPoints": "never",
            "spanNulls": False,
            "axisCenteredZero": False,
            "axisPlacement": "auto",
            "stacking": {"mode": "none", "group": "A"},
        }
    elif kind == "bargauge":
        out["fieldConfig"]["defaults"]["min"] = 0
        out["options"] = {
            "orientation": "horizontal",
            "displayMode": "basic",
            "showUnfilled": True,
            "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
        }
    elif kind == "table":
        for query in out["targets"]:
            query["format"] = "table"
        out["options"] = {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}
    return out


def q(metric: str, selector: str = BASE) -> str:
    return f"{metric}{{{selector}}}"


def variable(name: str, label: str, query: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DATASOURCE,
        "query": {"query": query, "refId": "StandardVariableQuery"},
        "definition": query,
        "refresh": 1,
        "sort": 1,
        "multi": False,
        "includeAll": False,
        "options": [],
        "current": {},
    }


panels = []
next_id = 1


def add(*args, **kwargs):
    global next_id
    panels.append(panel(next_id, *args, **kwargs))
    next_id += 1


add("Status", "row", 0, 0, 24, 1)
add("Exporter scrape", "stat", 0, 1, 4, 4,
    "Prometheus can reach the exporter. This does not prove Plex is reachable.",
    [target('min(up{job="$job",instance="$instance"})', instant=True)], "none",
    thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}])
panels[-1]["fieldConfig"]["defaults"]["mappings"] = [{"type": "value", "options": {
    "0": {"text": "Down", "color": "red"}, "1": {"text": "Up", "color": "green"}}}]
add("Plex session poll", "stat", 4, 1, 4, 4,
    "Whether the latest /status/sessions request succeeded. A successful idle response is healthy.",
    [target('min(plex_operation_success{operation="sessions",' + TARGET + '})', instant=True)], "none",
    thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}])
add("WebSocket", "stat", 8, 1, 4, 4,
    "Event connection state. Session polling continues independently when the WebSocket is down.",
    [target("plex_websocket_connected{" + TARGET + "}", instant=True)], "none",
    thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}])
add("Active streams", "stat", 12, 1, 4, 4,
    "Confirmed current sessions. Paused and buffering streams remain active while Plex reports them.",
    [target('sum(plex_sessions{state=~"playing|paused|buffering",' + BASE + '})', instant=True)], "short")
add("Stale session data", "stat", 16, 1, 4, 4,
    "Seconds since the last successful session poll. This is the freshness signal for playback data.",
    [target('(time() - max(plex_operation_last_success_timestamp_seconds{operation="sessions",' + TARGET + '})) '
            'and (max(plex_operation_last_success_timestamp_seconds{operation="sessions",' + TARGET + '}) > 0)', instant=True)], "s",
    thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 120}, {"color": "red", "value": 300}])
add("Exporter uptime", "stat", 20, 1, 4, 4,
    "Time since this exporter process started.",
    [target('time() - max(process_start_time_seconds{job="$job",instance="$instance"})', instant=True)], "s")

add("Session state", "timeseries", 0, 5, 12, 7,
    "Plex's current state for each tracked stream. Counts drop when a stream disappears from /status/sessions.",
    [target('sum by (state) (plex_sessions{' + BASE + '})', "{{state}}")], "short")
add("Plex request status", "timeseries", 12, 5, 12, 7,
    "Success by independent operation. A zero before the first successful request is uninitialized; pair it with the last-success timestamp to distinguish that from a recent failure.",
    [target('min by (operation) (plex_operation_success{' + TARGET + '})', "{{operation}}")], "none")

add("Server", "row", 0, 12, 24, 1)
add("Plex server", "table", 0, 13, 8, 5,
    "Identity from the last successful server refresh.",
    [target('max by (server, version, platform, platform_version) (' + q("server_info") + ')', instant=True)], "short")
panels[-1]["transformations"] = [{"id": "organize", "options": {
    "excludeByName": {"Time": True, "Value": True},
    "renameByName": {"server": "Server", "version": "Plex version", "platform": "Platform", "platform_version": "Platform version"}}}]
add("Host utilization", "timeseries", 8, 13, 8, 5,
    "Plex host CPU and memory percentages. These are host values, not exporter-container values.",
    [target(q("host_cpu_util"), "CPU"), target(q("host_mem_util"), "Memory", ref="B")], "percent", 100)
add("Network traffic", "timeseries", 16, 13, 8, 5,
    "Reported and estimated bytes sent by Plex. The estimate is derived from active playback bitrate and can diverge from Plex's counter.",
    [target('rate(' + q("transmit_bytes_total") + '[$__rate_interval])', "Reported", ref="A"),
     target('rate(' + q("estimated_transmit_bytes_total") + '[$__rate_interval])', "Estimated", ref="B")], "Bps")

add("Libraries", "row", 0, 18, 24, 1)
add("Library items", "bargauge", 0, 19, 8, 8,
    "Counts are cached and may lag a large Plex scan. TV libraries report shows here; plex_media_episodes reports episodes.",
    [target('max by (library) (' + q("plex_library_items") + ')', "{{library}}", instant=True)], "short")
add("Library storage", "bargauge", 8, 19, 8, 8,
    "Plex-reported storage by library. Overlapping libraries can count the same files more than once.",
    [target('max by (library) (' + q("library_storage_total") + ')', "{{library}}", instant=True)], "bytes")
add("Library duration", "bargauge", 16, 19, 8, 8,
    "Plex reports duration in milliseconds; this panel converts it to seconds.",
    [target('max by (library) (' + q("library_duration_total") + ') / 1000', "{{library}}", instant=True)], "s")

add("Playback detail", "row", 0, 27, 24, 1)
add("Current sessions", "table", 0, 28, 24, 8,
    "Current and recently retained session records. This table uses the bounded state metric and does not infer activity from plays_total.",
    [target('max by (session, user, state) (' + q("plex_session_state") + ')', instant=True)], "short")
panels[-1]["transformations"] = [{"id": "organize", "options": {
    "excludeByName": {"Time": True},
    "renameByName": {"session": "Session", "user": "User", "state": "State", "Value": "Current"}}}]
add("Playback time", "timeseries", 0, 36, 12, 8,
    "Per-session observed playback seconds. Labels are stable across bitrate changes; restarts and short sessions affect the history.",
    [target('sum by (user) (rate(' + q("plex_session_play_seconds_total") + '[$__rate_interval]))', "{{user}}")], "suffix:s/s")
add("Bitrate and bandwidth", "timeseries", 12, 36, 12, 8,
    "Numeric gauges avoid putting changing bitrate values in playback labels. Location is reported when Plex supplies it.",
    [target('max by (session, user, location) (' + q("plex_session_bitrate_kbps") + ')', "bitrate {{user}} / {{location}}"),
     target('max by (session, user, location) (' + q("plex_session_bandwidth_kbps") + ')', "bandwidth {{user}} / {{location}}", ref="B")], "Kbits")
add("Video transcode pipeline", "table", 0, 44, 12, 7,
    "A row exists when Plex reports video transcode evidence. Hardware/software fields are bounded values; unknown means Plex omitted the detail.",
    [target('max by (session, user, decode, encode) (' + q("plex_session_video_transcode") + ')', instant=True)], "none")
panels[-1]["transformations"] = [{"id": "organize", "options": {
    "excludeByName": {"Time": True, "Value": True},
    "renameByName": {"session": "Session", "user": "User", "decode": "Decode", "encode": "Encode"}}}]
add("Playback by transcode", "bargauge", 12, 44, 6, 7,
    "Observed playback-time rate by transcode type. This is sampled activity, not an exact concurrent count.",
    [target('sum by (transcode_type) (rate(' + q("play_seconds_total") + '[$__rate_interval]))', "{{transcode_type}}", instant=True)], "suffix:s/s")
add("Playback by subtitle action", "bargauge", 18, 44, 6, 7,
    "Observed playback-time rate by subtitle action.",
    [target('sum by (subtitle_action) (rate(' + q("play_seconds_total") + '[$__rate_interval]))', "{{subtitle_action}}", instant=True)], "suffix:s/s")

dashboard = {
    "id": None,
    "uid": "plex-exporter-overview",
    "title": "Plex Exporter",
    "description": "An operations view for Plex reachability, session state, libraries, traffic, and observed playback.",
    "tags": ["plex", "prometheus", "plex-exporter"],
    "timezone": "browser",
    "editable": True,
    "schemaVersion": 39,
    "version": 3,
    "graphTooltip": 1,
    "refresh": "1m",
    "time": {"from": "now-6h", "to": "now"},
    "timepicker": {"refresh_intervals": ["30s", "1m", "5m", "15m"]},
    "annotations": {"list": []},
    "links": [],
    "panels": panels,
    "templating": {"list": [
        {"name": "datasource", "label": "Prometheus", "type": "datasource", "query": "prometheus", "refresh": 1, "options": [], "current": {}},
        variable("job", "Job", 'label_values(server_info{server_type="plex"}, job)'),
        variable("instance", "Instance", 'label_values(server_info{job="$job",server_type="plex"}, instance)'),
        {"name": "scrape_interval", "label": "Scrape interval", "type": "interval", "query": "15s,30s,1m,2m,5m", "auto": False, "refresh": 2, "options": [], "current": {"text": "1m", "value": "1m"}},
    ]},
}

OUTPUT.parent.mkdir(exist_ok=True)
OUTPUT.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
print(f"wrote {OUTPUT} ({len(panels)} panels)")

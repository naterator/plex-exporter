#!/usr/bin/env node

// Render the dashboard through Grafana's snapshot viewer with synthetic data.
// The live Prometheus query endpoints are blocked by the local proxy.

const fs = require("fs");
const http = require("http");
const playwright = require(process.env.PLAYWRIGHT_MODULE || "playwright");

const root = process.env.REPO_ROOT || process.cwd();
const dashboardFile = process.env.DASHBOARD_JSON || root + "/grafana/plex-exporter.json";
const imageFile = process.env.PREVIEW_IMAGE || root + "/grafana/plex-exporter.png";
const upstreamURL = new URL(process.env.GRAFANA_URL || "http://127.0.0.1:31092");
const upstreamTransport = upstreamURL.protocol === "https:" ? require("https") : http;
const proxyPort = Number(process.env.PREVIEW_PORT || 34000);
const snapshotKey = "plex-exporter-preview";

function credentials() {
  const user = process.env.GRAFANA_USER;
  const password = process.env.GRAFANA_PASSWORD;
  if (!user || !password) throw new Error("set GRAFANA_USER and GRAFANA_PASSWORD");
  return { user: user, password: password };
}

function field(name, type, values, config) {
  return { name: name, type: type, values: values, config: config || {} };
}
function frame(refId, fields) {
  return { refId: refId, fields: fields };
}
function series(refId, name, values, times) {
  return frame(refId, [
    field("Time", "time", times),
    field(name, "number", values, { displayNameFromDS: name })
  ]);
}
function table(refId, columns) {
  return frame(refId, columns.map(function (column) {
    return field(column[0], column[1], column[2]);
  }));
}

function panelFrames(panel, times) {
  const id = panel.id;
  const ref = panel.targets && panel.targets[0] ? panel.targets[0].refId || "A" : "A";
  const last = [times[times.length - 1]];
  switch (id) {
    case 2: return [series(ref, "Exporter", [1], last)];
    case 3: return [series(ref, "Session poll", [1], last)];
    case 4: return [series(ref, "WebSocket", [1], last)];
    case 5: return [series(ref, "Active streams", [3], last)];
    case 6: return [series(ref, "Stale seconds", [18], last)];
    case 7: return [series(ref, "Uptime", [7200], last)];
    case 8: return [
      series(ref, "Playing", [1, 1, 2, 3, 2, 2], times),
      series(ref, "Paused", [0, 1, 1, 0, 1, 1], times),
      series(ref, "Buffering", [0, 0, 1, 0, 0, 0], times)
    ];
    case 9: return [
      series(ref, "sessions", [1, 1, 1, 1, 1, 1], times),
      series(ref, "library", [1, 1, 1, 1, 1, 1], times),
      series(ref, "server", [1, 1, 1, 1, 1, 1], times)
    ];
    case 11: return [table(ref, [
      ["server", "string", ["Example Plex Server"]],
      ["version", "string", ["1.41.0.8994"]],
      ["platform", "string", ["Linux"]],
      ["platform_version", "string", ["Example OS 24.04"]],
      ["Value", "number", [1]]
    ])];
    case 12: return [
      series("A", "CPU", [28, 34, 31, 42, 38, 36], times),
      series("B", "Memory", [54, 56, 55, 58, 57, 59], times)
    ];
    case 13: return [
      series("A", "Reported", [1800000, 2200000, 1950000, 2800000, 2500000, 2300000], times),
      series("B", "Estimated", [2000000, 2400000, 2100000, 3000000, 2700000, 2500000], times)
    ];
    case 15: return [frame(ref, [
      field("Example Movies", "number", [2400]),
      field("Example Shows", "number", [680])
    ])];
    case 16: return [frame(ref, [
      field("Example Movies", "number", [3200000000000]),
      field("Example Shows", "number", [840000000000])
    ])];
    case 17: return [frame(ref, [
      field("Example Movies", "number", [180000000]),
      field("Example Shows", "number", [46000000])
    ])];
    case 19: return [table(ref, [
      ["session", "string", ["example-session-01", "example-session-02", "example-session-03"]],
      ["user", "string", ["Example User A", "Example User B", "Example User A"]],
      ["state", "string", ["playing", "playing", "paused"]],
      ["Value", "number", [1, 1, 1]]
    ])];
    case 20: return [
      series(ref, "Example User A", [0.9, 1, 1, 1, 1, 1], times),
      series(ref, "Example User B", [0, 0.8, 0.8, 0.8, 0.8, 0.8], times)
    ];
    case 21: return [
      series("A", "bitrate Example User A / local", [4500, 4700, 4600, 4800, 4750, 4650], times),
      series("B", "bandwidth Example User A / local", [5000, 5200, 5100, 5300, 5250, 5150], times)
    ];
    case 22: return [table(ref, [
      ["session", "string", ["example-session-01", "example-session-02"]],
      ["user", "string", ["Example User A", "Example User B"]],
      ["decode", "string", ["hardware", "software"]],
      ["encode", "string", ["hardware", "unknown"]],
      ["Value", "number", [1, 1]]
    ])];
    case 23: return [frame(ref, [
      field("Direct play", "number", [1]),
      field("Transcode", "number", [0.7])
    ])];
    case 24: return [frame(ref, [
      field("None", "number", [1.3]),
      field("Burned in", "number", [0.4])
    ])];
    default: return [];
  }
}

function snapshot() {
  const dashboard = JSON.parse(fs.readFileSync(dashboardFile, "utf8"));
  const now = Date.now();
  const times = [5, 4, 3, 2, 1, 0].map(function (hours) {
    return now - hours * 60 * 60 * 1000;
  });
  dashboard.id = null;
  dashboard.uid = snapshotKey;
  dashboard.title = "Plex Exporter · Example data";
  if (dashboard.templating && dashboard.templating.list) {
    dashboard.templating.list.forEach(function (variable) {
      if (variable.name === "datasource") variable.current = { text: "Prometheus", value: "example-prometheus" };
      if (variable.name === "job") variable.current = { text: "plex-exporter", value: "plex-exporter" };
      if (variable.name === "instance") variable.current = { text: "example:9000", value: "example:9000" };
    });
  }
  dashboard.panels = dashboard.panels.map(function (panel) {
    const copy = Object.assign({}, panel);
    const frames = panelFrames(copy, times);
    if (frames.length) copy.snapshotData = frames;
    if (copy.targets) {
      copy.targets = copy.targets.map(function (target) {
        return Object.assign({}, target, { datasource: { type: "datasource", uid: "-- Mixed --" } });
      });
    }
    return copy;
  });
  return {
    meta: { isSnapshot: true, snapshotKey: snapshotKey, canSave: false, canEdit: false, canShare: false },
    dashboard: dashboard
  };
}

function runProxy(creds, data) {
  const auth = "Basic " + Buffer.from(creds.user + ":" + creds.password).toString("base64");
  const blocked = ["/api/ds/query", "/api/query", "/api/datasources/proxy", "/api/annotations"];
  const server = http.createServer(function (request, response) {
    if (request.url.indexOf("/api/snapshots/" + snapshotKey) === 0) {
      const body = JSON.stringify(data);
      response.writeHead(200, { "content-type": "application/json", "content-length": Buffer.byteLength(body) });
      response.end(body);
      return;
    }
    if (blocked.some(function (path) { return request.url.indexOf(path) === 0; })) {
      response.writeHead(403, { "content-type": "text/plain" });
      response.end("live data queries are disabled for the preview");
      return;
    }
    const headers = Object.assign({}, request.headers, {
      host: upstreamURL.host,
      authorization: auth
    });
    const upstream = upstreamTransport.request({
      hostname: upstreamURL.hostname,
      port: upstreamURL.port || (upstreamURL.protocol === "https:" ? 443 : 80),
      path: request.url,
      method: request.method,
      headers: headers
    }, function (upstreamResponse) {
      response.writeHead(upstreamResponse.statusCode, upstreamResponse.headers);
      upstreamResponse.pipe(response);
    });
    upstream.on("error", function (error) {
      response.writeHead(502, { "content-type": "text/plain" });
      response.end("upstream Grafana error: " + error.message);
    });
    request.pipe(upstream);
  });
  return new Promise(function (resolve) {
    server.listen(proxyPort, "127.0.0.1", function () { resolve(server); });
  });
}

async function main() {
  const creds = credentials();
  const data = snapshot();
  const server = await runProxy(creds, data);
  let browser;
  try {
    const engine = process.env.BROWSER_ENGINE === "webkit" ? playwright.webkit : playwright.chromium;
    const browserOptions = { headless: true };
    if (process.env.CHROMIUM_PATH && engine === playwright.chromium) browserOptions.executablePath = process.env.CHROMIUM_PATH;
    browser = await engine.launch(browserOptions);
    // Keep the complete dashboard in the initial viewport so Grafana's panel
    // virtualization does not unmount the lower table while capturing.
    const context = await browser.newContext({ viewport: { width: 1800, height: 2200 }, deviceScaleFactor: 1 });
    const page = await context.newPage();
    page.on("console", function (message) {
      if (message.type() === "error") console.error("browser: " + message.text());
    });
    page.on("response", function (response) {
      if (response.status() >= 400) console.error("http " + response.status() + " " + response.url());
    });
    await page.goto("http://127.0.0.1:" + proxyPort + "/dashboard/snapshot/" + snapshotKey + "?orgId=1&kiosk=1", {
      waitUntil: "domcontentloaded",
      timeout: 60000
    });
    await page.waitForTimeout(Number(process.env.PREVIEW_WAIT_MS || 12000));
    await page.waitForTimeout(5000);
    await page.evaluate(function () {
      Array.from(document.querySelectorAll("*"))
        .filter(function (element) {
          const rect = element.getBoundingClientRect();
          const text = (element.textContent || "").trim();
          return text.indexOf("Powered by") === 0 && rect.height <= 40 && rect.width >= 500;
        })
        .forEach(function (element) { element.style.setProperty("display", "none", "important"); });
    });
    const contentBottom = await page.evaluate(function () {
      return Math.max.apply(null, Array.from(document.querySelectorAll("section")).map(function (section) {
        return section.getBoundingClientRect().bottom;
      }));
    });
    const captureHeight = Math.ceil(Math.min(contentBottom + 24, 2200));
    await page.screenshot({ path: imageFile, clip: { x: 0, y: 0, width: 1800, height: captureHeight } });
    console.log(JSON.stringify({ imagePath: imageFile, title: await page.title(), url: page.url(), captureHeight: captureHeight }));
  } finally {
    if (browser) await browser.close();
    server.close();
    creds.user = "";
    creds.password = "";
  }
}

main().catch(function (error) {
  console.error(error.stack || error);
  process.exitCode = 1;
});

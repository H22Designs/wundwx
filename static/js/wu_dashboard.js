"use strict";

Chart.defaults.color = "#7a90ae";
Chart.defaults.borderColor = "#1e3050";
Chart.defaults.font.family = "'Inter', 'Segoe UI', system-ui, sans-serif";
Chart.defaults.font.size = 11;

// ── State ────────────────────────────────────────────────────────────────────
const charts = {};
let currentStation = "KALMILLP10";
let currentHours = 24;
let customStart = null;
let customEnd = null;
let refreshMs = 30000;
let useCelsius = false;
let refreshTimer = null;

// ── Helpers ──────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt = (v, dec = 1, fb = "—") => v == null ? fb : Number(v).toFixed(dec);
const toC = f => f == null ? null : (f - 32) * 5 / 9;
const tempStr = f => {
    if (f == null) return "—";
    return useCelsius ? toC(f).toFixed(1) + "°C" : Math.round(f) + "°F";
};
const tempVal = f => {
    if (f == null) return "—";
    return useCelsius ? toC(f).toFixed(0) : Math.round(f).toString();
};

const degToCardinal = d => {
    if (d == null) return "—";
    const dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"];
    return dirs[Math.round(d / 22.5) % 16];
};

const uvLabel = v => {
    if (v == null) return ["—", "", ""];
    if (v < 3) return [v.toFixed(1), "uv-low", "Low"];
    if (v < 6) return [v.toFixed(1), "uv-mod", "Moderate"];
    if (v < 8) return [v.toFixed(1), "uv-high", "High"];
    if (v < 11) return [v.toFixed(1), "uv-vhigh", "Very High"];
    return [v.toFixed(1), "uv-ext", "Extreme"];
};

const isNight = timeStr => {
    if (!timeStr) return false;
    const date = new Date(timeStr);
    const h = date.getHours();
    // Heuristic: night is between 7 PM and 6 AM
    return h >= 19 || h < 6;
};

const weatherIcon = (temp, uv, rain, obsTime) => {
    const night = isNight(obsTime);
    if (rain > 0) return "🌧️";
    if (night) {
        if (temp < 32) return "❄️\uFE0E🌙";
        return "🌙";
    }
    if (uv > 6) return "☀️";
    if (uv > 2) return "🌤️";
    if (temp < 32) return "❄️";
    return "⛅";
};

const baroTrend = rows => {
    if (!rows || rows.length < 3) return ["→", "steady"];
    const recent = rows.slice(-3).map(r => r.pressure_in).filter(v => v != null);
    if (recent.length < 2) return ["→", "steady"];
    const diff = recent[recent.length - 1] - recent[0];
    if (diff > 0.02) return ["↑", "rising"];
    if (diff < -0.02) return ["↓", "falling"];
    return ["→", "steady"];
};

function setText(id, val) { const el = $(id); if (el) el.textContent = val; }
function setWidth(id, val) { const el = $(id); if (el) el.style.width = val; }
function rotateNeedle(id, deg) { const el = $(id); if (el && deg != null) el.style.transform = `rotate(${deg}deg)`; }

// Min/max helpers from history
const hVals = (rows, key) => (rows || []).map(r => r[key]).filter(v => v != null);
const hMin = (rows, key) => { const v = hVals(rows, key); return v.length ? Math.min(...v) : null; };
const hMax = (rows, key) => { const v = hVals(rows, key); return v.length ? Math.max(...v) : null; };
const hAvg = (rows, key) => { const v = hVals(rows, key); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; };

// ── URL builders ─────────────────────────────────────────────────────────────
function historyUrl() {
    const base = `/api/history?station=${currentStation}`;
    if (customStart && customEnd) {
        const s = new Date(customStart).toISOString();
        const e = new Date(customEnd).toISOString();
        return `${base}&start=${encodeURIComponent(s)}&end=${encodeURIComponent(e)}&limit=1000`;
    }
    return `${base}&hours=${currentHours}&limit=500`;
}

const timeframeLabel = () => {
    if (customStart && customEnd) return "Custom";
    if (currentHours <= 6) return "6h";
    if (currentHours <= 24) return "24h";
    if (currentHours <= 48) return "48h";
    return "5d";
};

// ── Settings persistence (localStorage) ──────────────────────────────────────
function loadSettings() {
    try {
        const s = JSON.parse(localStorage.getItem("wx_settings") || "{}");
        if (s.station) currentStation = s.station;
        if (s.hours) currentHours = s.hours;
        if (s.refreshMs) refreshMs = s.refreshMs;
        if (s.useCelsius != null) useCelsius = s.useCelsius;

        // Card visibility
        if (s.hiddenCards) s.hiddenCards.forEach(id => {
            const cb = document.querySelector(`[data-card="${id}"]`);
            if (cb) cb.checked = false;
            const card = document.querySelector(`[data-card-id="${id}"]`);
            if (card) card.style.display = "none";
        });
        // Chart visibility
        if (s.hiddenCharts) s.hiddenCharts.forEach(id => {
            const cb = document.querySelector(`[data-chart="${id}"]`);
            if (cb) cb.checked = false;
            const card = document.querySelector(`[data-chart-id="${id}"]`);
            if (card) card.style.display = "none";
        });
        // Section toggles
        if (s.hideNearby) { $("toggle-nearby").checked = false; }
        if (s.hideDaily) { $("toggle-daily").checked = false; }
        if (s.hideAlerts) { $("toggle-alerts").checked = false; }
        // Refresh select
        const ri = $("refresh-interval");
        if (ri) ri.value = String(refreshMs / 1000);
        // Temp unit select
        const tu = $("temp-unit");
        if (tu) tu.value = useCelsius ? "c" : "f";
    } catch (e) { }
}

function saveSettings() {
    const hiddenCards = [];
    document.querySelectorAll("#card-toggles input").forEach(cb => {
        if (!cb.checked) hiddenCards.push(cb.dataset.card);
    });
    const hiddenCharts = [];
    document.querySelectorAll("#chart-toggles input").forEach(cb => {
        if (!cb.checked) hiddenCharts.push(cb.dataset.chart);
    });
    localStorage.setItem("wx_settings", JSON.stringify({
        station: currentStation,
        hours: currentHours,
        refreshMs,
        useCelsius,
        hiddenCards,
        hiddenCharts,
        hideNearby: !$("toggle-nearby")?.checked,
        hideDaily: !$("toggle-daily")?.checked,
        hideAlerts: !$("toggle-alerts")?.checked,
    }));
}

// ── Station tabs ─────────────────────────────────────────────────────────────
async function initStationTabs() {
    try {
        const stations = await fetch("/api/stations").then(r => r.json());
        const container = $("station-tabs");
        if (!container) return;
        container.innerHTML = "";
        stations.forEach(s => {
            const btn = document.createElement("button");
            btn.className = "station-tab" + (s.id === currentStation ? " active" : "");
            btn.textContent = s.id;
            btn.title = s.name;
            btn.addEventListener("click", () => {
                currentStation = s.id;
                container.querySelectorAll(".station-tab").forEach(b => b.classList.remove("active"));
                btn.classList.add("active");
                setText("header-station-name", s.name || s.id);
                saveSettings();
                fetchAll();
            });
            container.appendChild(btn);
        });
        // Set header name for current station
        const cur = stations.find(s => s.id === currentStation);
        if (cur) setText("header-station-name", cur.name || cur.id);
    } catch (e) { }
}

// ── Settings panel wiring ────────────────────────────────────────────────────
function initSettings() {
    const toggle = $("settings-toggle");
    const panel = $("settings-panel");
    const close = $("settings-close");
    if (toggle && panel) {
        toggle.addEventListener("click", () => {
            panel.style.display = panel.style.display === "none" ? "" : "none";
        });
    }
    if (close && panel) {
        close.addEventListener("click", () => panel.style.display = "none");
    }

    // Card toggles
    document.querySelectorAll("#card-toggles input").forEach(cb => {
        cb.addEventListener("change", () => {
            const card = document.querySelector(`[data-card-id="${cb.dataset.card}"]`);
            if (card) card.style.display = cb.checked ? "" : "none";
            saveSettings();
        });
    });

    // Chart toggles
    document.querySelectorAll("#chart-toggles input").forEach(cb => {
        cb.addEventListener("change", () => {
            const card = document.querySelector(`[data-chart-id="${cb.dataset.chart}"]`);
            if (card) card.style.display = cb.checked ? "" : "none";
            saveSettings();
        });
    });

    // Section toggles
    $("toggle-nearby")?.addEventListener("change", e => {
        const sec = $("nearby-section");
        if (sec) sec.style.display = e.target.checked ? "" : "none";
        saveSettings();
    });
    $("toggle-daily")?.addEventListener("change", e => {
        const sec = $("daily-section");
        if (sec) sec.style.display = e.target.checked ? "" : "none";
        saveSettings();
    });
    $("toggle-alerts")?.addEventListener("change", e => {
        const sec = $("alerts-container");
        if (sec) sec.style.display = e.target.checked ? "" : "none";
        saveSettings();
    });

    // Refresh interval
    $("refresh-interval")?.addEventListener("change", e => {
        refreshMs = parseInt(e.target.value) * 1000;
        clearInterval(refreshTimer);
        refreshTimer = setInterval(fetchAll, refreshMs);
        saveSettings();
    });

    // Temp unit
    $("temp-unit")?.addEventListener("change", e => {
        useCelsius = e.target.value === "c";
        setText("temp-unit-label", useCelsius ? "°C" : "°F");
        saveSettings();
        fetchAll();
    });
}

// ── DOM updaters ──────────────────────────────────────────────────────────────
function updateHero(d) {
    if (!d) return;
    setText("temp-val", tempVal(d.temp_f));
    setText("temp-unit-label", useCelsius ? "°C" : "°F");

    const iconEl = $("cond-icon");
    if (iconEl) iconEl.textContent = weatherIcon(d.temp_f, d.uv_index, d.precip_total_in || 0, d.obs_time_utc);

    const feels = d.wind_chill_f ?? d.heat_index_f ?? null;
    setText("feels-like", tempStr(feels));
    setText("dew-point", tempStr(d.dew_point_f));
    setText("humidity-val", d.humidity_pct != null ? Math.round(d.humidity_pct) + "%" : "—");

    const luEl = $("last-update-time");
    if (luEl && d.obs_time_utc) {
        const dt = new Date(d.obs_time_utc);
        luEl.textContent = dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }

    const noData = $("no-data-state");
    const dash = $("dashboard-state");
    if (noData) noData.style.display = "none";
    if (dash) dash.style.display = "block";
}

function updateSummary(histRows) {
    setText("summary-label", timeframeLabel());
    if (!histRows || !histRows.length) return;
    setText("summary-high", tempStr(hMax(histRows, "temp_f")));
    setText("summary-low", tempStr(hMin(histRows, "temp_f")));
    const avgT = hAvg(histRows, "temp_f");
    setText("summary-avg", tempStr(avgT));
    setText("summary-rain", fmt(hMax(histRows, "precip_total_in"), 2) + '"');
    setText("summary-uv", fmt(hMax(histRows, "uv_index"), 0));
    setText("summary-gust", fmt(hMax(histRows, "wind_gust_mph"), 1) + " mph");
}

function updateMetrics(d, histRows) {
    if (!d) return;

    // Wind
    setText("wind-speed", fmt(d.wind_speed_mph, 1));
    setText("wind-gust", fmt(d.wind_gust_mph, 1));
    setText("wind-dir-txt", degToCardinal(d.wind_dir_deg));
    setText("wind-dir-deg", d.wind_dir_deg != null ? d.wind_dir_deg + "°" : "—");
    rotateNeedle("compass-needle-el", d.wind_dir_deg);
    setText("wind-max", fmt(hMax(histRows, "wind_speed_mph"), 1) + " mph");
    setText("gust-max", fmt(hMax(histRows, "wind_gust_mph"), 1) + " mph");

    // Humidity
    setWidth("hum-bar", (d.humidity_pct || 0) + "%");
    setText("hum-val2", fmt(d.humidity_pct, 0) + "%");
    setText("hum-high", fmt(hMax(histRows, "humidity_pct"), 0) + "%");
    setText("hum-low", fmt(hMin(histRows, "humidity_pct"), 0) + "%");

    // Pressure
    setText("baro-val", fmt(d.pressure_in, 2));
    const [tArrow, tClass] = baroTrend(histRows);
    const trendEl = $("baro-trend");
    if (trendEl) { trendEl.textContent = tArrow; trendEl.className = "trend-" + tClass; }
    setText("baro-high", fmt(hMax(histRows, "pressure_in"), 2) + " inHg");
    setText("baro-low", fmt(hMin(histRows, "pressure_in"), 2) + " inHg");
    if (d.pressure_in != null) {
        const dev = (d.pressure_in - 29.92).toFixed(2);
        const devEl = $("baro-dev");
        if (devEl) {
            const sign = dev > 0 ? "+" : "";
            devEl.innerHTML = parseFloat(dev) < 0
                ? `<span class="text-red">${sign}${dev} inHg</span>`
                : `<span class="text-green">${sign}${dev} inHg</span>`;
        }
    }

    // Rain
    setText("rain-rate", fmt(d.precip_rate_in_hr, 2));
    setText("rain-daily", fmt(d.precip_total_in, 2));
    setText("rain-max-rate", fmt(hMax(histRows, "precip_rate_in_hr"), 2) + " in/hr");

    // UV
    const [uvVal, uvCls, uvLbl] = uvLabel(d.uv_index);
    const uvEl = $("uv-val");
    if (uvEl) { uvEl.textContent = uvVal; uvEl.className = "metric-value " + uvCls; }
    setText("uv-label", uvLbl);
    setText("uv-max-val", fmt(hMax(histRows, "uv_index"), 1));

    // Solar
    setText("solar-val", fmt(d.solar_radiation_wm2, 0));
    const solarPct = d.solar_radiation_wm2 ? Math.min(d.solar_radiation_wm2 / 1200 * 100, 100) : 0;
    setWidth("solar-bar", solarPct + "%");

    // Dew Point
    setText("dewpt-val", tempVal(d.dew_point_f));
    if (d.temp_f != null && d.dew_point_f != null) setText("dewpt-spread", (d.temp_f - d.dew_point_f).toFixed(1) + "°");
    setText("dewpt-high", tempStr(hMax(histRows, "dew_point_f")));
    setText("dewpt-low", tempStr(hMin(histRows, "dew_point_f")));

    // Feels Like
    const feelsLike = d.wind_chill_f ?? d.heat_index_f ?? d.temp_f;
    setText("feelslike-val", tempVal(feelsLike));
    setText("heat-index-val", d.heat_index_f != null ? tempStr(d.heat_index_f) : "—");
    setText("wind-chill-val", d.wind_chill_f != null ? tempStr(d.wind_chill_f) : "—");

    // Station
    setText("station-id-display", d.station_id || currentStation);
    setText("station-readings", histRows ? histRows.length + " pts" : "—");
}

// ── Alerts ────────────────────────────────────────────────────────────────────
function renderAlerts(alerts) {
    const container = $("alerts-container");
    if (!container) return;

    // Show station label
    setText("alerts-station-label", currentStation);

    // If no alerts, show the all-clear message
    if (!alerts || alerts.length === 0) {
        container.innerHTML = '<div class="alert-none">✅ No active watches or warnings for this area</div>';
        return;
    }

    container.innerHTML = "";
    alerts.forEach(a => {
        const sev = (a.severity || "").toLowerCase();
        let sevClass = "severity-advisory", icon = "⚠️";
        if (sev === "extreme" || sev === "severe") { sevClass = "severity-warning"; icon = "🚨"; }
        else if (sev === "moderate") { sevClass = "severity-watch"; }
        const div = document.createElement("div");
        div.className = `alert-banner ${sevClass}`;
        div.innerHTML = `
            <span class="alert-icon">${icon}</span>
            <div class="alert-content">
                <div class="alert-event">${a.event || "Weather Alert"}</div>
                <div class="alert-headline">${a.headline || ""}</div>
                ${a.expires ? `<div style="font-size:.7rem; color:var(--text-dim); margin-top:.3rem;">Expires: ${new Date(a.expires).toLocaleString()}</div>` : ""}
            </div>
        `;
        container.appendChild(div);
    });
}

// ── Nearby ────────────────────────────────────────────────────────────────────
function renderNearby(stations) {
    const section = $("nearby-section");
    const grid = $("nearby-grid");
    if (!section || !grid) return;
    if (!stations || stations.length === 0) { section.style.display = "none"; return; }
    if (!$("toggle-nearby")?.checked) { section.style.display = "none"; return; }
    section.style.display = "";
    grid.innerHTML = "";
    stations.forEach(s => {
        const card = document.createElement("div");
        card.className = "nearby-card";
        card.innerHTML = `<div class="nearby-station-id">${s.stationID}</div><div class="nearby-neighborhood">${s.neighborhood || ""}</div><div class="nearby-stats"><span>${s.temp_f != null ? s.temp_f + "°F" : "—"}</span><span>${s.humidity != null ? s.humidity + "%" : ""}</span><span>${s.wind_speed != null ? s.wind_speed + " mph" : ""}</span></div>`;
        grid.appendChild(card);
    });
}

// ── Charts ────────────────────────────────────────────────────────────────────
function buildChart(id, config) {
    const ctx = $(id);
    if (!ctx) return null;
    if (charts[id]) charts[id].destroy();
    charts[id] = new Chart(ctx, config);
    return charts[id];
}

const lineDs = (label, data, color, filled = false) => ({
    label, data, borderColor: color,
    backgroundColor: filled ? color.replace(")", ", .1)").replace("rgb", "rgba") : "transparent",
    borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, tension: 0.4, fill: filled,
});
const barDs = (label, data, color) => ({
    label, data, backgroundColor: color, borderRadius: 3, borderSkipped: false,
});

const xAxis = () => ({ type: "time", time: { tooltipFormat: "MMM d, h:mm a", displayFormats: { hour: "h a", minute: "h:mm a" } }, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 6 } });
const opts = yLabel => ({
    responsive: true, maintainAspectRatio: false, interaction: { mode: "index", intersect: false },
    plugins: { legend: { display: false }, tooltip: { backgroundColor: "#1a2436", borderColor: "#1e3050", borderWidth: 1, titleColor: "#f0f6ff", bodyColor: "#7a90ae", padding: 10 } },
    scales: { x: xAxis(), y: { grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 }, title: { display: !!yLabel, text: yLabel, color: "#4a607a", font: { size: 10 } } } },
});
const withLeg = o => ({ ...o, plugins: { ...o.plugins, legend: { display: true, labels: { color: "#7a90ae", boxWidth: 12, padding: 12 } } } });

function renderCharts(rows) {
    if (!rows || !rows.length) return;
    const pts = rows.map(r => r.obs_time_utc);
    const tLabel = useCelsius ? "°C" : "°F";
    const tempData = useCelsius ? rows.map(r => toC(r.temp_f)) : rows.map(r => r.temp_f);
    const dpData = useCelsius ? rows.map(r => toC(r.dew_point_f)) : rows.map(r => r.dew_point_f);
    const hiData = useCelsius ? rows.map(r => toC(r.heat_index_f)) : rows.map(r => r.heat_index_f);

    buildChart("chart-temp", { type: "line", data: { labels: pts, datasets: [lineDs("Temp", tempData, "rgb(249,115,22)", true), lineDs("Dew Pt", dpData, "rgb(20,184,166)"), lineDs("Heat Idx", hiData, "rgb(168,85,247)")] }, options: withLeg(opts(tLabel)) });
    buildChart("chart-hum", { type: "line", data: { labels: pts, datasets: [lineDs("Humidity (%)", rows.map(r => r.humidity_pct), "rgb(59,130,246)", true)] }, options: { ...opts("%"), scales: { ...opts().scales, y: { min: 0, max: 100, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } } } });
    buildChart("chart-baro", { type: "line", data: { labels: pts, datasets: [lineDs("Pressure", rows.map(r => r.pressure_in), "rgb(34,197,94)")] }, options: opts("inHg") });
    buildChart("chart-wind", { type: "line", data: { labels: pts, datasets: [lineDs("Wind", rows.map(r => r.wind_speed_mph), "rgb(96,165,250)"), lineDs("Gust", rows.map(r => r.wind_gust_mph), "rgb(249,115,22)")] }, options: withLeg(opts("mph")) });
    buildChart("chart-rain", { type: "bar", data: { labels: pts, datasets: [barDs("Rain Rate", rows.map(r => r.precip_rate_in_hr || 0), "rgba(59,130,246,.7)")] }, options: { ...opts("in/hr"), scales: { ...opts().scales, y: { min: 0, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } } } });
    buildChart("chart-uv", { type: "bar", data: { labels: pts, datasets: [barDs("UV", rows.map(r => r.uv_index || 0), "rgba(234,179,8,.7)")] }, options: { ...opts("UV"), scales: { ...opts().scales, y: { min: 0, max: 12, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } } } });
    buildChart("chart-winddir", { type: "line", data: { labels: pts, datasets: [{ label: "Dir (°)", data: rows.map(r => r.wind_dir_deg), borderColor: "rgb(168,85,247)", backgroundColor: "transparent", borderWidth: 1, pointRadius: 2, pointBackgroundColor: "rgb(168,85,247)", tension: 0, fill: false }] }, options: { ...opts("°"), scales: { x: xAxis(), y: { min: 0, max: 360, grid: { color: "#1e3050" }, ticks: { stepSize: 90, callback: v => ({ 0: "N", 90: "E", 180: "S", 270: "W", 360: "N" }[v] || v) } } } } });
    buildChart("chart-solar", { type: "line", data: { labels: pts, datasets: [lineDs("Solar", rows.map(r => r.solar_radiation_wm2 || 0), "rgb(234,179,8)", true)] }, options: { ...opts("W/m²"), scales: { ...opts().scales, y: { min: 0, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } } } });
}

// ── Daily table ───────────────────────────────────────────────────────────────
function renderDailyTable(days) {
    const tbody = $("daily-tbody");
    if (!tbody) return;
    tbody.innerHTML = "";
    days.forEach(d => {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${d.day}</td><td>${tempStr(d.temp_high_f)}</td><td>${tempStr(d.temp_low_f)}</td><td>${tempStr(d.temp_avg_f)}</td><td>${fmt(d.humidity_high, 0)}%</td><td>${fmt(d.humidity_low, 0)}%</td><td>${fmt(d.pressure_avg, 2)}</td><td>${fmt(d.wind_gust_max, 1)}</td><td>${fmt(d.rain_total, 2)}"</td><td>${fmt(d.uv_max, 0)}</td><td>${d.reading_count}</td>`;
        tbody.appendChild(tr);
    });
}

// ── Fetch all ─────────────────────────────────────────────────────────────────
async function fetchAll() {
    try {
        const s = currentStation;
        let [current, history, daily, alerts, nearby] = await Promise.all([
            fetch(`/api/current?station=${s}`).then(r => r.json()).catch(() => null),
            fetch(historyUrl()).then(r => r.json()).catch(() => []),
            fetch(`/api/daily?station=${s}&days=30`).then(r => r.json()).catch(() => []),
            fetch(`/api/alerts?station=${s}`).then(r => r.json()).catch(() => []),
            fetch(`/api/nearby?station=${s}`).then(r => r.json()).catch(() => []),
        ]);

        if (customStart && customEnd && history && history.length > 0) {
            // For custom historical ranges, override the "current" live conditions
            // with the final observation of the selected timeframe.
            current = history[history.length - 1];

            const liveBadge = document.querySelector(".live-badge");
            if (liveBadge) {
                liveBadge.innerHTML = '<span class="live-dot" style="background:#eab308;animation:none;"></span> HISTORICAL';
                liveBadge.style.color = "#eab308";
                liveBadge.style.borderColor = "rgba(234,179,8,0.3)";
                liveBadge.style.background = "rgba(234,179,8,0.1)";
            }
        } else {
            const liveBadge = document.querySelector(".live-badge");
            if (liveBadge) {
                liveBadge.innerHTML = '<span class="live-dot"></span> LIVE';
                liveBadge.style.color = "var(--green)";
                liveBadge.style.borderColor = "rgba(34,197,94,0.3)";
                liveBadge.style.background = "rgba(34,197,94,0.1)";
            }
        }

        updateHero(current);
        updateSummary(history);
        updateMetrics(current, history);
        renderCharts(history);
        renderDailyTable(daily);
        renderAlerts(alerts);
        renderNearby(nearby);
    } catch (e) {
        console.warn("Fetch error:", e);
    }
}

// ── Event wiring ─────────────────────────────────────────────────────────────
document.querySelectorAll(".tf-btn[data-hours]").forEach(btn => {
    btn.addEventListener("click", () => {
        document.querySelectorAll(".tf-btn[data-hours]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        currentHours = parseInt(btn.dataset.hours, 10);
        customStart = null; customEnd = null;
        const cs = $("chart-start"); if (cs) cs.value = "";
        const ce = $("chart-end"); if (ce) ce.value = "";
        saveSettings();
        fetchAll();
    });
});

const applyBtn = $("apply-custom-range");
if (applyBtn) {
    applyBtn.addEventListener("click", () => {
        const sv = $("chart-start")?.value;
        const ev = $("chart-end")?.value;
        if (sv && ev) {
            customStart = sv; customEnd = ev;
            document.querySelectorAll(".tf-btn[data-hours]").forEach(b => b.classList.remove("active"));
            fetchAll();
        }
    });
}

// ── Init ─────────────────────────────────────────────────────────────────────
loadSettings();
initSettings();
initStationTabs();
fetchAll();
refreshTimer = setInterval(fetchAll, refreshMs);

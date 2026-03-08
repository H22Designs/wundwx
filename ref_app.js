/* =====================================================================
   Weather Underground Mirror Dashboard — JavaScript
   ===================================================================== */

"use strict";

Chart.defaults.color = "#7a90ae";
Chart.defaults.borderColor = "#1e3050";
Chart.defaults.font.family = "'Inter', 'Segoe UI', system-ui, sans-serif";
Chart.defaults.font.size = 11;

const charts = {};
let currentHours = 24;

// ── Helpers ──────────────────────────────────────────────────────────────────
const $   = id => document.getElementById(id);
const fmt = (v, dec = 1, fallback = "—") =>
  v == null ? fallback : Number(v).toFixed(dec);

const degToCardinal = d => {
  if (d == null) return "—";
  const dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
                "S","SSW","SW","WSW","W","WNW","NW","NNW"];
  return dirs[Math.round(d / 22.5) % 16];
};

const uvLabel = v => {
  if (v == null) return ["—", ""];
  if (v < 3)  return [v.toFixed(1), "uv-low",   "Low"];
  if (v < 6)  return [v.toFixed(1), "uv-mod",   "Moderate"];
  if (v < 8)  return [v.toFixed(1), "uv-high",  "High"];
  if (v < 11) return [v.toFixed(1), "uv-vhigh", "Very High"];
  return [v.toFixed(1), "uv-ext", "Extreme"];
};

const weatherIcon = (temp, uv, rain) => {
  if (rain > 0)   return "🌧️";
  if (uv > 6)     return "☀️";
  if (uv > 2)     return "🌤️";
  if (temp < 32)  return "❄️";
  return "⛅";
};

const baroTrend = rows => {
  if (!rows || rows.length < 3) return ["→", "steady"];
  const recent = rows.slice(-3).map(r => r.pressure_in).filter(v => v != null);
  if (recent.length < 2) return ["→", "steady"];
  const diff = recent[recent.length - 1] - recent[0];
  if (diff >  0.02) return ["↑", "rising"];
  if (diff < -0.02) return ["↓", "falling"];
  return ["→", "steady"];
};

function setText(id, val) {
  const el = $(id);
  if (el) el.textContent = val;
}
function setWidth(id, val) {
  const el = $(id);
  if (el) el.style.width = val;
}
function rotateNeedle(id, deg) {
  const el = $(id);
  if (el && deg != null) el.style.transform = `rotate(${deg}deg)`;
}

// ── DOM updaters ──────────────────────────────────────────────────────────────
function updateHero(d, today) {
  if (!d) return;

  const tempEl = $("temp-val");
  if (tempEl) tempEl.textContent = d.temp_f != null ? Math.round(d.temp_f) : "—";

  const iconEl = $("cond-icon");
  if (iconEl) iconEl.textContent =
    weatherIcon(d.temp_f, d.uv_index, d.precip_total_in || 0);

  const feels = d.wind_chill_f ?? d.heat_index_f ?? null;
  setText("feels-like",   feels   != null ? Math.round(feels)       + "°F" : "—");
  setText("dew-point",    d.dew_point_f != null ? Math.round(d.dew_point_f) + "°F" : "—");
  setText("humidity-val", d.humidity_pct != null ? Math.round(d.humidity_pct) + "%" : "—");

  if (today) {
    setText("today-high",  today.temp_high_f  != null ? today.temp_high_f  + "°F"  : "—");
    setText("today-low",   today.temp_low_f   != null ? today.temp_low_f   + "°F"  : "—");
    setText("today-rain",  today.rain_total   != null ? today.rain_total   + '"'    : "—");
    setText("today-uv",    today.uv_max       != null ? today.uv_max               : "—");
    setText("today-gust",  today.wind_gust_max!= null ? today.wind_gust_max + " mph": "—");
    setText("today-solar", today.solar_max    != null ? today.solar_max    + " W/m²": "—");
  }

  const luEl = $("last-update-time");
  if (luEl && d.obs_time_utc) {
    const dt = new Date(d.obs_time_utc);
    luEl.textContent = dt.toLocaleTimeString([], {hour:"2-digit", minute:"2-digit", second:"2-digit"});
  }

  const noData = $("no-data-state");
  const dash   = $("dashboard-state");
  if (noData) noData.style.display = "none";
  if (dash)   dash.style.display   = "block";
}

function updateMetrics(d, histRows) {
  if (!d) return;

  // Wind
  setText("wind-speed",   fmt(d.wind_speed_mph, 1));
  setText("wind-gust",    fmt(d.wind_gust_mph, 1));
  setText("wind-dir-txt", degToCardinal(d.wind_dir_deg));
  setText("wind-dir-deg", d.wind_dir_deg != null ? d.wind_dir_deg + "°" : "—");
  rotateNeedle("compass-needle-el", d.wind_dir_deg);

  // Humidity
  const pct = d.humidity_pct || 0;
  setWidth("hum-bar", pct + "%");
  setText("hum-val2", fmt(d.humidity_pct, 0) + "%");

  // Pressure + trend
  setText("baro-val", fmt(d.pressure_in, 2));
  const [tArrow, tClass] = baroTrend(histRows);
  const trendEl = $("baro-trend");
  if (trendEl) {
    trendEl.textContent = tArrow;
    trendEl.className = "trend-" + tClass;
  }

  // Rain
  setText("rain-rate",  fmt(d.precip_rate_in_hr, 2));
  setText("rain-daily", fmt(d.precip_total_in,   2));

  // UV
  const [uvVal, uvCls, uvLbl] = uvLabel(d.uv_index);
  const uvEl = $("uv-val");
  if (uvEl) { uvEl.textContent = uvVal; uvEl.className = "metric-value " + uvCls; }
  setText("uv-label", uvLbl);

  // Solar
  setText("solar-val", fmt(d.solar_radiation_wm2, 0));
  const solarPct = d.solar_radiation_wm2
    ? Math.min(d.solar_radiation_wm2 / 1200 * 100, 100) : 0;
  setWidth("solar-bar", solarPct + "%");
}

// ── Charts ────────────────────────────────────────────────────────────────────
function buildChart(id, config) {
  const ctx = $(id);
  if (!ctx) return null;
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart(ctx, config);
  return charts[id];
}

const lineDataset = (label, data, color, filled = false) => ({
  label,
  data,
  borderColor: color,
  backgroundColor: filled
    ? color.replace(")", ", .1)").replace("rgb", "rgba") : "transparent",
  borderWidth: 2,
  pointRadius: 0,
  pointHoverRadius: 4,
  tension: 0.4,
  fill: filled,
});

const barDataset = (label, data, color) => ({
  label, data,
  backgroundColor: color,
  borderRadius: 3,
  borderSkipped: false,
});

const sharedXAxis = () => ({
  type: "time",
  time: { tooltipFormat: "MMM d, h:mm a",
          displayFormats: { hour: "h a", minute: "h:mm a" } },
  grid: { color: "#1e3050" },
  ticks: { maxTicksLimit: 6 },
});

const sharedOpts = (yLabel) => ({
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: "index", intersect: false },
  plugins: {
    legend: { display: false },
    tooltip: {
      backgroundColor: "#1a2436",
      borderColor: "#1e3050",
      borderWidth: 1,
      titleColor: "#f0f6ff",
      bodyColor: "#7a90ae",
      padding: 10,
    },
  },
  scales: {
    x: sharedXAxis(),
    y: {
      grid: { color: "#1e3050" },
      ticks: { maxTicksLimit: 5 },
      title: { display: !!yLabel, text: yLabel, color: "#4a607a", font: { size: 10 } },
    },
  },
});

const withLegend = opts => ({
  ...opts,
  plugins: {
    ...opts.plugins,
    legend: { display: true, labels: { color: "#7a90ae", boxWidth: 12, padding: 12 } },
  },
});

function renderCharts(rows) {
  if (!rows || !rows.length) return;
  const pts = rows.map(r => r.obs_time_utc);

  buildChart("chart-temp", {
    type: "line",
    data: {
      labels: pts,
      datasets: [
        lineDataset("Temp (°F)",      rows.map(r => r.temp_f),       "#f97316", true),
        lineDataset("Dew Point (°F)", rows.map(r => r.dew_point_f),  "#14b8a6"),
        lineDataset("Heat Index (°F)",rows.map(r => r.heat_index_f), "#a855f7"),
      ],
    },
    options: withLegend(sharedOpts("°F")),
  });

  buildChart("chart-hum", {
    type: "line",
    data: {
      labels: pts,
      datasets: [lineDataset("Humidity (%)", rows.map(r => r.humidity_pct), "#3b82f6", true)],
    },
    options: {
      ...sharedOpts("%"),
      scales: { ...sharedOpts().scales, y: { min: 0, max: 100, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } },
    },
  });

  buildChart("chart-baro", {
    type: "line",
    data: {
      labels: pts,
      datasets: [lineDataset("Pressure (inHg)", rows.map(r => r.pressure_in), "#22c55e")],
    },
    options: sharedOpts("inHg"),
  });

  buildChart("chart-wind", {
    type: "line",
    data: {
      labels: pts,
      datasets: [
        lineDataset("Wind (mph)", rows.map(r => r.wind_speed_mph), "#60a5fa"),
        lineDataset("Gust (mph)", rows.map(r => r.wind_gust_mph),  "#f97316"),
      ],
    },
    options: withLegend(sharedOpts("mph")),
  });

  buildChart("chart-rain", {
    type: "bar",
    data: {
      labels: pts,
      datasets: [barDataset("Rain Rate (in/hr)",
        rows.map(r => r.precip_rate_in_hr || 0), "rgba(59,130,246,.7)")],
    },
    options: { ...sharedOpts("in/hr"), scales: { ...sharedOpts().scales, y: { min: 0, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } } },
  });

  buildChart("chart-uv", {
    type: "bar",
    data: {
      labels: pts,
      datasets: [barDataset("UV Index", rows.map(r => r.uv_index || 0), "rgba(234,179,8,.7)")],
    },
    options: { ...sharedOpts("UV Index"), scales: { ...sharedOpts().scales, y: { min: 0, max: 12, grid: { color: "#1e3050" }, ticks: { maxTicksLimit: 5 } } } },
  });
}

// ── Daily table ───────────────────────────────────────────────────────────────
function renderDailyTable(days) {
  const tbody = $("daily-tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  days.forEach(d => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${d.day}</td>
      <td>${fmt(d.temp_high_f, 1)}°F</td>
      <td>${fmt(d.temp_low_f,  1)}°F</td>
      <td>${fmt(d.temp_avg_f,  1)}°F</td>
      <td>${fmt(d.humidity_high, 0)}%</td>
      <td>${fmt(d.humidity_low,  0)}%</td>
      <td>${fmt(d.pressure_avg,  2)} inHg</td>
      <td>${fmt(d.wind_gust_max, 1)} mph</td>
      <td>${fmt(d.rain_total,    2)}"</td>
      <td>${fmt(d.uv_max,        0)}</td>
      <td>${d.reading_count}</td>
    `;
    tbody.appendChild(tr);
  });
}

// ── Fetch & refresh ───────────────────────────────────────────────────────────
async function fetchAll() {
  try {
    const [current, history, daily, today] = await Promise.all([
      fetch("/api/current").then(r => r.json()).catch(() => null),
      fetch(`/api/history?hours=${currentHours}&limit=500`).then(r => r.json()).catch(() => []),
      fetch("/api/daily?days=30").then(r => r.json()).catch(() => []),
      fetch("/api/today").then(r => r.json()).catch(() => null),
    ]);

    updateHero(current, today);
    updateMetrics(current, history);
    renderCharts(history);
    renderDailyTable(daily);
  } catch (e) {
    console.warn("Fetch error:", e);
  }
}

document.querySelectorAll(".tf-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tf-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    currentHours = parseInt(btn.dataset.hours, 10);
    fetchAll();
  });
});

fetchAll();
setInterval(fetchAll, 60_000);

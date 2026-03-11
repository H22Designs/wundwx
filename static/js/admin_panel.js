"use strict";

const $ = id => document.getElementById(id);

function openModal(title, html, onSubmit) {
    $("modal-title").textContent = title;
    $("modal-body").innerHTML = html;
    $("modal-overlay").style.display = "";
    const form = $("modal-body").querySelector("form");
    if (form && onSubmit) form.addEventListener("submit", (e) => { e.preventDefault(); onSubmit(form); });
}

function closeModal() { $("modal-overlay").style.display = "none"; }
$("modal-close").addEventListener("click", closeModal);
$("modal-overlay").addEventListener("click", (e) => { if (e.target === $("modal-overlay")) closeModal(); });

// Tab switching
function switchTab(active) {
    ["users", "stations", "database"].forEach(name => {
        $(`tab-${name}`).classList.toggle("active", name === active);
        $(`panel-${name}`).style.display = name === active ? "" : "none";
    });
}
$("tab-users").addEventListener("click", () => switchTab("users"));
$("tab-stations").addEventListener("click", () => switchTab("stations"));
$("tab-database").addEventListener("click", () => { switchTab("database"); loadDbStats(); });

async function loadUsers() {
    const tbody = $("users-tbody");
    try {
        const users = await fetch("/api/admin/users").then(r => r.json());
        tbody.innerHTML = users.map(u => `<tr>
            <td>${u.id}</td>
            <td>${u.username}${u.is_admin ? ' <span class="admin-badge">admin</span>' : ''}</td>
            <td>${u.email}</td>
            <td><button class="toggle-btn ${u.is_admin ? 'on' : ''}" data-uid="${u.id}" data-field="is_admin">${u.is_admin ? 'Yes' : 'No'}</button></td>
            <td><button class="toggle-btn ${u.is_active ? 'on' : ''}" data-uid="${u.id}" data-field="is_active">${u.is_active ? 'Yes' : 'No'}</button></td>
            <td>${u.created_at ? u.created_at.split('T')[0] : '—'}</td>
            <td>${u.last_login ? u.last_login.split('T')[0] : 'never'}</td>
            <td><button class="panel-btn panel-btn-danger btn-del-user" data-uid="${u.id}" data-name="${u.username}">Delete</button></td>
        </tr>`).join("");

        tbody.querySelectorAll(".toggle-btn").forEach(btn => {
            btn.addEventListener("click", async () => {
                const uid = btn.dataset.uid;
                const field = btn.dataset.field;
                const current = btn.classList.contains("on");
                await fetch(`/api/admin/users/${uid}`, {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ [field]: !current }),
                });
                loadUsers();
            });
        });

        tbody.querySelectorAll(".btn-del-user").forEach(btn => {
            btn.addEventListener("click", async () => {
                if (!confirm(`Delete user "${btn.dataset.name}"?`)) return;
                const res = await fetch(`/api/admin/users/${btn.dataset.uid}`, { method: "DELETE" });
                if (res.ok) loadUsers();
                else { const d = await res.json(); alert(d.detail || "Error"); }
            });
        });
    } catch (e) { tbody.innerHTML = '<tr><td colspan="8">Error loading users</td></tr>'; }
}

async function loadStations() {
    const tbody = $("stations-tbody");
    try {
        const stations = await fetch("/api/admin/stations").then(r => r.json());
        tbody.innerHTML = stations.map(s => `<tr>
            <td>${s.id}</td>
            <td style="color:var(--accent);font-weight:600;">${s.station_id}</td>
            <td>${s.name}</td>
            <td>${s.lat}</td>
            <td>${s.lon}</td>
            <td>${s.cwop_callsign || '—'}</td>
            <td>${s.source_type}${s.has_api_key ? ' <span class="wu-badge" title="Weather Underground API key set">WU</span>' : ''}</td>
            <td><button class="toggle-btn ${s.is_active ? 'on' : ''}" data-sid="${s.id}" data-active="${s.is_active}">${s.is_active ? 'Yes' : 'No'}</button></td>
            <td style="display:flex;gap:.3rem;">
                <button class="panel-btn btn-edit-station" data-station='${JSON.stringify(s).replace(/'/g,"&#39;")}'>Edit</button>
                <button class="panel-btn panel-btn-danger btn-del-station" data-sid="${s.id}" data-name="${s.station_id}">Deactivate</button>
            </td>
        </tr>`).join("");

        // Active toggle
        tbody.querySelectorAll(".toggle-btn").forEach(btn => {
            btn.addEventListener("click", async () => {
                const active = btn.dataset.active === "true";
                await fetch(`/api/admin/stations/${btn.dataset.sid}`, {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ is_active: !active }),
                });
                loadStations();
            });
        });

        // Edit
        tbody.querySelectorAll(".btn-edit-station").forEach(btn => {
            btn.addEventListener("click", () => {
                const s = JSON.parse(btn.dataset.station);
                openModal("Edit Station", `
                    <form class="panel-form">
                        <div class="auth-field"><label>Name</label><input name="name" value="${s.name}" required class="setting-input" /></div>
                        <div class="auth-field"><label>Latitude</label><input name="lat" type="number" step="any" value="${s.lat}" required class="setting-input" /></div>
                        <div class="auth-field"><label>Longitude</label><input name="lon" type="number" step="any" value="${s.lon}" required class="setting-input" /></div>
                        <div class="auth-field"><label>CWOP Callsign</label><input name="cwop_callsign" value="${s.cwop_callsign || ''}" class="setting-input" /></div>
                        <div class="auth-field"><label>Source Type</label>
                            <select name="source_type" class="setting-select">
                                <option value="openmeteo" ${s.source_type==='openmeteo'?'selected':''}>Open-Meteo</option>
                                <option value="cwop" ${s.source_type==='cwop'?'selected':''}>CWOP</option>
                                <option value="wunderground" ${s.source_type==='wunderground'?'selected':''}>Weather Underground</option>
                            </select>
                        </div>
                        <div class="auth-field api-key-row" style="display:none;">
                            <label>WU API Key</label>
                            <input name="api_key" class="setting-input"
                                placeholder="${s.has_api_key ? 'Leave blank to keep existing key' : 'Enter Weather Underground API key'}" />
                            ${s.has_api_key ? '<small style="color:var(--muted);font-size:.75rem;">A key is already set — enter a new one to replace it</small>' : ''}
                        </div>
                        <button type="submit" class="panel-btn panel-btn-primary" style="margin-top:.5rem;">Save</button>
                    </form>
                `, async (form) => {
                    const fd = new FormData(form);
                    const body = {
                        name: fd.get("name"),
                        lat: parseFloat(fd.get("lat")),
                        lon: parseFloat(fd.get("lon")),
                        cwop_callsign: fd.get("cwop_callsign"),
                        source_type: fd.get("source_type"),
                    };
                    const apiKey = fd.get("api_key");
                    if (apiKey) body.api_key = apiKey;
                    await fetch(`/api/admin/stations/${s.id}`, {
                        method: "PUT",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify(body),
                    });
                    closeModal();
                    loadStations();
                });
                // Wire show/hide of API key field based on source type
                const sel = $("modal-body").querySelector("[name=source_type]");
                const apiKeyRow = $("modal-body").querySelector(".api-key-row");
                const toggleApiKey = () => { apiKeyRow.style.display = sel.value === "wunderground" ? "" : "none"; };
                sel.addEventListener("change", toggleApiKey);
                toggleApiKey();
            });
        });

        // Delete (soft)
        tbody.querySelectorAll(".btn-del-station").forEach(btn => {
            btn.addEventListener("click", async () => {
                if (!confirm(`Deactivate station "${btn.dataset.name}"?`)) return;
                await fetch(`/api/admin/stations/${btn.dataset.sid}`, { method: "DELETE" });
                loadStations();
            });
        });
    } catch (e) { tbody.innerHTML = '<tr><td colspan="9">Error loading stations</td></tr>'; }
}

// Create User modal
$("btn-create-user").addEventListener("click", () => {
    openModal("Create User", `
        <form class="panel-form">
            <div class="auth-field"><label>Username</label><input name="username" required class="setting-input" /></div>
            <div class="auth-field"><label>Email</label><input name="email" type="email" required class="setting-input" /></div>
            <div class="auth-field"><label>Password</label><input name="password" type="password" required minlength="4" class="setting-input" /></div>
            <div class="auth-field"><label><input name="is_admin" type="checkbox" style="margin-right:.4rem;" />Admin</label></div>
            <button type="submit" class="panel-btn panel-btn-primary" style="margin-top:.5rem;">Create</button>
        </form>
    `, async (form) => {
        const fd = new FormData(form);
        const res = await fetch("/api/admin/users", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                username: fd.get("username"),
                email: fd.get("email"),
                password: fd.get("password"),
                is_admin: !!fd.get("is_admin"),
            }),
        });
        if (res.ok) { closeModal(); loadUsers(); }
        else { const d = await res.json(); alert(d.detail || "Error"); }
    });
});

// Create Station modal
$("btn-create-station").addEventListener("click", () => {
    openModal("Add Station", `
        <form class="panel-form">
            <div class="auth-field"><label>Station ID</label><input name="station_id" required class="setting-input" placeholder="e.g. KALMYTOWN1" /></div>
            <div class="auth-field"><label>Name</label><input name="name" required class="setting-input" /></div>
            <div class="auth-field"><label>Latitude</label><input name="lat" type="number" step="any" required class="setting-input" /></div>
            <div class="auth-field"><label>Longitude</label><input name="lon" type="number" step="any" required class="setting-input" /></div>
            <div class="auth-field"><label>CWOP Callsign</label><input name="cwop_callsign" class="setting-input" /></div>
            <div class="auth-field"><label>Source Type</label>
                <select name="source_type" class="setting-select">
                    <option value="openmeteo">Open-Meteo</option>
                    <option value="cwop">CWOP</option>
                    <option value="wunderground">Weather Underground</option>
                </select>
            </div>
            <div class="auth-field api-key-row" style="display:none;">
                <label>WU API Key</label>
                <input name="api_key" class="setting-input" placeholder="Enter Weather Underground API key" />
            </div>
            <button type="submit" class="panel-btn panel-btn-primary" style="margin-top:.5rem;">Create</button>
        </form>
    `, async (form) => {
        const fd = new FormData(form);
        const body = {
            station_id: fd.get("station_id"),
            name: fd.get("name"),
            lat: parseFloat(fd.get("lat")),
            lon: parseFloat(fd.get("lon")),
            cwop_callsign: fd.get("cwop_callsign") || "",
            source_type: fd.get("source_type"),
        };
        const apiKey = fd.get("api_key");
        if (apiKey) body.api_key = apiKey;
        const res = await fetch("/api/admin/stations", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        if (res.ok) { closeModal(); loadStations(); }
        else { const d = await res.json(); alert(d.detail || "Error"); }
    });
    // Wire show/hide of API key field based on source type
    const sel = $("modal-body").querySelector("[name=source_type]");
    const apiKeyRow = $("modal-body").querySelector(".api-key-row");
    sel.addEventListener("change", () => { apiKeyRow.style.display = sel.value === "wunderground" ? "" : "none"; });
});

// ── Database Admin ─────────────────────────────────────────────────────────

function formatBytes(b) {
    if (b < 1048576) return (b / 1024).toFixed(1) + " KB";
    return (b / 1048576).toFixed(1) + " MB";
}

function dbMsg(id, html, ok = true) {
    $(id).innerHTML = `<div class="db-result ${ok ? "db-result-ok" : "db-result-err"}">${html}</div>`;
}

async function loadDbStats() {
    $("db-summary").innerHTML = '<div class="db-loading">Loading…</div>';
    try {
        const data = await fetch("/api/admin/db/stats").then(r => r.json());
        $("db-summary").innerHTML = `
            <div class="db-stat-card"><span class="db-stat-val">${data.total_records.toLocaleString()}</span><span class="db-stat-lbl">Total Records</span></div>
            <div class="db-stat-card"><span class="db-stat-val">${formatBytes(data.db_size_bytes)}</span><span class="db-stat-lbl">DB Size</span></div>
            <div class="db-stat-card"><span class="db-stat-val">${data.stations.length}</span><span class="db-stat-lbl">Stations w/ Data</span></div>
        `;
        $("db-stats-tbody").innerHTML = data.stations.map(s => `<tr>
            <td style="color:var(--accent);font-weight:600;">${s.station_id}</td>
            <td>${s.record_count.toLocaleString()}</td>
            <td>${s.oldest ? s.oldest.split("T")[0] : "—"}</td>
            <td>${s.newest ? s.newest.split("T")[0] : "—"}</td>
            <td>${s.corrupt_count > 0 ? `<span style="color:var(--red);">${s.corrupt_count}</span>` : "0"}</td>
        </tr>`).join("");

        // Populate station selectors for backfill / purge
        const opts = data.stations.map(s => `<option value="${s.station_id}">${s.station_id}</option>`).join("");
        $("backfill-station").innerHTML = opts;
        $("purge-station").innerHTML = '<option value="">All Stations</option>' + opts;
    } catch (e) {
        $("db-summary").innerHTML = '<div class="db-result db-result-err">Error loading stats</div>';
    }
}

$("btn-refresh-stats").addEventListener("click", loadDbStats);

// Integrity check
$("btn-check-integrity").addEventListener("click", async () => {
    $("integrity-results").innerHTML = '<div class="db-loading">Running check…</div>';
    try {
        const data = await fetch("/api/integrity").then(r => r.json());
        const entries = Object.entries(data);
        if (!entries.length) {
            $("integrity-results").innerHTML = '<div class="db-result db-result-ok">No stations to check.</div>';
            return;
        }
        const allOk = entries.every(([, v]) => v.corrupt_count === 0 && v.missing_slot_count === 0);
        $("integrity-results").innerHTML = entries.map(([sid, v]) => {
            const ok = v.corrupt_count === 0 && v.missing_slot_count === 0;
            const dates = v.missing_dates.length ? ` — missing dates: ${v.missing_dates.join(", ")}` : "";
            return `<div class="db-integrity-row ${ok ? "db-result-ok" : "db-result-err"}">
                <strong>${sid}</strong>: ${v.corrupt_count} corrupt, ${v.missing_slot_count} missing slots${dates}
            </div>`;
        }).join("") + (allOk ? '<div class="db-result db-result-ok" style="margin-top:.5rem;">All stations OK</div>' : "");
    } catch (e) {
        $("integrity-results").innerHTML = '<div class="db-result db-result-err">Error running integrity check</div>';
    }
});

// Integrity repair
$("btn-repair-integrity").addEventListener("click", async () => {
    $("integrity-results").innerHTML = '<div class="db-loading">Repair started…</div>';
    try {
        const data = await fetch("/api/integrity/repair", { method: "POST" }).then(r => r.json());
        dbMsg("integrity-results", data.message);
    } catch (e) {
        dbMsg("integrity-results", "Error starting repair", false);
    }
});

// Backfill form
$("backfill-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    $("backfill-results").innerHTML = '<div class="db-loading">Backfill started in background…</div>';
    const res = await fetch("/api/admin/db/backfill", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            station_id: fd.get("station_id"),
            start_date: fd.get("start_date"),
            end_date: fd.get("end_date"),
        }),
    });
    const data = await res.json();
    dbMsg("backfill-results", res.ok ? data.message : (data.detail || "Error"), res.ok);
});

// Import backup
$("import-file").addEventListener("change", async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    if (!confirm(`Import "${file.name}" and restore this database? Current data will be replaced.`)) {
        e.target.value = "";
        return;
    }
    const formData = new FormData();
    formData.append("file", file);
    $("import-results").innerHTML = '<div class="db-loading">Uploading and restoring…</div>';
    const res = await fetch("/api/admin/db/import", { method: "POST", body: formData });
    const data = await res.json();
    dbMsg("import-results", res.ok ? data.message : (data.detail || "Import failed"), res.ok);
    if (res.ok) loadDbStats();
    e.target.value = "";
});

// Purge form
$("purge-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const station = fd.get("station_id");
    const start = fd.get("start_date");
    const end = fd.get("end_date");
    const label = `${station || "ALL stations"}${start ? " from " + start : ""}${end ? " to " + end : ""}`;
    if (!confirm(`Permanently delete records for ${label}? This cannot be undone.`)) return;
    const body = {};
    if (station) body.station_id = station;
    if (start) body.start_date = start;
    if (end) body.end_date = end;
    const res = await fetch("/api/admin/db/purge", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });
    const data = await res.json();
    dbMsg("purge-results", res.ok ? `Deleted ${data.deleted.toLocaleString()} record(s).` : (data.detail || "Error"), res.ok);
    if (res.ok) loadDbStats();
});

// Rebuild
$("btn-rebuild").addEventListener("click", async () => {
    if (!confirm("Delete ALL weather records and rebuild? This cannot be undone.")) return;
    if (!confirm("Final confirmation: ALL data will be permanently deleted before backfill starts.")) return;
    $("rebuild-results").innerHTML = '<div class="db-loading">Rebuild started in background…</div>';
    const res = await fetch("/api/admin/db/rebuild", { method: "POST" });
    const data = await res.json();
    dbMsg("rebuild-results", res.ok ? data.message : (data.detail || "Error"), res.ok);
    if (res.ok) setTimeout(loadDbStats, 2000);
});

// Template only includes admin panel HTML when user is admin (server-side check).
// If the panel elements exist, load data; otherwise the access-denied message
// is already rendered by the template.
function init() {
    if (!$("users-tbody")) return;
    loadUsers();
    loadStations();
}

init();

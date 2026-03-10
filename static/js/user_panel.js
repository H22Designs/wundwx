"use strict";

const $ = id => document.getElementById(id);

let allStations = [];

async function init() {
    // The template only renders #settings-content when user is logged in,
    // so if it's not in the DOM at all we can skip init entirely.
    if (!$("settings-content")) return;

    // Load stations for dropdowns
    try {
        allStations = await fetch("/api/stations").then(r => r.json());
    } catch (e) { allStations = []; }

    // Populate station selects
    const stationSelect = $("pref-station");
    const favSelect = $("fav-add-select");
    allStations.forEach(s => {
        stationSelect.add(new Option(`${s.name} (${s.id})`, s.id));
        favSelect.add(new Option(`${s.name} (${s.id})`, s.id));
    });

    // Load user settings
    try {
        const settings = await fetch("/api/user/settings").then(r => r.json());
        if (settings.default_station) stationSelect.value = settings.default_station;
        if (settings.temp_unit) $("pref-unit").value = settings.temp_unit;
        if (settings.refresh_interval) $("pref-refresh").value = String(settings.refresh_interval);
    } catch (e) { }

    loadFavorites();

    // Password form
    $("pw-form").addEventListener("submit", async (e) => {
        e.preventDefault();
        const msg = $("pw-msg");
        if ($("pw-new").value !== $("pw-confirm").value) {
            msg.textContent = "Passwords do not match";
            msg.className = "panel-msg panel-msg-error";
            return;
        }
        try {
            const res = await fetch("/api/user/password", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    current_password: $("pw-current").value,
                    new_password: $("pw-new").value,
                }),
            });
            if (res.ok) {
                msg.textContent = "Password updated";
                msg.className = "panel-msg panel-msg-ok";
                $("pw-form").reset();
            } else {
                const data = await res.json();
                msg.textContent = data.detail || "Error";
                msg.className = "panel-msg panel-msg-error";
            }
        } catch (err) {
            msg.textContent = "Network error";
            msg.className = "panel-msg panel-msg-error";
        }
    });

    // Save preferences
    $("save-prefs").addEventListener("click", async () => {
        const msg = $("prefs-msg");
        try {
            const res = await fetch("/api/user/settings", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    default_station: $("pref-station").value,
                    temp_unit: $("pref-unit").value,
                    refresh_interval: parseInt($("pref-refresh").value),
                }),
            });
            msg.textContent = res.ok ? "Saved" : "Error saving";
            msg.className = res.ok ? "panel-msg panel-msg-ok" : "panel-msg panel-msg-error";
        } catch (e) {
            msg.textContent = "Network error";
            msg.className = "panel-msg panel-msg-error";
        }
    });

    // Add favorite
    $("fav-add-btn").addEventListener("click", async () => {
        const sid = $("fav-add-select").value;
        if (!sid) return;
        await fetch("/api/user/favorites", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ station_id: sid }),
        });
        loadFavorites();
    });
}

async function loadFavorites() {
    const list = $("fav-list");
    try {
        const favs = await fetch("/api/user/favorites").then(r => r.json());
        if (!favs.length) {
            list.innerHTML = '<div class="fav-empty">No favorite stations yet</div>';
            return;
        }
        list.innerHTML = favs.map(f => {
            const st = allStations.find(s => s.id === f.station_id);
            const name = st ? st.name : f.station_id;
            return `<div class="fav-item">
                <span class="fav-name">${name} <span class="fav-id">${f.station_id}</span></span>
                <button class="fav-remove" data-id="${f.id}" title="Remove">&times;</button>
            </div>`;
        }).join("");

        list.querySelectorAll(".fav-remove").forEach(btn => {
            btn.addEventListener("click", async () => {
                await fetch(`/api/user/favorites/${btn.dataset.id}`, { method: "DELETE" });
                loadFavorites();
            });
        });
    } catch (e) {
        list.innerHTML = '<div class="fav-empty">Error loading favorites</div>';
    }
}

init();

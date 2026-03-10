"use strict";

// Shared auth utilities used by all pages.

let _currentUser = null;

async function checkAuth() {
    try {
        const res = await fetch("/api/auth/me");
        if (res.ok) {
            _currentUser = await res.json();
            return _currentUser;
        }
    } catch (e) { }
    _currentUser = null;
    return null;
}

function getCurrentUser() {
    return _currentUser;
}

async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    _currentUser = null;
    window.location.href = "/";
}

/**
 * renderUserMenu() — wires up the user menu dropdown.
 *
 * If the server already rendered the menu (Jinja2 template), we just attach
 * event listeners to the existing DOM elements. If for some reason the server
 * didn't render it (e.g. old cached page), we fall back to generating the HTML.
 */
function renderUserMenu() {
    const container = document.getElementById("user-menu");
    if (!container) return;

    // ── Case 1: Server pre-rendered the user avatar ───────────────────────────
    const existingBtn = document.getElementById("user-avatar-btn");
    const existingDropdown = document.getElementById("user-dropdown");
    const existingLogout = document.getElementById("logout-btn");

    if (existingBtn && existingDropdown) {
        // Just wire events — don't touch the HTML
        existingBtn.addEventListener("click", (e) => {
            e.stopPropagation();
            existingDropdown.classList.toggle("open");
        });
        document.addEventListener("click", () => existingDropdown.classList.remove("open"));
        if (existingLogout) existingLogout.addEventListener("click", logout);
        return;
    }

    // ── Case 2: Server rendered "Login" link or nothing ─────────────────────
    // If _currentUser is known from checkAuth(), upgrade to avatar dynamically.
    if (!_currentUser) {
        if (!container.innerHTML.trim()) {
            container.innerHTML = '<a href="/login" class="user-menu-login">Login</a>';
        }
        return;
    }

    // Logged-in but no server HTML — generate it client-side
    const initial = (_currentUser.username || "?")[0].toUpperCase();
    const adminLink = _currentUser.is_admin
        ? '<a href="/admin" class="user-dropdown-item">Admin Panel</a>'
        : '';

    container.innerHTML = `
        <div class="user-menu-wrap">
            <button class="user-avatar" id="user-avatar-btn" title="${_currentUser.username}">${initial}</button>
            <div class="user-dropdown" id="user-dropdown">
                <div class="user-dropdown-header">
                    <strong>${_currentUser.username}</strong>
                    <span>${_currentUser.email}</span>
                </div>
                <div class="user-dropdown-divider"></div>
                <a href="/settings" class="user-dropdown-item">Settings</a>
                ${adminLink}
                <div class="user-dropdown-divider"></div>
                <button class="user-dropdown-item user-dropdown-logout" id="logout-btn">Logout</button>
            </div>
        </div>
    `;

    const avatarBtn = document.getElementById("user-avatar-btn");
    const dropdown = document.getElementById("user-dropdown");
    avatarBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        dropdown.classList.toggle("open");
    });
    document.addEventListener("click", () => dropdown.classList.remove("open"));
    document.getElementById("logout-btn").addEventListener("click", logout);
}

// Wire up server-rendered menu immediately on script load (no async needed)
// This handles the common case where base.html already rendered the avatar.
(function wireServerRenderedMenu() {
    const btn = document.getElementById("user-avatar-btn");
    const drop = document.getElementById("user-dropdown");
    const logoutBtn = document.getElementById("logout-btn");
    if (btn && drop) {
        btn.addEventListener("click", (e) => {
            e.stopPropagation();
            drop.classList.toggle("open");
        });
        document.addEventListener("click", () => drop.classList.remove("open"));
    }
    if (logoutBtn) logoutBtn.addEventListener("click", logout);
})();

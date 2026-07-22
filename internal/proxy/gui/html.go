package gui

import (
	"strings"

	"pgrollback/internal/tray"
)

const apiBasePlaceholder = "__API_BASE__"
const faviconPlaceholder = "__FAVICON_DATA_URI__"

// htmlTemplate is the full GUI page; __API_BASE__ and __FAVICON_DATA_URI__ are replaced at runtime.
const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PgRollback Sessions</title>
  <link rel="icon" type="image/x-icon" href="__FAVICON_DATA_URI__">
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body {
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      margin: 0;
      min-height: 100vh;
      background: linear-gradient(160deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
      color: #e2e8f0;
      line-height: 1.5;
    }
    .page { width: 100%; max-width: 100%; margin: 0; padding: 1.25rem 1.5rem; }
    .header {
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: 0.75rem;
      margin-bottom: 1.5rem;
      padding-bottom: 1rem;
      border-bottom: 1px solid rgba(51, 65, 85, 0.6);
    }
    .header-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 1rem;
    }
    .backend-conn {
      margin: 0;
      padding: 0.65rem 0.85rem;
      background: rgba(15, 23, 42, 0.75);
      border: 1px solid rgba(51, 65, 85, 0.7);
      border-radius: 8px;
      font-size: 0.8125rem;
      color: #94a3b8;
      line-height: 1.45;
      cursor: default;
    }
    .backend-conn-label {
      font-weight: 600;
      color: #cbd5e1;
      margin-right: 0.5rem;
      white-space: nowrap;
    }
    .backend-conn code.backend-conn-str {
      font-family: 'Consolas', 'Monaco', ui-monospace, monospace;
      font-size: 0.78rem;
      color: #7dd3fc;
      word-break: break-all;
      background: transparent;
      border: 0;
      padding: 0;
    }
    .proxy-conn code.backend-conn-str { color: #86efac; }
    .proxy-conn-hint {
      margin-top: 0.45rem;
      font-size: 0.72rem;
      color: #64748b;
      line-height: 1.4;
    }
    .header h1 {
      margin: 0;
      font-size: 1.5rem;
      font-weight: 600;
      letter-spacing: -0.02em;
      color: #f1f5f9;
    }
    .header h1 span { color: #38bdf8; font-weight: 700; }
    .toolbar { display: flex; gap: 0.5rem; align-items: center; }
    .toolbar button {
      padding: 0.5rem 1rem;
      border: 0;
      border-radius: 8px;
      font-size: 0.875rem;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s, transform 0.05s;
    }
    .toolbar button:active { transform: scale(0.98); }
    .toolbar #refresh {
      background: #0ea5e9;
      color: #fff;
    }
    .toolbar #refresh:hover { background: #0284c7; }
    .toolbar .settings-btn {
      background: rgba(51, 65, 85, 0.8);
      color: #cbd5e1;
      border: 1px solid #475569;
    }
    .toolbar .settings-btn:hover { background: #334155; color: #f1f5f9; }
    .toolbar .rollback-all-btn {
      background: #b91c1c;
      color: #fff;
    }
    .toolbar .rollback-all-btn:hover { background: #dc2626; }
    .toolbar .disconnect-all-btn {
      background: transparent;
      color: #fca5a5;
      border: 1px solid #b91c1c;
    }
    .toolbar .disconnect-all-btn:hover {
      background: rgba(185, 28, 28, 0.15);
      color: #fecaca;
    }
    .table-wrap {
      background: rgba(30, 41, 59, 0.85);
      border-radius: 12px;
      overflow: hidden;
      box-shadow: 0 4px 24px rgba(0, 0, 0, 0.25);
      border: 1px solid rgba(51, 65, 85, 0.5);
    }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 0.75rem 1rem; text-align: left; }
    thead th {
      background: rgba(15, 23, 42, 0.9);
      font-weight: 600;
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #94a3b8;
      border-bottom: 1px solid #334155;
    }
    tbody tr.session-row { transition: background 0.12s; }
    tbody tr.session-row:hover { background: rgba(51, 65, 85, 0.35); }
    tbody tr.session-row td { border-bottom: 1px solid rgba(51, 65, 85, 0.5); vertical-align: middle; }
    tbody tr.session-row:last-child td { border-bottom: 0; }
    .tx-status {
      width: 6rem;
      font-weight: 500;
      font-size: 0.875rem;
    }
    .tx-status.yes { color: #38bdf8; }
    .tx-status.no { color: #64748b; }
    .lock-status {
      width: 8rem;
      font-size: 0.8125rem;
      font-weight: 500;
      white-space: nowrap;
    }
    .lock-status.ok { color: #64748b; }
    .lock-status.blocked {
      color: #fb923c;
    }
    .lock-status.blocked .lock-badge {
      display: inline-block;
      padding: 0.15rem 0.45rem;
      border-radius: 999px;
      background: rgba(249, 115, 22, 0.18);
      border: 1px solid rgba(251, 146, 60, 0.55);
      color: #fdba74;
      font-size: 0.75rem;
      letter-spacing: 0.02em;
    }
    .lock-status.blocked.pgrollback .lock-badge {
      background: rgba(56, 189, 248, 0.12);
      border-color: rgba(56, 189, 248, 0.45);
      color: #7dd3fc;
    }
    .query {
      max-width: 42rem;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-family: 'Consolas', 'Monaco', ui-monospace, monospace;
      font-size: 0.8125rem;
      color: #94a3b8;
      padding-right: 0.5rem;
    }
    .query-duration { color: #64748b; font-weight: 500; margin-left: 0.25rem; }
    .query-duration.running { color: #f59e0b; }
    .query-db-proxy-split {
      color: #64748b;
      font-size: 0.75rem;
      margin-left: 0.4rem;
      font-family: 'Consolas', 'Monaco', ui-monospace, monospace;
    }
    .query-db-proxy-split .db-part { color: #38bdf8; }
    .query-db-proxy-split .proxy-part { color: #fb923c; }
    .actions { white-space: nowrap; }
    .history-btn, .close-btn, .clear-log-btn {
      padding: 0.35rem 0.75rem;
      border: 0;
      border-radius: 6px;
      font-size: 0.8125rem;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s, opacity 0.15s;
    }
    .history-btn { margin-right: 0.35rem; background: #475569; color: #e2e8f0; }
    .history-btn:hover { background: #64748b; }
    .clear-log-btn { margin-right: 0.35rem; background: #475569; color: #e2e8f0; }
    .clear-log-btn:hover { background: #64748b; }
    .close-btn { background: #dc2626; color: #fff; }
    .close-btn:hover { background: #ef4444; }
    .history-row td {
      background: rgba(15, 23, 42, 0.7);
      padding: 0.75rem 1rem 0.75rem 2.5rem;
      border-bottom: 1px solid rgba(51, 65, 85, 0.5);
      vertical-align: top;
      border-left: 3px solid #38bdf8;
    }
    .history-list-wrap { margin-top: 0.25rem; }
    .history-list-toolbar { margin-bottom: 0.35rem; }
    .history-height-btn {
      padding: 0.2rem 0.5rem;
      font-size: 0.75rem;
      background: transparent;
      color: #94a3b8;
      border: 1px solid #475569;
      border-radius: 4px;
      cursor: pointer;
    }
    .history-height-btn:hover { color: #cbd5e1; border-color: #64748b; }
    .history-list {
      font-family: 'Consolas', 'Monaco', ui-monospace, monospace;
      font-size: 0.8125rem;
      color: #cbd5e1;
      max-height: 14rem;
      overflow-y: auto;
      padding-right: 0.25rem;
    }
    .history-list ul {
      /* Newest-first is achieved by building <li> elements in reverse in the script (search for
         "Newest first" below), not flex-direction: column-reverse — that trick reverses paint
         order but not DOM/selection order, which makes click-drag text selection jump around
         unpredictably. */
      display: block;
    }
    .history-list.full-height { max-height: none; }
    .history-list::-webkit-scrollbar { width: 8px; }
    .history-list::-webkit-scrollbar-track { background: rgba(30, 41, 59, 0.5); border-radius: 4px; }
    .history-list::-webkit-scrollbar-thumb { background: #475569; border-radius: 4px; }
    .history-list li {
      margin: 0.4rem 0;
      padding: 0.35rem 0;
      white-space: pre-wrap;
      word-break: break-all;
      border-bottom: 1px solid rgba(51, 65, 85, 0.3);
      line-height: 1.45;
    }
    .history-list li:last-child { border-bottom: 0; }
    .history-list .qtime {
      display: inline-block;
      font-size: 0.7rem;
      color: #64748b;
      margin-right: 0.5rem;
      white-space: nowrap;
      vertical-align: top;
    }
    .empty {
      color: #64748b;
      padding: 2rem 1rem;
      text-align: center;
      font-size: 0.9375rem;
    }
    .modal {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.85);
      backdrop-filter: blur(4px);
      z-index: 20;
      align-items: center;
      justify-content: center;
      padding: 1rem;
      animation: fadeIn 0.2s ease;
    }
    .modal.visible { display: flex; }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
    .modal-content {
      background: #1e293b;
      padding: 1.75rem;
      border-radius: 12px;
      max-width: 28rem;
      width: 100%;
      max-height: 90vh;
      overflow-y: auto;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
      border: 1px solid #334155;
      animation: slideUp 0.25s ease;
    }
    @keyframes slideUp { from { opacity: 0; transform: translateY(12px); } to { opacity: 1; transform: translateY(0); } }
    .modal h2 { margin: 0 0 0.5rem; font-size: 1.25rem; font-weight: 600; color: #f1f5f9; }
    .config-path { font-size: 0.75rem; color: #64748b; margin-bottom: 1rem; }
    .modal label {
      display: block;
      margin-top: 0.75rem;
      font-size: 0.8125rem;
      color: #94a3b8;
      font-weight: 500;
    }
    .modal label:first-of-type { margin-top: 0; }
    .modal input, .modal select {
      width: 100%;
      padding: 0.5rem 0.65rem;
      margin-top: 0.25rem;
      background: #0f172a;
      border: 1px solid #334155;
      border-radius: 6px;
      color: #e2e8f0;
      font-size: 0.875rem;
      transition: border-color 0.15s;
    }
    .modal input:focus, .modal select:focus {
      outline: none;
      border-color: #38bdf8;
      box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2);
    }
    .modal .section {
      margin-top: 1.25rem;
      padding-top: 1.25rem;
      border-top: 1px solid #334155;
    }
    .modal .section-title {
      font-weight: 600;
      font-size: 0.875rem;
      margin-bottom: 0.5rem;
      color: #cbd5e1;
    }
    .modal-actions {
      margin-top: 1.5rem;
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
    }
    .modal-actions button {
      padding: 0.5rem 1rem;
      border-radius: 8px;
      border: 0;
      cursor: pointer;
      font-size: 0.875rem;
      font-weight: 500;
      transition: background 0.15s;
    }
    .modal-actions .save-btn { background: #0ea5e9; color: #fff; }
    .modal-actions .save-btn:hover { background: #0284c7; }
    .modal-actions .cancel-btn { background: #475569; color: #e2e8f0; }
    .modal-actions .cancel-btn:hover { background: #64748b; }
    .section .hint { font-size: 0.75rem; color: #64748b; margin-top: 0.5rem; line-height: 1.4; }
    .dur-color-preview {
      height: 0.5rem;
      border-radius: 999px;
      margin-top: 0.65rem;
      border: 1px solid rgba(51, 65, 85, 0.6);
    }
    .dur-color-stops { margin-top: 0.65rem; display: flex; flex-direction: column; gap: 0.4rem; }
    .dur-color-stop { display: flex; align-items: center; gap: 0.5rem; }
    .dur-color-stop input[type="text"] { flex: 1; margin-top: 0; }
    .dur-color-stop input[type="color"] {
      width: 2.6rem;
      height: 1.9rem;
      padding: 0.15rem;
      margin-top: 0;
      border: 1px solid #334155;
      border-radius: 6px;
      background: #0f172a;
      cursor: pointer;
    }
    .dur-color-stop input.invalid { border-color: #ef4444; }
    .dur-color-stop-remove {
      background: transparent;
      color: #fca5a5;
      border: 1px solid #7f1d1d;
      border-radius: 6px;
      padding: 0.3rem 0.55rem;
      cursor: pointer;
      font-size: 0.75rem;
      line-height: 1;
    }
    .dur-color-stop-remove:hover { background: rgba(185, 28, 28, 0.15); }
    .dur-color-add-btn {
      margin-top: 0.65rem;
      padding: 0.35rem 0.75rem;
      background: transparent;
      color: #94a3b8;
      border: 1px dashed #475569;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.8125rem;
    }
    .dur-color-add-btn:hover { color: #cbd5e1; border-color: #64748b; }
  </style>
</head>
<body>
  <div class="page">
    <header class="header">
      <div class="header-row">
        <h1><span>PgRollback</span> Sessions</h1>
        <div class="toolbar">
          <button type="button" id="refresh">Refresh</button>
          <button type="button" id="rollbackAllBtn" class="rollback-all-btn">Rollback All</button>
          <button type="button" id="disconnectAllBtn" class="disconnect-all-btn">Disconnect All</button>
          <button type="button" class="settings-btn" id="settingsBtn">Settings</button>
        </div>
      </div>
      <div class="backend-conn proxy-conn" id="proxy_conn_wrap" title="">
        <span class="backend-conn-label">Proxy listen</span><code class="backend-conn-str" id="proxy_conn_line"></code>
        <div class="proxy-conn-hint">Connect your client to this host and port using the same <strong>database name</strong>, <strong>user</strong>, and <strong>password</strong> as backend PostgreSQL. The proxy does not use a separate password.</div>
      </div>
      <div class="backend-conn" id="backend_conn_wrap" title="">
        <span class="backend-conn-label">Backend PostgreSQL</span><code class="backend-conn-str" id="backend_conn_masked"></code>
      </div>
    </header>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Test ID</th><th class="tx-status">In transaction</th><th class="lock-status">Lock</th><th>Last query</th><th class="actions">Actions</th></tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
  <div id="settingsModal" class="modal">
    <div class="modal-content">
      <h2>Settings</h2>
      <div class="config-path">
        <label>Config file path
          <input type="text" id="cfg_config_path" name="config_path" placeholder="">
        </label>
      </div>
      <form id="settingsForm">
        <div class="section">
          <div class="section-title">Postgres</div>
          <label>Host <input type="text" id="cfg_postgres_host" name="host"/></label>
          <label>Port <input type="number" id="cfg_postgres_port" name="port"/></label>
          <label>Database <input type="text" id="cfg_postgres_database" name="database"/></label>
          <label>User <input type="text" id="cfg_postgres_user" name="user"/></label>
          <label>Password <input type="password" id="cfg_postgres_password" name="password" placeholder="Leave blank to keep current" autocomplete="off"/></label>
          <label>Session timeout (e.g. 900s, 24h) <input type="text" id="cfg_postgres_session_timeout" name="session_timeout"/></label>
        </div>
        <div class="section">
          <div class="section-title">Proxy</div>
          <label>Listen host <input type="text" id="cfg_proxy_listen_host" name="listen_host"/></label>
          <label>Listen port <input type="number" id="cfg_proxy_listen_port" name="listen_port"/></label>
          <label>Timeout (seconds) <input type="number" id="cfg_proxy_timeout_sec" name="timeout_sec"/></label>
          <label>Keepalive interval (e.g. 300s) <input type="text" id="cfg_proxy_keepalive_interval" name="keepalive_interval"/></label>
        </div>
        <div class="section">
          <div class="section-title">Logging</div>
          <label>Level <select id="cfg_logging_level" name="level"><option value="debug">DEBUG</option><option value="info" selected>INFO</option><option value="warn">WARN</option><option value="error">ERROR</option></select></label>
          <label>File <input type="text" id="cfg_logging_file" name="file" placeholder="empty = stderr"/></label>
        </div>
        <div class="section">
          <div class="section-title">Test</div>
          <label>Schema <input type="text" id="cfg_test_schema" name="schema"/></label>
          <label>Context timeout (e.g. 10s) <input type="text" id="cfg_test_context_timeout" name="context_timeout"/></label>
          <label>Query timeout (e.g. 5s) <input type="text" id="cfg_test_query_timeout" name="query_timeout"/></label>
          <label>Ping timeout (e.g. 3s) <input type="text" id="cfg_test_ping_timeout" name="ping_timeout"/></label>
        </div>
        <div class="modal-actions">
          <button type="submit" class="save-btn">Save</button>
          <button type="button" id="settingsModalCancel" class="cancel-btn">Cancel</button>
        </div>
      </form>
      <div class="section" id="durationColorSection">
        <div class="section-title">Query duration colors</div>
        <label>Style
          <select id="durColorPreset">
            <option value="full">Full range (0ms &ndash; 1m)</option>
            <option value="optimized">Optimized queries (0ms &ndash; 20s)</option>
            <option value="custom">Custom</option>
          </select>
        </label>
        <div class="dur-color-preview" id="durColorPreview"></div>
        <div class="dur-color-stops" id="durColorStops"></div>
        <button type="button" class="dur-color-add-btn" id="durColorAddStop">+ Add color stop</button>
        <div class="hint">Colors each query's duration text from blue (fast) to red (at or beyond the last stop). Times accept ms/s/m, e.g. "500ms", "10s", "1m". Saved in this browser only.</div>
        <div class="modal-actions">
          <button type="button" class="save-btn" id="durColorSave">Save colors</button>
        </div>
      </div>
    </div>
  </div>
  <script>
    const tbody = document.getElementById('tbody');
    const refreshBtn = document.getElementById('refresh');
    // Must match config.PasswordMask in Go (fixed sentinel for "keep existing password" on save).
    const PASSWORD_MASK = '******';
    function applyConfigMeta(data) {
      var c = data && data.config;
      var px = c && c.proxy ? c.proxy : {};
      var pHost = (px.listen_host != null && String(px.listen_host).trim() !== '') ? String(px.listen_host).trim() : '';
      var pPort = (px.listen_port != null && px.listen_port !== '') ? String(px.listen_port) : '';
      var proxyLine = '';
      if (pHost || pPort) {
        proxyLine = 'host=' + (pHost || '?') + ' port=' + (pPort || '?');
      }
      var proxyEl = document.getElementById('proxy_conn_line');
      var proxyWrap = document.getElementById('proxy_conn_wrap');
      if (proxyEl) proxyEl.textContent = proxyLine;
      if (proxyWrap) {
        proxyWrap.setAttribute('title', proxyLine
          ? ('Proxy address: ' + proxyLine + '. Use backend DB user, password, and database; no separate proxy password.')
          : 'Proxy listen host and port (set in Settings)');
      }
      var wrap = document.getElementById('backend_conn_wrap');
      var el = document.getElementById('backend_conn_masked');
      var s = (data && data.postgres_connection_string_masked) ? data.postgres_connection_string_masked : '';
      if (el) el.textContent = s;
      if (wrap) {
        wrap.setAttribute('title', s ? ('Real database (password hidden): ' + s) : 'Real database connection string');
      }
    }
    function refreshBackendConnLine() {
      fetch('__API_BASE__/config').then(function(r) { return r.json(); }).then(function(data) {
        if (data && data.error) return;
        applyConfigMeta(data || {});
      }).catch(function() {});
    }
    var settingsModal = document.getElementById('settingsModal');
    var settingsBtn = document.getElementById('settingsBtn');
    var settingsForm = document.getElementById('settingsForm');
    function escapeHtml(s) {
      const div = document.createElement('div');
      div.textContent = s;
      return div.innerHTML;
    }
    function formatHistoryAt(at) {
      if (!at) return '';
      try {
        var d = new Date(at);
        return isNaN(d.getTime()) ? at : d.toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' });
      } catch (e) { return at; }
    }
    function formatDuration(raw) {
      if (!raw || typeof raw !== 'string') return raw;
      var s = raw.trim();
      if (!s) return s;
      var m = s.match(/^([\d.]+)\s*([a-zA-Zµμ]*)$/);
      if (!m) return s;
      var v = parseFloat(m[1]);
      if (!isFinite(v)) return s;
      var unit = m[2] || '';
      var abs = Math.abs(v);
      var digits = abs === 0 ? 1 : Math.floor(Math.log10(abs)) + 1;
      var decimals = 0;
      if (digits < 3) {
        decimals = 3 - digits;
      }
      var factor = Math.pow(10, decimals);
      var rounded = Math.round(v * factor) / factor;
      var numStr = decimals > 0 ? rounded.toFixed(decimals) : String(Math.round(rounded));
      if (numStr.indexOf('.') !== -1) {
        numStr = numStr.replace(/\.0+$/, '').replace(/(\.\d)0+$/, '$1');
      }
      return numStr + unit;
    }
    function prettySql(sql) {
      if (!sql || typeof sql !== 'string') return sql;
      var s = sql.trim();
      if (s.length > 2000) return s;
      var re = /\b(SELECT|FROM|WHERE|AND|OR|LEFT JOIN|RIGHT JOIN|INNER JOIN|JOIN|ON|GROUP BY|ORDER BY|LIMIT|OFFSET|INSERT INTO|UPDATE|SET|VALUES|RETURNING|DELETE FROM|CREATE |ALTER |DROP |BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE SAVEPOINT|WITH|UNION|HAVING)\b/gi;
      return s.replace(re, function(m) { return '\n' + m; }).replace(/\n+/g, '\n').trim();
    }
    // Formats how long a still-running query has been running, e.g. "running 3s" / "running 1m 5s".
    function formatElapsedMs(ms) {
      if (!isFinite(ms) || ms < 1000) return 'running…';
      var totalSec = Math.floor(ms / 1000);
      var m = Math.floor(totalSec / 60);
      var s = totalSec % 60;
      return 'running ' + (m > 0 ? (m + 'm ' + s + 's') : (s + 's'));
    }

    // --- Query duration color scale: colors each duration badge's text from blue (fast) to red
    // (at/beyond the last configured stop). Purely a per-browser display preference (localStorage),
    // not server config - each developer reads logs differently, and there's no server-side
    // consumer of this value.
    var DURATION_COLOR_STORAGE_KEY = 'pgrollback_duration_color_scale';
    // 12 stops each, evenly spaced across the preset's range, interpolated in HSL from blue
    // (hue 217) to red (hue 0) at fixed saturation/lightness - a smooth blue->cyan->green->
    // yellow->orange->red ramp rather than 6 hand-picked anchor colors.
    var DURATION_COLOR_PRESETS = {
      full: [
        { ms: 300, color: '#3c83f6' },
        { ms: 1000, color: '#3cc0f6' },
        { ms: 2000, color: '#3cf6ee' },
        { ms: 6000, color: '#3cf6b1' },
        { ms: 15000, color: '#3cf674' },
        { ms: 22000, color: '#41f63c' },
        { ms: 30000, color: '#7ef63c' },
        { ms: 38182, color: '#bbf63c' },
        { ms: 43636, color: '#f6f33c' },
        { ms: 49091, color: '#f6b63c' },
        { ms: 54545, color: '#f6793c' },
        { ms: 60000, color: '#f63c3c' }
      ],
      optimized: [
        { ms: 200, color: '#3c83f6' },
        { ms: 500, color: '#3cc0f6' },
        { ms: 1000, color: '#3cf6ee' },
        { ms: 2000, color: '#3cf6b1' },
        { ms: 5000, color: '#3cf674' },
        { ms: 7000, color: '#41f63c' },
        { ms: 10000, color: '#7ef63c' },
        { ms: 12727, color: '#bbf63c' },
        { ms: 14545, color: '#f6f33c' },
        { ms: 16364, color: '#f6b63c' },
        { ms: 18182, color: '#f6793c' },
        { ms: 20000, color: '#f63c3c' }
      ]
    };
    function clonePreset(name) {
      return DURATION_COLOR_PRESETS[name].map(function(s) { return { ms: s.ms, color: s.color }; });
    }
    function presetMatches(name, stops) {
      var preset = DURATION_COLOR_PRESETS[name];
      if (!stops || stops.length !== preset.length) return false;
      for (var i = 0; i < preset.length; i++) {
        if (stops[i].ms !== preset[i].ms || stops[i].color.toLowerCase() !== preset[i].color.toLowerCase()) return false;
      }
      return true;
    }
    function detectPreset(stops) {
      if (presetMatches('full', stops)) return 'full';
      if (presetMatches('optimized', stops)) return 'optimized';
      return 'custom';
    }
    function loadDurationColorState() {
      try {
        var raw = localStorage.getItem(DURATION_COLOR_STORAGE_KEY);
        if (raw) {
          var parsed = JSON.parse(raw);
          if (parsed && Array.isArray(parsed.stops)) {
            var stops = parsed.stops.filter(function(s) { return s && isFinite(s.ms) && typeof s.color === 'string'; });
            if (stops.length >= 2) return { stops: stops };
          }
        }
      } catch (e) {}
      return { stops: clonePreset('full') };
    }
    var durationColorState = loadDurationColorState();
    function saveDurationColorState(stops) {
      durationColorState = { stops: stops };
      try { localStorage.setItem(DURATION_COLOR_STORAGE_KEY, JSON.stringify(durationColorState)); } catch (e) {}
    }
    function hexToRgb(hex) {
      var h = (hex || '').replace('#', '');
      if (h.length === 3) h = h.split('').map(function(c) { return c + c; }).join('');
      var n = parseInt(h, 16);
      if (isNaN(n)) return { r: 148, g: 163, b: 184 };
      return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
    }
    function rgbToHex(r, g, b) {
      function h(v) { v = Math.max(0, Math.min(255, Math.round(v))); var s = v.toString(16); return s.length === 1 ? '0' + s : s; }
      return '#' + h(r) + h(g) + h(b);
    }
    // Interpolates the configured stops for a given elapsed time in ms; clamps to the first/last
    // color outside the configured range (so anything at or beyond the last stop reads as fully "hot").
    function colorForDurationMs(ms) {
      var stops = (durationColorState.stops || []).slice().sort(function(a, b) { return a.ms - b.ms; });
      if (!stops.length || !isFinite(ms)) return null;
      if (ms <= stops[0].ms) return stops[0].color;
      if (ms >= stops[stops.length - 1].ms) return stops[stops.length - 1].color;
      for (var i = 0; i < stops.length - 1; i++) {
        var a = stops[i], b = stops[i + 1];
        if (ms >= a.ms && ms <= b.ms) {
          var t = (b.ms === a.ms) ? 0 : (ms - a.ms) / (b.ms - a.ms);
          var ca = hexToRgb(a.color), cb = hexToRgb(b.color);
          return rgbToHex(ca.r + (cb.r - ca.r) * t, ca.g + (cb.g - ca.g) * t, ca.b + (cb.b - ca.b) * t);
        }
      }
      return stops[stops.length - 1].color;
    }
    // Converts a Go time.Duration.String() value (e.g. "142µs", "1.81ms", "10.0093244s") to
    // milliseconds. Go only switches to a compound "1m0s" / "2h3m4s" style at >= 1 minute, so any
    // string that doesn't match the single-unit pattern below is treated as >= 60000ms - correct
    // since the color scale clamps to the last stop's color at/beyond its own max anyway.
    function durationStringToMs(raw) {
      if (!raw || typeof raw !== 'string') return null;
      var s = raw.trim();
      if (!s) return null;
      var m = s.match(/^([\d.]+)\s*(ns|µs|us|μs|ms|s)$/);
      if (!m) return /^\d/.test(s) ? 60000 : null;
      var v = parseFloat(m[1]);
      if (!isFinite(v)) return null;
      switch (m[2]) {
        case 'ns': return v / 1e6;
        case 'µs': case 'us': case 'μs': return v / 1000;
        case 'ms': return v;
        case 's': return v * 1000;
      }
      return null;
    }
    // Parses a user-typed stop time like "500ms", "10s", "1m30s", or a bare number (ms) into ms.
    function parseDurationToMs(s) {
      if (s == null) return NaN;
      s = String(s).trim();
      if (s === '') return NaN;
      if (/^\d+(\.\d+)?$/.test(s)) return parseFloat(s);
      var total = 0, matched = false, re = /(\d+(?:\.\d+)?)\s*(ms|h|m|s)/gi, m;
      while ((m = re.exec(s))) {
        matched = true;
        var val = parseFloat(m[1]), unit = m[2].toLowerCase();
        if (unit === 'ms') total += val;
        else if (unit === 's') total += val * 1000;
        else if (unit === 'm') total += val * 60000;
        else if (unit === 'h') total += val * 3600000;
      }
      return matched ? total : NaN;
    }
    function formatMsAsLabel(ms) {
      if (ms === 0) return '0ms';
      if (ms < 1000) return (Math.round(ms * 100) / 100) + 'ms';
      if (ms < 60000) return (Math.round((ms / 1000) * 100) / 100) + 's';
      var m = Math.floor(ms / 60000), rem = Math.round((ms % 60000) / 1000);
      return rem ? (m + 'm' + rem + 's') : (m + 'm');
    }
    var pendingDurationColorStops = [];
    function updateDurationColorPreview() {
      var preview = document.getElementById('durColorPreview');
      if (!preview) return;
      var stops = pendingDurationColorStops.slice().sort(function(a, b) { return a.ms - b.ms; });
      if (!stops.length) { preview.style.background = 'transparent'; return; }
      var maxMs = stops[stops.length - 1].ms || 1;
      var parts = stops.map(function(s) { return s.color + ' ' + Math.round((s.ms / maxMs) * 100) + '%'; });
      preview.style.background = 'linear-gradient(90deg, ' + parts.join(', ') + ')';
    }
    function renderDurationColorEditor() {
      var wrap = document.getElementById('durColorStops');
      var presetSelect = document.getElementById('durColorPreset');
      if (!wrap) return;
      wrap.innerHTML = '';
      pendingDurationColorStops.forEach(function(stop, idx) {
        var row = document.createElement('div');
        row.className = 'dur-color-stop';
        row.innerHTML =
          '<input type="text" class="dur-stop-time" value="' + escapeHtml(formatMsAsLabel(stop.ms)) + '" placeholder="e.g. 5s">' +
          '<input type="color" class="dur-stop-color" value="' + stop.color + '">' +
          '<button type="button" class="dur-color-stop-remove" title="Remove stop">✕</button>';
        var timeInput = row.querySelector('.dur-stop-time');
        var colorInput = row.querySelector('.dur-stop-color');
        var removeBtn = row.querySelector('.dur-color-stop-remove');
        timeInput.addEventListener('change', function() {
          var parsed = parseDurationToMs(timeInput.value);
          if (isNaN(parsed) || parsed < 0) { timeInput.classList.add('invalid'); return; }
          pendingDurationColorStops[idx].ms = parsed;
          pendingDurationColorStops.sort(function(a, b) { return a.ms - b.ms; });
          if (presetSelect) presetSelect.value = detectPreset(pendingDurationColorStops);
          renderDurationColorEditor();
        });
        colorInput.addEventListener('input', function() {
          pendingDurationColorStops[idx].color = colorInput.value;
          if (presetSelect) presetSelect.value = detectPreset(pendingDurationColorStops);
          updateDurationColorPreview();
        });
        removeBtn.addEventListener('click', function() {
          if (pendingDurationColorStops.length <= 2) { alert('At least 2 color stops are required.'); return; }
          pendingDurationColorStops.splice(idx, 1);
          if (presetSelect) presetSelect.value = detectPreset(pendingDurationColorStops);
          renderDurationColorEditor();
        });
        wrap.appendChild(row);
      });
      updateDurationColorPreview();
    }
    // Returns the "(12.345ms)" / "(running...)" badge for a query. While running, the span carries
    // data-started-at (ms epoch) so tickRunningIndicators() can update the live elapsed time without
    // a full re-render (the row otherwise only updates on the next start/finish push event). Both
    // states get a text color from the duration scale so a running query visibly heats up over time.
    function durationOrRunningHtml(running, startedAtRaw, durationRaw) {
      if (running) {
        var ts = startedAtRaw ? (new Date(startedAtRaw)).getTime() : NaN;
        var attr = (ts && !isNaN(ts)) ? (' data-started-at="' + ts + '"') : '';
        var liveColor = (ts && !isNaN(ts)) ? colorForDurationMs(Date.now() - ts) : null;
        var runStyle = liveColor ? (' style="color:' + liveColor + '"') : '';
        return ' <span class="query-duration running"' + attr + runStyle + '>(running…)</span>';
      }
      var d = formatDuration(durationRaw);
      if (!d || !d.trim()) return '';
      var ms = durationStringToMs(durationRaw);
      var color = (ms != null) ? colorForDurationMs(ms) : null;
      var style = color ? (' style="color:' + color + '"') : '';
      return ' <span class="query-duration"' + style + '>(' + escapeHtml(d) + ')</span>';
    }
    function tickRunningIndicators() {
      var now = Date.now();
      document.querySelectorAll('.query-duration.running[data-started-at]').forEach(function(el) {
        var startedAt = parseInt(el.getAttribute('data-started-at'), 10);
        if (!startedAt || isNaN(startedAt)) return;
        el.textContent = '(' + formatElapsedMs(now - startedAt) + ')';
        var color = colorForDurationMs(now - startedAt);
        if (color) el.style.color = color;
      });
    }
    // Renders "[db 10.1ms / proxy 2.2ms]" next to a finished query's duration so it's clear how
    // much of the total time was the real PostgreSQL round trip vs. the proxy's own processing
    // (interception, protocol handling, GUI logging). Empty when not tracked for this entry (see
    // DBDuration on proxy.QueryHistoryEntry) - e.g. a composite multi-statement batch.
    function dbProxyBreakdownHtml(item) {
      if (!item || !item.db_duration || !item.proxy_duration) return '';
      var db = escapeHtml(formatDuration(item.db_duration));
      var proxyD = escapeHtml(formatDuration(item.proxy_duration));
      return ' <span class="query-db-proxy-split" title="Time in the real PostgreSQL round trip vs. proxy-side processing (query interception, protocol handling, GUI logging)">[db <span class="db-part">' + db + '</span> / proxy <span class="proxy-part">' + proxyD + '</span>]</span>';
    }
    function historyItemHtml(item) {
      var query = '';
      var at = '';
      var dur = '';
      var split = '';
      if (item && typeof item === 'object' && item.query !== undefined) {
        query = item.query || '';
        at = item.at ? '<span class="qtime">' + escapeHtml(formatHistoryAt(item.at)) + '</span>' : '';
        dur = durationOrRunningHtml(item.running === true, item.at, item.duration);
        split = dbProxyBreakdownHtml(item);
      } else {
        query = typeof item === 'string' ? item : '';
      }
      return at + dur + split + ' ' + escapeHtml(prettySql(query));
    }
    var openHistoryIds = {};
    var historyScrollTops = {};
    var lastRenderedSessions = null;
    function lockStatusHtml(s) {
      var ls = s && s.lock_status;
      if (!ls || ls.waiting_on_lock !== true) {
        return '<span class="lock-status ok" title="Not waiting on a lock">—</span>';
      }
      var parts = ['Waiting on lock'];
      if (ls.locked_relation) parts.push('relation: ' + ls.locked_relation);
      if (ls.lock_mode) parts.push('mode: ' + ls.lock_mode);
      if (ls.wait_event) parts.push('wait: ' + ls.wait_event);
      if (ls.blocker_pid) parts.push('blocked by PID ' + ls.blocker_pid);
      if (ls.blocker_is_pgrollback && ls.blocker_test_id) {
        parts.push('blocker: pgrollback session ' + ls.blocker_test_id);
      } else if (ls.blocker_application_name) {
        parts.push('blocker app: ' + ls.blocker_application_name);
      }
      if (ls.blocker_query_snippet) parts.push('blocker query: ' + ls.blocker_query_snippet);
      var title = parts.join('\\n');
      var label = 'Blocked';
      var extraClass = '';
      if (ls.blocker_is_pgrollback && ls.blocker_test_id) {
        label = 'Blocked by pgrollback';
        extraClass = ' pgrollback';
      }
      return '<span class="lock-status blocked' + extraClass + '" title="' + escapeHtml(title) + '"><span class="lock-badge">' + escapeHtml(label) + '</span></span>';
    }
    function sessionKeys(sessions) {
      var ids = [];
      for (var i = 0; i < sessions.length; i++) ids.push(sessions[i].test_id);
      ids.sort();
      return ids.join('\n');
    }
    function fullReplace(sessions) {
      var html = '';
      sessions.forEach(function(s) {
        var q = escapeHtml(s.last_query || '');
        var qTitle = (s.last_query || '');
        var hist = s.query_history || [];
        var lastHist = hist.length ? hist[hist.length - 1] : null;
        var dur = durationOrRunningHtml(s.running === true, lastHist ? lastHist.at : null, s.last_query_duration);
        var n = hist.length;
        var txLabel = (s.in_transaction === true) ? 'Yes' : 'No';
        var txClass = (s.in_transaction === true) ? 'tx-status yes' : 'tx-status no';
        html += '<tr class="session-row" data-id="' + escapeHtml(s.test_id) + '"><td>' + escapeHtml(s.test_id) + '</td><td class="' + txClass + '">' + txLabel + '</td><td>' + lockStatusHtml(s) + '</td><td class="query" title="' + escapeHtml(qTitle) + '">' + q + dur + '</td><td><button type="button" class="history-btn" data-id="' + escapeHtml(s.test_id) + '">History (' + n + ')</button><button type="button" class="clear-log-btn" data-id="' + escapeHtml(s.test_id) + '">Clear log</button><button type="button" class="close-btn" data-id="' + escapeHtml(s.test_id) + '">Disconnect</button></td></tr>';
        html += '<tr class="history-row" data-id="' + escapeHtml(s.test_id) + '" style="display:none"><td colspan="5"><div class="history-list-wrap"><div class="history-list-toolbar"><button type="button" class="history-height-btn">Full height</button></div><div class="history-list"><ul>';
        // Newest first, in actual DOM order (not via CSS column-reverse - see .history-list ul).
        for (var j = hist.length - 1; j >= 0; j--) {
          html += '<li>' + historyItemHtml(hist[j]) + '</li>';
        }
        html += '</ul></div></div></td></tr>';
      });
      tbody.innerHTML = html;
      tbody.querySelectorAll('.history-row').forEach(function(row) {
        var id = row.getAttribute('data-id');
        if (openHistoryIds[id]) {
          row.style.display = '';
          var list = row.querySelector('.history-list');
          if (list && historyScrollTops[id] != null) list.scrollTop = historyScrollTops[id];
        }
      });
      tbody.querySelectorAll('.history-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var row = this.closest('tr').nextElementSibling;
          if (row && row.classList.contains('history-row')) row.style.display = row.style.display === 'none' ? '' : 'none';
        });
      });
      tbody.querySelectorAll('.history-height-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var wrap = this.closest('.history-list-wrap');
          if (!wrap) return;
          var list = wrap.querySelector('.history-list');
          if (!list) return;
          list.classList.toggle('full-height');
          this.textContent = list.classList.contains('full-height') ? 'Limit height' : 'Full height';
        });
      });
      tbody.querySelectorAll('.clear-log-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var id = this.getAttribute('data-id');
          fetch('__API_BASE__/sessions/clear-history', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ test_id: id }) })
            .then(function(r) {
              if (!r.ok) { r.text().then(function(t) { alert(t); }); return; }
              // Force fullReplace on the next render so history <ul> and counts match the server (incremental updateRow can miss some shrink cases).
              lastRenderedSessions = null;
              load();
            })
            .catch(function(e) { alert('Clear log failed: ' + (e && e.message ? e.message : e)); });
        });
      });
      tbody.querySelectorAll('.close-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var id = this.getAttribute('data-id');
          fetch('__API_BASE__/sessions/close', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ test_id: id }) })
            .then(function(r) { if (r.ok) load(); else r.text().then(function(t) { alert(t); }); });
        });
      });
    }
    function selectorEscape(id) {
      return CSS.escape ? CSS.escape(id) : id.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    }
    function updateRow(id, s) {
      var sel = 'tr.session-row[data-id="' + selectorEscape(id) + '"]';
      var mainRow = tbody.querySelector(sel);
      if (!mainRow) return;
      var q = s.last_query || '';
      var hist = s.query_history || [];
      var lastHist = hist.length ? hist[hist.length - 1] : null;
      var dur = durationOrRunningHtml(s.running === true, lastHist ? lastHist.at : null, s.last_query_duration);
      var n = hist.length;
      mainRow.cells[0].textContent = s.test_id;
      var txLabel = (s.in_transaction === true) ? 'Yes' : 'No';
      mainRow.cells[1].textContent = txLabel;
      mainRow.cells[1].className = (s.in_transaction === true) ? 'tx-status yes' : 'tx-status no';
      if (mainRow.cells[2]) mainRow.cells[2].innerHTML = lockStatusHtml(s);
      var queryCell = mainRow.cells[3];
      queryCell.innerHTML = escapeHtml(q) + dur;
      queryCell.title = q;
      queryCell.className = 'query';
      var histBtn = mainRow.querySelector('.history-btn');
      if (histBtn) histBtn.textContent = 'History (' + n + ')';
      var historyRow = tbody.querySelector('tr.history-row[data-id="' + selectorEscape(id) + '"]');
      if (!historyRow) return;
      var listWrap = historyRow.querySelector('.history-list');
      var ul = listWrap ? listWrap.querySelector('ul') : null;
      if (!ul) return;
      var prevHist = lastRenderedSessions ? (function() {
        for (var i = 0; i < lastRenderedSessions.length; i++)
          if (lastRenderedSessions[i].test_id === id) return lastRenderedSessions[i].query_history || [];
        return [];
      }()) : [];
      // Compare full contents, not just length: server history is a capped FIFO window (oldest
      // dropped once maxQueryHistory is hit), so length alone stops changing once the cap is
      // reached and a length-only diff would silently stop appending new queries forever. This
      // also catches a query's Running -> finished transition, which changes the last entry's
      // duration/running fields without changing the count.
      if (!historyListMatches(prevHist, hist)) {
        var scrollTop = listWrap.scrollTop;
        ul.innerHTML = '';
        // Newest first, in actual DOM order (not via CSS column-reverse - see .history-list ul).
        for (var j = hist.length - 1; j >= 0; j--) {
          var li = document.createElement('li');
          li.innerHTML = historyItemHtml(hist[j]);
          ul.appendChild(li);
        }
        listWrap.scrollTop = scrollTop;
      }
    }
    function historyListMatches(a, b) {
      if (a.length !== b.length) return false;
      for (var i = 0; i < a.length; i++) {
        if (a[i].query !== b[i].query || a[i].at !== b[i].at || a[i].duration !== b[i].duration ||
            a[i].db_duration !== b[i].db_duration || a[i].proxy_duration !== b[i].proxy_duration ||
            a[i].running !== b[i].running) return false;
      }
      return true;
    }
    // Preserve UI state across polling updates so modal/history don't close unexpectedly.
    function render(sessions) {
      var settingsModalOpen = settingsModal && settingsModal.classList.contains('visible');
      var rows = tbody.querySelectorAll('tr');
      for (var i = 0; i < rows.length; i++) {
        if (rows[i].classList.contains('history-row')) continue;
        var btn = rows[i].querySelector('.history-btn');
        if (btn) {
          var id = btn.getAttribute('data-id');
          var next = rows[i].nextElementSibling;
          if (id && next && next.classList.contains('history-row') && next.style.display !== 'none') {
            openHistoryIds[id] = true;
            var list = next.querySelector('.history-list');
            if (list) historyScrollTops[id] = list.scrollTop;
          }
        }
      }
      if (sessions.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="empty">No sessions</td></tr>';
        lastRenderedSessions = null;
        if (settingsModalOpen && settingsModal) settingsModal.classList.add('visible');
        return;
      }
      var keysNow = sessionKeys(sessions);
      var keysPrev = lastRenderedSessions ? sessionKeys(lastRenderedSessions) : null;
      if (keysPrev === null || keysNow !== keysPrev) {
        fullReplace(sessions);
        lastRenderedSessions = JSON.parse(JSON.stringify(sessions));
        if (settingsModalOpen && settingsModal) settingsModal.classList.add('visible');
        return;
      }
      for (var i = 0; i < sessions.length; i++) {
        updateRow(sessions[i].test_id, sessions[i]);
      }
      lastRenderedSessions = JSON.parse(JSON.stringify(sessions));
      if (settingsModalOpen && settingsModal) settingsModal.classList.add('visible');
    }
    // Applies a single-session push update (query started/finished) without touching the rest
    // of the table. Falls back to a full resync if the row isn't present yet (e.g. a
    // session_update raced ahead of the snapshot that introduces its row).
    function handleSessionUpdate(s) {
      if (!s || !s.test_id) return;
      var exists = tbody.querySelector('tr.session-row[data-id="' + selectorEscape(s.test_id) + '"]');
      if (!exists) { load(); return; }
      updateRow(s.test_id, s);
      if (lastRenderedSessions) {
        var found = false;
        for (var i = 0; i < lastRenderedSessions.length; i++) {
          if (lastRenderedSessions[i].test_id === s.test_id) {
            lastRenderedSessions[i] = JSON.parse(JSON.stringify(s));
            found = true;
            break;
          }
        }
        if (!found) lastRenderedSessions.push(JSON.parse(JSON.stringify(s)));
      }
    }
    // One-shot fetch of the full session list. Used for the Refresh button, the periodic
    // safety-net resync, and as a fallback if the event stream hasn't connected yet.
    function load() {
      fetch('__API_BASE__/sessions').then(function(r) { return r.json(); }).then(render).catch(function(e) { tbody.innerHTML = '<tr><td colspan="4" class="empty">Error: ' + escapeHtml(e.message) + '</td></tr>'; });
    }
    // Live updates: the server pushes events over SSE instead of us polling on a timer, so the
    // table updates the moment a query starts/finishes instead of waiting up to 1s for the next
    // poll (and instead of that poll blocking behind a slow query). EventSource reconnects
    // automatically; on (re)connect the server always sends a fresh "snapshot" first.
    var sseGotData = false;
    function connectEventStream() {
      var es = new EventSource('__API_BASE__/sessions/stream');
      es.addEventListener('snapshot', function(ev) {
        sseGotData = true;
        try { render(JSON.parse(ev.data).sessions || []); } catch (e) {}
      });
      es.addEventListener('session_update', function(ev) {
        sseGotData = true;
        try { handleSessionUpdate(JSON.parse(ev.data).session); } catch (e) {}
      });
      es.onerror = function() {
        if (!sseGotData) load();
      };
      return es;
    }
    refreshBtn.addEventListener('click', load);
    var rollbackAllBtn = document.getElementById('rollbackAllBtn');
    if (rollbackAllBtn) {
      rollbackAllBtn.addEventListener('click', function() {
        fetch('__API_BASE__/sessions/rollback-all', { method: 'POST' })
          .then(function(r) { if (!r.ok) throw new Error(r.statusText); return r.json(); })
          .then(function() { load(); })
          .catch(function(e) { alert('Rollback All failed: ' + (e && e.message ? e.message : e)); });
      });
    }
    var disconnectAllBtn = document.getElementById('disconnectAllBtn');
    if (disconnectAllBtn) {
      disconnectAllBtn.addEventListener('click', function() {
        if (!confirm('Disconnect ALL sessions? This will close all connections and rollback their transactions.')) return;
        fetch('__API_BASE__/sessions/disconnect-all', { method: 'POST' })
          .then(function(r) { if (!r.ok) throw new Error(r.statusText); return r.json(); })
          .then(function() { load(); })
          .catch(function(e) { alert('Disconnect All failed: ' + (e && e.message ? e.message : e)); });
      });
    }
    refreshBackendConnLine();
    connectEventStream();
    // Safety-net resync in case a push event was ever dropped (Hub.Publish does not block or
    // queue for a slow/disconnected subscriber); cheap now that GetSessions never blocks behind
    // a running query.
    setInterval(load, 30000);
    // Ticks the "(running Xs)" badges so a long/stale query's elapsed time updates live even
    // when no new push event arrives (start/finish are the only events; nothing fires while a
    // query is just sitting there running).
    setInterval(tickRunningIndicators, 1000);

    var durColorPresetSelect = document.getElementById('durColorPreset');
    if (durColorPresetSelect) {
      durColorPresetSelect.addEventListener('change', function() {
        var v = durColorPresetSelect.value;
        if (v === 'full' || v === 'optimized') {
          pendingDurationColorStops = clonePreset(v);
          renderDurationColorEditor();
        }
      });
    }
    var durColorAddStopBtn = document.getElementById('durColorAddStop');
    if (durColorAddStopBtn) {
      durColorAddStopBtn.addEventListener('click', function() {
        var last = pendingDurationColorStops[pendingDurationColorStops.length - 1];
        var nextMs = last ? last.ms + 5000 : 0;
        pendingDurationColorStops.push({ ms: nextMs, color: '#94a3b8' });
        if (durColorPresetSelect) durColorPresetSelect.value = detectPreset(pendingDurationColorStops);
        renderDurationColorEditor();
      });
    }
    var durColorSaveBtn = document.getElementById('durColorSave');
    if (durColorSaveBtn) {
      durColorSaveBtn.addEventListener('click', function() {
        var stops = pendingDurationColorStops
          .map(function(s) { return { ms: s.ms, color: s.color }; })
          .sort(function(a, b) { return a.ms - b.ms; });
        if (stops.length < 2) { alert('At least 2 color stops are required.'); return; }
        saveDurationColorState(stops);
        if (lastRenderedSessions) fullReplace(lastRenderedSessions); else load();
      });
    }
    if (settingsBtn && settingsModal) {
      settingsBtn.addEventListener('click', function() {
        settingsModal.classList.add('visible');
        pendingDurationColorStops = durationColorState.stops.map(function(s) { return { ms: s.ms, color: s.color }; });
        if (durColorPresetSelect) durColorPresetSelect.value = detectPreset(pendingDurationColorStops);
        renderDurationColorEditor();
        fetch('__API_BASE__/config').then(function(r) { return r.json(); }).then(function(data) {
          if (data.error) { alert(data.error); return; }
          applyConfigMeta(data);
          var c = data.config || {};
          var p = c.postgres || {};
          var px = c.proxy || {};
          var l = c.logging || {};
          var t = c.test || {};
          var cfgPath = data.config_path || '';
          document.getElementById('cfg_postgres_host').value = p.host || '';
          document.getElementById('cfg_postgres_port').value = p.port || '';
          document.getElementById('cfg_postgres_database').value = p.database || '';
          document.getElementById('cfg_postgres_user').value = p.user || '';
          document.getElementById('cfg_postgres_password').value = '';
          document.getElementById('cfg_postgres_password').placeholder = (p.password && p.password !== '') ? PASSWORD_MASK : 'Leave blank to keep current';
          document.getElementById('cfg_postgres_session_timeout').value = (p.session_timeout || '').toString();
          document.getElementById('cfg_proxy_listen_host').value = px.listen_host || '';
          document.getElementById('cfg_proxy_listen_port').value = px.listen_port || '';
          document.getElementById('cfg_proxy_timeout_sec').value = px.timeout ? Math.round(px.timeout / 1e9) : '';
          document.getElementById('cfg_proxy_keepalive_interval').value = (px.keepalive_interval || '').toString();
          document.getElementById('cfg_logging_level').value = l.level || 'info';
          document.getElementById('cfg_logging_file').value = l.file || '';
          document.getElementById('cfg_test_schema').value = t.schema || '';
          document.getElementById('cfg_test_context_timeout').value = (t.context_timeout || '').toString();
          document.getElementById('cfg_test_query_timeout').value = (t.query_timeout || '').toString();
          document.getElementById('cfg_test_ping_timeout').value = (t.ping_timeout || '').toString();
          var cfgPathInput = document.getElementById('cfg_config_path');
          if (cfgPathInput) {
            cfgPathInput.value = cfgPath;
            if (!cfgPathInput.placeholder) cfgPathInput.placeholder = cfgPath || cfgPathInput.placeholder;
          }
        }).catch(function(e) { alert('Failed to load config: ' + e.message); });
      });
    }
    function closeSettingsModal() {
      if (settingsModal) settingsModal.classList.remove('visible');
    }
    var settingsModalCancel = document.getElementById('settingsModalCancel');
    if (settingsModalCancel) settingsModalCancel.addEventListener('click', closeSettingsModal);
    if (settingsModal) settingsModal.addEventListener('click', function(e) {
      if (e.target === settingsModal) closeSettingsModal();
    });
    if (settingsForm) {
      settingsForm.addEventListener('submit', function(e) {
        e.preventDefault();
        var cfgPathInput = document.getElementById('cfg_config_path');
        var cfgPathVal = cfgPathInput ? cfgPathInput.value : '';
        var pw = document.getElementById('cfg_postgres_password').value;
        var p = {
          host: document.getElementById('cfg_postgres_host').value,
          port: parseInt(document.getElementById('cfg_postgres_port').value, 10) || 0,
          database: document.getElementById('cfg_postgres_database').value,
          user: document.getElementById('cfg_postgres_user').value,
          password: (pw === '') ? PASSWORD_MASK : pw,
          session_timeout: document.getElementById('cfg_postgres_session_timeout').value
        };
        var px = {
          listen_host: document.getElementById('cfg_proxy_listen_host').value,
          listen_port: parseInt(document.getElementById('cfg_proxy_listen_port').value, 10) || 0,
          timeout: Math.round(parseFloat(document.getElementById('cfg_proxy_timeout_sec').value) || 0) * 1e9,
          keepalive_interval: document.getElementById('cfg_proxy_keepalive_interval').value
        };
        var l = {
          level: document.getElementById('cfg_logging_level').value,
          file: document.getElementById('cfg_logging_file').value
        };
        var t = {
          schema: document.getElementById('cfg_test_schema').value,
          context_timeout: document.getElementById('cfg_test_context_timeout').value,
          query_timeout: document.getElementById('cfg_test_query_timeout').value,
          ping_timeout: document.getElementById('cfg_test_ping_timeout').value
        };
        var payload = { config: { postgres: p, proxy: px, logging: l, test: t }, config_path: cfgPathVal };
        fetch('__API_BASE__/config/save', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        }).then(function(r) {
          if (r.ok) { closeSettingsModal(); refreshBackendConnLine(); }
          else { r.text().then(function(t) { alert(t); }); }
        }).catch(function(e) { alert('Save failed: ' + e.message); });
      });
    }
  </script>
</body>
</html>
`

func apiBaseFrom(base string) string {
	if base != "" && base != "/" {
		return base + "/api"
	}
	return "/api"
}

// HTMLWithBase returns the GUI page HTML with API path prefix and favicon set.
func HTMLWithBase(base string) string {
	s := strings.ReplaceAll(htmlTemplate, apiBasePlaceholder, apiBaseFrom(base))
	s = strings.ReplaceAll(s, faviconPlaceholder, tray.FaviconDataURI())
	return s
}

// HTML returns the GUI page HTML for the default route (API at /api).
func HTML() string {
	return HTMLWithBase("")
}

'use strict';

// Minimal DOM shim: just enough for internal/proxy/gui/html.go's embedded <script> to load and
// run its bootstrap code under plain Node (no browser). Deliberately dumb: every method is a
// no-op unless a specific assertion below depends on its result (see tbodyEl / escapeHtml).
function dumbEl() {
  var el = {
    _text: '',
    _html: '',
    value: '',
    checked: false,
    placeholder: '',
    className: '',
    title: '',
    style: {},
    addEventListener: function () {},
    removeEventListener: function () {},
    setAttribute: function () {},
    getAttribute: function () { return null; },
    classList: { add: function () {}, remove: function () {}, toggle: function () {}, contains: function () { return false; } },
    querySelector: function () { return null; },
    querySelectorAll: function () { return []; },
    closest: function () { return null; },
    appendChild: function () {},
  };
  Object.defineProperty(el, 'textContent', {
    get: function () { return el._text; },
    // escapeHtml() relies on real <div> behavior: setting textContent then reading innerHTML
    // back HTML-escapes &, <, > (not quotes) via the round trip through the parser/serializer.
    set: function (v) {
      el._text = v;
      el._html = String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    },
  });
  Object.defineProperty(el, 'innerHTML', {
    get: function () { return el._html; },
    set: function (v) { el._html = v; },
  });
  return el;
}

// tbody is the one element the assertions actually inspect: render()/fullReplace() write the
// whole sessions table into tbody.innerHTML as a single string. querySelectorAll('tr') stays
// stubbed to return [] - accurate for a first render, where nothing exists yet to preserve
// open-history UI state for.
var tbodyEl = dumbEl();

var document = {
  getElementById: function (id) { return id === 'tbody' ? tbodyEl : dumbEl(); },
  createElement: function () { return dumbEl(); },
  querySelectorAll: function () { return []; },
};
var CSS = {};
function fetch() { return Promise.reject(new Error('fetch not available in this test')); }
function EventSource() { return { addEventListener: function () {}, onerror: null }; }
function setInterval() { return 0; }
function alert() {}
function confirm() { return false; }

function assert(cond, msg) {
  if (!cond) throw new Error('ASSERTION FAILED: ' + msg);
}

/*__PGROLLBACK_SCRIPT__*/

// --- Assertions below run against the real script extracted from html.go, not a copy. ---

assert(typeof sessionKeys === 'function',
  'sessionKeys must be defined (regression: a past edit overwrote its "function sessionKeys(sessions) {" ' +
  'line, leaving its body as a bare top-level return - a syntax error that broke the entire page script)');
assert(sessionKeys([{ test_id: 'b' }, { test_id: 'a' }]) === 'a\nb',
  'sessionKeys must return sorted, newline-joined test IDs');

assert(typeof historyListMatches === 'function', 'historyListMatches must be defined');
assert(historyListMatches([], []) === true, 'two empty histories match');
assert(
  historyListMatches(
    [{ query: 'SELECT 1', at: 't1', duration: '', running: true }],
    [{ query: 'SELECT 1', at: 't1', duration: '1.2ms', running: false }]
  ) === false,
  'a query transitioning from Running to finished (same length, same query/at, different duration/running) must be detected as a change'
);

// Regression test for the FIFO-cap freeze: once history is capped at maxQueryHistory, the server
// drops the oldest entry and appends the newest, so length never changes again. A length-only
// diff (the old bug) treats this as "no change" forever and silently stops showing new queries.
(function () {
  var capped = [];
  var rolledOver = [];
  for (var i = 0; i < 100; i++) {
    capped.push({ query: 'q' + i, at: 't' + i, duration: '1ms', running: false });
    rolledOver.push({ query: 'q' + (i + 1), at: 't' + (i + 1), duration: '1ms', running: false });
  }
  assert(capped.length === rolledOver.length, 'test setup: both windows must be the same (capped) length');
  assert(historyListMatches(capped, rolledOver) === false,
    'a rolled-over FIFO window (same length, different content) must be detected as a change');
})();

// End-to-end-ish: run the real render() the page uses for both the initial snapshot and the SSE
// safety-net resync, and check the marker query text actually ends up in the rendered table HTML
// - i.e. the log message is shown, not just recorded server-side. Also carries a db/proxy duration
// split so this doubles as a check that the breakdown gets rendered into the query history line.
var marker = 'ui_visibility_marker_' + Date.now();
render([{
  test_id: 'ui_test_session',
  in_transaction: false,
  last_query: 'SELECT 1 AS ' + marker,
  last_query_duration: '1.234ms',
  running: false,
  query_history: [{
    query: 'SELECT 1 AS ' + marker, at: new Date().toISOString(),
    duration: '1.234ms', db_duration: '1.1ms', proxy_duration: '134µs', running: false,
  }],
}]);
assert(tbodyEl.innerHTML.indexOf(marker) !== -1,
  'rendered table HTML must contain the query that was logged (marker: ' + marker + ')');
assert(tbodyEl.innerHTML.indexOf('db') !== -1 && tbodyEl.innerHTML.indexOf('proxy') !== -1,
  'rendered table HTML must show the db/proxy duration breakdown when the entry carries one');

// --- Query duration color scale ---

assert(typeof durationStringToMs === 'function', 'durationStringToMs must be defined');
assert(durationStringToMs('142µs') === 0.142, 'durationStringToMs must parse µs (' + durationStringToMs('142µs') + ')');
assert(durationStringToMs('1.81ms') === 1.81, 'durationStringToMs must parse ms');
assert(Math.abs(durationStringToMs('10.0093244s') - 10009.3244) < 1e-6, 'durationStringToMs must parse s');
assert(durationStringToMs('1m0s') === 60000, 'durationStringToMs must treat compound (>=1m) Go durations as 60000ms');
assert(durationStringToMs('') === null, 'durationStringToMs must return null for empty input');

assert(typeof parseDurationToMs === 'function', 'parseDurationToMs must be defined');
assert(parseDurationToMs('500ms') === 500, 'parseDurationToMs must parse ms');
assert(parseDurationToMs('10s') === 10000, 'parseDurationToMs must parse s');
assert(parseDurationToMs('1m') === 60000, 'parseDurationToMs must parse m');
assert(isNaN(parseDurationToMs('bogus')), 'parseDurationToMs must reject unparseable input');

// durationColorState defaults to the "full range" preset since this Node shim has no
// localStorage. Read expected colors from DURATION_COLOR_PRESETS itself (defined in the real
// script, not re-declared here) so this test tracks html.go's actual stops instead of a second,
// hand-copied set of hex values that would silently drift out of sync.
assert(typeof colorForDurationMs === 'function', 'colorForDurationMs must be defined');
assert(typeof DURATION_COLOR_PRESETS === 'object' && Array.isArray(DURATION_COLOR_PRESETS.full),
  'DURATION_COLOR_PRESETS.full must be defined');
var fullStops = DURATION_COLOR_PRESETS.full;
var firstStop = fullStops[0];
var lastStop = fullStops[fullStops.length - 1];
assert(colorForDurationMs(firstStop.ms).toLowerCase() === firstStop.color.toLowerCase(),
  'colorForDurationMs must match the first stop\'s color exactly at its own ms');
assert(colorForDurationMs(lastStop.ms).toLowerCase() === lastStop.color.toLowerCase(),
  'colorForDurationMs must match the last stop\'s color exactly at its own ms');
assert(colorForDurationMs(lastStop.ms * 10).toLowerCase() === lastStop.color.toLowerCase(),
  'colorForDurationMs must clamp to the last stop\'s color beyond it');
(function () {
  var midMs = (firstStop.ms + lastStop.ms) / 2;
  var mid = colorForDurationMs(midMs);
  assert(typeof mid === 'string' && mid[0] === '#' && mid.length === 7, 'colorForDurationMs must return a hex color mid-scale');
  assert(mid.toLowerCase() !== firstStop.color.toLowerCase() && mid.toLowerCase() !== lastStop.color.toLowerCase(),
    'colorForDurationMs at the midpoint must differ from both endpoints');
})();
assert(DURATION_COLOR_PRESETS.full.length >= 2, '"full" preset must have at least 2 color stops');
assert(DURATION_COLOR_PRESETS.optimized.length >= 2, '"optimized" preset must have at least 2 color stops');

// durationOrRunningHtml must attach a scale color to a finished query's duration badge.
(function () {
  var html = durationOrRunningHtml(false, null, '1.234ms');
  assert(html.indexOf('style="color:') !== -1, 'finished duration badge must carry an inline scale color: ' + html);
})();

console.log('OK');

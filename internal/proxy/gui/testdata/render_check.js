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

console.log('OK');

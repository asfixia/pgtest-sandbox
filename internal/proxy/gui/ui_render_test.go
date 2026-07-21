package gui

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestGUIPageScript_RendersLoggedQuery is the regression test for two incidents where the GUI
// page silently stopped showing logged queries even though the backend was recording them
// correctly: (1) a paste error overwrote the "function sessionKeys(sessions) {" line, leaving its
// body as a top-level return - a JavaScript syntax error that broke the entire embedded <script>,
// so nothing on the page rendered or updated; (2) the history list's change detection compared
// only array length, which stops changing forever once server-side history hits its cap
// (maxQueryHistory) and starts dropping the oldest entry per new query.
//
// This runs the actual <script> extracted from HTML() (not a reimplementation) under Node against
// a small DOM shim (testdata/render_check.js), so a regression in either area fails this test the
// same way it broke the real page. Skips if node is not on PATH (same "skip on missing optional
// external dependency" pattern as the isPostgreSQLAvailable-gated integration tests elsewhere in
// this repo).
func TestGUIPageScript_RendersLoggedQuery(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Skipping - node not found on PATH")
	}

	script := extractScript(t, HTML())

	shimTemplate, err := os.ReadFile(filepath.Join("testdata", "render_check.js"))
	if err != nil {
		t.Fatalf("read testdata/render_check.js: %v", err)
	}
	const placeholder = "/*__PGROLLBACK_SCRIPT__*/"
	if !strings.Contains(string(shimTemplate), placeholder) {
		t.Fatalf("testdata/render_check.js missing %s placeholder", placeholder)
	}
	combined := strings.Replace(string(shimTemplate), placeholder, script, 1)

	dir := t.TempDir()
	combinedPath := filepath.Join(dir, "combined.js")
	if err := os.WriteFile(combinedPath, []byte(combined), 0o644); err != nil {
		t.Fatalf("write combined script: %v", err)
	}

	out, err := exec.Command(nodePath, combinedPath).CombinedOutput()
	if err != nil {
		t.Fatalf("GUI page script failed under Node:\n%s", out)
	}
	if !strings.Contains(string(out), "OK") {
		t.Fatalf("expected script to print OK, got:\n%s", out)
	}
}

// extractScript pulls the contents of the page's single <script>...</script> block out of the
// full HTML document returned by HTML().
func extractScript(t *testing.T, html string) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)<script>(.*?)</script>`)
	m := re.FindStringSubmatch(html)
	if m == nil {
		t.Fatal("no <script> block found in HTML()")
	}
	return m[1]
}

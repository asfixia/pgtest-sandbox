package proxy

import "pgrollback/internal/proxy/gui"

// SetCachedLockStatus stores the latest known lock status for GUI reads. Called only from live
// lookups (lock_inspector.go), never from the query hot path.
func (g *guiState) SetCachedLockStatus(st *gui.LockStatus) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lockStatus = st
}

// CachedLockStatus returns the latest known lock status without touching Postgres (nil if never
// computed or currently not waiting on a lock).
func (g *guiState) CachedLockStatus() *gui.LockStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lockStatus
}

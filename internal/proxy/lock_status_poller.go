package proxy

import (
	"time"

	"pgrollback/internal/proxy/gui"
)

// lockStatusPollInterval is how often the background poller refreshes LockStatus for all
// sessions from Postgres. This is the only place that does so; keeping it off the
// query-execution and session-creation paths is the point (see sessionInfoWithoutLockStatus).
const lockStatusPollInterval = 1 * time.Second

// startLockStatusPoller launches the background refresh goroutine. Safe to call more than once;
// only the first call has any effect.
func (p *PgRollback) startLockStatusPoller() {
	p.lockPollerOnce.Do(func() {
		p.lockPollerStop = make(chan struct{})
		go p.runLockStatusPoller()
	})
}

// StopLockStatusPoller stops the background poller. Safe to call even if it was never started,
// and safe to call concurrently or more than once.
func (p *PgRollback) StopLockStatusPoller() {
	p.startLockStatusPoller() // ensures lockPollerStop is initialized before we might close it
	p.lockPollerStopOnce.Do(func() {
		close(p.lockPollerStop)
	})
}

func (p *PgRollback) runLockStatusPoller() {
	ticker := time.NewTicker(lockStatusPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.lockPollerStop:
			return
		case <-ticker.C:
			p.pollLockStatusOnce()
		}
	}
}

// pollLockStatusOnce refreshes LockStatus for every current session with one batched round trip
// (enrichSessionsLockStatus, which also warms each session's cache) and pushes the result to GUI
// subscribers. No-op when there are no sessions, so an idle proxy never opens the inspector
// connection.
func (p *PgRollback) pollLockStatusOnce() {
	sessions := p.GetAllSessions()
	if len(sessions) == 0 {
		return
	}
	list := make([]gui.SessionInfo, 0, len(sessions))
	for testID := range sessions {
		list = append(list, gui.SessionInfo{TestID: testID})
	}
	p.enrichSessionsLockStatus(list)
	p.PublishSnapshot()
}

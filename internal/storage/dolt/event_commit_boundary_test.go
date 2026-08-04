package dolt

import (
	"errors"
	"testing"
)

func TestAddCommentSQLCommitResponseLossAccountsCircuitOnce(t *testing.T) {
	t.Setenv("BEADS_TEST_MODE", "")
	breaker := newTestCircuitBreaker(t)
	driver := &claimCommitBoundaryDriver{sqlCommitErr: testConnectionLoss}
	store := newClaimCommitBoundaryStore(driver)
	store.breaker = breaker
	t.Cleanup(func() { _ = store.db.Close() })

	err := store.AddComment(t.Context(), "comment-boundary", "alice", "hello")
	if !errors.Is(err, ErrCommitIndeterminate) {
		t.Fatalf("AddComment() error = %v, want ErrCommitIndeterminate", err)
	}
	if !errors.Is(err, testConnectionLoss) {
		t.Fatalf("AddComment() error = %v, want cause %v", err, testConnectionLoss)
	}

	driver.mu.Lock()
	if driver.eventInserts != 1 || driver.txAttempts != 1 || driver.txCommits != 1 {
		driver.mu.Unlock()
		t.Fatalf("AddComment attempts = events:%d transactions:%d commits:%d, want 1, 1, 1",
			driver.eventInserts, driver.txAttempts, driver.txCommits)
	}
	driver.mu.Unlock()

	state := breaker.readState()
	if state.State != circuitClosed || state.Failures != 1 {
		t.Fatalf("circuit state after one lost response = %+v, want closed with one failure", state)
	}
}

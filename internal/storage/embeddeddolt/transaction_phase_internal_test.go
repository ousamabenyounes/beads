//go:build cgo

package embeddeddolt

import (
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
)

func TestTransactionCleanupAfterSQLCommitIsIndeterminate(t *testing.T) {
	cleanupErr := errors.New("connection cleanup failed")
	err := joinTransactionCleanupError(nil, cleanupErr, true)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("error = %v, want cleanup cause", err)
	}
	if !errors.Is(err, storage.ErrCommitIndeterminate) {
		t.Fatalf("error = %v, want ErrCommitIndeterminate", err)
	}
}

func TestTransactionCleanupBeforeSQLCommitRemainsDefinite(t *testing.T) {
	callbackErr := errors.New("callback failed")
	cleanupErr := errors.New("connection cleanup failed")
	err := joinTransactionCleanupError(callbackErr, cleanupErr, false)
	if !errors.Is(err, callbackErr) {
		t.Fatalf("error = %v, want callback cause", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("error = %v, want cleanup cause", err)
	}
	if errors.Is(err, storage.ErrCommitIndeterminate) {
		t.Fatalf("error = %v must remain definite before SQL commit", err)
	}
}

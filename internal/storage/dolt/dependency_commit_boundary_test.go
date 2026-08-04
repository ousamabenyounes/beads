package dolt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// dependencyCommitBoundaryDriver models dependency removal through its SQL
// mutation and transaction commit. A lost commit response is returned after
// the delete so an unsafe replay is visible as a second mutation attempt.
type dependencyCommitBoundaryDriver struct {
	mu sync.Mutex

	activeWisp bool
	commitErr  error

	deletes     int
	txAttempts  int
	commitCalls int
}

func (d *dependencyCommitBoundaryDriver) Open(string) (driver.Conn, error) {
	return &dependencyCommitBoundaryConn{driver: d}, nil
}

func (d *dependencyCommitBoundaryDriver) Connect(context.Context) (driver.Conn, error) {
	return &dependencyCommitBoundaryConn{driver: d}, nil
}

func (d *dependencyCommitBoundaryDriver) Driver() driver.Driver { return d }

type dependencyCommitBoundaryConn struct {
	driver *dependencyCommitBoundaryDriver
}

func (c *dependencyCommitBoundaryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("dependency commit boundary driver does not prepare statements")
}

func (c *dependencyCommitBoundaryConn) Close() error { return nil }

func (c *dependencyCommitBoundaryConn) Begin() (driver.Tx, error) {
	c.driver.mu.Lock()
	c.driver.txAttempts++
	c.driver.mu.Unlock()
	return &dependencyCommitBoundaryTx{driver: c.driver}, nil
}

func (c *dependencyCommitBoundaryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()

	switch {
	case strings.Contains(query, "SELECT 1 FROM wisps WHERE id = ? LIMIT 1"):
		if c.driver.activeWisp {
			return &dependencyCommitBoundaryRows{columns: []string{"exists"}, values: [][]driver.Value{{int64(1)}}}, nil
		}
		return &dependencyCommitBoundaryRows{columns: []string{"exists"}}, nil
	case strings.Contains(query, "SELECT type FROM dependencies"),
		strings.Contains(query, "SELECT type FROM wisp_dependencies"):
		return &dependencyCommitBoundaryRows{columns: []string{"type"}, values: [][]driver.Value{{"related"}}}, nil
	default:
		return &dependencyCommitBoundaryRows{columns: []string{"value"}}, nil
	}
}

func (c *dependencyCommitBoundaryConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	if strings.Contains(query, "DELETE FROM dependencies") || strings.Contains(query, "DELETE FROM wisp_dependencies") {
		c.driver.deletes++
	}
	return driver.RowsAffected(1), nil
}

type dependencyCommitBoundaryTx struct {
	driver *dependencyCommitBoundaryDriver
}

func (t *dependencyCommitBoundaryTx) Commit() error {
	t.driver.mu.Lock()
	defer t.driver.mu.Unlock()
	t.driver.commitCalls++
	return t.driver.commitErr
}

func (t *dependencyCommitBoundaryTx) Rollback() error { return nil }

type dependencyCommitBoundaryRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *dependencyCommitBoundaryRows) Columns() []string { return r.columns }
func (r *dependencyCommitBoundaryRows) Close() error      { return nil }
func (r *dependencyCommitBoundaryRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func TestRemoveDependencySQLCommitResponseLossIsIndeterminateAndNotReplayed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		activeWisp bool
	}{
		{name: "regular"},
		{name: "wisp", activeWisp: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BEADS_TEST_MODE", "")
			breaker := newTestCircuitBreaker(t)
			driver := &dependencyCommitBoundaryDriver{
				activeWisp: tc.activeWisp,
				commitErr:  testConnectionLoss,
			}
			store := &DoltStore{db: sql.OpenDB(driver), breaker: breaker}
			t.Cleanup(func() { _ = store.db.Close() })

			err := store.RemoveDependency(context.Background(), "dependency-source", "dependency-target", "alice")
			if !errors.Is(err, ErrCommitIndeterminate) {
				t.Fatalf("RemoveDependency() error = %v, want ErrCommitIndeterminate", err)
			}
			if !errors.Is(err, testConnectionLoss) {
				t.Fatalf("RemoveDependency() error = %v, want cause %v", err, testConnectionLoss)
			}

			driver.mu.Lock()
			defer driver.mu.Unlock()
			if driver.deletes != 1 {
				t.Fatalf("dependency delete attempts = %d, want 1", driver.deletes)
			}
			if driver.txAttempts != 1 || driver.commitCalls != 1 {
				t.Fatalf("transaction attempts = %d, commit calls = %d, want 1 and 1", driver.txAttempts, driver.commitCalls)
			}

			state := breaker.readState()
			if state.State != circuitClosed || state.Failures != 1 {
				t.Fatalf("circuit state after one lost response = %+v, want closed with one failure", state)
			}
		})
	}
}

var _ driver.Connector = (*dependencyCommitBoundaryDriver)(nil)
var _ driver.ExecerContext = (*dependencyCommitBoundaryConn)(nil)
var _ driver.QueryerContext = (*dependencyCommitBoundaryConn)(nil)

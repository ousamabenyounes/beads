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

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

// claimCommitBoundaryDriver models a permanent claim through its SQL mutation,
// DOLT_ADD, and DOLT_COMMIT phases. It deliberately keeps the issue open after
// rollback so an unsafe retry is visible as a second claim mutation.
type claimCommitBoundaryDriver struct {
	mu sync.Mutex

	stageErr        error
	commitErr       error
	sqlCommitErr    error
	nothingToCommit bool
	checkedUpdate   bool
	verifyAssignee  string
	verifyStatus    types.Status
	activeWisp      bool

	claimMutations  int
	claimedIDs      []string
	updateMutations int
	eventInserts    int
	claimStateReads int
	stageCalls      int
	doltCommits     int
	txCommits       int
	txRollbacks     int
	txAttempts      int
	activeID        string
	readyIDs        []string
}

func (d *claimCommitBoundaryDriver) Open(string) (driver.Conn, error) {
	return &claimCommitBoundaryConn{driver: d}, nil
}

func (d *claimCommitBoundaryDriver) Connect(context.Context) (driver.Conn, error) {
	return &claimCommitBoundaryConn{driver: d}, nil
}

func (d *claimCommitBoundaryDriver) Driver() driver.Driver { return d }

type claimCommitBoundaryConn struct {
	driver *claimCommitBoundaryDriver
}

func (c *claimCommitBoundaryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("claim commit boundary driver does not prepare statements")
}

func (c *claimCommitBoundaryConn) Close() error { return nil }

func (c *claimCommitBoundaryConn) Begin() (driver.Tx, error) {
	c.driver.mu.Lock()
	c.driver.txAttempts++
	c.driver.activeID = "claim-boundary"
	if len(c.driver.readyIDs) > 0 {
		index := c.driver.txAttempts - 1
		if index >= len(c.driver.readyIDs) {
			index = len(c.driver.readyIDs) - 1
		}
		c.driver.activeID = c.driver.readyIDs[index]
	}
	c.driver.mu.Unlock()
	return &claimCommitBoundaryTx{driver: c.driver}, nil
}

func (c *claimCommitBoundaryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()

	switch {
	case strings.Contains(query, "SELECT 1 FROM wisps WHERE id = ? LIMIT 1"):
		if c.driver.activeWisp {
			return &claimCommitBoundaryRows{columns: []string{"exists"}, values: [][]driver.Value{{int64(1)}}}, nil
		}
		return &claimCommitBoundaryRows{columns: []string{"exists"}}, nil
	case strings.Contains(query, "SELECT assignee, status FROM issues WHERE id = ?"):
		c.driver.claimStateReads++
		assignee := ""
		status := types.StatusOpen
		if c.driver.checkedUpdate && c.driver.claimStateReads > 1 {
			assignee = c.driver.verifyAssignee
			status = c.driver.verifyStatus
		}
		return &claimCommitBoundaryRows{
			columns: []string{"assignee", "status"},
			values:  [][]driver.Value{{assignee, string(status)}},
		}, nil
	case strings.Contains(query, "SELECT id FROM issues"):
		return &claimCommitBoundaryRows{
			columns: []string{"id"},
			values:  [][]driver.Value{{c.driver.activeID}},
		}, nil
	case strings.Contains(query, "FROM issues") && strings.Contains(query, "LEFT JOIN leases") && strings.Contains(query, "WHERE id IN ("):
		return &claimCommitBoundaryRows{
			columns: claimBoundaryIssueColumns(),
			values:  [][]driver.Value{claimBoundaryIssueValues(c.driver.activeID)},
		}, nil
	case strings.Contains(query, "FROM issues") && strings.Contains(query, "LEFT JOIN leases") && strings.Contains(query, "WHERE id = ?"):
		return &claimCommitBoundaryRows{
			columns: claimBoundaryIssueColumns(),
			values:  [][]driver.Value{claimBoundaryIssueValues(c.driver.activeID)},
		}, nil
	case strings.Contains(query, "SELECT label FROM labels"):
		return &claimCommitBoundaryRows{columns: []string{"label"}}, nil
	case strings.Contains(query, "SELECT issue_id, label FROM labels"):
		return &claimCommitBoundaryRows{columns: []string{"issue_id", "label"}}, nil
	case strings.Contains(query, "SELECT name, category FROM custom_statuses"):
		return &claimCommitBoundaryRows{columns: []string{"name", "category"}}, nil
	case strings.Contains(query, "SELECT value FROM config"):
		return &claimCommitBoundaryRows{columns: []string{"value"}}, nil
	case strings.Contains(query, "CALL DOLT_ADD"):
		c.driver.stageCalls++
		if c.driver.stageErr != nil {
			return nil, c.driver.stageErr
		}
		return &claimCommitBoundaryRows{columns: []string{"status"}}, nil
	case strings.Contains(query, "CALL DOLT_COMMIT"):
		c.driver.doltCommits++
		if c.driver.commitErr != nil && c.driver.doltCommits == 1 {
			return nil, c.driver.commitErr
		}
		if c.driver.nothingToCommit {
			return nil, errors.New("nothing to commit")
		}
		return &claimCommitBoundaryRows{columns: []string{"hash"}}, nil
	default:
		return &claimCommitBoundaryRows{columns: []string{"value"}}, nil
	}
}

func (c *claimCommitBoundaryConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()
	if strings.Contains(query, "UPDATE issues") && strings.Contains(query, "SET assignee") {
		c.driver.claimMutations++
		c.driver.claimedIDs = append(c.driver.claimedIDs, c.driver.activeID)
	}
	if strings.Contains(query, "UPDATE issues") && strings.Contains(query, "`title`") {
		c.driver.updateMutations++
	}
	if strings.Contains(query, "INSERT INTO events") {
		c.driver.eventInserts++
	}
	return driver.RowsAffected(1), nil
}

type claimCommitBoundaryTx struct {
	driver *claimCommitBoundaryDriver
}

func (t *claimCommitBoundaryTx) Commit() error {
	t.driver.mu.Lock()
	defer t.driver.mu.Unlock()
	t.driver.txCommits++
	return t.driver.sqlCommitErr
}

func (t *claimCommitBoundaryTx) Rollback() error {
	t.driver.mu.Lock()
	defer t.driver.mu.Unlock()
	t.driver.txRollbacks++
	return nil
}

type claimCommitBoundaryRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *claimCommitBoundaryRows) Columns() []string { return r.columns }
func (r *claimCommitBoundaryRows) Close() error      { return nil }
func (r *claimCommitBoundaryRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func claimBoundaryIssueColumns() []string {
	parts := strings.Split(strings.ReplaceAll(issueops.IssueSelectColumns, "\n", " "), ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func claimBoundaryIssueValues(id string) []driver.Value {
	values := make([]driver.Value, 0, len(claimBoundaryIssueColumns()))
	for _, column := range claimBoundaryIssueColumns() {
		switch column {
		case "id":
			values = append(values, id)
		case "title":
			values = append(values, "claim boundary")
		case "description", "design", "acceptance_criteria", "notes":
			values = append(values, "")
		case "status":
			values = append(values, string(types.StatusOpen))
		case "priority":
			values = append(values, int64(2))
		case "issue_type":
			values = append(values, string(types.TypeTask))
		case "compaction_level":
			values = append(values, int64(0))
		default:
			values = append(values, nil)
		}
	}
	return values
}

func newClaimCommitBoundaryStore(d *claimCommitBoundaryDriver) *DoltStore {
	return &DoltStore{db: sql.OpenDB(d)}
}

func TestClaimIssueDoltCommitResponseLossIsIndeterminateAndNotReplayed(t *testing.T) {
	driver := &claimCommitBoundaryDriver{commitErr: testConnectionLoss}
	store := newClaimCommitBoundaryStore(driver)
	t.Cleanup(func() { _ = store.db.Close() })

	err := store.ClaimIssue(context.Background(), "claim-boundary", "alice")
	if !errors.Is(err, ErrCommitIndeterminate) {
		t.Fatalf("ClaimIssue() error = %v, want ErrCommitIndeterminate", err)
	}
	if !errors.Is(err, testConnectionLoss) {
		t.Fatalf("ClaimIssue() error = %v, want cause %v", err, testConnectionLoss)
	}

	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.claimMutations != 1 {
		t.Fatalf("claim mutations = %d, want 1", driver.claimMutations)
	}
	if driver.doltCommits != 1 {
		t.Fatalf("DOLT_COMMIT calls = %d, want 1", driver.doltCommits)
	}
	if driver.txCommits != 0 || driver.txRollbacks != 1 {
		t.Fatalf("SQL transaction outcomes = commits:%d rollbacks:%d, want commits:0 rollbacks:1", driver.txCommits, driver.txRollbacks)
	}
}

func TestClaimIssueDoltAddFailureCannotReportSuccess(t *testing.T) {
	stageErr := errors.New("stage failed")
	driver := &claimCommitBoundaryDriver{stageErr: stageErr, nothingToCommit: true}
	store := newClaimCommitBoundaryStore(driver)
	t.Cleanup(func() { _ = store.db.Close() })

	err := store.ClaimIssue(context.Background(), "claim-boundary", "alice")
	if !errors.Is(err, stageErr) {
		t.Fatalf("ClaimIssue() error = %v, want stage failure %v", err, stageErr)
	}

	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.stageCalls != 1 {
		t.Fatalf("DOLT_ADD calls = %d, want 1", driver.stageCalls)
	}
	if driver.doltCommits != 0 {
		t.Fatalf("DOLT_COMMIT calls = %d, want 0 after staging failure", driver.doltCommits)
	}
	if driver.txCommits != 0 || driver.txRollbacks != 1 {
		t.Fatalf("SQL transaction outcomes = commits:%d rollbacks:%d, want commits:0 rollbacks:1", driver.txCommits, driver.txRollbacks)
	}
}

func TestClaimReadyIssueDoltCommitResponseLossDoesNotDoubleClaim(t *testing.T) {
	driver := &claimCommitBoundaryDriver{
		commitErr: testConnectionLoss,
		readyIDs:  []string{"ready-first", "ready-second"},
	}
	store := newClaimCommitBoundaryStore(driver)
	t.Cleanup(func() { _ = store.db.Close() })

	claimed, err := store.ClaimReadyIssue(context.Background(), types.WorkFilter{}, "alice")
	if !errors.Is(err, ErrCommitIndeterminate) {
		t.Fatalf("ClaimReadyIssue() error = %v, want ErrCommitIndeterminate", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimReadyIssue() claimed = %+v, want nil while commit outcome is indeterminate", claimed)
	}

	driver.mu.Lock()
	defer driver.mu.Unlock()
	if got, want := strings.Join(driver.claimedIDs, ","), "ready-first"; got != want {
		t.Fatalf("claimed IDs = %q, want %q (no replay onto another ready issue)", got, want)
	}
	if driver.txAttempts != 1 {
		t.Fatalf("transaction attempts = %d, want 1", driver.txAttempts)
	}
	if driver.doltCommits != 1 {
		t.Fatalf("DOLT_COMMIT calls = %d, want 1", driver.doltCommits)
	}
}

func TestUpdateIssueCheckedMixedCoordinationCommitLossIsNotMasked(t *testing.T) {
	driver := &claimCommitBoundaryDriver{
		commitErr:      testConnectionLoss,
		checkedUpdate:  true,
		verifyAssignee: "alice",
		verifyStatus:   types.StatusInProgress,
	}
	store := newClaimCommitBoundaryStore(driver)
	store.serverMode = true
	t.Cleanup(func() { _ = store.db.Close() })

	expectedAssignee := ""
	expectedStatus := string(types.StatusOpen)
	err := store.UpdateIssueChecked(context.Background(), "claim-boundary", map[string]interface{}{
		"assignee": "alice",
		"status":   string(types.StatusInProgress),
		"title":    "ordinary field must not be masked",
	}, "alice", storage.UpdateIssueOptions{
		ExpectedAssignee: &expectedAssignee,
		ExpectedStatus:   &expectedStatus,
	})
	if !errors.Is(err, ErrCommitIndeterminate) {
		t.Fatalf("UpdateIssueChecked() error = %v, want ErrCommitIndeterminate", err)
	}
	if !errors.Is(err, testConnectionLoss) {
		t.Fatalf("UpdateIssueChecked() error = %v, want cause %v", err, testConnectionLoss)
	}

	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.updateMutations != 1 || driver.eventInserts != 1 {
		t.Fatalf("mixed update attempts = updates:%d events:%d, want updates:1 events:1", driver.updateMutations, driver.eventInserts)
	}
	if driver.txAttempts != 1 || driver.doltCommits != 1 || driver.txRollbacks != 1 {
		t.Fatalf("transaction outcomes = attempts:%d Dolt commits:%d rollbacks:%d, want 1, 1, 1", driver.txAttempts, driver.doltCommits, driver.txRollbacks)
	}
}

var _ driver.Connector = (*claimCommitBoundaryDriver)(nil)
var _ driver.ExecerContext = (*claimCommitBoundaryConn)(nil)
var _ driver.QueryerContext = (*claimCommitBoundaryConn)(nil)

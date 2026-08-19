package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
)

// Tx is a cool MySQL transaction
type Tx struct {
	db *Database

	Tx   *sql.Tx
	Time time.Time

	PostCommitHooks   []func() error
	PostRollbackHooks []func()

	// PostAbandonHooks fire every time this Tx ends without a durable COMMIT:
	// a RunInTx retried-away attempt (in-tx deadlock or retryable COMMIT
	// failure), the final failed attempt, and Cancel() on an uncommitted tx.
	// Unlike PostRollbackHooks — which fire only for the transaction's final
	// outcome, and only after a confirmed rollback — these are per-attempt
	// and are not gated on rollback succeeding. Once the attempt is over
	// without a durable commit the driver tx is dead server-side either way
	// (a failed COMMIT already ended it; the later raw rollback reports
	// sql.ErrTxDone), so externally-held per-attempt resources such as
	// distributed locks are both safe and necessary to release. A replayed
	// RunInTx closure runs on a fresh Tx and re-acquires them. Never fires
	// after a successful COMMIT, and fires at most once per Tx even when
	// multiple ending paths run (a Cancel inside a RunInTx closure, a
	// repeated Cancel).
	PostAbandonHooks []func()

	// Values is per-transaction storage for tx-scoped state — collectors that
	// batch work into a PostCommitHook, for example. RunInTx's final-outcome
	// hooks never fire for a retried-away attempt, so a package-level map
	// keyed by *Tx would leak an entry per abandoned attempt. Storage on the
	// Tx itself is released with the abandoned Tx by the garbage collector;
	// no cleanup path is needed. Per-attempt resources that must be released
	// before a replay (locks) belong in PostAbandonHooks.
	Values sync.Map

	// committed is set when commit() observes a nil from Tx.Commit(), so the
	// non-commit ending paths (RunInTx's per-attempt defer, Cancel) know a
	// durable commit happened even when the closure committed the tx itself.
	// abandoned makes runPostAbandonHooks once-only when multiple ending
	// paths run for the same Tx. Single-goroutine use; no locking.
	committed bool
	abandoned bool
}

// runPostAbandonHooks fires PostAbandonHooks at most once per Tx, and never
// once the tx durably committed. Both ending paths (RunInTx's per-attempt
// defer and Cancel) route through here so a Cancel inside a RunInTx closure,
// or a repeated Cancel, can't double-fire non-idempotent cleanup.
func (tx *Tx) runPostAbandonHooks() {
	if tx.committed || tx.abandoned {
		return
	}
	tx.abandoned = true
	for _, hook := range tx.PostAbandonHooks {
		hook()
	}
}

type txCancelFunc func() error

func (db *Database) beginTx(conn *sql.DB, ctx context.Context) (*Tx, txCancelFunc, error) {
	start := time.Now()

	t, err := conn.BeginTx(ctx, nil)
	tx := &Tx{
		db:   db,
		Tx:   t,
		Time: time.Now(),
	}

	db.callLog(LogDetail{
		Query:    "start transaction",
		Duration: time.Since(start),
		Tx:       tx.Tx,
		Attempt:  1,
		Error:    err,
	})
	if err != nil {
		return nil, tx.Cancel, err
	}

	return tx, tx.Cancel, nil
}

// BeginTx begins and returns a new transaction on the writes connection
func (db *Database) BeginTx() (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Writes.(*sql.DB), context.Background())
}

// BeginTxContext begins and returns a new transaction on the writes connection
func (db *Database) BeginTxContext(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Writes.(*sql.DB), ctx)
}

// BeginReadsTx begins and returns a new transaction on the writes connection
func (db *Database) BeginReadsTx() (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Reads, context.Background())
}

// BeginReadsTxContext begins and returns a new transaction on the reads connection
func (db *Database) BeginReadsTxContext(ctx context.Context) (tx *Tx, cancel func() error, err error) {
	return db.beginTx(db.Reads, ctx)
}

// RunInTx runs fn inside a transaction it owns, retrying the WHOLE closure from a
// fresh Begin when fn fails with a deadlock (1213, SQLSTATE 40001) or a lock-wait
// timeout (1205). Because the entire unit of work — the caller's Go logic and
// every statement it issues — lives inside fn, re-running it is atomic and
// correct, unlike the statement-level replay removed in #167.
//
// RunInTx puts the transaction it begins into the context passed to fn, so
// TxOrDatabaseFromContext / GetOrCreateTxFromContext inside fn (and any helpers
// fn calls) transparently reuse it. On success it commits; on failure it rolls
// back. PostCommitHooks / PostRollbackHooks set on the in-context tx fire exactly
// once, for the final outcome: a retried-away attempt's rollback hooks never
// fire, so they cannot prematurely undo work that is about to be redone.
// PostAbandonHooks fire per attempt whenever that attempt's Tx ends without a
// durable COMMIT, so per-attempt external resources can be released before a
// replay.
//
// If a transaction already exists in ctx, RunInTx does NOT begin a new one or
// retry: it runs fn exactly once on the existing tx and lets any deadlock
// propagate to whoever owns the outermost transaction — only that owner can
// safely restart from Begin. Nesting is therefore a transparent pass-through;
// only the outermost RunInTx retries.
//
// Retries are bounded by the package-level MaxAttempts (total tries, when >0)
// and an elapsed-time budget — db.retryElapsedBudget(ctx): the ctx deadline
// when set, else db.MaxExecutionTime — with exponential backoff, mirroring
// exec/select/exists. Unlike those, an attempt's wall-clock time spent
// genuinely blocked on innodb_lock_wait_timeout (MySQL error 1205) is excused
// from that budget, up to MaxExcusedLockWaits attempts: that time isn't retry
// effort, it's the unavoidable cost of correctly waiting for someone else's
// transaction to release a lock, and charging it against the same budget that
// bounds real retries made a genuine prod 1205 (lock-wait timeout 50s,
// MaxExecutionTime 27s) structurally un-retriable — the whole budget was
// always spent inside the first attempt's own block (#7829). Deadlocks (1213)
// get the mirror-image guarantee at the budget gate: their attempt time is
// real work and is charged in full, but once the budget is exhausted a
// deadlock is still granted up to MaxExcusedDeadlocks replays — otherwise a
// closure whose single attempt outruns the budget under load would surface
// its first 1213 with zero replays, structurally un-retried. A ctx deadline
// is never excused by either mechanism: it is an absolute ceiling on the
// caller's behalf, and ctx cancellation already interrupts a blocked attempt
// on its own (see gateRetry below).
func (db *Database) RunInTx(ctx context.Context, fn func(ctx context.Context) error) error {
	// A tx already in ctx means we're nested inside a unit of work we don't own.
	// Run fn once on the existing tx and let any deadlock propagate to the
	// outermost RunInTx — the only layer that can correctly restart from Begin.
	if TxFromContext(ctx) != nil {
		return fn(ctx)
	}

	b := backoff.NewExponentialBackOff()
	// lastTx is the most recent transaction we began; lastRolledBack records
	// whether its raw rollback actually rolled the tx back (Rollback() == nil).
	// Together they drive the final-outcome rollback-hook firing after the
	// retry loop settles, so hooks fire once, only on a confirmed rollback,
	// and never on a committed tx (tx.committed, set by commit(), covers the
	// durable-commit case — including a closure committing the tx itself).
	var lastTx *Tx
	var lastRolledBack bool

	// budget is the total elapsed-time allowance for this RunInTx call: the
	// ctx deadline when the caller set one (never excused — see gateRetry),
	// else db.MaxExecutionTime (the historical, excusable budget). Computed
	// once, matching backoff's own startedAt-relative accounting style.
	budget := db.retryElapsedBudget(ctx)
	_, hasDeadline := ctx.Deadline()
	var chargeable time.Duration
	var excusedLockWaits int
	var excusedDeadlocks int

	// gateRetry decides whether a retryable tx-fatal error (checkTxRetryError
	// == true) gets another attempt or becomes terminal. It replaces passing
	// db.MaxExecutionTime to backoff.WithMaxElapsedTime: that option gates on
	// RAW wall-clock time since backoff.Retry's own internal loop start
	// (cenkalti/backoff/v5 retry.go), which always includes time an attempt
	// spent blocked in a lock wait — making the excuse below impossible to
	// implement through backoff's own accounting. gateRetry is the sole
	// elapsed-time authority; RunInTx passes backoff.WithMaxElapsedTime(0)
	// (uncapped) below so backoff's internal gate never independently fires.
	gateRetry := func(attemptStart time.Time, err error) error {
		elapsed := time.Since(attemptStart)
		if !hasDeadline && isLockWaitTimeout(err) && excusedLockWaits < MaxExcusedLockWaits {
			excusedLockWaits++
			return err
		}
		chargeable += elapsed
		if budget > 0 && chargeable > budget {
			// A deadlock's own kill is near-instant, so unlike a 1205 there is
			// no blocked time to excuse — but when the closure's real work
			// alone outruns the budget (a slow attempt under load), a 1213 on
			// the first attempt would surface with ZERO replays, making it
			// structurally un-retriable exactly like the pre-#7829 1205. So
			// past-budget deadlocks (and 40001 vendor variants — anything here
			// that isn't a 1205) are still granted up to MaxExcusedDeadlocks
			// replays before the budget gate turns terminal. Exhausted-excuse
			// 1205s falling through to here do NOT get a second excuse pool:
			// their cap already bounded a permanently stuck lock, and stacking
			// the two would double the worst case.
			if !hasDeadline && !isLockWaitTimeout(err) && excusedDeadlocks < MaxExcusedDeadlocks {
				excusedDeadlocks++
				return err
			}
			return backoff.Permanent(err)
		}
		return err
	}

	operation := func() (struct{}, error) {
		// Reset both first so that if this attempt's Begin fails, the previous
		// (already retried-away) attempt's tx can't be mistaken for the final
		// outcome and fire its rollback hooks below.
		lastTx = nil
		lastRolledBack = false

		attemptStart := time.Now()

		tx, _, err := db.BeginTxContext(ctx)
		if err != nil {
			// Begin failures are not the in-tx deadlock RunInTx exists to retry;
			// surface them immediately.
			return struct{}{}, backoff.Permanent(err)
		}
		lastTx = tx

		// Belt-and-suspenders rollback: on any non-commit exit (including a panic
		// in fn) this releases the connection. It is a raw rollback that does NOT
		// fire PostRollbackHooks — those are owned by the final-outcome handling
		// below so a retried attempt's hooks never fire. lastRolledBack records
		// whether it was a confirmed rollback: a failed COMMIT already marked the
		// driver tx done, so this returns sql.ErrTxDone and no rollback hooks
		// fire (matching Cancel()/#139). A no-op once the tx durably committed.
		defer func() {
			if !tx.committed {
				lastRolledBack = tx.Tx.Rollback() == nil
				// PostAbandonHooks run synchronously in this attempt's defer,
				// before backoff schedules the next attempt, so a replay can
				// never race its own release. Not gated on lastRolledBack: a
				// failed retryable COMMIT leaves sql.ErrTxDone yet still
				// needs release before the replay. Once-only via tx.abandoned
				// (a Cancel inside fn already fired them).
				tx.runPostAbandonHooks()
			}
		}()

		if err := fn(NewContextWithTx(ctx, tx)); err != nil {
			// A closure that durably committed via the exposed Tx.Commit must
			// never be replayed, even when its error happens to wrap a
			// deadlock code (e.g. a post-commit hook's failure) — a replay
			// would duplicate the committed writes.
			if !tx.committed && checkTxRetryError(err) {
				return struct{}{}, gateRetry(attemptStart, err)
			}
			return struct{}{}, backoff.Permanent(err)
		}

		// Commit in two steps so a post-commit-hook failure is never mistaken for
		// a transaction failure. A driver COMMIT failure leaves the tx not durable
		// and is retryable on a deadlock; once the COMMIT succeeds the tx is
		// durable and must never be retried (that would duplicate the committed
		// writes) or rolled back, even if a hook error happens to wrap a deadlock
		// code.
		if err := tx.commit(); err != nil {
			if checkTxRetryError(err) {
				return struct{}{}, gateRetry(attemptStart, err)
			}
			return struct{}{}, backoff.Permanent(err)
		}

		if err := tx.runPostCommitHooks(); err != nil {
			return struct{}{}, backoff.Permanent(err)
		}

		return struct{}{}, nil
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(b),
		// gateRetry above owns all elapsed-time accounting (#7829); backoff's
		// own MaxElapsedTime gate uses raw wall-clock time that always
		// includes lock-wait-blocked time, which would defeat the excuse.
		// ctx cancellation (checked by backoff.Retry itself) still bounds a
		// caller-supplied deadline.
		backoff.WithMaxElapsedTime(0),
	}
	if MaxAttempts > 0 {
		options = append(options, backoff.WithMaxTries(uint(MaxAttempts)))
	}

	_, err := backoff.Retry(ctx, operation, options...)
	if err != nil {
		// Fire the final attempt's rollback hooks once — but only for a CONFIRMED
		// rollback, mirroring Cancel()/#139. A durable commit whose post-commit
		// hook failed (committed, never rolled back) and a failed/ambiguous COMMIT
		// (rollback returned sql.ErrTxDone) both leave lastRolledBack false, so
		// their rollback hooks correctly do not fire. unwrapBackoffPermanent
		// strips the internal stop signal so it never hijacks a caller's own
		// backoff.Retry (#142).
		if lastTx != nil && lastRolledBack {
			for _, hook := range lastTx.PostRollbackHooks {
				hook()
			}
		}
		return unwrapBackoffPermanent(err)
	}

	return nil
}

// commit runs the SQL COMMIT and logs it, without firing PostCommitHooks.
// RunInTx calls this directly so it can tell a (retryable) driver commit failure
// apart from a (terminal) post-commit-hook failure on an already-durable tx.
func (tx *Tx) commit() error {
	start := time.Now()
	err := tx.Tx.Commit()
	if err == nil {
		tx.committed = true
	}
	tx.db.callLog(LogDetail{
		Query:    "commit",
		Duration: time.Since(start),
		Tx:       tx.Tx,
		Attempt:  1,
		Error:    err,
	})
	return err
}

// runPostCommitHooks fires PostCommitHooks in order, stopping at the first
// failure. Only meaningful after a successful commit.
func (tx *Tx) runPostCommitHooks() error {
	for _, hook := range tx.PostCommitHooks {
		if err := hook(); err != nil {
			return fmt.Errorf("post commit hook failed: %w", err)
		}
	}
	return nil
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if err := tx.commit(); err != nil {
		return err
	}

	return tx.runPostCommitHooks()
}

// Cancel the transaction
// this should be deferred after creating new tx every time
func (tx *Tx) Cancel() error {
	if tx.Tx == nil {
		return nil
	}

	start := time.Now()
	err := tx.Tx.Rollback()
	rolledBack := err == nil
	if errors.Is(err, sql.ErrTxDone) {
		err = nil
	}
	tx.db.callLog(LogDetail{
		Query:    "rollback",
		Duration: time.Since(start),
		Tx:       tx.Tx,
		Attempt:  1,
		Error:    err,
	})

	tx.runPostAbandonHooks()

	if rolledBack {
		for _, hook := range tx.PostRollbackHooks {
			hook()
		}
	}

	return err
}

func (tx *Tx) DefaultInsertOptions() *Inserter {
	return &Inserter{
		db:   tx.db,
		conn: tx.Tx,
		tx:   tx,
	}
}

func (tx *Tx) I() *Inserter {
	return tx.DefaultInsertOptions()
}

func (tx *Tx) Insert(insert string, source any) error {
	return tx.I().Insert(insert, source)
}

func (tx *Tx) InsertContext(ctx context.Context, insert string, source any) error {
	return tx.I().InsertContext(ctx, insert, source)
}

// ExecContextResult executes a query and nothing more
func (tx *Tx) ExecContextResult(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return tx.db.exec(tx.Tx, ctx, tx, query, params...)
}

// ExecContext executes a query and nothing more
func (tx *Tx) ExecContext(ctx context.Context, query string, params ...any) error {
	_, err := tx.ExecContextResult(ctx, query, params...)
	return err
}

// ExecResult executes a query and nothing more
func (tx *Tx) ExecResult(query string, params ...any) (sql.Result, error) {
	return tx.ExecContextResult(context.Background(), query, params...)
}

// Exec executes a query and nothing more
func (tx *Tx) Exec(query string, params ...any) error {
	_, err := tx.ExecContextResult(context.Background(), query, params...)
	return err
}

func (tx *Tx) Select(dest any, q string, cache time.Duration, params ...any) error {
	return tx.db.query(tx.Tx, context.Background(), dest, q, cache, params...)
}

func (tx *Tx) SelectRows(q string, cache time.Duration, params ...any) (Rows, error) {
	var rows Rows
	err := tx.db.query(tx.Tx, context.Background(), &rows, q, cache, params...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (tx *Tx) SelectContext(ctx context.Context, dest any, q string, cache time.Duration, params ...any) error {
	return tx.db.query(tx.Tx, ctx, dest, q, cache, params...)
}

func (tx *Tx) SelectJSON(dest any, query string, cache time.Duration, params ...any) error {
	return tx.SelectJSONContext(context.Background(), dest, query, cache, params...)
}

func (tx *Tx) SelectJSONContext(ctx context.Context, dest any, query string, cache time.Duration, params ...any) error {
	var j []byte
	err := tx.SelectContext(ctx, &j, query, cache, params...)
	if err != nil {
		return err
	}

	err = json.Unmarshal(j, dest)
	if err != nil {
		return err
	}

	return nil
}

// Exists efficiently checks if there are any rows in the given query using the `Reads` connection
func (tx *Tx) Exists(query string, cache time.Duration, params ...any) (bool, error) {
	return tx.db.exists(tx.Tx, context.Background(), query, cache, params...)
}

// ExistsContext efficiently checks if there are any rows in the given query using the `Reads` connection
func (tx *Tx) ExistsContext(ctx context.Context, query string, cache time.Duration, params ...any) (bool, error) {
	return tx.db.exists(tx.Tx, ctx, query, cache, params...)
}

func (tx *Tx) Upsert(insert string, uniqueColumns, updateColumns []string, where string, source any) error {
	return tx.I().Upsert(insert, uniqueColumns, updateColumns, where, source)
}

func (tx *Tx) UpsertContext(ctx context.Context, insert string, uniqueColumns, updateColumns []string, where string, source any) error {
	return tx.I().UpsertContext(ctx, insert, uniqueColumns, updateColumns, where, source)
}

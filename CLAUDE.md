# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`cool-mysql` is a Go library that wraps `database/sql` with MySQL-oriented helpers: dual read/write pools, `@@name` named parameter interpolation, `text/template` support inside queries, pluggable caching, chunked insert/upsert, and automatic retries on transient MySQL errors. Module path: `github.com/StirlingMarketingGroup/cool-mysql`. Go `1.24+` (uses the stdlib `weak` package in `weak_cache.go`).

## Common commands

```bash
go vet ./...
go test ./...                              # full suite (CI runs -v -race-free with coverage)
go test -run TestInterpolateParams ./...   # single test by name
go test -run TestSelect/subtest ./...      # single subtest
go test -v -coverprofile=coverage.txt -covermode=atomic ./...  # matches CI
golangci-lint run --timeout=3m             # lint (configured in .golangci.yml: govet, staticcheck, ineffassign, unused)
```

Tests do **not** require a live MySQL — they use `github.com/DATA-DOG/go-sqlmock`. There is no build step beyond `go build ./...`; this is a library, not a binary.

## Architecture

### Two-pool model

`Database` (database.go) holds two connections:
- `Writes handlerWithContext` — used by `Exec*`, `Insert`, `Upsert`, `BeginTx`, and the `*Writes` read variants
- `Reads *sql.DB` — used by `Select`, `Exists`, `Count`

`handlerWithContext` (sql.go) is the minimal `ExecContext` + `QueryContext` interface, which lets `Writes` also be a non-DB sink: `sqlWriter` (file), `writer` (io.Writer), or a `*sql.Tx`. That's why `db.Writes.(*sql.DB)` appears in `BeginTx` — transactions only work when writes point at a real pool. `NewLocalWriter` / `NewWriter` produce a DB that can render SQL to disk/stdout without executing it.

`Handler` (sql.go) is the public interface implemented by both `*Database` and `*Tx` so callers can take either. `TxOrDatabaseFromContext(ctx)` returns whichever is in the context — prefer this over branching on tx presence.

### Query pipeline

Every read path (`query` in select.go) and write path (`exec` in exec.go) runs the same prelude:

1. `interpolateParams` (params.go) — templates first (if `{{` present), then `@@name` substitution. Named params are **case-insensitive** by default; values go through `valuerFuncs`, the `Valueser` interface, `driver.Valuer`, or reflection-based struct walking.
2. If `db.die` is true, the replaced query + params are printed and the process exits — a debug aid, not used in prod.
3. Errors are wrapped in `Error` (error.go), which includes the original + replaced query and truncates to `QueryErrorLoggingLength` (env `COOL_MYSQL_MAX_QUERY_LOG_LENGTH`, default 4KB).
4. `backoff.Retry` with exponential backoff bounded by `MaxExecutionTime` and optionally `MaxAttempts`. Retryable MySQL error numbers are enumerated in `checkRetryError` (error.go:60) — 1213 (deadlock), 1205, 2006, 2003, 1047, 1452, 1317, 1146, 1305, 1105.
5. `backoff.PermanentError` is an **internal** signal only — `unwrapBackoffPermanent` must strip it before returning so callers' own `backoff.Retry` loops aren't hijacked. This was added in commit f2d0b1b; preserve this invariant when touching exec/select/exists.

### Half-open connections and socket timeouts

`select.go`/`exec.go`/`exists.go` already recover from a dead pooled connection: a read that fails with `mysql.ErrInvalidConn` / `driver.ErrBadConn` triggers `swapConn` (reconnect the pool if down, swap the dead dedicated conn for a fresh one) and the query re-runs. But that only fires if the driver *produces* a connection error. A connection that goes **half-open** (peer vanished with no FIN/RST) writes into the void and then blocks on the packet read forever — with no socket deadline there's no error to retry on, so the query hangs until the caller's context deadline and hard-fails (#172). `applyNetTimeoutsToConfig` (database.go) fixes this by setting go-sql-driver's `ReadTimeout`/`WriteTimeout`/`Timeout` on the parsed DSN config in `openPool` from the package-level `ReadTimeout`/`WriteTimeout`/`DialTimeout` vars (env `COOL_READ_TIMEOUT`/`COOL_WRITE_TIMEOUT`/`COOL_DIAL_TIMEOUT`, all **off by default** to preserve behavior; the DSN's own *non-zero* values win — a parsed zero is indistinguishable from omitted, so it can't override a non-zero default). `ReadTimeout` is the load-bearing one — the driver resets it before *every* socket read, so a healthy streaming query is unaffected; only a fully-silent connection trips it, surfacing `ErrInvalidConn` so the existing swap-and-retry kicks in. `NewFromConn` can't apply these (pools are pre-built). The wire-level behavior is verified in `conn_timeout_test.go` against a tiny in-process fake MySQL server.

### Transactions and deadlock handling

A 1213 deadlock (and a 1205 lock-wait timeout under `innodb_rollback_on_timeout`) is retryable in autocommit (`checkRetryError` lists it), but **inside an explicit transaction it is not retried**: it rolls back *and* ends the whole transaction on the session, leaving the connection in autocommit mode. All three in-tx paths — `exec`, `query` (select.go), and `exists` — share one guard, `tx != nil && checkTxRetryError(err)` (1213/40001 deadlock, 1205 lock-wait timeout), which returns `backoff.Permanent(err)` so the error surfaces to the caller unchanged instead of being statement-retried (which would run in autocommit and strand phantom rows, or silently drop `Select ... FOR UPDATE` locks). The caller must restart the whole transaction from `Begin` — the only way to preserve atomicity, including any non-recorded `Select ... FOR UPDATE` locks and read-snapshot state. Earlier versions replayed the transaction's recorded writes (in autocommit, then later inside a re-opened tx); both were unsound and were removed in #167 — do not reintroduce mid-tx replay.

`db.RunInTx(ctx, fn)` (tx.go) is the safe primitive for that restart: it owns the `Begin`/`Commit`/rollback boundary, puts its tx in the ctx it passes to `fn` (so `GetOrCreateTxFromContext`/`TxOrDatabaseFromContext` inside `fn` reuse it), and re-runs the **whole closure** from a fresh `Begin` on a tx-fatal retryable error (`checkTxRetryError`: 1213/40001 deadlock, 1205 lock-wait timeout). Re-invoking `fn` regenerates every statement *and* the Go logic that produced them, so it's atomic and correct — this is **not** the mid-tx replay #167 removed. Bounded by `MaxAttempts`/`MaxExecutionTime` with backoff, mirroring `exec`. Nesting is a no-op pass-through: if a tx is already in ctx, `RunInTx` runs `fn` once and lets the deadlock propagate to the outermost owner (only it can restart). Commit/rollback hooks fire once for the **final** outcome — a retried-away attempt rolls back raw (hooks suppressed) so they never fire prematurely.

`PostCommitHooks` fire only after a successful `Commit`. `PostRollbackHooks` fire after a rollback via `Cancel()` but **not** when `Cancel()` runs after a successful commit (detected via `sql.ErrTxDone`).

`GetOrCreateTxFromContext` returns noop commit/cancel funcs when a tx already exists in context — the caller who *created* the tx owns its lifecycle. Always `defer cancel()` immediately after this call.

### Caching

`Cache` (cache.go) is `Get`/`Set` with a TTL. `Locker` is optional and enables single-flight query execution to prevent stampedes. Concrete caches: `RedisCache` (also implements `Locker` via redsync), `MemcacheCache`, `WeakCache` (in-memory `weak.Pointer` — GC may reclaim entries), and `MultiCache` (stacked; reads propagate hits up to earlier caches, writes fan out). The `cacheDuration` argument on `Select*`/`Exists*` is the TTL; `0` disables caching for that call even if a cache is configured.

### Parameter interpolation & struct tags

The `mysql:"..."` struct tag (tag.go) controls column names and insert/select behavior. Tag options are parsed via `github.com/fatih/structtag`:
- `defaultzero` / `insertDefault` / `omitempty` — all aliases; emit `default(\`col\`)` when the field's zero value is written
- `noinsert` — skip on inserts but keep for selects and param interpolation
- `"-"` — **deprecated**, behaves like `noinsert` (not a full exclusion, despite appearances). See commit 5024083.
- Column names support `0x2c`-style hex escapes for special characters.

Template branches (`{{ if .Name }}...`) look up the **Go field name**, not the `mysql` tag name. Interpolation looks up `@@name` case-insensitively by default.

### Insert chunking

`Inserter.insert` (insert.go) chunks slices/channels to stay under `MaxInsertSize` (set from `@@max_allowed_packet` at connect time, stored in a `synct[int]`). A bare table name like `"users"` is expanded to `` insert into`users` ``. Channel sources stream — useful for large batches.

## Conventions specific to this repo

- **Context-first is *not* enforced here.** Both context and non-context variants exist side-by-side (`Select` vs `SelectContext`). The non-context form delegates to the context form with `context.Background()`. When adding new methods, provide both.
- Errors returned to callers should always be `Error` or wrap it — this is what preserves the original query for logging. Use `Wrap(err, originalQuery, replacedQuery, params)` if constructing manually.
- `LogFunc` fires on every attempt (success or failure) with `LogDetail.Attempt` incrementing — useful for observability of retries. Don't assume a single log call per query.
- `synct[T]` (sync.go) is a tiny mutex-wrapped value — used for things like `MaxInsertSize` that are read everywhere but mutated rarely.
- Go `1.24+` is required because `weak_cache.go` imports `weak` (stdlib) and is gated by `//go:build go1.24`.

## Environment variables

| Var | Default | Effect |
|---|---|---|
| `COOL_MAX_EXECUTION_TIME_TIME` | 27s | Seeds `MaxExecutionTime` (the retry budget per query) and `MaxConnectionTime` (`SetConnMaxLifetime`). Both are copied onto `*Database.MaxExecutionTime` / `.MaxConnectionTime` at construction — set the field (or call `SetMaxConnectionTime`) per instance to override for long-running processes. |
| `COOL_MAX_ATTEMPTS` | 0 (uncapped) | Hard cap on retry attempts |
| `COOL_REDIS_LOCK_RETRY_DELAY` | 20ms | Redis-backed `Locker` poll interval |
| `COOL_MYSQL_MAX_QUERY_LOG_LENGTH` | 4096 | Truncation point for query text in `Error.Error()` |
| `COOL_READ_TIMEOUT` | 0 (off) | Seeds `ReadTimeout` (whole seconds), applied in `openPool` as go-sql-driver's per-packet socket read deadline on every pool. Off by default preserves the historical no-deadline behavior. The DSN's own `readTimeout=` always wins. See "Half-open connections" below and #172. |
| `COOL_WRITE_TIMEOUT` | 0 (off) | Seeds `WriteTimeout` — the symmetric socket write deadline. |
| `COOL_DIAL_TIMEOUT` | 0 (off) | Seeds `DialTimeout` — bounds new-connection establishment (driver dial `Timeout`). |

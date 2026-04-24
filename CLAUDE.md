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

### Transactions and deadlock replay

`Tx` (tx.go) tracks every `exec` call's replaced query in `tx.updates.queries`. On a 1213 deadlock mid-transaction, the `handleDeadlock` closure in exec.go replays every prior query on the same `*sql.Tx` before returning the error, so the outer `backoff.Retry` can retry the *current* statement against a state equivalent to before the deadlock. Replayed queries pass `newQuery=false` so they don't re-append to `updates.queries` and don't recurse into their own replay. Only `Exec`-style queries are tracked; `Select` is not.

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

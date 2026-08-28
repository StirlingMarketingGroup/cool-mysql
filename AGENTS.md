# AGENTS.md

Guidance for AI coding agents working in this repository. `CLAUDE.md` is a symlink to this file so Claude Code picks it up too.

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

## Every changed line must be test-covered

A PR must not change an executable line that no test executes — **100% coverage of changed lines**, not of the whole package. Before calling a change done, run the suite with `-coverprofile` and map every line your diff touches against the profile; add tests for any changed line the profile shows at count 0 (constructor seeding, error branches, and wrapper methods count too). A branch that is genuinely unreachable from tests is the only exception — say so explicitly in the PR. Non-executable lines (comments, type/var declarations, imports) are exempt.

## Architecture

### Two-pool model

`Database` (database.go) holds two connections:
- `Writes handlerWithContext` — used by `Exec*`, `Insert`, `Upsert`, `BeginTx`, and the `*Writes` read variants
- `Reads *sql.DB` — used by `Select`, `Exists`, `Count`

Read-your-writes is a property of a **session** (`db.NewSession()` — a copy sharing pools/caches/settings with its own write marker). After a durable write through the writer pool on a session (`Exec`, `Insert`, `Upsert`, or a committed writer `Tx` / `RunInTx`), `Select` / `Exists` / `Count` on that session (including a `Clone()` of it — the marker is a shared pointer) within `ReadYourWritesWindow` (default 5s, env `COOL_READ_YOUR_WRITES_WINDOW`; `0` disables) are routed to the `Writes` pool and skip the query cache, so a lagging Aurora replica cannot hide those rows. A raw constructed `Database` has no marker and never pins — a process-wide singleton serving many requests must hand each request its own session, or one request's write would route every other request's reads to the writer and bypass their caches. `SelectWrites` / `ExistsWrites` still always hit Writes, cache included. The marker covers cool-mysql methods only: direct `db.Writes.ExecContext` / `tx.Tx.ExecContext` use is outside the guarantee. Provenance is stated by the API (`Insert`/`Exec`/`BeginTx` = writer, `InsertReads`/`BeginReadsTx` = reader); `Inserter.SetExecutor` drops provenance, so statements through a custom executor never mark.

`handlerWithContext` (sql.go) is the minimal `ExecContext` + `QueryContext` interface, which lets `Writes` also be a non-DB sink: `sqlWriter` (file), `writer` (io.Writer), or a `*sql.Tx`. That's why `db.Writes.(*sql.DB)` appears in `BeginTx` — transactions only work when writes point at a real pool. `NewLocalWriter` / `NewWriter` produce a DB that can render SQL to disk/stdout without executing it.

`Handler` (sql.go) is the public interface implemented by both `*Database` and `*Tx` so callers can take either. `TxOrDatabaseFromContext(ctx)` returns whichever is in the context — prefer this over branching on tx presence.

### Query pipeline

Every read path (`query` in select.go) and write path (`exec` in exec.go) runs the same prelude:

1. `interpolateParams` (params.go) — templates first (if `{{` present), then `@@name` substitution. Named params are **case-insensitive** by default; values go through `valuerFuncs`, the `Valueser` interface, `driver.Valuer`, or reflection-based struct walking.
2. If `db.die` is true, the replaced query + params are printed and the process exits — a debug aid, not used in prod.
3. Errors are wrapped in `Error` (error.go), which includes the original + replaced query and truncates to `QueryErrorLoggingLength` (env `COOL_MYSQL_MAX_QUERY_LOG_LENGTH`, default 4KB).
4. `backoff.Retry` with exponential backoff. The retry budget (`WithMaxElapsedTime`) is **the caller's ctx deadline when one is set**, else the fixed `MaxExecutionTime` — see `retryElapsedBudget` (retry.go). This means a caller with a deadline larger than the Lambda-oriented 27s default is no longer cut short at 27s, and one with a smaller deadline is bounded by it (#174). Optionally capped by `MaxAttempts`. Retryable MySQL error numbers are enumerated in `checkRetryError` (error.go:60) — 1213 (deadlock), 1205, 2006, 2003, 1047, 1452, 1317, 1146, 1305, 1105. Note 3024 (`ER_QUERY_TIMEOUT`) is deliberately **not** retryable: it's what the injected `MAX_EXECUTION_TIME` hint (below) produces, so an over-budget read fails once, cleanly, leaving the conn valid.
5. `backoff.PermanentError` is an **internal** signal only — `unwrapBackoffPermanent` must strip it before returning so callers' own `backoff.Retry` loops aren't hijacked. This was added in commit f2d0b1b; preserve this invariant when touching exec/select/exists.

### Context deadline governs the query budget (#174)

A query's time budget should be the **caller's ctx deadline**, not a fixed process-global. Three pieces, all surgical (no per-query overhead, no default-behavior change for the deadline-free non-ctx APIs which pass `context.Background()`):

- **`MAX_EXECUTION_TIME` injection** (`db.queryWithBudgetHint` → `injectMaxExecutionTime`/`maxExecutionTimeMillis` in query_hint.go; called by select.go/exists.go). When the ctx has a deadline, the read paths inject a `/*+ MAX_EXECUTION_TIME(remaining − buffer) */` optimizer hint onto the **outermost SELECT** (recomputed per attempt). An over-budget read then aborts **server-side** with `ER_QUERY_TIMEOUT` (3024 — non-retryable), so it fails fast with a clean MySQL error, the connection stays valid, and there's no blind replay. The injector only touches statements that lead with `SELECT` (after skipping leading whitespace/comments), merges into an existing `/*+ … */` hint block rather than shadowing it, and leaves a query with its own `MAX_EXECUTION_TIME` alone. It is **read-only** — `exec` (writes) never injects, since the hint only applies to SELECTs. The hint is execution-only: the canonical `replacedQuery` (cache key, `Error`, log) stays hint-free. **Known limits:** a statement leading with `WITH` (CTE) is left unhinted (the hint must physically lead the SELECT, and parsing past CTE definitions safely would need a quote/comment/paren-aware scanner), and MySQL ignores `MAX_EXECUTION_TIME` on locking reads (`FOR UPDATE` / `LOCK IN SHARE MODE`). Both fall back to the **ctx deadline** as the bound (still bounded, just via ctx cancellation rather than a clean 3024).
- **ctx-derived retry budget** — see step 4 above (`retryElapsedBudget`).
- **replay guard** (`retryWithinBudget`, retry.go) — before a `swapConn`/`ErrInvalidConn` retry, check that enough budget remains for another attempt (estimated from the one that just failed). This stops a first attempt that ran ~to the budget (e.g. a `ReadTimeout` trip at 25s) from being followed by a second full-length attempt that overshoots to ~2×. An unbounded budget (no deadline and `MaxExecutionTime == 0`) always allows the retry, preserving the #172/#163 connection-recovery behavior.

Recommended deployment with this in place: run with `COOL_READ_TIMEOUT` **off**, `COOL_TCP_KEEPALIVE` **on** (see below), `COOL_DIAL_TIMEOUT=12` + `COOL_DIAL_ATTEMPT_TIMEOUT=3` so a brief connect-timeout window is absorbed at the dial layer, and let `MAX_EXECUTION_TIME` + the ctx deadline bound query *duration* while keepalive handles *liveness*. A clean server-side abort, no socket-deadline collateral damage on healthy non-streaming aggregations, and fast recovery from a genuinely dead peer.

### Half-open connections and socket timeouts

`select.go`/`exec.go`/`exists.go` already recover from a dead pooled connection: a read that fails with `mysql.ErrInvalidConn` / `driver.ErrBadConn` triggers `swapConn` (reconnect the pool if down, swap the dead dedicated conn for a fresh one) and the query re-runs. But that only fires if the driver *produces* a connection error. A connection that goes **half-open** (peer vanished with no FIN/RST) writes into the void and then blocks on the packet read forever — with no socket deadline there's no error to retry on, so the query hangs until the caller's context deadline and hard-fails (#172). `applyNetTimeoutsToConfig` (database.go) fixes this by setting go-sql-driver's `ReadTimeout`/`WriteTimeout`/`Timeout` on the parsed DSN config in `openPool` from the package-level `ReadTimeout`/`WriteTimeout`/`DialTimeout` vars (env `COOL_READ_TIMEOUT`/`COOL_WRITE_TIMEOUT`/`COOL_DIAL_TIMEOUT`, all **off by default** to preserve behavior; the DSN's own *non-zero* values win — a parsed zero is indistinguishable from omitted, so it can't override a non-zero default). `ReadTimeout` resets before *every* socket read, so a healthy *streaming* query is unaffected; only a fully-silent connection trips it, surfacing `ErrInvalidConn` so the existing swap-and-retry kicks in. The caveat (#174): a healthy but **non-streaming** read (heavy `json_arrayagg`/`GROUP BY`/filesort — silent until the whole result is computed) looks fully-silent and trips `ReadTimeout` too, so a tight `ReadTimeout` caps such queries regardless of the caller's real budget. That's why `ReadTimeout` is **no longer the recommended liveness mechanism** — it conflates "conn dead" with "query slow."

**TCP keepalive (`COOL_TCP_KEEPALIVE`, `applyDialerToConfig` in database.go) is the decoupled replacement (#174).** When set, `openPool` installs a per-config `cfg.DialFunc` wrapping a keepalive-tuned `*net.Dialer` (`cfg.Net` stays `tcp`, so DSN FormatDSN/ParseDSN round-trips stay clean and other go-sql-driver pools in the process are untouched). The OS then probes idle conns and tears down a peer that's gone (no FIN/RST) in ~`TCPKeepAlive*(1+TCPKeepAliveCount)`, surfacing `ErrInvalidConn` for the swap-and-retry path — **without** a whole-query read deadline, so a healthy long read (whose peer answers the probes at the OS layer even mid-computation) is never cut. It catches the network-death half-open (#172's real scenario); a server that accepted the query then hung at the app layer is instead bounded by the ctx deadline / `MAX_EXECUTION_TIME` (3024). The two mechanisms are complementary: keepalive = liveness (no per-query cost, doesn't cut healthy reads), ctx+`MAX_EXECUTION_TIME` = duration. `NewFromConn` can't apply either (pools are pre-built). Wire-level behavior is verified in `conn_timeout_test.go` (`ReadTimeout`) and `keepalive_test.go` (dialer plumbing) against a tiny in-process fake MySQL server; OS-level keepalive teardown timing isn't deterministically unit-testable (needs packet loss).

**Dial retry (`COOL_DIAL_ATTEMPT_TIMEOUT` + a total dial budget)** is also installed by `applyDialerToConfig` as that same `DialFunc`. Both knobs are required: the per-attempt cap (`COOL_DIAL_ATTEMPT_TIMEOUT`) **and** a total budget (`COOL_DIAL_TIMEOUT` or a DSN `timeout=` — a DSN value takes precedence and must not silently disable retry). Without a total budget the pool's background-opener ctx can be unbounded, which would make the retry loop unbounded. `0` on the attempt cap is off (single-attempt, historical). Recommended: 12s total / 3s per attempt. Failures are retried at the dial layer (timeouts and fast failures like connection-refused) so call-site retry semantics (`RunInTx` Begin-is-permanent, `swapConn`, `checkRetryError`) stay untouched. Each redial re-resolves DNS.

### Transactions and deadlock handling

A 1213 deadlock (and a 1205 lock-wait timeout under `innodb_rollback_on_timeout`) is retryable in autocommit (`checkRetryError` lists it), but **inside an explicit transaction it is not retried**: it rolls back *and* ends the whole transaction on the session, leaving the connection in autocommit mode. All three in-tx paths — `exec`, `query` (select.go), and `exists` — share one guard, `tx != nil && checkTxRetryError(err)` (1213/40001 deadlock, 1205 lock-wait timeout), which returns `backoff.Permanent(err)` so the error surfaces to the caller unchanged instead of being statement-retried (which would run in autocommit and strand phantom rows, or silently drop `Select ... FOR UPDATE` locks). The caller must restart the whole transaction from `Begin` — the only way to preserve atomicity, including any non-recorded `Select ... FOR UPDATE` locks and read-snapshot state. Earlier versions replayed the transaction's recorded writes (in autocommit, then later inside a re-opened tx); both were unsound and were removed in #167 — do not reintroduce mid-tx replay.

`db.RunInTx(ctx, fn)` (tx.go) is the safe primitive for that restart: it owns the `Begin`/`Commit`/rollback boundary, puts its tx in the ctx it passes to `fn` (so `GetOrCreateTxFromContext`/`TxOrDatabaseFromContext` inside `fn` reuse it), and re-runs the **whole closure** from a fresh `Begin` on a tx-fatal retryable error (`checkTxRetryError`: 1213/40001 deadlock, 1205 lock-wait timeout). Re-invoking `fn` regenerates every statement *and* the Go logic that produced them, so it's atomic and correct — this is **not** the mid-tx replay #167 removed. Bounded by `MaxAttempts`/`MaxExecutionTime` with backoff, mirroring `exec`. Nesting is a no-op pass-through: if a tx is already in ctx, `RunInTx` runs `fn` once and lets the deadlock propagate to the outermost owner (only it can restart). Commit/rollback hooks fire once for the **final** outcome — a retried-away attempt rolls back raw (hooks suppressed) so they never fire prematurely.

`PostCommitHooks` fire only after a successful `Commit`. `PostRollbackHooks` fire after a rollback via `Cancel()` but **not** when `Cancel()` runs after a successful commit (detected via `sql.ErrTxDone`).

`GetOrCreateTxFromContext` returns noop commit/cancel funcs when a tx already exists in context — the caller who *created* the tx owns its lifecycle. Always `defer cancel()` immediately after this call.

### Caching

`Cache` (cache.go) is `Get`/`Set` with a TTL. `Locker` is optional and enables single-flight query execution to prevent stampedes. Concrete caches: `RedisCache` (also implements `Locker` via redsync), `MemcacheCache`, `WeakCache` (in-memory `weak.Pointer` — GC may reclaim entries), and `MultiCache` (stacked; reads propagate hits up to earlier caches, writes fan out). `TTLCache` is an optional `GetWithTTL` interface (implemented by `RedisCache` via a pipelined GET+PTTL, `WeakCache`, and `MultiCache` itself); `MultiCache` back-populates earlier tiers with the source entry's **remaining** TTL, and skips back-population entirely when the source tier can't report one, because an entry stored with no expiry would outlive every declared `cacheDuration`; `WeakCache.Set` refuses a `ttl <= 0` for the same reason. The `cacheDuration` argument on `Select*`/`Exists*` is the TTL; `0` disables caching for that call even if a cache is configured.

### Parameter interpolation & struct tags

The `mysql:"..."` struct tag (tag.go) controls column names and insert/select behavior. Tag options are parsed via `github.com/fatih/structtag`:
- `defaultzero` / `insertDefault` / `omitempty` — all aliases; emit `default(\`col\`)` when the field's zero value is written
- `noinsert` — skip on inserts but keep for selects and param interpolation
- `"-"` — **deprecated**, behaves like `noinsert` (not a full exclusion, despite appearances). See commit 5024083.
- Column names support `0x2c`-style hex escapes for special characters.

Template branches (`{{ if .Name }}...`) look up the **Go field name**, not the `mysql` tag name. Interpolation looks up `@@name` case-insensitively by default.

### Insert chunking

`Inserter.insert` (insert.go) chunks slices/channels to stay under `MaxInsertSize` (set from `@@max_allowed_packet` at connect time, stored in a `synct[int]`). A bare table name like `"users"` is expanded to `` insert into`users` ``. Channel sources stream — useful for large batches.

### Insert hook

`Database.AfterInsert` fires once per **plain** INSERT chunk once those rows are durable: immediately after a successful autocommit `Insert`, or at successful `Tx.Commit` (before `PostCommitHooks`) for inserts executed on that tx (`tx.Insert`, `tx.I()`; a `SetExecutor` inserter is judged by the executor it actually runs on — see durability below). Events are buffered on the `Tx` itself (`pendingInserts`); a rollback (`Cancel`, `RunInTx` failure) or a retried-away `RunInTx` attempt discards them — they never fire. Plain means `INSERT [priority modifiers] [INTO] table` with no `ON DUPLICATE KEY UPDATE`: `IGNORE` anywhere in the prefix, `REPLACE`, (an executable `/*! … */` comment is read as the SQL it is, so `/*! ignore */` counts) never fire it, and neither does `Upsert`'s UPDATE path — the session cannot vouch it created those rows. Only the rows an `Upsert`'s UPDATE missed and it then inserted are reported. The event's `Rows` are the caller's values: `nil` wherever the statement sent NULL (absent/nil) or DEFAULT (`defaultzero` zero), `[]byte` snapshotted, a `driver.Valuer` passed as-is. A tx's hooked insert holds `Tx.inserting` (a plain mutex, so events buffer in statement order) from exec through buffering; `commit()` takes it before publishing, so it never overtakes a chunk that has landed but not yet been buffered (abandon only discards, without taking it). Concurrent `Commit` and `Cancel` on one tx is not a supported use — database/sql doesn't support it either. Durability is stated per Inserter (`insertDurability`) from the executor the chunk runs on, and only for executors this package can vouch for: a `*sql.DB` pool is durable when exec returns (so `tx.I().SetExecutor(pool)` fires at once), the Inserter's own cool-mysql `Tx` at its commit, and anything else — the SQL renderers (`NewWriter` / `NewLocalWriter`), a foreign `*sql.Tx` or any custom wrapper handed to `SetExecutor` — never. Publication happens inside `Tx.commit()` right after the driver's COMMIT, before `Log` and `PostCommitHooks`. `InsertEvent.Table` is read with MySQL's identifier grammar (unquoted `[0-9A-Za-z$_]`/non-ASCII, or backtick-quoted with `` `` `` unescaped), qualified components joined with `.`; typed zeros that `marshalAppend` renders as NULL (a zero `time.Time`) are reported as the zero value, not `nil` — only absent/nil values and `defaultzero` zeros are `nil`. `Clone` / `NewSession` copy the hook with the rest of the struct. Raw `Exec("insert …")` is outside the hook.

## Conventions specific to this repo

- **Context-first is *not* enforced here.** Both context and non-context variants exist side-by-side (`Select` vs `SelectContext`). The non-context form delegates to the context form with `context.Background()`. When adding new methods, provide both.
- Errors returned to callers should always be `Error` or wrap it — this is what preserves the original query for logging. Use `Wrap(err, originalQuery, replacedQuery, params)` if constructing manually.
- `LogFunc` fires on every attempt (success or failure) with `LogDetail.Attempt` incrementing — useful for observability of retries. Don't assume a single log call per query.
- `synct[T]` (sync.go) is a tiny mutex-wrapped value — used for things like `MaxInsertSize` that are read everywhere but mutated rarely.
- Go `1.24+` is required because `weak_cache.go` imports `weak` (stdlib) and is gated by `//go:build go1.24`.

## Environment variables

| Var | Default | Effect |
|---|---|---|
| `COOL_MAX_EXECUTION_TIME_TIME` | 27s | Seeds `MaxExecutionTime` (the **fallback** retry budget per query, used only when the caller's ctx has no deadline — a ctx deadline governs the budget instead, #174) and `MaxConnectionTime` (`SetConnMaxLifetime`). Both are copied onto `*Database.MaxExecutionTime` / `.MaxConnectionTime` at construction — set the field (or call `SetMaxConnectionTime`) per instance to override for long-running processes. |
| `COOL_MAX_ATTEMPTS` | 0 (uncapped) | Hard cap on retry attempts |
| `COOL_REDIS_LOCK_RETRY_DELAY` | 20ms | Redis-backed `Locker` poll interval |
| `COOL_MYSQL_MAX_QUERY_LOG_LENGTH` | 4096 | Truncation point for query text in `Error.Error()` |
| `COOL_READ_TIMEOUT` | 0 (off) | Seeds `ReadTimeout` (whole seconds), applied in `openPool` as go-sql-driver's per-packet socket read deadline on every pool. Off by default preserves the historical no-deadline behavior. The DSN's own `readTimeout=` always wins. With #174 in place, prefer leaving this off and using `COOL_TCP_KEEPALIVE` for liveness + ctx deadline/`MAX_EXECUTION_TIME` for duration; a tight value here still caps healthy non-streaming reads. See "Half-open connections" below and #172/#174. |
| `COOL_WRITE_TIMEOUT` | 0 (off) | Seeds `WriteTimeout` — the symmetric socket write deadline. |
| `COOL_DIAL_TIMEOUT` | 0 (off) | Seeds `DialTimeout` — the **total** new-connection dial budget (driver dial `Timeout`). A DSN `timeout=` wins. Together with `COOL_DIAL_ATTEMPT_TIMEOUT`, this enables dial retry; a DSN-provided budget counts, not just this package default. Recommended with the attempt cap: 12s total. |
| `COOL_DIAL_ATTEMPT_TIMEOUT` | 0 (off) | Seeds `DialAttemptTimeout` — per-attempt cap on TCP connection establishment. Dial retry is active only when this is > 0 **and** the pool's effective `cfg.Timeout` is > 0 (from `COOL_DIAL_TIMEOUT` or a DSN `timeout=`). Without a total budget the retry loop would be unbounded. Recommended: 3s per attempt (with 12s total). `0` = single-attempt (historical). See "Half-open connections" / dial retry above. |
| `COOL_TCP_KEEPALIVE` | 0 (off) | Seeds `TCPKeepAlive` (whole seconds). When set, `openPool` installs a keepalive-tuned `cfg.DialFunc` (`applyDialerToConfig`; `cfg.Net` stays `tcp`) so a dead peer is torn down by the OS in ~`TCPKeepAlive*(1+count)` and surfaces `ErrInvalidConn` for the swap-and-retry path — the recommended half-open detector that, unlike `ReadTimeout`, doesn't double as a whole-query read cap. TCP only. See "Half-open connections" and #174. |
| `COOL_TCP_KEEPALIVE_COUNT` | 3 | Seeds `TCPKeepAliveCount` — unanswered probes before teardown (only used when `COOL_TCP_KEEPALIVE` is set; some platforms apply their own count). |
| `COOL_READ_YOUR_WRITES_WINDOW` | 5s | Seeds `ReadYourWritesWindow` (whole seconds). After a durable writer-pool write, `Select`/`Exists`/`Count` on the same `*Database` (or a `Clone()`) are routed to `Writes` and skip the query cache for this long so a lagging replica cannot hide those rows. `0` disables. Copied onto `*Database.ReadYourWritesWindow` at construction. |

package mysql

import (
	"strconv"
	"strings"
	"time"
)

// maxExecutionTimeMillis converts a remaining context budget into the
// millisecond value for a MAX_EXECUTION_TIME optimizer hint. It subtracts a
// small buffer so the server-side abort (ER_QUERY_TIMEOUT, 3024) fires just
// before the caller's ctx deadline, yielding a clean, non-retryable MySQL error
// that leaves the connection valid — instead of a context cancellation (or a
// socket ReadTimeout trip that gets blindly replayed). The buffer is 5% of the
// remaining budget, clamped to [50ms, 1s]. Returns 0 (the caller then skips the
// hint and lets the ctx govern) when the remaining budget is too small to be
// useful. See #174.
func maxExecutionTimeMillis(remaining time.Duration) int64 {
	if remaining <= 0 {
		return 0
	}

	buffer := remaining / 20 // 5%
	buffer = min(buffer, time.Second)
	buffer = max(buffer, 50*time.Millisecond)

	ms := (remaining - buffer).Milliseconds()
	if ms <= 0 {
		return 0
	}
	return ms
}

// injectMaxExecutionTime returns query with a MySQL MAX_EXECUTION_TIME(ms)
// optimizer hint placed on the leading SELECT, so an over-budget read aborts
// server-side with ER_QUERY_TIMEOUT (3024) rather than running to the caller's
// deadline. The hint is honored only on the outermost SELECT, so it must lead
// the statement. Queries that don't begin with SELECT (after skipping leading
// whitespace and comments) are returned unchanged with ok=false. An existing
// optimizer-hint block (/*+ ... */) immediately after SELECT is merged into
// rather than shadowed; if it already carries a MAX_EXECUTION_TIME the query is
// left untouched (the caller's explicit value wins). ms must be > 0.
func injectMaxExecutionTime(query string, ms int64) (string, bool) {
	if ms <= 0 {
		return query, false
	}

	i := skipLeadingWhitespaceAndComments(query)

	const sel = "select"
	if i+len(sel) > len(query) || !strings.EqualFold(query[i:i+len(sel)], sel) {
		return query, false
	}
	end := i + len(sel)
	// Require a word boundary so "selected" / "selective" aren't mistaken for
	// the SELECT keyword.
	if end < len(query) {
		switch c := query[end]; c {
		case ' ', '\t', '\n', '\r', '\f', '\v', '/', '(':
		default:
			return query, false
		}
	}

	hint := "MAX_EXECUTION_TIME(" + strconv.FormatInt(ms, 10) + ")"

	// If an optimizer-hint block already follows SELECT, merge into it instead
	// of prepending a second /*+ ... */ (MySQL honors only the first such block,
	// so prepending would silently disable the caller's existing hints).
	j := end
	for j < len(query) && isSQLSpace(query[j]) {
		j++
	}
	if strings.HasPrefix(query[j:], "/*+") {
		closeRel := strings.Index(query[j+3:], "*/")
		if closeRel >= 0 {
			closeAt := j + 3 + closeRel
			existing := query[j+3 : closeAt]
			if strings.Contains(strings.ToUpper(existing), "MAX_EXECUTION_TIME") {
				return query, false
			}
			var b strings.Builder
			b.Grow(len(query) + len(hint) + 1)
			b.WriteString(query[:closeAt])
			if !isSQLSpace(query[closeAt-1]) {
				b.WriteByte(' ')
			}
			b.WriteString(hint)
			b.WriteByte(' ')
			b.WriteString(query[closeAt:])
			return b.String(), true
		}
		// Unterminated hint block — leave the query alone rather than risk
		// corrupting it.
		return query, false
	}

	var b strings.Builder
	b.Grow(len(query) + len(hint) + 8)
	b.WriteString(query[:end])
	b.WriteString(" /*+ ")
	b.WriteString(hint)
	b.WriteString(" */")
	b.WriteString(query[end:])
	return b.String(), true
}

func isSQLSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	}
	return false
}

// skipLeadingWhitespaceAndComments returns the index of the first significant
// character in q, skipping leading whitespace and SQL comments (/* block */,
// -- line, and # line). It is deliberately conservative: an unterminated
// comment makes it return len(q) (nothing significant follows).
func skipLeadingWhitespaceAndComments(q string) int {
	i := 0
	for i < len(q) {
		switch c := q[i]; {
		case isSQLSpace(c):
			i++
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			rel := strings.Index(q[i+2:], "*/")
			if rel < 0 {
				return len(q)
			}
			i += 2 + rel + 2
		case c == '#':
			rel := strings.IndexByte(q[i:], '\n')
			if rel < 0 {
				return len(q)
			}
			i += rel + 1
		case c == '-' && i+1 < len(q) && q[i+1] == '-' &&
			(i+2 >= len(q) || isSQLSpace(q[i+2])):
			rel := strings.IndexByte(q[i:], '\n')
			if rel < 0 {
				return len(q)
			}
			i += rel + 1
		default:
			return i
		}
	}
	return i
}

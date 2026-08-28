package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/fatih/structtag"
)

type Inserter struct {
	db   *Database
	conn handlerWithContext
	tx   *Tx
	role poolRole

	AfterChunkExec func(start time.Time)
	AfterRowExec   func(start time.Time)
	HandleResult   func(sql.Result)
}

func (in *Inserter) SetAfterChunkExec(fn func(start time.Time)) *Inserter {
	in.AfterChunkExec = fn

	return in
}

func (in *Inserter) SetAfterRowExec(fn func(start time.Time)) *Inserter {
	in.AfterRowExec = fn

	return in
}

func (in *Inserter) SetResultHandler(fn func(sql.Result)) *Inserter {
	in.HandleResult = fn

	return in
}

// SetExecutor replaces where statements run. The Inserter can no longer
// vouch for where their effects land, so it drops its pool provenance:
// statements issued through a custom executor never mark read-your-writes
// and never count toward an enclosing tx's wrote flag. The tx association
// is deliberately kept — it is lifecycle, not provenance: a deadlock on a
// tx-created Inserter must stay tx-fatal (#167) even through a custom
// executor, or the statement retry would run in autocommit and strand
// phantom rows.
func (in *Inserter) SetExecutor(conn handlerWithContext) *Inserter {
	in.conn = conn
	in.role = poolUnknown
	return in
}

func (in *Inserter) Insert(insert string, source any) error {
	return in.insert(context.Background(), insert, source)
}

func (in *Inserter) InsertContext(ctx context.Context, insert string, source any) error {
	return in.insert(ctx, insert, source)
}

var ErrNoColumnNames = fmt.Errorf("no column names given")

func (in *Inserter) insert(ctx context.Context, query string, source any) (err error) {
	sv := reflectUnwrap(reflect.ValueOf(source))
	st := sv.Type()

	rt := st

	multiRow := isMultiRow(st)
	if multiRow {
		rt = reflectUnwrapType(st.Elem())

		switch st.Kind() {
		case reflect.Slice, reflect.Array:
			if sv.Len() == 0 {
				return nil
			}
		}
	}

	queryTokens := parseQuery(query)
	if len(queryTokens) == 1 {
		query = "insert into`" + parseName(query) + "`"
		queryTokens = parseQuery(query)
	}

	insertPart := query
	var onDuplicateKeyUpdate string

	var firstToken *queryToken
	var prevToken *queryToken
DUPE_KEY_SEARCH:
	for _, t := range queryTokens {
		switch {
		case t.kind == queryTokenKindMisc:
			// skip
		case prevToken == nil && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "on"):
			firstToken = p(t)
			prevToken = p(t)
		case prevToken != nil && strings.EqualFold(prevToken.string, "on") && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "duplicate"):
			prevToken = p(t)
		case prevToken != nil && strings.EqualFold(prevToken.string, "duplicate") && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "key"):
			prevToken = p(t)
		case prevToken != nil && strings.EqualFold(prevToken.string, "key") && t.kind == queryTokenKindWord && strings.EqualFold(t.string, "update"):
			onDuplicateKeyUpdate = query[firstToken.pos:]
			insertPart = query[:firstToken.pos]

			break DUPE_KEY_SEARCH
		default:
			if prevToken != nil {
				break DUPE_KEY_SEARCH
			}
		}
	}

	columnNames := colNamesFromQuery(parseQuery(insertPart))

	currentRow := sv
	currentRowIndex := 0
	next := func() bool {
		if !multiRow {
			return false
		}

		switch st.Kind() {
		case reflect.Slice, reflect.Array:
			if currentRowIndex >= sv.Len() {
				return false
			}

			currentRow = reflectUnwrap(sv.Index(currentRowIndex))
			currentRowIndex++
			return true
		case reflect.Chan:
			var ok bool
			currentRow, ok = sv.Recv()
			if !ok {
				return false
			}

			currentRow = reflectUnwrap(currentRow)
			return true
		}

		return false
	}
	if multiRow && !next() {
		return nil
	}

	var colOpts map[string]insertColOpts
	if len(columnNames) == 0 {
		if typeHasColNames(rt) {
			switch rt.Kind() {
			case reflect.Map:
				columnNames = colNamesFromMap(currentRow)
			case reflect.Struct:
				columnNames, colOpts, _, err = colNamesFromStruct(rt)
				if err != nil {
					return err
				}
			}
		}

		s := new(strings.Builder)
		s.WriteByte('(')
		for i, name := range columnNames {
			if i != 0 {
				s.WriteByte(',')
			}
			s.WriteByte('`')
			s.WriteString(name)
			s.WriteByte('`')
		}
		s.WriteByte(')')
		insertPart += s.String()
	} else if rt.Kind() == reflect.Struct {
		_, colOpts, _, err = colNamesFromStruct(rt)
		if err != nil {
			return err
		}
	}

	if len(columnNames) == 0 {
		err = ErrNoColumnNames
		return err
	}

	// AfterInsert reports only durable plain inserts: IGNORE (in any modifier
	// position), ON DUPLICATE KEY UPDATE and REPLACE can land on rows this
	// session did not create, and an executor that only renders SQL makes
	// nothing durable at all.
	// Nothing below runs for a nil hook — the common case pays one nil check.
	var prefix insertPrefix
	var durability insertDurability
	hooked := in.db.AfterInsert != nil && onDuplicateKeyUpdate == ""
	if hooked {
		durability = in.durability()
		// A plain classification always carries the table: no table name
		// means no classification, so the hook simply stays off.
		prefix = parseInsertPrefix(query)
		hooked = durability != durableNever && prefix.kind == insertPlain
	}

	insertPart += "values"

	// Both the insert buffer (whole INSERT statement) and the row scratch are
	// pooled so a long stream of Insert() calls amortizes allocation to near
	// zero per call. The buffer grows on demand via append — do not preallocate
	// to MaxInsertSize, since @@max_allowed_packet is commonly hundreds of MB
	// and concurrent small inserts would each retain that full capacity in the
	// pool.
	maxSize := int(in.db.MaxInsertSize.Get())
	threshold := int(float64(maxSize) * 0.80)

	insertBufP := insertScratchPool.Get().(*[]byte)
	defer func() {
		// Discard outsized backings so a single large insert can't leave a
		// multi-MB array in the pool for later tiny inserts to retain.
		if cap(*insertBufP) > insertBufPoolMaxCap {
			return
		}
		*insertBufP = (*insertBufP)[:0]
		insertScratchPool.Put(insertBufP)
	}()
	insertBuf := (*insertBufP)[:0]
	insertBuf = append(insertBuf, insertPart...)
	insertPartLen := len(insertBuf)

	rowBufP := rowScratchPool.Get().(*[]byte)
	defer func() {
		if cap(*rowBufP) > rowBufPoolMaxCap {
			return
		}
		*rowBufP = (*rowBufP)[:0]
		rowScratchPool.Put(rowBufP)
	}()
	rowBuf := *rowBufP
	rowBuf = rowBuf[:0]

	var rowBuffered bool
	var pendingRows [][]any
	var rowValues []any

	multiCol := isMultiColumn(rt)
	valuerFuncs := in.db.valuerFuncs
	loc := in.db.location()

	buildRow := func(row reflect.Value) error {
		rowBuf = append(rowBuf[:0], '(')
		if hooked {
			rowValues = rowValues[:0]
		}

		// The one place a logical column becomes a value: the SQL literal and,
		// when hooked, the InsertEvent value come from the same visit, so the
		// event can never disagree with what was sent.
		writeValue := func(r reflect.Value, opts marshalOpt, fieldName string) error {
			supplied := r
			r = reflectUnwrap(r)

			if hooked {
				rowValues = append(rowValues, insertEventValue(supplied, r))
			}

			if !r.IsValid() {
				rowBuf = append(rowBuf, "null"...)
				return nil
			}

			v := r.Interface()

			var err error
			rowBuf, err = marshalAppend(rowBuf, v, opts|marshalOptJSONSlice, fieldName, valuerFuncs, loc)
			if err != nil {
				return fmt.Errorf("failed to marshal value: %w", err)
			}
			return nil
		}

		switch k := row.Kind(); {
		case !multiCol:
			if err := writeValue(row, marshalOptNone, ""); err != nil {
				return err
			}
		case k == reflect.Struct:
			for i, col := range columnNames {
				if i != 0 {
					rowBuf = append(rowBuf, ',')
				}

				f := row.FieldByIndex(colOpts[col].index)
				v := reflectUnwrap(f)

				// One decision per column: DEFAULT (the bare keyword — valid in
				// a VALUES list, unlike the DEFAULT(`col`) function form, which
				// evaluates to the ZERO DATE for DEFAULT CURRENT_TIMESTAMP
				// columns) or a value. Both the SQL and the event follow it.
				useDefault := false
				if colOpts[col].insertDefault {
					pv := v
					if v.Kind() != reflect.Pointer {
						pv = reflect.New(v.Type())
						pv.Elem().Set(v)
					}

					if zeroer, ok := pv.Interface().(Zeroer); ok {
						if pv.IsNil() {
							_, hasIsZero := pv.Type().Elem().MethodByName("IsZero")
							useDefault = hasIsZero
						}
						if !useDefault && zeroer.IsZero() {
							useDefault = true
						}
					}

					if !useDefault && (!f.IsValid() || f.IsZero()) {
						useDefault = true
					}
				}
				if !useDefault && colOpts[col].defaultZero && v.IsValid() && isZero(v.Interface()) {
					useDefault = true
				}
				if useDefault {
					rowBuf = append(rowBuf, "default"...)
					if hooked {
						rowValues = append(rowValues, nil)
					}
					continue
				}

				if err := writeValue(f, marshalOptNone, ""); err != nil {
					return err
				}
			}
		case k == reflect.Map:
			for i, col := range columnNames {
				if i != 0 {
					rowBuf = append(rowBuf, ',')
				}

				v := row.MapIndex(reflect.ValueOf(col))
				if !v.IsValid() {
					rowBuf = append(rowBuf, "default"...)
					if hooked {
						rowValues = append(rowValues, nil)
					}
					continue
				}

				if err := writeValue(v, marshalOptNone, col); err != nil {
					return err
				}
			}
		case k == reflect.Slice || k == reflect.Array:
			for i := 0; i < row.Len(); i++ {
				if i != 0 {
					rowBuf = append(rowBuf, ',')
				}

				if err := writeValue(row.Index(i), marshalOptNone, ""); err != nil {
					return err
				}
			}
		}

		rowBuf = append(rowBuf, ')')
		return nil
	}

	var start time.Time
	chunkStart := time.Now()

	insert := func() error {
		if !rowBuffered {
			return nil
		}

		insertBuf = append(insertBuf, onDuplicateKeyUpdate...)

		// One string copy per chunk (not per row) — amortized across thousands
		// of rows, negligible next to the row-build savings.
		//
		// A tx-scoped insert holds the tx's inserting lock from exec through
		// buffering (released by defer, so a panicking Log callback can't
		// leak it), so Commit can't take the buffer in between and publish
		// without this chunk's event.
		result, err := func() (sql.Result, error) {
			if hooked && durability == durableAtCommit {
				defer in.tx.holdForInsert()()
			}
			result, err := in.db.exec(in.conn, ctx, in.tx, in.role, string(insertBuf))
			if err != nil {
				return nil, err
			}
			// The rows are durable (or tx-buffered) from here, so the event is
			// recorded before the caller's optional callbacks get a chance to
			// panic past it.
			if hooked {
				ev := InsertEvent{
					Table:   prefix.table,
					Columns: columnNames,
					Rows:    pendingRows,
					Result:  result,
				}
				if durability == durableAtCommit {
					in.tx.bufferInsert(ev)
				} else {
					in.db.AfterInsert(ev)
				}
				// Fresh slice: the hook may retain Rows.
				pendingRows = nil
			}
			return result, nil
		}()
		if err != nil {
			return err
		}

		if in.AfterChunkExec != nil {
			in.AfterChunkExec(chunkStart)
			chunkStart = time.Now()
		}

		if in.HandleResult != nil && result != nil {
			in.HandleResult(result)
		}

		insertBuf = insertBuf[:insertPartLen]
		rowBuffered = false
		return nil
	}

	for {
		start = time.Now()

		if err = buildRow(currentRow); err != nil {
			return err
		}

		// Flush before appending this row if it would push us past threshold.
		// +1 for the comma separator when rowBuffered is true.
		if len(insertBuf)+len(rowBuf)+len(onDuplicateKeyUpdate)+1 > threshold {
			if err = insert(); err != nil {
				return err
			}
		}

		if rowBuffered {
			insertBuf = append(insertBuf, ',')
		}

		insertBuf = append(insertBuf, rowBuf...)
		if hooked {
			// rowValues is scratch reused per row; the event keeps its own copy.
			pendingRows = append(pendingRows, append([]any(nil), rowValues...))
		}

		rowBuffered = true

		if in.AfterRowExec != nil {
			in.AfterRowExec(start)
		}

		if !next() {
			break
		}
	}

	if err = insert(); err != nil {
		return err
	}

	// Capture the grown backings back into the pool so the next Insert() call
	// starts with the same reserved capacity instead of reallocating.
	*insertBufP = insertBuf
	*rowBufP = rowBuf

	return nil
}

// insertEventValue is the Go value an InsertEvent carries for a column that
// is being written as a value (DEFAULT was decided before this): the value
// the caller supplied, or nil where the statement sends NULL for an
// absent/nil one. A driver.Valuer is passed as the Valuer, not pre-rendered
// — the consumer that keys rows can call Value itself — so a pointer whose
// Value method has a pointer receiver stays a pointer. Byte slices (any
// defined type) are snapshotted so a caller reusing its buffer after Insert
// can't rewrite a buffered event.
func insertEventValue(supplied, unwrapped reflect.Value) any {
	if !unwrapped.IsValid() {
		return nil
	}
	switch unwrapped.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		if unwrapped.IsNil() {
			return nil
		}
	}
	v := unwrapped.Interface()
	if _, valuer := v.(driver.Valuer); !valuer && supplied.IsValid() {
		if _, ok := supplied.Interface().(driver.Valuer); ok {
			return supplied.Interface()
		}
	}
	if unwrapped.Kind() == reflect.Slice && unwrapped.Type().Elem().Kind() == reflect.Uint8 {
		snapshot := reflect.MakeSlice(unwrapped.Type(), unwrapped.Len(), unwrapped.Len())
		reflect.Copy(snapshot, unwrapped)
		return snapshot.Interface()
	}
	return v
}

// insertDurability is when a chunk this Inserter executes becomes durable —
// the moment AfterInsert may report it. Only executors this package can
// vouch for are classified; everything else is never.
type insertDurability int

const (
	// durableNever: nothing this package can vouch for — an executor that
	// only renders SQL (NewWriter / NewLocalWriter), a *sql.Tx that isn't the
	// Inserter's own cool-mysql Tx (use Tx.I() for commit-time publication),
	// or any custom SetExecutor wrapper.
	durableNever insertDurability = iota
	// durableNow: autocommit on a *sql.DB pool; durable when exec returns.
	durableNow
	// durableAtCommit: the Inserter's own cool-mysql Tx; buffered until it commits.
	durableAtCommit
)

// durability is decided by the executor the chunk actually runs on, not by
// the Inserter's tx association: tx.I().SetExecutor(pool) autocommits on the
// pool and is durable at once, whatever the tx later does.
func (in *Inserter) durability() insertDurability {
	switch conn := in.conn.(type) {
	case *sql.DB:
		return durableNow
	case *sql.Tx:
		if in.tx != nil && conn == in.tx.Tx {
			return durableAtCommit
		}
	}
	return durableNever
}

// insertKind classifies an INSERT statement's prefix (everything before the
// table name) for AfterInsert eligibility.
type insertKind int

const (
	// insertOther is anything the hook must not treat as a plain insert: a
	// statement that isn't INSERT (REPLACE), an unrecognised modifier, an
	// executable comment that could hide one, or no table name.
	insertOther insertKind = iota
	insertPlain
	insertIgnore
)

// insertPrefix is the one parse of `INSERT [modifiers] [INTO] table` the hook
// relies on: what kind of insert it is and which table it targets.
type insertPrefix struct {
	kind  insertKind
	table string
}

// parseInsertPrefix scans the statement up to and including the table name
// and stops there; what follows (columns, VALUES, a partition clause) is not
// its concern. It reads MySQL's own identifier grammar — unquoted
// `[0-9A-Za-z$_]` plus any non-ASCII byte, or backtick-quoted with “
// doubling — so the table it reports is the table MySQL will insert into,
// components of a qualified name joined with a dot. MySQL accepts the
// priority modifiers and IGNORE in any order before the table and makes INTO
// optional, so IGNORE counts wherever it appears in that prefix. Ordinary
// comments and whitespace are skipped; an executable comment (`/*! … */`,
// optionally versioned) is SQL to the server, so its body is read as such —
// `insert /*! ignore */ into t` is an INSERT IGNORE.
func parseInsertPrefix(query string) insertPrefix {
	s := insertPrefixScanner{query: query}
	if word, quoted, ok := s.identifier(); !ok || quoted || !strings.EqualFold(word, "insert") {
		return insertPrefix{}
	}
	kind := insertPlain
	var tableParts []string
	for len(tableParts) == 0 {
		word, quoted, ok := s.identifier()
		if !ok {
			return insertPrefix{}
		}
		if quoted {
			tableParts = append(tableParts, word)
			break
		}
		switch strings.ToLower(word) {
		case "into", "low_priority", "high_priority", "delayed":
		case "ignore":
			kind = insertIgnore
		default:
			tableParts = append(tableParts, word)
		}
	}
	for s.dot() {
		word, _, ok := s.identifier()
		if !ok {
			return insertPrefix{}
		}
		tableParts = append(tableParts, word)
	}
	return insertPrefix{kind: kind, table: strings.Join(tableParts, ".")}
}

type insertPrefixScanner struct {
	query string
	i     int
}

// skip consumes whitespace and comments. An executable comment is spliced
// into the scan as the SQL it is (`/*!50100 body */` reads as ` body `);
// only reached between tokens, so a quoted identifier containing one is
// never touched.
func (s *insertPrefixScanner) skip() {
	for s.i < len(s.query) {
		switch c := s.query[s.i]; {
		case c == ' ', c == '\t', c == '\n', c == '\r':
			s.i++
		case c == '#' || startsDashComment(s.query, s.i):
			for s.i < len(s.query) && s.query[s.i] != '\n' {
				s.i++
			}
		case strings.HasPrefix(s.query[s.i:], "/*!"):
			end := strings.Index(s.query[s.i+3:], "*/")
			if end < 0 {
				s.i = len(s.query)
				return
			}
			body := strings.TrimLeft(s.query[s.i+3:s.i+3+end], "0123456789")
			s.query = s.query[:s.i] + " " + body + " " + s.query[s.i+3+end+2:]
			s.i++
		case strings.HasPrefix(s.query[s.i:], "/*"):
			end := strings.Index(s.query[s.i+2:], "*/")
			if end < 0 {
				s.i = len(s.query)
				return
			}
			s.i += 2 + end + 2
		default:
			return
		}
	}
}

func isMySQLIdentifierByte(c byte) bool {
	return c == '$' || c == '_' || c >= 0x80 ||
		('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

// identifier reads the next identifier, unquoted or backtick-quoted (with
// “ doubling unescaped). ok is false when what follows isn't one.
func (s *insertPrefixScanner) identifier() (word string, quoted, ok bool) {
	s.skip()
	if s.i >= len(s.query) {
		return "", false, false
	}
	if s.query[s.i] == '`' {
		var b strings.Builder
		for s.i++; s.i < len(s.query); s.i++ {
			if s.query[s.i] != '`' {
				b.WriteByte(s.query[s.i])
				continue
			}
			if s.i+1 < len(s.query) && s.query[s.i+1] == '`' {
				b.WriteByte('`')
				s.i++
				continue
			}
			s.i++
			return b.String(), true, b.Len() != 0
		}
		return "", true, false // unterminated
	}
	start := s.i
	for s.i < len(s.query) && isMySQLIdentifierByte(s.query[s.i]) {
		s.i++
	}
	return s.query[start:s.i], false, s.i != start
}

// dot consumes a qualifier dot, with any whitespace or comments around it.
func (s *insertPrefixScanner) dot() bool {
	s.skip()
	if s.i < len(s.query) && s.query[s.i] == '.' {
		s.i++
		return true
	}
	return false
}

// Retention caps for the scratch pools. Buffers that grow past these bounds
// are dropped instead of returned to the pool so that a single large insert
// can't leave a huge backing array live for the next caller. Vars (not
// consts) so tests can lower them to exercise the discard path.
var (
	insertBufPoolMaxCap = 4 << 20  // 4 MiB
	rowBufPoolMaxCap    = 64 << 10 // 64 KiB
)

// insertScratchPool holds the full-statement buffer for a single Insert() call.
// sync.Pool keys on *[]byte so callers can grow the slice in place and have
// the larger cap survive Put/Get, up to insertBufPoolMaxCap.
var insertScratchPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1<<20)
		return &b
	},
}

// rowScratchPool holds the per-row scratch used by buildRow. Starts small and
// grows to whatever the widest row needs, up to rowBufPoolMaxCap.
var rowScratchPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func colNamesFromMap(v reflect.Value) (columns []string) {
	keys := make([]string, 0, v.Len())
	for _, k := range v.MapKeys() {
		keys = append(keys, k.String())
	}
	return keys
}

type insertColOpts struct {
	index         []int
	insertDefault bool
	defaultZero   bool
}

func colNamesFromStruct(t reflect.Type) (columns []string, colOpts map[string]insertColOpts, colFieldMap map[string]string, err error) {
	structFieldIndexes := StructFieldIndexes(t)
	colOpts = make(map[string]insertColOpts, len(structFieldIndexes))
	colFieldMap = make(map[string]string, len(structFieldIndexes))

	for _, fieldIndex := range structFieldIndexes {
		f := t.FieldByIndex(fieldIndex)
		if f.PkgPath != "" {
			continue
		}

		column := f.Name
		opts := insertColOpts{
			index: fieldIndex,
		}

		t, _ := structtag.Parse(string(f.Tag))
		if t, _ := t.Get("mysql"); t != nil {
			// Deprecated: mysql:"-" is supported for backwards compatibility but
			// is misleading because it only skips inserts, not selects or parameter
			// interpolation. Use the "noinsert" option instead:
			//   mysql:"column_name,noinsert"
			if t.Name == "-" || t.HasOption("noinsert") {
				continue
			}

			if len(t.Name) != 0 {
				column, err = decodeHex(t.Name)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to decode hex in struct tag name %q: %w", t.Name, err)
				}
			}

			opts.insertDefault = t.HasOption("insertDefault") || t.HasOption("omitempty")
			opts.defaultZero = t.HasOption("defaultzero")
		}

		columns = append(columns, column)
		colOpts[column] = opts
		colFieldMap[column] = f.Name
	}

	return columns, colOpts, colFieldMap, err
}

func colNamesFromQuery(queryTokens []queryToken) (columns []string) {
	for i, t := range queryTokens {
		// find the first paren
		if t.kind == queryTokenKindParen && t.string == "(" {
			queryTokens = queryTokens[i:]
			for i, t := range queryTokens {
				// if we found an end paren then we are done
				if t.kind == queryTokenKindParen && t.string == ")" {
					return columns
				}

				if t.kind != queryTokenKindWord && t.kind != queryTokenKindString {
					continue
				}

				// are we the last token or the next token is not a word or string?
				// we only want to push the last name of the "column" iun the case of "table.column"
				if i+i == len(queryTokens) || (queryTokens[i+1].kind != queryTokenKindWord && queryTokens[i+1].kind != queryTokenKindString) {
					col := t.string
					if len(col) != 0 && col[0] == '`' {
						col = parseName(col)
					}

					columns = append(columns, col)
				}
			}

			break
		}
	}

	return columns
}

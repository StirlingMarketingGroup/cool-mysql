package mysql

import (
	"context"
	"database/sql"
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

func (in *Inserter) SetExecutor(conn handlerWithContext) *Inserter {
	in.conn = conn

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

	multiCol := isMultiColumn(rt)
	valuerFuncs := in.db.valuerFuncs

	buildRow := func(row reflect.Value) error {
		rowBuf = append(rowBuf[:0], '(')

		writeValue := func(r reflect.Value, opts marshalOpt, fieldName string) error {
			r = reflectUnwrap(r)

			if !r.IsValid() {
				rowBuf = append(rowBuf, "null"...)
				return nil
			}

			v := r.Interface()

			var err error
			rowBuf, err = marshalAppend(rowBuf, v, opts|marshalOptJSONSlice, fieldName, valuerFuncs)
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

				if colOpts[col].insertDefault {
					pv := v
					if v.Kind() != reflect.Ptr {
						pv = reflect.New(v.Type())
						pv.Elem().Set(v)
					}

					if v, ok := pv.Interface().(Zeroer); ok {
						if pv.IsNil() {
							if _, ok := pv.Type().Elem().MethodByName("IsZero"); ok {
								rowBuf = append(rowBuf, "default"...)
								continue
							}
						}

						if v.IsZero() {
							rowBuf = append(rowBuf, "default"...)
							continue
						}
					}

					if !f.IsValid() || f.IsZero() {
						rowBuf = append(rowBuf, "default"...)
						continue
					}
				}

				marshalOpts := marshalOptNone
				if colOpts[col].defaultZero {
					marshalOpts |= marshalOptDefaultZero
				}
				if err := writeValue(v, marshalOpts, col); err != nil {
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
		result, err := in.db.exec(in.conn, ctx, in.tx, true, string(insertBuf))
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

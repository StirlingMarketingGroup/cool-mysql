package mysql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"
)

// Upsert performs an "insert or update" style operation on the given table.
//
// The `query` parameter should normally just be the table name.  If only a
// single token is passed (e.g. `people`), the function will internally build an
// `INSERT INTO` statement for that table.  A full INSERT statement may also be
// provided if desired.
//
// `uniqueColumns` represents the columns used to determine whether a row
// already exists.  These columns are compared against the incoming row values in
// the WHERE clause of the generated UPDATE/SELECT statement.
//
// `updateColumns` are the columns that will be updated when a matching row is
// found.  If no columns are supplied then the function will simply check for the
// existence of a row using a SELECT query and perform an INSERT if none is
// found.
//
// `where` is an optional additional WHERE predicate that is appended after the
// uniqueness checks.  It is commonly used for soft delete filters such as
// `"Deleted=0"`.

func (in *Inserter) Upsert(query string, uniqueColumns, updateColumns []string, where string, source any) error {
	return in.upsert(context.Background(), query, uniqueColumns, updateColumns, where, source)
}

func (in *Inserter) UpsertContext(ctx context.Context, query string, uniqueColumns, updateColumns []string, where string, source any) error {
	return in.upsert(ctx, query, uniqueColumns, updateColumns, where, source)
}

func (in *Inserter) upsert(ctx context.Context, query string, uniqueColumns, updateColumns []string, where string, source any) error {
	modifiedQuery := query
	queryTokens := parseQuery(query)
	if len(queryTokens) == 1 {
		modifiedQuery = "insert into`" + parseName(query) + "`"
		queryTokens = parseQuery(modifiedQuery)
	}

	columnNames := colNamesFromQuery(queryTokens)
	tableName, err := rawTableNameFromQuery(queryTokens)
	if err != nil {
		return Wrap(err, query, modifiedQuery, source)
	}

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

			currentRow = reflect.Indirect(sv.Index(currentRowIndex))
			currentRowIndex++
			return true
		case reflect.Chan:
			var ok bool
			currentRow, ok = sv.Recv()
			if !ok {
				return false
			}

			currentRow = reflect.Indirect(currentRow)
			return true
		}

		return false
	}
	if multiRow && !next() {
		return nil
	}

	var colFieldMap map[string]string
	if len(columnNames) == 0 {
		if typeHasColNames(rt) {
			switch rt.Kind() {
			case reflect.Map:
				columnNames = colNamesFromMap(currentRow)
			case reflect.Struct:
				columnNames, _, colFieldMap, err = colNamesFromStruct(rt)
				if err != nil {
					return Wrap(err, query, modifiedQuery, source)
				}
			}
		}
	} else {
		switch rt.Kind() {
		case reflect.Array, reflect.Slice:
			if rt.Elem().Kind() == reflect.Uint8 {
				break
			}

			colFieldMap = make(map[string]string, len(columnNames))
			for i, c := range columnNames {
				colFieldMap[c] = strconv.Itoa(i)
			}
		case reflect.Struct:
			_, _, colFieldMap, err = colNamesFromStruct(rt)
			if err != nil {
				return Wrap(err, query, modifiedQuery, source)
			}
		}
	}

	if len(columnNames) == 0 {
		return Wrap(ErrNoColumnNames, query, modifiedQuery, source)
	}

	// Build the initial UPDATE or SELECT statement used to determine if the
	// row already exists. When updateColumns are provided we issue an UPDATE
	// and check the rows affected. Otherwise we issue a simple SELECT to
	// test for existence.
	s := new(strings.Builder)
	if len(updateColumns) != 0 {
		s.WriteString("update ")
		s.WriteString(tableName)
		s.WriteString(" set")

		for i, c := range updateColumns {
			if i != 0 {
				s.WriteByte(',')
			}

			s.WriteByte('`')
			s.WriteString(c)
			s.WriteString("`=@@")

			if colFieldMap != nil {
				s.WriteString(colFieldMap[c])
			} else {
				s.WriteString(c)
			}
		}
	} else {
		s.WriteString("select 0 from ")
		s.WriteString(tableName)
	}

	if len(uniqueColumns) != 0 || len(where) != 0 {
		s.WriteString(" where")
	}

	if len(uniqueColumns) != 0 {
		for i, c := range uniqueColumns {
			if i != 0 {
				s.WriteString(" and")
			}

			s.WriteByte('`')
			s.WriteString(c)
			s.WriteString("`<=>@@")

			if colFieldMap != nil {
				s.WriteString(colFieldMap[c])
			} else {
				s.WriteString(c)
			}
		}

		if len(where) != 0 {
			s.WriteString(" and")
		}
	}

	if len(where) != 0 {
		s.WriteByte('(')
		s.WriteString(where)
		s.WriteByte(')')
	}

	q := s.String()

	ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, rt), 0)
	grp := new(errgroup.Group)

	var sliceToMap func(slice reflect.Value) map[string]any
	switch rt.Kind() {
	case reflect.Array, reflect.Slice:
		sliceToMap = func(slice reflect.Value) map[string]any {
			l := slice.Len()
			m := make(map[string]any, l)
			for i := 0; i < l; i++ {
				m[strconv.Itoa(i)] = slice.Index(i).Interface()
			}

			return m
		}
	}

	grp.Go(func() error {
		defer ch.Close()

		for {
			r := currentRow.Interface()
			if sliceToMap != nil {
				r = sliceToMap(currentRow)
			}

			if len(updateColumns) != 0 {
				res, err := in.db.exec(in.conn, ctx, in.tx, true, q, r)
				if err != nil {
					return Wrap(fmt.Errorf("failed to update: %w", err), query, q, r)
				}

				if m, _ := res.RowsAffected(); m != 0 {
					goto NEXT
				}
			} else {
				ok, err := in.db.exists(in.conn, ctx, q, 0, r)
				if err != nil {
					return Wrap(fmt.Errorf("failed to check if exists: %w", err), query, q, r)
				}

				if ok {
					goto NEXT
				}
			}

			// If the UPDATE affected no rows (or nothing exists), send the
			// row down the channel so that insert() can add it.
			ch.Send(currentRow)

		NEXT:
			if !next() {
				break
			}
		}

		return nil
	})

	grp.Go(func() error {
		return in.insert(ctx, query, ch.Interface())
	})

	return grp.Wait()
}

var ErrNoTableName = errors.New("no table name found")

func rawTableNameFromQuery(queryTokens []queryToken) (string, error) {
	var tableNameParts []string

	for i, t := range queryTokens {
		if t.kind == queryTokenKindWord && strings.EqualFold(t.string, "into") {
			if i+1 >= len(queryTokens) {
				break
			}

			for _, t := range queryTokens[i+1:] {
				if t.kind == queryTokenKindMisc {
					continue
				}

				if t.kind == queryTokenKindWord || t.kind == queryTokenKindString {
					tableNameParts = append(tableNameParts, t.string)
				} else {
					break
				}
			}

			break
		}
	}

	if len(tableNameParts) == 0 {
		return "", ErrNoTableName
	}

	return strings.Join(tableNameParts, "."), nil
}

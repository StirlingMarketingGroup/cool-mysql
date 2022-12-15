package mysql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"golang.org/x/sync/errgroup"
)

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

	sourceRef := reflect.Indirect(reflect.ValueOf(source))
	sourceType := sourceRef.Type()

	rowType := sourceType

	multiRow := isMultiRow(sourceType)
	if multiRow {
		rowType = sourceType.Elem()
		if rowType.Kind() == reflect.Ptr {
			rowType = rowType.Elem()
		}

		switch sourceType.Kind() {
		case reflect.Slice, reflect.Array:
			if sourceRef.Len() == 0 {
				return nil
			}
		}
	}

	currentRow := sourceRef
	currentRowIndex := 0
	next := func() bool {
		if !multiRow {
			return false
		}

		switch sourceType.Kind() {
		case reflect.Slice, reflect.Array:
			if currentRowIndex >= sourceRef.Len() {
				return false
			}

			currentRow = reflect.Indirect(sourceRef.Index(currentRowIndex))
			currentRowIndex++
			return true
		case reflect.Chan:
			var ok bool
			currentRow, ok = sourceRef.Recv()
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
		if typeHasColNames(rowType) {
			switch rowType.Kind() {
			case reflect.Map:
				columnNames = colNamesFromMap(currentRow)
			case reflect.Struct:
				columnNames, _, colFieldMap = colNamesFromStruct(rowType)
			}
		}
	} else {
		switch rowType.Kind() {
		case reflect.Array, reflect.Slice:
			if rowType.Elem().Kind() == reflect.Uint8 {
				break
			}

			colFieldMap = make(map[string]string, len(columnNames))
			for i, c := range columnNames {
				colFieldMap[c] = strconv.Itoa(i)
			}
		case reflect.Struct:
			_, _, colFieldMap = colNamesFromStruct(rowType)
		}
	}

	if len(columnNames) == 0 {
		return Wrap(ErrNoColumnNames, query, modifiedQuery, source)
	}

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

	ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, rowType), 0)
	grp := new(errgroup.Group)

	var sliceToMap func(slice reflect.Value) map[string]any
	switch rowType.Kind() {
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

			spew.Dump(r)

			if len(updateColumns) != 0 {
				res, err := in.db.exec(in.conn, ctx, q, r)
				if err != nil {
					return Wrap(fmt.Errorf("failed to update: %w", err), query, q, r)
				}

				if m, _ := res.RowsAffected(); m != 0 {
					goto NEXT
				}
			} else {
				ok, err := exists(in.db, in.conn, ctx, q, 0, r)
				if err != nil {
					return Wrap(fmt.Errorf("failed to check if exists: %w", err), query, q, r)
				}

				if ok {
					goto NEXT
				}
			}

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

package mysql

import (
	"crypto/sha3"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/civil"
)

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var valuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

var paramsType = reflect.TypeOf((*Params)(nil)).Elem()
var sliceRowType = reflect.TypeOf((*SliceRow)(nil)).Elem()
var mapRowType = reflect.TypeOf((*MapRow)(nil)).Elem()
var interfaceType = reflect.TypeOf((*any)(nil)).Elem()

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var civilDateType = reflect.TypeOf((*civil.Date)(nil)).Elem()

var structFieldIndexesCache sync.Map

// StructFieldIndexes recursively gets all the struct field index,
// including the indexes from embedded structs
func StructFieldIndexes(t reflect.Type) [][]int {
	if v, ok := structFieldIndexesCache.Load(t); ok {
		return v.([][]int)
	}

	indexes := structFieldIndexes(t, nil)
	structFieldIndexesCache.Store(t, indexes)
	return indexes
}

func structFieldIndexes(t reflect.Type, indexPrefix []int) [][]int {
	indexes := make([][]int, 0)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		newIndex := append(indexPrefix, i)

		indexes = append(indexes, newIndex)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			indexes = append(indexes, structFieldIndexes(f.Type, newIndex)...)
		}
	}

	return indexes
}

var typeFingerprints sync.Map

// typeFingerprint hashes a type's whole reflected definition: its package,
// name and kind and, for every struct it reaches, each field's name, type and
// tag. The select cache key carries it so two binaries whose models differ
// never read each other's cached rows (#181).
func typeFingerprint(t reflect.Type) string {
	if v, ok := typeFingerprints.Load(t); ok {
		return v.(string)
	}

	definition := new(strings.Builder)
	writeTypeDefinition(definition, t, make(map[reflect.Type]bool))
	h := sha3.Sum224([]byte(definition.String()))
	fingerprint := hex.EncodeToString(h[:])
	typeFingerprints.Store(t, fingerprint)
	return fingerprint
}

func writeTypeDefinition(b *strings.Builder, t reflect.Type, seen map[reflect.Type]bool) {
	// a named type's String has only its package's name, which two packages
	// can share, and says nothing of what the type is underneath
	b.WriteString(t.PkgPath())
	b.WriteByte(' ')
	b.WriteString(t.String())
	b.WriteByte(' ')
	b.WriteString(t.Kind().String())

	// a type is spelled out once; a recursive type would never end otherwise
	if seen[t] {
		return
	}
	seen[t] = true

	switch t.Kind() {
	case reflect.Array:
		b.WriteString(strconv.Itoa(t.Len()))
		fallthrough
	case reflect.Pointer, reflect.Slice:
		b.WriteByte('<')
		writeTypeDefinition(b, t.Elem(), seen)
		b.WriteByte('>')
	case reflect.Map:
		b.WriteByte('<')
		writeTypeDefinition(b, t.Key(), seen)
		b.WriteByte(',')
		writeTypeDefinition(b, t.Elem(), seen)
		b.WriteByte('>')
	case reflect.Struct:
		b.WriteByte('{')
		for i := range t.NumField() {
			f := t.Field(i)
			if f.Anonymous {
				b.WriteString("embedded ")
			}
			b.WriteString(f.Name)
			b.WriteByte(' ')
			writeTypeDefinition(b, f.Type, seen)
			b.WriteByte(' ')
			b.WriteString(strconv.Quote(string(f.Tag)))
			b.WriteByte(';')
		}
		b.WriteByte('}')
	}
}

func reflectUnwrap(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		// stop "early" if the pointer/interface is nil
		// since a nil pointer/interface of a type is more useful
		// than an untyped nil value
		v2 := v.Elem()
		if !v2.IsValid() {
			return v
		}
		return reflectUnwrap(v2)
	default:
		return v
	}
}

func reflectUnwrapType(t reflect.Type) reflect.Type {
	switch t.Kind() {
	case reflect.Pointer:
		return reflectUnwrapType(t.Elem())
	default:
		return t
	}
}

// isMultiColumn returns true if the value should be interpreted as multiple rows of values.
// Expects an unwrapped reflect type.
func isMultiRow(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Chan:
		return true
	case reflect.Slice, reflect.Array:
		switch t.Elem().Kind() {
		case reflect.Uint8, reflect.Interface:
			return false
		default:
			return true
		}
	default:
		return false
	}
}

// isMultiColumn returns true if the value should be interpreted as multiple values.
// Expects an unwrapped reflect type.
func isMultiColumn(t reflect.Type) bool {
	if t == timeType ||
		t == civilDateType ||
		reflect.New(t).Type().Implements(valuerType) {
		return false
	}

	switch t.Kind() {
	case reflect.Map, reflect.Struct:
		return true
	case reflect.Slice, reflect.Array:
		return t.Elem().Kind() != reflect.Uint8
	default:
		return false
	}
}

func typeHasColNames(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer:
		return typeHasColNames(t.Elem())
	case reflect.Map:
		return t.Key().Kind() == reflect.String
	case reflect.Struct:
		return true
	default:
		return false
	}
}

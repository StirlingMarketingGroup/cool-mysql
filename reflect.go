package mysql

import "reflect"

// StructFieldIndexes recursively gets all the struct field index,
// including the indexes from embedded structs
func StructFieldIndexes(t reflect.Type) [][]int {
	return structFieldIndexes(t, nil)
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

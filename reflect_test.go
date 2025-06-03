package mysql

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type embedded struct {
	A int
}

type example struct {
	embedded
	B string
}

func TestStructFieldIndexes_Cached(t *testing.T) {
	t1 := StructFieldIndexes(reflect.TypeOf(example{}))
	t2 := StructFieldIndexes(reflect.TypeOf(example{}))
	require.Equal(t, t1, t2)
	require.ElementsMatch(t, [][]int{{0}, {0, 0}, {1}}, t1)
}

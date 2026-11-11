package mysql

import (
	"reflect"
	"testing"
	"time"

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

func TestTypeFingerprint(t *testing.T) {
	t.Run("identical definitions", func(t *testing.T) {
		a := func() reflect.Type {
			type row struct {
				Name string
				N    int
			}
			return reflect.TypeOf(row{})
		}()
		b := func() reflect.Type {
			type row struct {
				Name string
				N    int
			}
			return reflect.TypeOf(row{})
		}()
		require.Equal(t, a.String(), b.String())
		require.NotEqual(t, a, b)
		require.Equal(t, typeFingerprint(a), typeFingerprint(b))
	})

	t.Run("pointer field versus value field", func(t *testing.T) {
		ptr := func() reflect.Type {
			type row struct {
				Created *time.Time
			}
			return reflect.TypeOf(row{})
		}()
		val := func() reflect.Type {
			type row struct {
				Created time.Time
			}
			return reflect.TypeOf(row{})
		}()
		require.NotEqual(t, typeFingerprint(ptr), typeFingerprint(val))
	})

	t.Run("renamed field", func(t *testing.T) {
		a := func() reflect.Type {
			type row struct {
				Name string
			}
			return reflect.TypeOf(row{})
		}()
		b := func() reflect.Type {
			type row struct {
				Title string
			}
			return reflect.TypeOf(row{})
		}()
		require.NotEqual(t, typeFingerprint(a), typeFingerprint(b))
	})

	t.Run("changed tag", func(t *testing.T) {
		a := func() reflect.Type {
			type row struct {
				A string `mysql:"a"`
			}
			return reflect.TypeOf(row{})
		}()
		b := func() reflect.Type {
			type row struct {
				A string `mysql:"b"`
			}
			return reflect.TypeOf(row{})
		}()
		require.NotEqual(t, typeFingerprint(a), typeFingerprint(b))
	})

	t.Run("embedded versus named field", func(t *testing.T) {
		type inner struct {
			N int
		}
		emb := func() reflect.Type {
			type row struct {
				inner
			}
			return reflect.TypeOf(row{})
		}()
		named := func() reflect.Type {
			type row struct {
				Item inner
			}
			return reflect.TypeOf(row{})
		}()
		require.NotEqual(t, typeFingerprint(emb), typeFingerprint(named))
	})

	t.Run("named basic kind changed", func(t *testing.T) {
		a := func() reflect.Type {
			type status int
			return reflect.TypeOf(status(0))
		}()
		b := func() reflect.Type {
			type status string
			return reflect.TypeOf(status(""))
		}()
		require.Equal(t, a.String(), b.String())
		require.NotEqual(t, typeFingerprint(a), typeFingerprint(b))
	})

	t.Run("named array length changed", func(t *testing.T) {
		a := func() reflect.Type {
			type id [12]byte
			return reflect.TypeOf(id{})
		}()
		b := func() reflect.Type {
			type id [16]byte
			return reflect.TypeOf(id{})
		}()
		require.NotEqual(t, typeFingerprint(a), typeFingerprint(b))
	})

	t.Run("nested field through slice", func(t *testing.T) {
		old := func() reflect.Type {
			type nested struct{ A int }
			type row struct{ Items []nested }
			return reflect.TypeOf(row{})
		}()
		neu := func() reflect.Type {
			type nested struct{ A string }
			type row struct{ Items []nested }
			return reflect.TypeOf(row{})
		}()
		require.Equal(t, old.String(), neu.String())
		require.NotEqual(t, typeFingerprint(old), typeFingerprint(neu))
	})

	t.Run("nested field through pointer", func(t *testing.T) {
		old := func() reflect.Type {
			type nested struct{ A int }
			type row struct{ Item *nested }
			return reflect.TypeOf(row{})
		}()
		neu := func() reflect.Type {
			type nested struct{ A string }
			type row struct{ Item *nested }
			return reflect.TypeOf(row{})
		}()
		require.Equal(t, old.String(), neu.String())
		require.NotEqual(t, typeFingerprint(old), typeFingerprint(neu))
	})

	t.Run("nested field through map value", func(t *testing.T) {
		old := func() reflect.Type {
			type nested struct{ A int }
			type row struct{ Items map[string]nested }
			return reflect.TypeOf(row{})
		}()
		neu := func() reflect.Type {
			type nested struct{ A string }
			type row struct{ Items map[string]nested }
			return reflect.TypeOf(row{})
		}()
		require.Equal(t, old.String(), neu.String())
		require.NotEqual(t, typeFingerprint(old), typeFingerprint(neu))
	})

	t.Run("self referential", func(t *testing.T) {
		type node struct {
			Next     *node
			Children []node
		}
		got := typeFingerprint(reflect.TypeOf(node{}))
		require.NotEmpty(t, got)
		require.Equal(t, got, typeFingerprint(reflect.TypeOf(node{})))
	})
}

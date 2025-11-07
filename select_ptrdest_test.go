package mysql

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssignUintFromInterfaceValue(t *testing.T) {
	var dest uint64

	pd := &ptrDest{
		tempDest:   reflect.New(reflect.PointerTo(interfaceType)),
		finalDest:  reflect.ValueOf(&dest),
		baseType:   reflect.TypeOf(uint64(0)),
		targetType: reflect.TypeOf(dest),
	}

	valPtr := reflect.New(interfaceType)
	valPtr.Elem().Set(reflect.ValueOf(float64(99)))
	pd.tempDest.Elem().Set(valPtr)

	require.NoError(t, assignUintFromInterface(pd))
	require.Equal(t, uint64(99), dest)
}

func TestAssignUintFromInterfacePointerField(t *testing.T) {
	var dest *uint64

	pd := &ptrDest{
		tempDest:   reflect.New(reflect.PointerTo(interfaceType)),
		finalDest:  reflect.ValueOf(&dest),
		baseType:   reflect.TypeOf(uint64(0)),
		targetType: reflect.TypeOf((*uint64)(nil)),
	}

	valPtr := reflect.New(interfaceType)
	valPtr.Elem().Set(reflect.ValueOf(float64(77)))
	pd.tempDest.Elem().Set(valPtr)

	require.NoError(t, assignUintFromInterface(pd))
	require.NotNil(t, dest)
	require.Equal(t, uint64(77), *dest)
}

func TestAssignUintFromInterfaceNilValue(t *testing.T) {
	var dest uint64

	pd := &ptrDest{
		tempDest:   reflect.New(reflect.PointerTo(interfaceType)),
		finalDest:  reflect.ValueOf(&dest),
		baseType:   reflect.TypeOf(uint64(0)),
		targetType: reflect.TypeOf(dest),
	}

	pd.tempDest.Elem().Set(reflect.Zero(reflect.PointerTo(interfaceType)))

	require.NoError(t, assignUintFromInterface(pd))
	require.Equal(t, uint64(0), dest)
}

func TestNeedsUintWorkaround(t *testing.T) {
	base, ok := needsUintWorkaround(reflect.TypeOf((*uint64)(nil)))
	require.True(t, ok)
	require.Equal(t, reflect.TypeOf(uint64(0)), base)

	_, ok = needsUintWorkaround(reflect.TypeOf(""))
	require.False(t, ok)
}

func TestNewPtrDestForScanTypeUint(t *testing.T) {
	pd := newPtrDestForScanType(reflect.TypeOf(uint64(0)))
	require.NotNil(t, pd.assignFn)
	require.Equal(t, reflect.TypeOf(uint64(0)), pd.baseType)

	pdPointer := newPtrDestForScanType(reflect.TypeOf((*uint64)(nil)))
	require.NotNil(t, pdPointer.assignFn)
}

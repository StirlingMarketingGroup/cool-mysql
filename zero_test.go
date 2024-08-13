package mysql

import (
	"testing"

	"github.com/shopspring/decimal"
)

type coolTestPtrZeroer struct {
	zero bool
}

func (c *coolTestPtrZeroer) IsZero() bool {
	return c.zero
}

var _ Zeroer = (*coolTestPtrZeroer)(nil)

type coolTestZeroer struct {
	zero bool
}

func (c coolTestZeroer) IsZero() bool {
	return c.zero
}

var _ Zeroer = (*coolTestZeroer)(nil)

func TestIsZero(t *testing.T) {
	// Test case 1: Test with a non-zero value
	nonZeroValue := 42
	if isZero(nonZeroValue) {
		t.Errorf("Expected isZero to return false for non-zero value, but got true")
	}

	// Test case 2: Test with a zero value
	zeroValue := 0
	if !isZero(zeroValue) {
		t.Errorf("Expected isZero to return true for zero value, but got false")
	}

	// Test case 3: Test with a nil pointer to Zeroer
	var nilZeroer *Zeroer
	if !isZero(nilZeroer) {
		t.Errorf("Expected isZero to return true for nil pointer to Zeroer, but got false")
	}

	// Test case 4: Test with a non-nil pointer to Zeroer
	nonNilZeroer := decimal.Zero
	if !isZero(nonNilZeroer) {
		t.Errorf("Expected isZero to return true for non-nil pointer to Zeroer, but got false")
	}

	// Test case 5: Test with a nil value
	var nilValue *int
	if !isZero(nilValue) {
		t.Errorf("Expected isZero to return true for nil value, but got false")
	}

	// Test case 6: Test with a non-nil value
	nonNilValue := new(int)
	*nonNilValue = 42
	if isZero(nonNilValue) {
		t.Errorf("Expected isZero to return false for non-nil value, but got true")
	}

	// Test case 7: Test with a nil pointer to Zeroer
	nonNilValueZero := new(int)
	if isZero(nonNilValueZero) {
		t.Errorf("Expected isZero to return false for non-nil value with zero value, but got true")
	}

	// Test case 8: Test with a non-nil pointer to Zeroer
	nonNilValueNonZero := &coolTestPtrZeroer{zero: false}
	if isZero(nonNilValueNonZero) {
		t.Errorf("Expected isZero to return false for non-nil value with non-zero value, but got true")
	}

	// Test case 9: Test with a non-nil pointer to Zeroer
	nonNilValueZeroer := &coolTestPtrZeroer{zero: true}
	if !isZero(nonNilValueZeroer) {
		t.Errorf("Expected isZero to return true for non-nil value with zero value, but got false")
	}

	// Test case 10: Test with a nil pointer to Zeroer
	nilZeroerValue := (*coolTestZeroer)(nil)
	if !isZero(nilZeroerValue) {
		t.Errorf("Expected isZero to return false for nil pointer to Zeroer with non-zero value, but got true")
	}

	// Test case 11: Test Zeroer with value receiver with zero=true
	zeroerValue := coolTestZeroer{zero: true}
	if !isZero(zeroerValue) {
		t.Errorf("Expected isZero to return true for Zeroer with value receiver with zero=true, but got false")
	}

	// Test case 12: Test Zeroer with value receiver with zero=false
	nonZeroerValue := coolTestZeroer{zero: false}
	if isZero(nonZeroerValue) {
		t.Errorf("Expected isZero to return false for Zeroer with value receiver with zero=false, but got true")
	}

	// Test case 13: Test ptr to Zeroer with value receiver with zero=true
	zeroerPtrValue := &coolTestZeroer{zero: true}
	if !isZero(zeroerPtrValue) {
		t.Errorf("Expected isZero to return true for ptr to Zeroer with value receiver with zero=true, but got false")
	}
}

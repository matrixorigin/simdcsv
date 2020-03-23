package simdcsv

import (
	"reflect"
	"testing"
)

func TestFlattenBitsIncremental(t *testing.T) {

	testCases := []struct {
		masks    []uint64
		expected []uint32
	}{
		// Single mask
		{[]uint64{0b0000000000000000000000000000000000000000000000000000000000001011}, []uint32{0, 0, 1, 0, 2, 1}},
		{[]uint64{0b0000000000000000000000000000000000000000000000000000010001001011}, []uint32{0, 0, 1, 0, 2, 1, 4, 2, 7, 3}},
		{[]uint64{0b0000000000000000000000000000000000000000001000001000010001001011}, []uint32{0, 0, 1, 0, 2, 1, 4, 2, 7, 3, 11, 4, 16, 5}},
		{[]uint64{0b0000000000000000000000000000000010000000001000001000010001001011}, []uint32{0, 0, 1, 0, 2, 1, 4, 2, 7, 3, 11, 4, 16, 5, 22, 9}},
		{[]uint64{0b0000000000000100000000000000000010000000001000001000010001001011}, []uint32{0, 0, 1, 0, 2, 1, 4, 2, 7, 3, 11, 4, 16, 5, 22, 9, 32, 18}},
		{[]uint64{0b1000000000000100000000000000000010000000001000001000010001001011}, []uint32{0, 0, 1, 0, 2, 1, 4, 2, 7, 3, 11, 4, 16, 5, 22, 9, 32, 18, 51, 12}},
		{[]uint64{0b0101010101010101010101010101010101010101010101010101010101010101}, []uint32{0, 0, 1, 1, 3, 1, 5, 1, 7, 1, 9, 1, 11, 1, 13, 1, 15, 1, 17, 1, 19, 1, 21, 1, 23, 1, 25, 1, 27, 1, 29, 1, 31, 1, 33, 1, 35, 1, 37, 1, 39, 1, 41, 1, 43, 1, 45, 1, 47, 1, 49, 1, 51, 1, 53, 1, 55, 1, 57, 1, 59, 1, 61, 1}},
		////
		//// Multiple masks
		//{[]uint64{0x1, 0x1000}, []uint32{0x1, 0x4c}},
		//{[]uint64{0x1, 0x4000000000000000}, []uint32{0x1, 0x7e}},
		//{[]uint64{0x1, 0x8000000000000000}, []uint32{0x1, 0x7f}},
		//{[]uint64{0x1, 0x0, 0x8000000000000000}, []uint32{0x1, 0xbf}},
		//{[]uint64{0x1, 0x0, 0x0, 0x8000000000000000}, []uint32{0x1, 0xff}},
		//{[]uint64{0x100100100100100, 0x100100100100100}, []uint32{0x9, 0xc, 0xc, 0xc, 0xc, 0x10, 0xc, 0xc, 0xc, 0xc}},
		//{[]uint64{0xffffffffffffffff, 0xffffffffffffffff}, []uint32{
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//	0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
		//}},
	}

	for i, tc := range testCases {

		indexes := &[INDEX_SIZE]uint32{}
		length := 0
		carried := 0
		position := uint64(0)
		quote_bits := uint64(0)

		for _, mask := range tc.masks {
			flatten_bits_incremental(indexes, &length, mask, quote_bits, &carried, &position)
		}

		if length != len(tc.expected) {
			t.Errorf("TestFlattenBitsIncremental(%d): got: %d want: %d", i, length, len(tc.expected))
		}

		compare := make([]uint32, 0, 1024)
		for idx := 0; idx < length; idx++ {
			compare = append(compare, indexes[idx])
		}

		if !reflect.DeepEqual(compare, tc.expected) {
			t.Errorf("TestFlattenBitsIncremental(%d): got: %v want: %v", i, compare, tc.expected)
		}
	}
}

func TestFlattenBitsWithQuoteBits(t *testing.T) {

	testCases := []struct {
		masks    []uint64
		qbits    []uint64
		expected []uint32
	}{
		// Single mask
		{[]uint64{0b0000000000000000000000000000000000000000000000000000001000010000},
			[]uint64{0b0},
			[]uint32{0, 4, 5, 4}},
		{[]uint64{0b0000000000000000000000000000000000000000000000000000001000010000},
			[]uint64{0b0000000000000000000000000000000000000000000000000000000010010000},
			[]uint32{0, 4, 6, 2}},
		{[]uint64{0b0000000000000000010000000000000000000000100000000000001000010000},
			[]uint64{0b0},
			[]uint32{0, 4, 5, 4, 10, 13, 24, 22}},
		{[]uint64{0b0000000000000000010000000000000000000000100000000000001000010000},
			[]uint64{0b0000000000000000000000000000000000000000001000000000001000000000},
			[]uint32{0, 4, 5, 4, 11, 11, 24, 22}},
		{[]uint64{0b0000001100000000010000000000100000000000100000000000001000010000},
			[]uint64{0b0},
			[]uint32{0, 4, 5, 4, 10, 13, 24, 11, 36, 10, 47, 9, 57, 0}},
		{[]uint64{0b0000001100000000010000000000100000000000100000000000001000010000},
			[]uint64{0b0000000000000000000100000000101000000000100000000000000000000000},
			[]uint32{0, 4, 5, 4, 10, 13, 25, 9, 37, 8, 47, 9, 57, 0}},
	}

	for i, tc := range testCases {

		indexes := &[INDEX_SIZE]uint32{}
		length := 0
		carried := 0
		position := uint64(0)

		for j := range tc.masks {
			flatten_bits_incremental(indexes, &length, tc.masks[j], tc.qbits[j], &carried, &position)
		}

		if length != len(tc.expected) {
			t.Errorf("TestFlattenBitsWithQuoteBits(%d): got: %d want: %d", i, length, len(tc.expected))
		}

		compare := make([]uint32, 0, 1024)
		for idx := 0; idx < length; idx++ {
			compare = append(compare, indexes[idx])
		}

		if !reflect.DeepEqual(compare, tc.expected) {
			t.Errorf("TestFlattenBitsIncremental(%d): got: %v want: %v", i, compare, tc.expected)
		}
	}
}

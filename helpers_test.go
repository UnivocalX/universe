package universe

import (
	"context"
	"time"
)

var (
	numbers1To10        = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	lowerEnglishAlphabt = []rune{
		'a', 'b', 'c', 'd', 'e',
		'f', 'g', 'h', 'i', 'j',
		'k', 'l', 'm', 'n', 'o',
		'p', 'q', 'r', 's', 't',
		'u', 'v', 'w', 'x', 'y',
		'z',
	}
	upperEnglishAlphabt = []rune{
		'A', 'B', 'C', 'D', 'E',
		'F', 'G', 'H', 'I', 'J',
		'K', 'L', 'M', 'N', 'O',
		'P', 'Q', 'R', 'S', 'T',
		'U', 'V', 'W', 'X', 'Y',
		'Z',
	}
	reversUpperEnglishAlphabt = []rune{
		'Z', 'Y', 'X', 'W', 'V', 'U', 'T',
		'S', 'R', 'Q', 'P', 'O', 'N',
		'M', 'L', 'K', 'J', 'I', 'H',
		'G', 'F', 'E', 'D', 'C', 'B', 'A',
	}
)

type testCase[T, U any] struct {
	Input    T
	Expected U
	Context  context.Context
	Cancel   context.CancelFunc
}

func newTestCase[T, U any](input T, expected U) testCase[T, U] {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	return testCase[T, U]{Input: input, Expected: expected, Context: ctx, Cancel: cancel}
}

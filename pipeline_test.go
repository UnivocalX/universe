package universe

import (
	"errors"
	"runtime"
	"testing"
	"unicode"
)

func TestFrom(t *testing.T) {
	tc := newTestCase(numbers1To10, 10)
	defer tc.Cancel()

	// run function
	result := From(tc.Context, tc.Input...).Execute()

	// check results
	seen := make(Set[int])
	count := 0
	for env := range Collect(tc.Context, result) {
		if env.Err != nil {
			t.Fatalf("unexpected error: %v", env.Err)
		}
		if seen.Contains(env.Value) {
			t.Fatalf("received duplicate value: %v", env.Value)
		}
		seen.Add(env.Value)
		count++
	}

	if count != tc.Expected {
		t.Fatalf("expected %d values, got %d", tc.Expected, count)
	}

	for _, v := range tc.Input {
		if !seen.Contains(v) {
			t.Fatalf("missing expected value: %v", v)
		}
	}

	t.Logf("success: \"From\" emitted %d items", count)
}

func TestTransform(t *testing.T) {
	/*
		Transformer is applied to every value and the correct output is emitted
	*/
	tc := newTestCase(lowerEnglishAlphabt, upperEnglishAlphabt)
	defer tc.Cancel()

	// run
	toUpper := func(r rune) (rune, error) { return unicode.ToUpper(r), nil }
	result := From(tc.Context, tc.Input...).Transform(toUpper).Execute()

	// check
	count := 0
	for env := range result.Collect(tc.Context) {
		if env.Err != nil {
			t.Fatalf("unexpected error: %v", env.Err)
		}

		if env.Value != tc.Expected[count] {
			t.Fatalf("index: %d, expected %d, got %v", count, tc.Expected[count], env.Value)
		}
		count++
	}

	if count != len(tc.Expected) {
		t.Fatalf("expected %v total, got %v", count, len(tc.Expected))
	}

	t.Logf("success: \"TestTransform_MapsValues\" transformed every value and the correct output is emitted")
}

func TestTransform_ForwardsErrors(t *testing.T) {
	/*
		Envelopes that already carry an error are forwarded untouched, transformer is not called
	*/
	ErrFoo := errors.New("Foo error")
	tc := newTestCase(numbers1To10, ErrFoo)
	defer tc.Cancel()

	result := From(tc.Context, lowerEnglishAlphabt...).
		Transform(
			func(v rune) (rune, error) {
				return 0, ErrFoo
			},
		).
		Transform(
			func(v rune) (rune, error) {
				t.Fatalf("transformer should not be called when error is present")
				return v, nil
			},
		).
		Execute()

	for env := range result.Collect(tc.Context) {
		if !errors.Is(env.Err, ErrFoo) {
			t.Fatalf("expected error: %v, got %v", ErrFoo, env.Err)
		}
	}

	t.Logf("success: \"TestTransform_ForwardsErrors\" forwarded every error")
}

func TestTransform_FailFast_StopsOnError(t *testing.T) {
	/*
	   With FailFast policy, a transformer error cancels the pipeline
	   and no further items are emitted.
	*/

	ErrFoo := errors.New("Foo error")

	tc := newTestCase(lowerEnglishAlphabt, ErrFoo)
	defer tc.Cancel()

	count := 0

	result := From(tc.Context, lowerEnglishAlphabt...).
		Configure(WithFailFast()).
		Transform(func(v rune) (rune, error) {
			// First item triggers failure
			return 0, ErrFoo
		}).
		Execute()

	for env := range result.Collect(tc.Context) {
		count++

		if !errors.Is(env.Err, tc.Expected) {
			t.Fatalf("expected error: %v, got %v", ErrFoo, env.Err)
		}
	}

	if count != 1 {
		t.Fatalf("expected pipeline to stop after 1 item, got %d", count)
	}

	t.Logf("success: TestTransform_FailFast_StopsOnError stopped after first error")
}

func TestTransform_EmptyInput(t *testing.T) {
	/*
	   No values in, no values out, channel closes cleanly
	*/
	tc := newTestCase([]rune{}, []rune{})
	defer tc.Cancel()

	result := From[rune](tc.Context, []rune{}...).Execute()

	for env := range result.Collect(tc.Context) {
		t.Fatalf("expected no envelopes, got: %v", env)
	}

	t.Logf("success: TestTransform_EmptyInput - no values in, no values out")
}
func TestTransform_ContextCancelled(t *testing.T) {
	tc := newTestCase(lowerEnglishAlphabt, []rune{'a', 'b', 'c', 'd', 'e'})
	defer tc.Cancel()

	result := From(tc.Context, lowerEnglishAlphabt...).
		Transform(func(v rune) (rune, error) {
			return v, nil
		}).
		Execute()

	i := 0
	for env := range result.Collect(tc.Context) {
		if i >= len(tc.Expected) {
			t.Fatalf("received more items than expected: got %v at index %d", env.Value, i)
		}

		if env.Value != tc.Expected[i] {
			t.Fatalf("expected %v at index %d, got %v", tc.Expected[i], i, env.Value)
		}

		i++

		// Cancel after receiving the last expected item
		if env.Value == tc.Expected[len(tc.Expected)-1] {
			tc.Cancel()
		}
	}

	if i != len(tc.Expected) {
		t.Fatalf("expected %d items before cancel, got %d", len(tc.Expected), i)
	}

	t.Logf("success: TestTransform_ContextCancelled emitted expected prefix and stopped")
}

func TestTransform_Chained(t *testing.T) {
	/*
		Two Transform calls chained together apply both transformations in order
	*/
	tc := newTestCase(lowerEnglishAlphabt, reversUpperEnglishAlphabt)
	defer tc.Cancel()

	toUpper := func(r rune) (rune, error) { return unicode.ToUpper(r), nil }
	reverse := func(r rune) (rune, error) {
		if r >= 'A' && r <= 'Z' {
			return 'Z' - (r - 'A'), nil
		}
		return r, nil
	}

	result := From(tc.Context, lowerEnglishAlphabt...).
		Transform(toUpper).
		Transform(reverse).
		Execute()

	i := 0
	for env := range result.Collect(tc.Context) {
		if i >= len(tc.Expected) {
			t.Fatalf("received more items than expected: got %v at index %d", env.Value, i)
		}

		if env.Value != tc.Expected[i] {
			t.Fatalf("expected %v at index %d, got %v", tc.Expected[i], i, env.Value)
		}

		i++
	}

	t.Logf("success: TestTransform_Chained emitted expected values")
}

func TestFilter_Basic(t *testing.T) {
	/*
		Predicate is applied to each successful value and only matching values are emitted.
	*/
	tc := newTestCase(numbers1To10, []int{2, 4, 6, 8, 10})
	defer tc.Cancel()

	result := From(tc.Context, tc.Input...).
		Filter(func(v int) bool {
			return v%2 == 0
		}).
		Execute()

	i := 0
	for env := range result.Collect(tc.Context) {
		if env.Err != nil {
			t.Fatalf("unexpected error: %v", env.Err)
		}

		if i >= len(tc.Expected) {
			t.Fatalf("received more items than expected: %v", env.Value)
		}

		if env.Value != tc.Expected[i] {
			t.Fatalf("expected %v at index %d, got %v", tc.Expected[i], i, env.Value)
		}

		i++
	}

	if i != len(tc.Expected) {
		t.Fatalf("expected %d filtered items, got %d", len(tc.Expected), i)
	}

	t.Logf("success: TestFilter_Basic emitted only matching values")
}

func TestFilter_NoMatches(t *testing.T) {
	/*
		If predicate never matches, no values are emitted and stream closes cleanly.
	*/
	tc := newTestCase(numbers1To10, 0)
	defer tc.Cancel()

	result := From(tc.Context, tc.Input...).
		Filter(func(v int) bool {
			return v > 100
		}).
		Execute()

	count := 0
	for env := range result.Collect(tc.Context) {
		if env.Err != nil {
			t.Fatalf("unexpected error: %v", env.Err)
		}
		count++
	}

	if count != tc.Expected {
		t.Fatalf("expected %d items, got %d", tc.Expected, count)
	}

	t.Logf("success: TestFilter_NoMatches emitted no values")
}

func TestFilter_ForwardsErrors(t *testing.T) {
	/*
		Errors from previous stages are forwarded untouched and predicate is not applied to them.
	*/
	ErrFoo := errors.New("Foo error")
	tc := newTestCase(lowerEnglishAlphabt, ErrFoo)
	defer tc.Cancel()

	predicateCalls := 0

	result := From(tc.Context, tc.Input...).
		Transform(func(v rune) (rune, error) {
			return 0, ErrFoo
		}).
		Filter(func(v rune) bool {
			predicateCalls++
			return true
		}).
		Execute()

	count := 0
	for env := range result.Collect(tc.Context) {
		count++
		if !errors.Is(env.Err, tc.Expected) {
			t.Fatalf("expected error: %v, got %v", tc.Expected, env.Err)
		}
	}

	if predicateCalls != 0 {
		t.Fatalf("predicate should not run for errored envelopes, got %d calls", predicateCalls)
	}

	if count != len(tc.Input) {
		t.Fatalf("expected %d forwarded errors, got %d", len(tc.Input), count)
	}

	t.Logf("success: TestFilter_ForwardsErrors forwarded errors unchanged")
}

func TestPeek_Basic(t *testing.T) {
	/*
		Peek should observe each envelope while forwarding values unchanged.
	*/
	tc := newTestCase(numbers1To10, numbers1To10)
	defer tc.Cancel()

	observed := make([]int, 0, len(tc.Input))

	result := From(tc.Context, tc.Input...).
		Peek(func(v int, err error) {
			if err != nil {
				t.Fatalf("unexpected observer error: %v", err)
			}
			observed = append(observed, v)
		}).
		Execute()

	forwarded := make([]int, 0, len(tc.Input))
	for env := range result.Collect(tc.Context) {
		if env.Err != nil {
			t.Fatalf("unexpected stream error: %v", env.Err)
		}
		forwarded = append(forwarded, env.Value)
	}

	if len(observed) != len(tc.Expected) {
		t.Fatalf("expected %d observed values, got %d", len(tc.Expected), len(observed))
	}

	if len(forwarded) != len(tc.Expected) {
		t.Fatalf("expected %d forwarded values, got %d", len(tc.Expected), len(forwarded))
	}

	for i := range tc.Expected {
		if observed[i] != tc.Expected[i] {
			t.Fatalf("observer expected %v at index %d, got %v", tc.Expected[i], i, observed[i])
		}

		if forwarded[i] != tc.Expected[i] {
			t.Fatalf("stream expected %v at index %d, got %v", tc.Expected[i], i, forwarded[i])
		}
	}

	t.Logf("success: TestPeek_Basic observed and forwarded every value")
}

func TestPeek_ObservesErrors(t *testing.T) {
	/*
		Peek should receive and forward envelopes that carry errors.
	*/
	ErrFoo := errors.New("Foo error")
	tc := newTestCase(lowerEnglishAlphabt, ErrFoo)
	defer tc.Cancel()

	observedErrors := 0

	result := From(tc.Context, tc.Input...).
		Transform(func(v rune) (rune, error) {
			return 0, ErrFoo
		}).
		Peek(func(v rune, err error) {
			if !errors.Is(err, ErrFoo) {
				t.Fatalf("expected observer error: %v, got %v", ErrFoo, err)
			}
			observedErrors++
		}).
		Execute()

	forwardedErrors := 0
	for env := range result.Collect(tc.Context) {
		if !errors.Is(env.Err, ErrFoo) {
			t.Fatalf("expected forwarded error: %v, got %v", ErrFoo, env.Err)
		}
		forwardedErrors++
	}

	if observedErrors != len(tc.Input) {
		t.Fatalf("expected %d observed errors, got %d", len(tc.Input), observedErrors)
	}

	if forwardedErrors != len(tc.Input) {
		t.Fatalf("expected %d forwarded errors, got %d", len(tc.Input), forwardedErrors)
	}

	t.Logf("success: TestPeek_ObservesErrors observed and forwarded every error")
}

func TestMap_BasicTypeChange(t *testing.T) {
	/*
		Map should transform values and support output type changes.
	*/
	input := []rune{'a', 'b', 'c'}
	expected := []string{"a", "b", "c"}
	tc := newTestCase(input, expected)
	defer tc.Cancel()

	result := Map(
		From(tc.Context, tc.Input...),
		func(v rune) (string, error) {
			return string(v), nil
		},
	).Execute()

	i := 0
	for env := range result.Collect(tc.Context) {
		if env.Err != nil {
			t.Fatalf("unexpected error: %v", env.Err)
		}

		if i >= len(tc.Expected) {
			t.Fatalf("received more items than expected: %v", env.Value)
		}

		if env.Value != tc.Expected[i] {
			t.Fatalf("expected %q at index %d, got %q", tc.Expected[i], i, env.Value)
		}

		i++
	}

	if i != len(tc.Expected) {
		t.Fatalf("expected %d mapped values, got %d", len(tc.Expected), i)
	}

	t.Logf("success: TestMap_BasicTypeChange mapped runes to strings")
}

func TestMap_ForwardsErrors(t *testing.T) {
	/*
		Map should forward upstream errors and skip mapper for errored envelopes.
	*/
	ErrFoo := errors.New("Foo error")
	tc := newTestCase(lowerEnglishAlphabt, ErrFoo)
	defer tc.Cancel()

	mapperCalls := 0

	p := From(tc.Context, tc.Input...).
		Transform(func(v rune) (rune, error) {
			return 0, ErrFoo
		})

	result := Map(p, func(v rune) (string, error) {
		mapperCalls++
		return string(v), nil
	}).Execute()

	count := 0
	for env := range result.Collect(tc.Context) {
		if !errors.Is(env.Err, ErrFoo) {
			t.Fatalf("expected error: %v, got %v", ErrFoo, env.Err)
		}
		count++
	}

	if mapperCalls != 0 {
		t.Fatalf("mapper should not run for errored envelopes, got %d calls", mapperCalls)
	}

	if count != len(tc.Input) {
		t.Fatalf("expected %d forwarded errors, got %d", len(tc.Input), count)
	}

	t.Logf("success: TestMap_ForwardsErrors forwarded errors unchanged")
}

func TestAutoWorkerCount_Bounds(t *testing.T) {
	gomax := runtime.GOMAXPROCS(0)
	workers := resolveTotalWorkers(0)

	if workers < 1 {
		t.Fatalf("expected workers >= 1, got %d", workers)
	}

	if workers > gomax {
		t.Fatalf("expected workers <= GOMAXPROCS (%d), got %d", gomax, workers)
	}

	if workers > 8 {
		t.Fatalf("expected workers <= 8, got %d", workers)
	}
}

func TestConcurrent_AutoWorkers_ProcessesAllValues(t *testing.T) {
	tc := newTestCase(numbers1To10, 10)
	defer tc.Cancel()

	result := From(tc.Context, tc.Input...).
		Transform(func(v int) (int, error) { return v, nil }).
		Concurrent(0).
		Execute()

	seen := make(Set[int])
	count := 0
	for env := range result.Collect(tc.Context) {
		if env.Err != nil {
			t.Fatalf("unexpected error: %v", env.Err)
		}
		if seen.Contains(env.Value) {
			t.Fatalf("received duplicate value: %v", env.Value)
		}
		seen.Add(env.Value)
		count++
	}

	if count != tc.Expected {
		t.Fatalf("expected %d values, got %d", tc.Expected, count)
	}

	for _, v := range tc.Input {
		if !seen.Contains(v) {
			t.Fatalf("missing expected value: %v", v)
		}
	}
}

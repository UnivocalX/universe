package universe

import (
	"context"
	"iter"
)

// Stream represents a stream of values of type T, where each value is wrapped in an Envelope that may contain an error. The stream is read-only and can be consumed using the provided methods. Streams are designed to be used with pipelines for concurrent processing of data. If an error occurs in any stage of the pipeline, it will be emitted as part of the stream and can be handled by downstream stages.
type Stream[T any] <-chan Envelope[T]

// Drain consumes all values from the stream and discards them. This is useful for pipelines where you only care about side effects and not the final output. It also allows upstream stages to continue processing without being blocked by a full channel if the downstream stages are not consuming values.
func (s Stream[T]) Drain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-s:
			if !ok {
				return
			}
		}
	}
}

// Await consumes the stream until it is closed or an error is encountered. If the context is canceled, it will stop consuming and return the context's error. If any envelope in the stream contains an error, Await will return that error immediately. If the stream is consumed successfully without errors, it returns nil.
func (s Stream[T]) Await(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env, ok := <-s:
			if !ok {
				return nil
			}

			if env.Err != nil {
				return env.Err
			}
		}
	}
}

// All collects all values from the stream into a slice of Envelopes. If the context is canceled while collecting, it will return the values collected so far along with the context's error. This is useful for testing or when you need to work with all results at once, but be cautious when using it with large streams as it may consume a lot of memory.
func (s Stream[T]) All(ctx context.Context) []Envelope[T] {
	var results []Envelope[T]
	for {
		select {
		case <-ctx.Done():
			return results
		case env, ok := <-s:
			if !ok {
				return results
			}
			results = append(results, env)
		}
	}
}

// Collect returns a Seq that allows iterating over the values in the stream. The Seq will yield each value wrapped in an Envelope, which may contain an error. If the context is canceled while collecting, the Seq will stop yielding values and return. This allows you to use the stream with any code that can consume a Seq, such as for loops or other iteration patterns.
func (s Stream[T]) Collect(ctx context.Context) iter.Seq[Envelope[T]] {
	return func(yield func(Envelope[T]) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-s:
				if !ok {
					return
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// Take returns a slice containing up to the specified number of values from the stream. If the context is canceled while taking values, it will return the values collected so far along with the context's error. If the stream is closed before reaching the specified size, it will return all available values. This is useful for batch processing or when you want to limit the number of items processed at a time.
func (s Stream[T]) Take(ctx context.Context, size int) []Envelope[T] {
	out := make([]Envelope[T], 0, size)

	for len(out) < size {
		select {
		case <-ctx.Done():
			return out
		case v, ok := <-s:
			if !ok {
				return out
			}
			out = append(out, v)
		}
	}

	return out
}

// ForEach iterates over each value in the stream and applies the provided function to it. The function receives both the value and any error that may be associated with it. If the context is canceled while processing, ForEach will stop iterating and return. This is useful for performing side effects or processing each item in the stream without needing to collect them into a slice first.
func (s Stream[T]) ForEach(ctx context.Context, fn func(T, error)) {
	for env := range s.Collect(ctx) {
		fn(env.Unwrap())
	}
}

// Count returns the total number of values in the stream. If the context is canceled while counting, it will return the count so far along with the context's error. This is useful for determining how many items were processed or for debugging purposes to see how many items are flowing through a pipeline.
func (s Stream[T]) Count(ctx context.Context) int {
	i := 0
	for range s.Collect(ctx) {
		i++
	}
	return i
}

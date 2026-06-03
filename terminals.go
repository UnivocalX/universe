package universe

import (
	"context"
	"iter"
)

type Stream[T any] <-chan Envelope[T]

func (s Stream[T]) Drain(ctx context.Context) {
	Drain(ctx, s)
}

func Drain[T any](ctx context.Context, stream <-chan T) {
	if stream == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-stream:
			if !ok {
				return
			}
		}
	}
}

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
				// Drain the remaining items in the background so upstream goroutines
				// are not blocked trying to send to an unread channel.
				go s.Drain(ctx)
				return env.Err
			}
		}
	}
}

func (s Stream[T]) Collect(ctx context.Context) iter.Seq[Envelope[T]] {
	return Collect(ctx, (<-chan Envelope[T])(s))
}

func Collect[T any](ctx context.Context, stream <-chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-stream:
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

func (s Stream[T]) Take(ctx context.Context, size int) []Envelope[T] {
	return Take(ctx, s, size)
}

func Take[T any](ctx context.Context, stream <-chan T, size int) []T {
	out := make([]T, 0, size)

	for len(out) < size {
		select {
		case <-ctx.Done():
			return out

		case v, ok := <-stream:
			if !ok {
				return out
			}
			out = append(out, v)
		}
	}

	return out
}

func (s Stream[T]) ForEach(ctx context.Context, fn func(T, error)) {
	for env := range s.Collect(ctx) {
		fn(env.Unwrap())
	}
}

func (s Stream[T]) Count(ctx context.Context) int {
	i := 0 
	for range s.Collect(ctx) {
		i++
	}
	return i
}
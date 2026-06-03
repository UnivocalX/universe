package universe

import (
	"context"
	"sync"
)

// emit writes a value to a channel, dropping it if the context is done.
func emit[T any](ctx context.Context, stream chan<- T, value T) {
	select {
	case <-ctx.Done():
	case stream <- value:
	}
}

func Source[T any](ctx context.Context, values []T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for _, v := range values {
			emit(ctx, out, v)
		}
	}()
	return out
}

func Repeat[T any](ctx context.Context, fn func() T, size int) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for range size {
			emit(ctx, out, fn())
		}
	}()

	return out
}

// FanIn merges multiple input channels into a single output channel.
// Each input channel is consumed by its own goroutine, and all values
// are forwarded to the unified output channel concurrently.
// The output channel is closed once all input channels have been drained or
// the context is cancelled.
func FanIn[T any](ctx context.Context, streams ...<-chan T) <-chan T {
	out := make(chan T)

	var wg sync.WaitGroup
	wg.Add(len(streams))

	for _, s := range streams {
		go func(s <-chan T) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-s:
					if !ok {
						return
					}
					emit(ctx, out, v)
				}
			}
		}(s)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Tee splits a single input stream into two output channels that each receive
// the same values, similar to the Unix tee command.
// For every value read from stream, it is sent to both o1 and o2 before
// the next value is read. Both output channels are closed when the input
// stream is exhausted or the context is cancelled.
//
// Note: both consumers must be actively reading, otherwise Tee will block.
func Tee[T any](ctx context.Context, stream <-chan T) (<-chan T, <-chan T) {
	o1 := make(chan T)
	o2 := make(chan T)

	go func() {
		defer close(o1)
		defer close(o2)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-stream:
				if !ok {
					return
				}

				var t1, t2 = o1, o2

				// Send v to both channels. Each iteration of this loop delivers
				// the value to one channel; setting the channel to nil afterwards
				// prevents it from being selected again for the same value.
				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
						return
					case t1 <- v:
						t1 = nil
					case t2 <- v:
						t2 = nil
					}
				}
			}
		}
	}()

	return o1, o2
}

func Merge[T any](ctx context.Context, s1 <-chan T, s2 <-chan T) <-chan T {
	out := make(chan T)
	var wg sync.WaitGroup

	load := func(s <-chan T) {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-s:
				if !ok {
					return
				}
				emit(ctx, out, v)
			}
		}
	}

	wg.Add(2)
	go load(s1)
	go load(s2)

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Bridge flattens a channel of channels into a single output channel.
// It sequentially drains each inner channel received from sos, forwarding
// all values to the output channel. Once an inner channel is exhausted,
// Bridge waits for the next one from sos.
// The output channel is closed when sos is closed or the context is cancelled.
//
// This is useful for ordered pipelines where each stage produces a new
// stream of results that should be consumed in sequence.
func Bridge[T any](
	ctx context.Context,
	sos <-chan <-chan T,
) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)
		for {
			var stream <-chan T
			select {
			case <-ctx.Done():
				return
			case s, ok := <-sos:
				if !ok {
					return
				}
				stream = s
			}

		loop:
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-stream:
					if !ok {
						break loop
					}
					emit(ctx, out, v)
				}
			}
		}
	}()

	return out
}

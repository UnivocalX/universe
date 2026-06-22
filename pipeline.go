package universe

import (
	"context"
	"fmt"
	"runtime"
)

type Envelope[T any] struct {
	Value T
	Err   error
}

func (e Envelope[T]) String() string {
	if e.Err != nil {
		return fmt.Sprintf("Envelope{Value: %v, Err: %v}", e.Value, e.Err)
	}

	return fmt.Sprintf("Envelope{Value: %v}", e.Value)
}

func (e *Envelope[T]) Unwrap() (T, error) {
	return e.Value, e.Err
}

type Pipe[In, Out any] struct {
	policy *Policy
	ctx    context.Context
	cancel context.CancelFunc
	source <-chan Envelope[In]
	run    func(<-chan Envelope[In]) <-chan Envelope[Out]
}

func NewPipe[In, Out any](
	policy *Policy,
	ctx context.Context,
	source <-chan Envelope[In],
	run func(<-chan Envelope[In]) <-chan Envelope[Out],
) *Pipe[In, Out] {
	managedCtx, cancel := context.WithCancel(ctx)
	return &Pipe[In, Out]{
		policy: policy,
		ctx:    managedCtx,
		cancel: cancel,
		source: source,
		run:    run,
	}
}

// From creates a new pipeline from a list of values. The pipeline will emit each value as an envelope and then close.
func From[In any](ctx context.Context, values ...In) *Pipe[In, In] {
	source := make(chan Envelope[In])
	go func() {
		defer close(source)
		for _, v := range values {
			select {
			case <-ctx.Done():
				return
			case source <- Envelope[In]{Value: v}:
			}
		}
	}()

	p := NewPipe(
		NewPolicy(),
		ctx,
		source,
		func(in <-chan Envelope[In]) <-chan Envelope[In] { return in },
	)
	return p
}

// Ingest creates a new pipeline from a channel of values. The pipeline will emit each value as an envelope and then close when the input channel is closed.
func Ingest[In any](ctx context.Context, values <-chan In) *Pipe[In, In] {
	source := make(chan Envelope[In])
	go func() {
		defer close(source)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-values:
				if !ok {
					return
				}
				emit(ctx, source, Envelope[In]{Value: v})
			}
		}
	}()

	p := NewPipe(
		NewPolicy(),
		ctx,
		source,
		func(in <-chan Envelope[In]) <-chan Envelope[In] { return in },
	)

	return p
}

// clone returns a shallow copy of the pipe.
func (p *Pipe[In, Out]) clone() *Pipe[In, Out] {
	np := *p
	return &np
}

// send applies policy checks and emits the envelope. Returns false if the pipeline should stop.
func (p *Pipe[In, Out]) send(out chan<- Envelope[Out], env Envelope[Out]) bool {
	emit(p.ctx, out, env)

	if env.Err != nil && p.policy.mode == FailFast {
		p.cancel()
		return false
	}

	return true
}

// Execute runs the pipeline and returns a channel of output envelopes. The pipeline will continue to process until the input channel is closed or an error occurs (depending on the policy).
func (p *Pipe[In, Out]) Execute() Stream[Out] {
	return p.run(p.source)
}

// Configure returns a new pipeline with the given policy options applied. The original pipeline is not modified.
func (p *Pipe[In, Out]) Configure(opts ...PolicyOption) *Pipe[In, Out] {
	np := p.clone()
	np.policy = NewPolicy(opts...)
	return np
}

// Buffer returns a new pipeline with a buffered output channel of the given size. The original pipeline is not modified.
func (p *Pipe[In, Out]) Buffer(size int) *Pipe[In, Out] {
	previousRun := p.run

	if size < 0 {
		size = 0
	}

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		intermediate := previousRun(in)
		out := make(chan Envelope[Out], size)

		go func() {
			defer close(out)

			for {
				select {
				case <-np.ctx.Done():
					return
				case v, ok := <-intermediate:
					if !ok {
						return
					}
					emit(np.ctx, out, v)
				}
			}
		}()
		return out
	}

	return np
}

// Transform returns a new pipeline that applies the given transformation function to each value. The original pipeline is not modified. If the transformer returns an error, it will be emitted according to the pipeline's policy.
func (p *Pipe[In, Out]) Transform(transformer func(Out) (Out, error)) *Pipe[In, Out] {
	previousRun := p.run

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		intermediate := previousRun(in)
		out := make(chan Envelope[Out])

		go func() {
			defer close(out)

			for {
				select {
				case <-np.ctx.Done():
					return
				case incoming, ok := <-intermediate:
					if !ok {
						return
					}

					if incoming.Err != nil {
						emit(np.ctx, out, incoming)
						continue
					}

					result, err := transformer(incoming.Value)
					if !np.send(out, Envelope[Out]{Value: result, Err: err}) {
						return
					}
				}
			}
		}()

		return out
	}
	return np
}

// Filter returns a new pipeline that only emits values for which the predicate returns true. The original pipeline is not modified. Errors are emitted according to the pipeline's policy.
func (p *Pipe[In, Out]) Filter(predicate func(Out) bool) *Pipe[In, Out] {
	previousRun := p.run

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		intermediate := previousRun(in)
		out := make(chan Envelope[Out])

		go func() {
			defer close(out)
			// Filter is part of the hot path, so we keep the direct select loop instead of OrDone.
			for {
				select {
				case <-np.ctx.Done():
					return
				case incoming, ok := <-intermediate:
					if !ok {
						return
					}

					if incoming.Err != nil {
						emit(np.ctx, out, incoming)
						continue
					}

					if predicate(incoming.Value) {
						if !np.send(out, incoming) {
							return
						}
					}
				}
			}
		}()

		return out
	}
	return np
}

// Expand applies a generator function to each value in the pipeline, returning a new pipeline that emits all values produced by the generator. The original pipeline is not modified. If the generator returns an empty slice, no values are emitted for that input. Errors are emitted according to the pipeline's policy.
func (p *Pipe[In, Out]) Expand(generator func(Out) []Out) *Pipe[In, Out] {
	previousRun := p.run

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		intermediate := previousRun(in)
		out := make(chan Envelope[Out])

		go func() {
			defer close(out)
			// Expand is still core pipeline work, so we avoid OrDone's extra wrapper here too.
			for {
				select {
				case <-np.ctx.Done():
					return
				case incoming, ok := <-intermediate:
					if !ok {
						return
					}

					if incoming.Err != nil {
						emit(np.ctx, out, incoming) // forward error as-is
						continue
					}

					for _, result := range generator(incoming.Value) {
						if !np.send(out, Envelope[Out]{Value: result}) {
							return
						}
					}
				}
			}
		}()

		return out
	}

	return np
}

func resolveTotalWorkers(workers int) int {
	if workers > 0 {
		return workers
	}

	workers = runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}

	// Keep default auto-parallelism conservative to avoid oversubscription overhead.
	if workers > 8 {
		workers = 8
	}

	return workers
}

// Concurrent returns a new pipeline that processes values concurrently using the specified number of workers. The original pipeline is not modified. If workers is less than or equal to zero, it will default to the number of available CPU cores (up to a maximum of 8). Each worker will process values independently, and the output will be merged into a single stream. Errors are emitted according to the pipeline's policy.
func (p *Pipe[In, Out]) Concurrent(workers int) *Pipe[In, Out] {
	previousRun := p.run
	workers = resolveTotalWorkers(workers)

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		streams := make([]<-chan Envelope[Out], workers)
		for i := 0; i < workers; i++ {
			streams[i] = previousRun(in)
		}

		// Concurrent fans out work, so we keep the direct fan-in path to avoid extra OrDone wrappers.
		return FanIn(np.ctx, streams...)
	}

	return np
}

// Peek returns a new pipeline that allows observing each value and error as they pass through, without modifying them. The original pipeline is not modified. The observer function will be called for every value and error emitted by the pipeline, allowing for side effects such as logging or metrics. If the observer panics, the pipeline will be canceled and the panic will propagate.
func (p *Pipe[In, Out]) Peek(observer func(Out, error)) *Pipe[In, Out] {
	previousRun := p.run

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		intermediate := previousRun(in)
		out := make(chan Envelope[Out])

		go func() {
			defer close(out)
			for {
				select {
				case <-np.ctx.Done():
					return
				case incoming, ok := <-intermediate:
					if !ok {
						return
					}
					observer(incoming.Value, incoming.Err)
					emit(np.ctx, out, incoming) // forward error as-is
				}
			}
		}()

		return out
	}

	return np
}

// Operate returns a new pipeline that applies the given operator function to each envelope. The original pipeline is not modified. The operator function receives the entire envelope, allowing it to inspect and modify both the value and error. If the operator returns an error, it will be emitted according to the pipeline's policy.
func (p *Pipe[In, Out]) Operate(operator func(Envelope[Out]) Envelope[Out]) *Pipe[In, Out] {
	previousRun := p.run

	np := p.clone()
	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Out] {
		intermediate := previousRun(in)
		out := make(chan Envelope[Out])

		go func() {
			defer close(out)
			for {
				select {
				case <-np.ctx.Done():
					return
				case incoming, ok := <-intermediate:
					if !ok {
						return
					}
					result := operator(incoming)
					if !np.send(out, result) {
						return
					}
				}
			}
		}()

		return out
	}
	return np
}

// Handle is a convenience method for operators that only need to modify the value and error directly, without needing access to the full envelope. The handler function receives the value and error as separate parameters and returns the new value and error. If the handler returns an error, it will be emitted according to the pipeline's policy.
func (p *Pipe[In, Out]) Handle(handler func(Out, error) (Out, error)) *Pipe[In, Out] {
	return p.Operate(func(env Envelope[Out]) Envelope[Out] {
		v, err := handler(env.Value, env.Err)
		return Envelope[Out]{Value: v, Err: err}
	})
}

// Map returns a new pipeline that applies a transformation function to the output of another pipeline. The original pipeline is not modified. If the transformer returns an error, it will be emitted according to the pipeline's policy. This is a convenience method for common cases of Transform where the output type changes.
func Map[In, Out, Transformed any](p *Pipe[In, Out], transformer func(Out) (Transformed, error)) *Pipe[In, Transformed] {
	np := NewPipe[In, Transformed](p.policy, p.ctx, p.source, nil)

	np.run = func(in <-chan Envelope[In]) <-chan Envelope[Transformed] {
		intermediate := p.run(in)
		out := make(chan Envelope[Transformed])

		go func() {
			defer close(out)
			for {
				select {
				case <-np.ctx.Done():
					return
				case incoming, ok := <-intermediate:
					if !ok {
						return
					}

					if incoming.Err != nil {
						// retype the error envelope — can't forward directly due to type change
						emit(np.ctx, out, Envelope[Transformed]{Err: incoming.Err}) // forward error as-is
						continue
					}

					result, err := transformer(incoming.Value)
					if !np.send(out, Envelope[Transformed]{Value: result, Err: err}) {
						return
					}
				}
			}
		}()

		return out
	}

	return np
}
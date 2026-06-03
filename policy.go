package universe

type Mode int

const (
	Durable  Mode = iota // Pass errors through, continue processing
	FailFast             // Stop immediately on first error
)

type Policy struct {
	mode Mode
}

type PolicyOption func(*Policy)

func WithDurable() PolicyOption {
	return func(p *Policy) {
		p.mode = Durable
	}
}

func WithFailFast() PolicyOption {
	return func(p *Policy) {
		p.mode = FailFast
	}
}

func NewPolicy(opts ...PolicyOption) *Policy {
	p := &Policy{
		mode: Durable, // default
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

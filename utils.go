package universe

type Void struct{}
type Set[T comparable] map[T]Void

func (s *Set[T]) Add(v T) bool {
	if *s == nil {
		*s = make(Set[T])
	}
	_, exists := (*s)[v]
	(*s)[v] = Void{}
	return !exists
}

func (s *Set[T]) Remove(v T) bool {
	if *s == nil { return false}
	delete(*s, v)
	return true
}

func (s *Set[T]) IsEmpty() bool {
	return *s == nil
}

func (s *Set[T]) Contains(v T) bool {
	_, ok := (*s)[v]
	return ok
}
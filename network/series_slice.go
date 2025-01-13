package network

type datumSlice[T any] struct {
	index int
	m     []T
}

func (ss *datumSlice[T]) Add(t T) {
	ss.m = append(ss.m, t)
	ss.index++
}

func (ss *datumSlice[T]) Len() int {
	return ss.index
}

func (ss *datumSlice[T]) reset() {
	ss.m = ss.m[:0]
	ss.index = 0
}

func (ss *datumSlice[T]) SliceAndReset() []T {
	sl := ss.m[:ss.index]
	ss.reset()
	return sl
}

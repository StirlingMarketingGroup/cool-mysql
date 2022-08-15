package mysql

import "sync"

type synct[T any] struct {
	mx sync.RWMutex
	v  T
}

func (s *synct[T]) Get() T {
	s.mx.RLock()
	defer s.mx.RUnlock()

	return s.v
}

func (s *synct[T]) Set(v T) {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.v = v
}

package data

import "sync"

// Set struct
type Set struct {
	m map[string]bool
	sync.RWMutex
}

// New func
func New() *Set {
	return &Set{
		m: map[string]bool{},
	}
}

// Add func
func (s *Set) Add(item string) {
	s.Lock()
	defer s.Unlock()
	s.m[item] = true
}

// Remove func
func (s *Set) Remove(item string) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, item)
}

// Has func
func (s *Set) Has(item string) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[item]
	return ok
}

// Len func
func (s *Set) Len() int {
	return len(s.List())
}

// Clear func
func (s *Set) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = map[string]bool{}
}

// IsEmpty func
func (s *Set) IsEmpty() bool {
	if s.Len() == 0 {
		return true
	}
	return false
}

func (s *Set) List() []string {
	s.RLock()
	defer s.RUnlock()
	list := []string{}
	for item := range s.m {
		list = append(list, item)
	}
	return list
}

// SortList func
// func (s *Set) SortList() []string {
// 	s.RLock()
// 	defer s.RUnlock()
// 	list := []string{}
// 	for item := range s.m {
// 		list = append(list, item)
// 	}
// 	sort.Ints(list)
// 	return list
// }

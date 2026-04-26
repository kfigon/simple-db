package sciore

import (
	"fmt"
)

const PageSize = 4 * 1024

func must[T any](v T, err error) T {
	if err != nil {
		panic(err.Error())
	}
	return v
}

func assert(v bool, format string, args ...any) {
	if !v {
		panic(fmt.Sprintf(format, args...))
	}
}

// file manager
type Storage struct {
	pages []Page
}

func NewStorage() *Storage {
	return &Storage{
		pages: make([]Page, 0, 20),
	}
}

func (s *Storage) ReadPage(pid PageID) *Page {
	assert(int(pid) < len(s.pages), "invalid size %d >= %d", pid, len(s.pages))
	out := s.pages[pid]
	return &out
}

func (s *Storage) WritePage(pid PageID, p *Page) {
	assert(int(pid) < len(s.pages), "invalid size %d >= %d", pid, len(s.pages))
	s.pages[pid] = *p
}

func (s *Storage) Append(p *Page) PageID {
	s.pages = append(s.pages, *p)
	return PageID(len(s.pages) - 1)
}

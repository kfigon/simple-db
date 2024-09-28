package storagemanager

import (
	"simple-db/page"
)

type InMemoryPager struct {
	data []byte
}

func NewInMemoryPager() *InMemoryPager {
	return &InMemoryPager{
		data: make([]byte, 30*page.PageSize),
	}
}

func (i *InMemoryPager) ReadPage(pageId page.PageId) []byte {
	startIdx := pageId*page.PageSize
	return i.data[startIdx:startIdx+page.PageSize]
}

// todo: add grow method
func (i *InMemoryPager) WritePage(pageId page.PageId, data []byte) error {
	startIdx := pageId*page.PageSize
	copy(i.data[startIdx:], data)
	return nil
}

func (i *InMemoryPager) PageNum() int {
	return len(i.data)/page.PageSize
}
package storagemanager

import (
	"bytes"
	"simple-db/page"
)

type InMemoryPager struct {
	data bytes.Buffer
}

func (i *InMemoryPager) ReadPage(pageId page.PageId) []byte {
	return nil
}

func (i *InMemoryPager) WritePage(pageId page.PageId, data []byte) error {
	return nil
}
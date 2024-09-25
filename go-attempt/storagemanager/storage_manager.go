package storagemanager

import (
	"simple-db/page"
)

type OsInterface interface {
	ReadPage(page.PageId) []byte
	WritePage(page.PageId, []byte) error
}

type StorageManager struct {
	RootPage *page.RootPage
	Directory page.DirectoryPage
	SchemaPages []page.SchemaPage
	DataPages []page.DataPage

	OsInterface
}

func NewStorageManager(root *page.RootPage) *StorageManager {
	// todo: init. build internal state of directory and schemas
	// from basic pages
	return &StorageManager{
		Directory:   page.DirectoryPage{},
		SchemaPages: []page.SchemaPage{},
		DataPages:   []page.DataPage{},
		OsInterface: &InMemoryPager{},
	}
}
package storagemanager

import (
	"simple-db/page"
)

type OsInterface interface {
	ReadPage(page.PageId) []byte
	WritePage(page.PageId, []byte) error
}

type StorageManager struct {
	RootPage page.RootPage
	Directory page.GenericPage[page.DirectoryEntry]
	Schema page.GenericPage[page.SchemaEntry]

	OsInterface
}

func NewEmptyStorageManager() *StorageManager {
	rootPage := page.NewRootPage()
	directory := page.NewDirectoryPage()
	schema := page.NewSchemaPage()

	rootPage.DirectoryPageRootID = 1
	rootPage.SchemaPageRootID = 2

	// todo: serialize
	
	return &StorageManager{
		RootPage: page.NewRootPage(),
		Directory: directory,
		Schema: schema,
		OsInterface: NewInMemoryPager(),
	}
}

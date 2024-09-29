package storagemanager

import (
	"simple-db/page"
	"simple-db/utils"
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
	rootPage.LastFreePage = 3

	out := &StorageManager{
		RootPage: page.NewRootPage(),
		Directory: directory,
		Schema: schema,
		OsInterface: NewInMemoryPager(),
	}

	out.OsInterface.WritePage(0, out.RootPage.Serialize())
	out.OsInterface.WritePage(rootPage.DirectoryPageRootID, out.Directory.Serialize())
	out.OsInterface.WritePage(rootPage.SchemaPageRootID, out.Schema.Serialize())

	return out
}

func (s *StorageManager) CreateTable(name string, schema []utils.Pair[string, string]) {
	// todo: create directory entry
	// create schema entries and link together

	schemaEntries := []page.SchemaEntry{}

	dirEntry := page.DirectoryEntry{
		DataRootPageID:   0,
		SchemaRootRecord: page.RecordID{},
		ObjectType:       0,
		ObjectName:       "",
	}
}

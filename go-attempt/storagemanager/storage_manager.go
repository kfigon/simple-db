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

	os OsInterface
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
		os: NewInMemoryPager(),
	}

	out.os.WritePage(0, out.RootPage.Serialize())
	out.os.WritePage(rootPage.DirectoryPageRootID, out.Directory.Serialize())
	out.os.WritePage(rootPage.SchemaPageRootID, out.Schema.Serialize())

	return out
}

func (s *StorageManager) CreateTable(name string, schema []utils.Pair[string, page.FieldType]) {
	schemaEntries := []page.SchemaEntry{}
	for _, sch := range schema {
		schemaEntries = append(schemaEntries, page.SchemaEntry{
			FieldTyp:  sch.B,
			IsNull:    false,
			FieldName: sch.A,
			Next:      page.RecordID{}, // todo: fill with data when persisting
		}) // this is a common pattern - create object, assign ids later
	}

	dirEntry := page.DirectoryEntry{
		DataRootPageID:   0,
		SchemaRootRecord: page.RecordID{},
		ObjectType:       0,
		ObjectName:       "",
	}
	_ = dirEntry
}

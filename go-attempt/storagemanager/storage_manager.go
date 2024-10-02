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


	dirEntries []page.DirectoryEntry // todo: for now just in mem
	schemaEntries []page.SchemaEntry // todo: for now just in mem

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

func (s *StorageManager) nextFreeDataPage(name string) page.PageId {
	for _, directoryEntry := range s.dirEntries {
		if directoryEntry.ObjectName == page.String(name) {
			
			var lastPageId page.PageId
			pageId := directoryEntry.DataRootPageID
			for pageId != 0 {
				lastPageId = pageId
				pageId = directoryEntry.DataRootPageID
			}
			return lastPageId
		}
	}
	return s.RootPage.LastFreePage
}

func (s *StorageManager) CreateTable(name string, schema []utils.Pair[string, page.FieldType]) {
	for _, sch := range schema {
		s.schemaEntries = append(s.schemaEntries, page.SchemaEntry{
			FieldTyp:  sch.B,
			IsNull:    false,
			FieldName: page.String(sch.A),
			// Next:      page.RecordID{}, // todo: fill with data when persisting
		}) // this is a common pattern - create object, assign ids later
	}

	s.dirEntries = append(s.dirEntries, page.DirectoryEntry{
		// DataRootPageID:   0,
		// SchemaRootRecord: page.RecordID{},
		ObjectType:       page.DataPageType,
		ObjectName:       page.String(name),
	})
}

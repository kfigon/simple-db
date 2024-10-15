package storagemanager

import (
	"simple-db/page"
	"simple-db/sql"
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
	data []page.SlottedPage

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

func (s *StorageManager) CreateTable(statement *sql.CreateStatement) error {
	return nil
}

func (s *StorageManager) Insert(statement *sql.InsertStatement) (page.RecordID, error) {
	var out page.RecordID
	return out, nil
}

func (s *StorageManager) Select(statement *sql.SelectStatement) ([]string, [][]string){
	return nil, nil
}
package storagemanager

import (
	"fmt"
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
	for _, v := range s.dirEntries {
		if string(v.ObjectName) == statement.Table {
			return fmt.Errorf("error creating table: %v already present in the db", statement.Table)
		}
	}

	newDirEntry := page.DirectoryEntry{
		DataRootPageID:   page.PageId(len(s.data)), // todo: get next free page
		SchemaRootRecord: page.RecordID{
			PageID: nextPageId(),
			SlotID: page.SlotIdx(len(s.schemaEntries)),
		},
		ObjectType:       page.DataPageType,
		ObjectName:       page.String(statement.Table),
	}
	s.dirEntries = append(s.dirEntries, newDirEntry)
	s.data = append(s.data, *page.NewEmptySlottedPage())

	for i, column := range statement.Columns {
		field, err := toFieldType(column.Typ)
		if err != nil {
			return fmt.Errorf("error creating table: %w", err)
		}

		var nextPage page.RecordID
		if i < len(statement.Columns)-1 {
			nextPage = page.RecordID{
				PageID: nextPageId(),
				SlotID: page.SlotIdx(len(s.schemaEntries) + 1),
			}
		}

		s.schemaEntries = append(s.schemaEntries, page.SchemaEntry{
			Next:      nextPage,
			FieldTyp:  field,
			IsNull:    false,
			FieldName: page.String(column.Name),
		})
	}

	return nil
}

func nextPageId() page.PageId {
	return 1 // todo
}

func toFieldType(typ string) (page.FieldType, error) {
	switch typ {
	case "varchar", "string": return page.StringType, nil
	case "boolean": return page.I8Type, nil
	case "short": return page.I16Type, nil
	case "int": return page.I32Type, nil
	case "int64": return page.I64Type, nil
	default: return 0, fmt.Errorf("unknown column type: %v", typ)
	}
}

func (s *StorageManager) Insert(statement *sql.InsertStatement) (page.RecordID, error) {
	found := func() bool {
		for _, dir := range s.dirEntries {
			if string(dir.ObjectName) == statement.Table {
				return true
			}
		}
		return false
	}
	
	if !found() {
		return page.RecordID{}, fmt.Errorf("error on insert: %v table not found", statement.Table)
	}

	var out page.RecordID
	return out, nil
}

func (s *StorageManager) Select(statement *sql.SelectStatement) ([]string, [][]string){
	return nil, nil
}
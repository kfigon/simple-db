package naive

import (
	"bytes"
	"iter"
	"simple-db/sql"
)

type StorageEngine struct {
	root     RootPage
	allPages []byte
}

func NewStorageEngine() *StorageEngine {
	return &StorageEngine{}
}

func (s *StorageEngine) AllocatePage(pageTyp PageType, name string) (PageID, *GenericPage) {
	// find last page of the type
	// connect to previous page
	// or allocate complete new page, mark as start
	// count number of pages
	return 0, nil
}

func (s *StorageEngine) GetSchema2() Schema2 {
	out := Schema2{}

	for sch := range s.SchemaTuples() {
		got, err := sql.Parse(sql.Lex(sch.SqlStatement))
		debugAsserErr(err, "schema corruption, invalid sql statement for table: %s", sch.Name)
		createStmt, ok := got.(*sql.CreateStatement)
		debugAssert(ok, "schema corruption, invalid sql statement for table: %s, should be create statement, got %T", sch.Name, got)

		res := TableSchema2{}
		for _, data := range createStmt.Columns {
			f, err := FieldTypeFromString(data.Typ)
			debugAsserErr(err, "schema corruption, invalid type for table %s: ", sch.Name)

			res.FieldNames = append(res.FieldNames, FieldName(data.Name))
			res.FieldsTypes = append(res.FieldsTypes, f)
		}
		res.StartPage = sch.StartingPageID
		out[TableName(sch.Name)] = res
	}

	return out
}

func (s *StorageEngine) SchemaTuples() iter.Seq[SchemaTuple] {
	return func(yield func(SchemaTuple) bool) {
		for _, page := range s.ReadPages(s.root.SchemaPageStart) {
			if page.PageTyp != SchemaPageType {
				// should not happen
				continue
			}

			p := must(DeserializeGenericPage(&page.GenericPageHeader, bytes.NewBuffer(page.data)))
			for tup := range p.Iterator() {
				sch := must(SchemaTupleFromTuple(tup))
				if !yield(*sch) {
					return
				}
			}
		}
	}
}

func (s *StorageEngine) AllocateOverflowPage(data []byte) PageID {
	// allocate pages to fit all data
	// count pages
	// return first page ID
	return 0
}

func (s *StorageEngine) ReadPage(p PageID) (*GenericPageHeader, []byte, bool) {
	return nil, nil, false
}

func (s *StorageEngine) persistPage(id PageID, pageData []byte) {
	// store in persistance medium (in mem now)
}

func (s *StorageEngine) AddTuple(pid PageID, t Tuple) error {
	// add tuple to the page id
	// check if the page type is fine
	return nil
}

func (s *StorageEngine) ReadPages(startingPageID PageID) PageIteratorCombined {
	// generic pager for all types, byte content. Add convenience methods to deserialize
	return nil
}

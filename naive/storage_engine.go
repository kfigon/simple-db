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
	s := &StorageEngine{
		allPages: make([]byte, 20*PageSize),
	}

	s.root = NewRootPage()
	schemaID, _ := s.AllocatePage(SchemaPageType, schemaName)

	s.root.SchemaPageStart = schemaID
	// todo: optimise this, root persist is done also in dir and schema allocations, but misses setting dir and schema ids
	s.persistPage(0, s.root.Serialize())

	return s
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

func (s *StorageEngine) AllocatePage(pageTyp PageType, name string) (PageID, *GenericPage) {
	// find last page of the type
	// connect to previous page
	// or allocate complete new page, mark as start
	// count number of pages

	p := NewPage(pageTyp, PageSize)
	newPageID := PageID(s.root.NumberOfPages)

	// link last page to the new one
	if startPage, ok := findStartingPage(s.GetSchema2(), pageTyp, name); ok {
		var lastPageID PageID
		for id := range s.ReadPages(startPage) {
			lastPageID = id
		}
		lastPage, _ := s.ReadGenericPage(lastPageID)
		lastPage.Header.NextPage = newPageID
		s.persistPage(lastPageID, lastPage.Serialize())
	}

	s.root.NumberOfPages++
	s.persistPage(0, s.root.Serialize())
	s.persistPage(newPageID, p.Serialize())

	return newPageID, p
}

func findStartingPage(s Schema2, pageTyp PageType, name string) (PageID, bool) {
	for tableName, tableSchema := range s {
		if pageTyp == tableSchema.PageTyp && name == string(tableName) {
			return tableSchema.StartPage, true
		}
	}
	return 0, false
}

func (s *StorageEngine) AllocateOverflowPage(data []byte) PageID {
	// allocate pages to fit all data
	// count pages
	// return first page ID
	firstPageID := PageID(s.root.NumberOfPages)

	type pair struct {
		pid  PageID
		page *OverflowPage
	}

	overFlowPages := make([]*pair, 0)
	idx := 0
	for {
		newPage, rest := NewOverflowPage(PageSize, data)
		newPageID := PageID(s.root.NumberOfPages)

		overFlowPages = append(overFlowPages, &pair{newPageID, newPage})

		if idx > 0 {
			overFlowPages[idx-1].page.Header.NextPage = newPageID
		}

		s.root.NumberOfPages++

		if len(rest) == 0 {
			break
		}
		idx++
	}

	for _, p := range overFlowPages {
		s.persistPage(p.pid, p.page.Serialize())
	}

	s.persistPage(0, s.root.Serialize())
	return firstPageID
}

func (s *StorageEngine) ReadPage(id PageID) (*GenericPageHeader, []byte, bool) {
	offset := byteOffsetFromPageID(id)
	if offset >= len(s.allPages) {
		return nil, nil, false
	}

	pageBytes := s.allPages[offset : offset+PageSize]
	buf := bytes.NewBuffer(pageBytes)
	header := must(DeserializeGenericHeader(buf))
	return header, buf.Bytes(), true
}

// convenience method to read and deserialize page
func (s *StorageEngine) ReadGenericPage(id PageID) (*GenericPage, bool) {
	header, rest, ok := s.ReadPage(id)
	if !ok {
		return nil, false
	}
	p := must(DeserializeGenericPage(header, bytes.NewBuffer(rest)))
	return p, true
}

// store in persistance medium (in mem now)
func (s *StorageEngine) persistPage(id PageID, pageData []byte) {
	debugAssert(len(pageData) == PageSize, "enforcing page size")
	offset := byteOffsetFromPageID(id)

	// realloc if needed
	if offset+len(pageData) >= len(s.allPages) {
		newBytes := make([]byte, PageSize*2*s.root.NumberOfPages)
		copy(newBytes, s.allPages)
		s.allPages = newBytes
	}
	copy(s.allPages[offset:offset+len(pageData)], pageData)
}

func (s *StorageEngine) AddTuple(pid PageID, t Tuple) error {
	// add tuple to the page id
	// todo: check if the page type is fine?

	got, ok := s.ReadGenericPage(pid)
	debugAssert(ok, "invalid page id: %d", pid)
	_, err := got.Add(t)
	return err
}

func (s *StorageEngine) ReadPages(startingPageID PageID) PageIteratorCombined {
	// generic pager for all types, byte content
	return func(yield func(PageID, CombinedPageIteratorEntry) bool) {
		for pageID := startingPageID; pageID != 0; {
			header, bytes, ok := s.ReadPage(pageID)
			debugAssert(ok, "invalid pageID stored in the chain, page not found: %d", pageID)

			if !yield(pageID, CombinedPageIteratorEntry{*header, bytes}) {
				return
			}
			pageID = header.NextPage
		}
	}
}

package naive

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"simple-db/sql"
)

type StorageEngine struct {
	root     RootPage
	allPages []byte
}

// to support generic pages and overflows
type CombinedPageIteratorEntry struct {
	GenericPageHeader
	data []byte
}
type PageIteratorCombined iter.Seq2[PageID, CombinedPageIteratorEntry]

func byteOffsetFromPageID(p PageID) int {
	return int(p) * PageSize
}

func NewStorageEngine() *StorageEngine {
	s := &StorageEngine{
		allPages: make([]byte, 20*PageSize),
	}

	s.root = NewRootPage()
	schemaID, schemaPage := s.AllocatePage(DataPageType, schemaName)

	s.root.SchemaPageStart = schemaID
	// todo: this is a bad workaround, that I need a schema for schema tuple. can we do better?
	schemaPage.Add(SchemaTuple{
		PageTyp:        DataPageType,
		StartingPageID: schemaID,
		Name:           schemaName,
		SqlStatement:   SchemaTypleSql,
	}.ToTuple())

	// todo: optimise this, root persist is done also in dir and schema allocations, but misses setting dir and schema ids
	s.persistPage(0, s.root.Serialize())
	s.persistPage(schemaID, schemaPage.Serialize())

	return s
}

func NewStorageEngineWithData(root *RootPage, allPages []byte) *StorageEngine {
	return &StorageEngine{*root, allPages}
}

func (s *StorageEngine) GetSchema() Schema {
	out := Schema{}

	for sch := range s.SchemaTuples() {
		got, err := sql.Parse(sql.Lex(sch.SqlStatement))
		debugAsserErr(err, "schema corruption, invalid sql statement for table: %s", sch.Name)
		createStmt, ok := got.(*sql.CreateStatement)
		debugAssert(ok, "schema corruption, invalid sql statement for table: %s, should be create statement, got %T", sch.Name, got)

		res := TableSchema{}
		for _, data := range createStmt.Columns {
			f, err := FieldTypeFromString(data.Typ)
			debugAsserErr(err, "schema corruption, invalid type for table %s: ", sch.Name)

			res.FieldNames = append(res.FieldNames, FieldName(data.Name))
			res.FieldsTypes = append(res.FieldsTypes, f)
		}
		res.StartPage = sch.StartingPageID
		res.PageTyp = sch.PageTyp
		out[TableName(sch.Name)] = res
	}

	return out
}

func (s *StorageEngine) SchemaTuples() iter.Seq[SchemaTuple] {
	return func(yield func(SchemaTuple) bool) {
		for tup := range s.Tuples(s.root.SchemaPageStart) {
			sch := must(SchemaTupleFromTuple(tup))
			if !yield(*sch) {
				return
			}
		}
	}
}

func (s *StorageEngine) Tuples(startingPageId PageID) iter.Seq[Tuple] {
	return func(yield func(Tuple) bool) {
		for _, page := range s.ReadPages(startingPageId) {
			if page.PageTyp != DataPageType {
				debugAssert(false, "page type %v != dataPageType", page.PageTyp)
				continue
			}

			p := must(DeserializeGenericPage(&page.GenericPageHeader, bytes.NewBuffer(page.data)))
			for tup := range p.Iterator() {
				if !yield(tup) {
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
	if startPage, ok := FindStartingPage(s.GetSchema(), name); ok {
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

func (s *StorageEngine) AddTuple(name string, t Tuple) (PageID, *GenericPage, error) {
	schema := s.GetSchema()

	pid, ok := s.FindLastPage(schema, name)
	if !ok {
		return 0, nil, fmt.Errorf("page for %v not found", name)
	}
	page, ok := s.ReadGenericPage(pid)
	debugAssert(ok, "data corruption, can't find page %d", pid)

	t = s.repackTupleForOverflows(t)

	_, err := page.Add(t)
	if errors.Is(err, errNoSpace) {
		// realloc
		newPageID, newPage := s.AllocatePage(page.Header.PageTyp, name)
		_, err = newPage.Add(t)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to realloc page for %s: %w", name, err)
		}
		s.persistPage(newPageID, newPage.Serialize())
		return newPageID, newPage, nil
	} else if err != nil {
		return 0, nil, fmt.Errorf("failed to add tuple to page %d for %s: %w", pid, name, err)
	}

	s.persistPage(pid, page.Serialize())
	return pid, page, nil
}

func (s *StorageEngine) repackTupleForOverflows(t Tuple) Tuple {
	for i := 0; i < int(t.NumberOfFields); i++ {
		typ := t.ColumnTypes[i]
		val := t.ColumnDatas[i]

		if typ == StringField && len(val) >= PageSize/2 {
			overFlowPageStartID := s.AllocateOverflowPage(val)
			first := SerializeInt(int32(len(val)))
			second := SerializeInt(int32(overFlowPageStartID))
			serializedData := make([]byte, 0, 4+4)
			serializedData = append(serializedData, first...)
			serializedData = append(serializedData, second...)
			t.ColumnTypes[i] = OverflowField
			t.ColumnDatas[i] = serializedData
		}
	}
	return t
}

func FindStartingPage(s Schema, name string) (PageID, bool) {
	got, ok := s[TableName(name)]
	if !ok {
		return 0, false
	}
	return got.StartPage, true
}

func (s *StorageEngine) FindLastPage(sch Schema, name string) (PageID, bool) {
	pid, ok := FindStartingPage(sch, name)
	if !ok {
		return 0, false
	}

	var lastPageID PageID
	for id := range s.ReadPages(pid) {
		lastPageID = id
	}
	return lastPageID, true
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

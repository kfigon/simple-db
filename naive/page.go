package naive

import (
	"fmt"
	"io"
	"iter"
)

type RootPage struct {
	PageTyp PageType
	MagicNumber     int32
	PageSize        int32
	SchemaStartPage PageID
	DataPageStart   PageID
}

func NewRootPage(schemaStart PageID, dataStart PageID) RootPage {
	return RootPage{
		PageTyp: RootPageType,
		MagicNumber: 	 MagicNumber,
		PageSize:        PageSize,
		SchemaStartPage: schemaStart,
		DataPageStart: 	 dataStart,
	}
}

const MagicNumber int32 = 0xc0de

func (r *RootPage) Serialize() []byte {
	return SerializeAll(
		SerializeInt(r.MagicNumber),
		SerializeInt(r.PageSize),
		SerializeInt(int32(r.SchemaStartPage)),
		SerializeInt(int32(r.DataPageStart)))
}

func DeserializeRootPage(r io.Reader) (*RootPage, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("error reading root page: %w", err)
	}
	root, err := DeserializeAll(bytes,
		compose("magic num", func(st *RootPage) *int32 { return &st.MagicNumber}, intDeser),
		compose("page size", func(st *RootPage) *int32 { return &st.PageSize}, intDeser),
		compose("schema starting page", func(st *RootPage) *int32 { return (*int32)(&st.SchemaStartPage)}, intDeser),
		compose("data page start", func(st *RootPage) *int32 { return (*int32)(&st.DataPageStart)}, intDeser),
	)
	if err != nil {
		return nil, err
	}

	if root.MagicNumber != MagicNumber {
		return nil, fmt.Errorf("invalid magic num, got: %x", root.MagicNumber)
	}
	return root, nil
}

type PageType int32

const (
	RootPageType PageType = iota
	DataPageType
	SchemaPageType
	DirectoryPageType
)

type PageID int32
type PageOffset int32

type GenericPageHeader struct {
	PageTyp       PageType
	NextPage      PageID
	SlotArraySize int32 // might be too big, leave for now
}

type GenericPage struct {
	Header    GenericPageHeader
	SlotArray *Slotted
}

func NewPage(pageType PageType, pageSize int) *GenericPage {
	return &GenericPage{
		Header: GenericPageHeader{
			PageTyp: pageType,
		},
		SlotArray: NewSlotted(pageSize - 4 - 4 - 4), //12 bytes for header
	}
}

func (g *GenericPage) Add(b []byte) (SlotIdx, error) {
	r, err := g.SlotArray.Add(b)
	if err != nil {
		return 0, err
	}
	g.Header.SlotArraySize = int32(len(g.SlotArray.Indexes))
	return r, nil
}

func (g *GenericPage) Read(r SlotIdx) ([]byte, error) {
	return g.SlotArray.Read(r)
}

func (g *GenericPage) Put(r SlotIdx, b []byte) error {
	if err := g.SlotArray.Put(r, b); err != nil {
		return err
	}
	g.Header.SlotArraySize = int32(len(g.SlotArray.Indexes))
	return nil
}

func (g *GenericPage) Serialize() []byte {
	return SerializeAll(
		SerializeInt(int32(g.Header.PageTyp)),
		SerializeInt(int32(g.Header.NextPage)),
		SerializeInt(int32(g.Header.SlotArraySize)),
		g.SlotArray.Serialize())
}

func Deserialize(r io.Reader) (*GenericPage, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	header, err := DeserializeAll[GenericPageHeader](bytes,
		compose("page type", func(t *GenericPageHeader) *int32 { return (*int32)(&t.PageTyp)}, intDeser),
		compose("next page", func(t *GenericPageHeader) *int32 { return (*int32)(&t.NextPage)}, intDeser),
		compose("slot array size", func(t *GenericPageHeader) *int32 { return (*int32)(&t.SlotArraySize)}, intDeser),
	)
	if err != nil {
		return nil, fmt.Errorf("error deserializing page header: %w", err)
	}

	slotted, err := DeserializeSlotted(bytes, int(header.SlotArraySize))
	if err != nil {
		return nil, err
	}

	return &GenericPage{
		Header: *header,
		SlotArray: slotted,
	}, nil
}

type PageIterator iter.Seq2[PageID, *GenericPage]

func NewPageIteratorByType(storage *Storage, pageType PageType) PageIterator {
	var startPage PageID
	if pageType == DataPageType {
		startPage = storage.root.DataPageStart
	} else if pageType == SchemaPageType {
		startPage = storage.root.SchemaStartPage
	} // else: startPage == 0 -> empty iter

	return NewPageIteratorFromPageID(storage, startPage)
}

func NewPageIteratorFromPageID(storage *Storage, startingPage PageID) PageIterator {
	currentPageId := startingPage
	return func(yield func(PageID, *GenericPage) bool) {
		for currentPageId != 0 {
			currentPage := &storage.allPages[currentPageId]
			if !yield(currentPageId, currentPage) {
				break
			}
			currentPageId = currentPage.Header.NextPage
		}
	}
}

type PageTupleIterator iter.Seq[[]byte]

func IterTuplesByPages(storage *Storage, pages PageIterator) PageTupleIterator {
	return func(yield func([]byte) bool) {
		for _, thisPage := range pages {
			for tuple := range thisPage.SlotArray.Iterator() {
				if !yield(tuple) {
					return
				}
			}
		}
	}
}

func NewTupleIterator(storage *Storage, pageType PageType) PageTupleIterator {
	return IterTuplesByPages(storage, NewPageIteratorByType(storage, pageType))
}

func NewDataIterator(storage *Storage, tableName TableName) PageTupleIterator {
	var startId PageID
	for d := range NewTupleIterator(storage, DirectoryPageType){
		dir, err := DeserializeDirectoryTuple(d)
		if err != nil {
			break
		} else if string(tableName) == dir.Name {
			startId = dir.StartingPage
			break
		}
	}

	return IterTuplesByPages(storage, NewPageIteratorFromPageID(storage, startId))
}

// for lookup where given data/index page starts 
type DirectoryTuple struct {
	PageTyp PageType
	StartingPage PageID
	Name string
}

func (d DirectoryTuple) Serialize() []byte {
	return SerializeAll(
		SerializeInt(int32(d.PageTyp)),
		SerializeInt(int32(d.StartingPage)),
		SerializeString(d.Name))
}

func DeserializeDirectoryTuple(b []byte) (*DirectoryTuple, error) {
	return DeserializeAll[DirectoryTuple](b,
		compose("page type", func(t *DirectoryTuple) *int32 { return (*int32)(&t.PageTyp)}, intDeser),
		compose("starting page", func(st *DirectoryTuple) *int32 { return (*int32)(&st.StartingPage)}, intDeser),
		compose("name", func(st *DirectoryTuple) *string { return &st.Name}, strDeser),
	)
}

type SchemaTuple struct {
	TableNameV TableName
	FieldNameV FieldName
	FieldTypeV FieldType
}

func (s SchemaTuple) Serialize() []byte {
	return SerializeAll(
		SerializeString(string(s.TableNameV)),
		SerializeString(string(s.FieldNameV)),
		SerializeInt(int32(s.FieldTypeV)))
}

func DeserializeSchemaTuple(b []byte) (*SchemaTuple, error) {
	return DeserializeAll[SchemaTuple](b,
		compose("table name", func(t *SchemaTuple) *string { return (*string)(&t.TableNameV) }, strDeser),
		compose("field name", func(st *SchemaTuple) *string { return (*string)(&st.FieldNameV) }, strDeser),
		compose("field type", func(st *SchemaTuple) *int32 { return (*int32)(&st.FieldTypeV) }, intDeser))
}
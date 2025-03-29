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
	DirectoryPageStart   PageID
}

func NewRootPage(directoryStart PageID) RootPage {
	return RootPage{
		PageTyp: RootPageType,
		MagicNumber: 	 MagicNumber,
		PageSize:        PageSize,
		DirectoryPageStart: 	 directoryStart,
	}
}

const MagicNumber int32 = 0xc0de

func (r *RootPage) Serialize() []byte {
	got := SerializeAll(
		SerializeInt(r.MagicNumber),
		SerializeInt(r.PageSize),
		SerializeInt(int32(r.DirectoryPageStart)),
		make([]byte, PageSize-4*3),
	)
	debugAssert(len(got) == PageSize, "root page should also be size of a page")
	return got
}

func DeserializeRootPage(r io.Reader) (*RootPage, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("error reading root page: %w", err)
	}
	root, err := DeserializeAll(bytes,
		compose("magic num", func(rp *RootPage, i int32) { rp.MagicNumber = i}, DeserializeIntAndEat), 
		compose("page size", func(rp *RootPage, i int32) { rp.PageSize = i}, DeserializeIntAndEat), 
		compose("directory page start", func(rp *RootPage, i int32) { rp.DirectoryPageStart = PageID(i)}, DeserializeIntAndEat), 
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
		compose("page type", func(t *GenericPageHeader,i int32) {t.PageTyp = PageType(i)}, DeserializeIntAndEat),
		compose("next page", func(t *GenericPageHeader,i int32) {t.NextPage = PageID(i)}, DeserializeIntAndEat),
		compose("slot array size", func(t *GenericPageHeader,i int32) {t.SlotArraySize = i}, DeserializeIntAndEat),
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

func NewPageIterator(storage *Storage, startingPage PageID) PageIterator {
	currentPageId := startingPage
	return func(yield func(PageID, *GenericPage) bool) {
		for currentPageId != 0 && int(currentPageId) < len(storage.allPages){
			currentPage := &storage.allPages[currentPageId]
			if !yield(currentPageId, currentPage) {
				break
			}
			currentPageId = currentPage.Header.NextPage
		}
	}
}

func directoryPages(s *Storage) PageIterator {
	return NewPageIterator(s, s.root.DirectoryPageStart)
}

type TupleIterator iter.Seq[[]byte]

func tuplesIterator(pages PageIterator) TupleIterator {
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

func FindStartingPageForEntity(storage *Storage, pageType PageType, name string) (PageID, bool) {
	for dir := range DirectoryEntriesIterator(storage) {
		if dir.Name == name && dir.PageTyp == pageType {
			return dir.StartingPage, true
		}
	}
	return 0, false
}

func NewEntityIterator(storage *Storage, pageType PageType, name string) TupleIterator {
	startId, _ := FindStartingPageForEntity(storage, pageType, name)
	return tuplesIterator(NewPageIterator(storage, startId))
}

func DirectoryEntriesIterator(storage *Storage) iter.Seq[DirectoryTuple] {
	return func(yield func(DirectoryTuple) bool) {
		for d := range tuplesIterator(directoryPages(storage)) {
			dir := must(DeserializeDirectoryTuple(d))
			if !yield(*dir) {
				break
			}
		}
	}
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
		compose("page type", func(t *DirectoryTuple, i int32) { t.PageTyp = PageType(i)}, DeserializeIntAndEat),
		compose("starting page", func(t *DirectoryTuple, i int32) { t.StartingPage = PageID(i)}, DeserializeIntAndEat),
		compose("name", func(t *DirectoryTuple, s string) { t.Name = s}, DeserializeStringAndEat),
	)
}

type SchemaTuple struct {
	FieldNameV FieldName
	FieldTypeV FieldType
}

func (s SchemaTuple) Serialize() []byte {
	return SerializeAll(
		SerializeString(string(s.FieldNameV)),
		SerializeInt(int32(s.FieldTypeV)))
}

func DeserializeSchemaTuple(b []byte) (*SchemaTuple, error) {
	return DeserializeAll[SchemaTuple](b,
		compose("field name", func(st *SchemaTuple, s string) { st.FieldNameV = FieldName(s)}, DeserializeStringAndEat),
		compose("field type", func(st *SchemaTuple, s int32) { st.FieldTypeV = FieldType(s)}, DeserializeIntAndEat))
}
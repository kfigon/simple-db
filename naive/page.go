package naive

import (
	"bytes"
	"fmt"
	"io"
)

type RootPage struct {
	PageTyp            PageType
	MagicNumber        int32
	PageSize           int32
	DirectoryPageStart PageID
	SchemaPageStart    PageID
	LogPageStart       PageID
	NumberOfPages      int32
}

func NewRootPage() RootPage {
	return RootPage{
		PageTyp:     RootPageType,
		MagicNumber: MagicNumber,
		PageSize:    PageSize,
	}
}

const MagicNumber int32 = 0xc0de

func (r *RootPage) Serialize() []byte {
	got := SerializeStruct(r,
		WithInt(func(r *RootPage) int32 { return int32(r.PageTyp) }),
		WithInt(func(r *RootPage) int32 { return r.MagicNumber }),
		WithInt(func(r *RootPage) int32 { return r.PageSize }),
		WithInt(func(r *RootPage) int32 { return int32(r.DirectoryPageStart) }),
		WithInt(func(r *RootPage) int32 { return int32(r.SchemaPageStart) }),
		WithInt(func(r *RootPage) int32 { return int32(r.NumberOfPages) }),
		func(_ *RootPage, b *bytes.Buffer) { b.Write(make([]byte, PageSize-4*6)) }, // 6 fields, each has 4 bytes
	)
	debugAssert(len(got) == PageSize, "root page should also be size of a page")
	return got
}

func DeserializeRootPage(r io.Reader) (*RootPage, error) {
	root, err := DeserializeStruct(r,
		DeserWithInt("page type", func(rp *RootPage, i *int32) { rp.PageTyp = PageType(*i) }),
		DeserWithInt("magic num", func(rp *RootPage, i *int32) { rp.MagicNumber = *i }),
		DeserWithInt("page size", func(rp *RootPage, i *int32) { rp.PageSize = *i }),
		DeserWithInt("directory page start", func(rp *RootPage, i *int32) { rp.DirectoryPageStart = PageID(*i) }),
		DeserWithInt("schema page start", func(rp *RootPage, i *int32) { rp.SchemaPageStart = PageID(*i) }),
		DeserWithInt("number of pages", func(rp *RootPage, i *int32) { rp.NumberOfPages = *i }),
		func(_ *RootPage, r io.Reader) error {
			_, err := r.Read(make([]byte, PageSize-4*6)) // discard rest of the page
			return err
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error deserializing root page: %w", err)
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
	LogPageType
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
	got := SerializeStruct(g,
		WithInt(func(g *GenericPage) int32 { return int32(g.Header.PageTyp) }),
		WithInt(func(g *GenericPage) int32 { return int32(g.Header.NextPage) }),
		WithInt(func(g *GenericPage) int32 { return int32(g.Header.SlotArraySize) }),
		func(g *GenericPage, b *bytes.Buffer) {
			// todo: compose better
			got := g.SlotArray.Serialize()
			b.Write(got)
		},
	)

	debugAssert(len(got) == PageSize, "generic page size should be consistent")
	return got
}

func Deserialize(r io.Reader) (*GenericPage, error) {
	header, err := DeserializeStruct[GenericPageHeader](r,
		DeserWithInt("page type", func(t *GenericPageHeader, i *int32) { t.PageTyp = PageType(*i) }),
		DeserWithInt("next page", func(t *GenericPageHeader, i *int32) { t.NextPage = PageID(*i) }),
		DeserWithInt("slot array size", func(t *GenericPageHeader, i *int32) { t.SlotArraySize = *i }),
	)
	if err != nil {
		return nil, fmt.Errorf("error deserializing page header: %w", err)
	}

	slottedSize := PageSize - 4*3
	slotted, err := DeserializeSlotted(r, slottedSize, int(header.SlotArraySize))
	if err != nil {
		return nil, err
	}

	return &GenericPage{
		Header:    *header,
		SlotArray: slotted,
	}, nil
}

// for lookup where given data/index page starts
type DirectoryTuple struct {
	PageTyp      PageType
	StartingPage PageID
	Name         string
}

func (d DirectoryTuple) Serialize() []byte {
	return SerializeStruct(
		&d,
		WithInt(func(d *DirectoryTuple) int32 { return int32(d.PageTyp) }),
		WithInt(func(d *DirectoryTuple) int32 { return int32(d.StartingPage) }),
		WithString(func(d *DirectoryTuple) string { return d.Name }),
	)
}

func DeserializeDirectoryTuple(b []byte) (*DirectoryTuple, error) {
	return DeserializeStruct[DirectoryTuple](bytes.NewReader(b),
		DeserWithInt("page type", func(t *DirectoryTuple, i *int32) { t.PageTyp = PageType(*i) }),
		DeserWithInt("starting page", func(t *DirectoryTuple, i *int32) { t.StartingPage = PageID(*i) }),
		DeserWithStr("name", func(t *DirectoryTuple, s *string) { t.Name = *s }),
	)
}

type SchemaTuple struct {
	TableNameV TableName
	FieldNameV FieldName
	FieldTypeV FieldType
}

func (s SchemaTuple) Serialize() []byte {
	return SerializeStruct(&s,
		WithString(func(s *SchemaTuple) string { return string(s.TableNameV) }),
		WithString(func(s *SchemaTuple) string { return string(s.FieldNameV) }),
		WithInt(func(s *SchemaTuple) int32 { return int32(s.FieldTypeV) }))
}

func DeserializeSchemaTuple(b []byte) (*SchemaTuple, error) {
	return DeserializeStruct[SchemaTuple](bytes.NewReader(b),
		DeserWithStr("table name", func(st *SchemaTuple, s *string) { st.TableNameV = TableName(*s) }),
		DeserWithStr("field name", func(st *SchemaTuple, s *string) { st.FieldNameV = FieldName(*s) }),
		DeserWithInt("field type", func(st *SchemaTuple, s *int32) { st.FieldTypeV = FieldType(*s) }))
}

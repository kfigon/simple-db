package naive

import (
	"bytes"
	"fmt"
	"io"
)

type PageType int32

const (
	RootPageType PageType = iota
	DataPageType
	SchemaPageType
	OverflowPageType
	LogPageType
)

type PageID int32
type PageOffset int32

type GenericPageHeader struct {
	PageTyp       PageType
	NextPage      PageID
	SlotArraySize int32 // int32 might be too big, leave for now
}

const MagicNumber int32 = 0xc0de

type RootPage struct {
	PageTyp         PageType
	MagicNumber     int32
	PageSize        int32
	SchemaPageStart PageID
	LogPageStart    PageID
	NumberOfPages   int32
}

func NewRootPage() RootPage {
	return RootPage{
		PageTyp:       RootPageType,
		MagicNumber:   MagicNumber,
		PageSize:      PageSize,
		NumberOfPages: 1, //root itself
	}
}

func (r *RootPage) Serialize() []byte {
	got := SerializeStruct(r,
		WithInt(func(r *RootPage) int32 { return int32(r.PageTyp) }),
		WithInt(func(r *RootPage) int32 { return r.MagicNumber }),
		WithInt(func(r *RootPage) int32 { return r.PageSize }),
		WithInt(func(r *RootPage) int32 { return int32(r.SchemaPageStart) }),
		WithInt(func(r *RootPage) int32 { return int32(r.NumberOfPages) }),
		func(_ *RootPage, b *bytes.Buffer) { b.Write(make([]byte, PageSize-4*5)) }, // 5 fields, each has 4 bytes
	)
	debugAssert(len(got) == PageSize, "root page should also be size of a page")
	return got
}

func DeserializeRootPage(r io.Reader) (*RootPage, error) {
	root, err := DeserializeStruct(r,
		DeserWithInt("page type", func(rp *RootPage, i *int32) { rp.PageTyp = PageType(*i) }),
		DeserWithInt("magic num", func(rp *RootPage, i *int32) { rp.MagicNumber = *i }),
		DeserWithInt("page size", func(rp *RootPage, i *int32) { rp.PageSize = *i }),
		DeserWithInt("schema page start", func(rp *RootPage, i *int32) { rp.SchemaPageStart = PageID(*i) }),
		DeserWithInt("number of pages", func(rp *RootPage, i *int32) { rp.NumberOfPages = *i }),
		func(_ *RootPage, r io.Reader) error {
			_, err := r.Read(make([]byte, PageSize-4*5)) // discard rest of the page
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

type GenericPage struct {
	Header    GenericPageHeader
	SlotArray *Slotted
}

type OverflowPage struct {
	Header GenericPageHeader
	Data   []byte
}

func NewPage(pageType PageType, pageSize int) *GenericPage {
	return &GenericPage{
		Header: GenericPageHeader{
			PageTyp: pageType,
		},
		SlotArray: NewSlotted(pageSize - 4 - 4 - 4), //12 bytes for header
	}
}

func NewOverflowPage(pageSize int, data []byte) (page *OverflowPage, rest []byte) {
	page = &OverflowPage{
		Header: GenericPageHeader{
			PageTyp:       OverflowPageType,
			NextPage:      0, // next pageID
			SlotArraySize: 0, // not used
		},
		Data: make([]byte, pageSize-4-4-4), //12 bytes for header
	}

	if len(data) >= len(page.Data) {
		copy(page.Data, data[:len(page.Data)])
		rest = data[len(page.Data):]
	} else {
		copy(page.Data, data)
		rest = nil
	}
	return page, rest
}

func (o *OverflowPage) Serialize() []byte {
	got := SerializeStruct(o,
		WithInt(func(g *OverflowPage) int32 { return int32(g.Header.PageTyp) }),
		WithInt(func(g *OverflowPage) int32 { return int32(g.Header.NextPage) }),
		WithInt(func(g *OverflowPage) int32 { return int32(g.Header.SlotArraySize) }),
		func(g *OverflowPage, b *bytes.Buffer) {
			b.Write(g.Data)
		},
	)

	debugAssert(len(got) == PageSize, "overflow page size should be consistent")
	return got
}

func (g *GenericPage) Add(t Tuple) (SlotIdx, error) {
	r, err := g.SlotArray.Add(t.Serialize())
	if err != nil {
		return 0, err
	}
	g.Header.SlotArraySize = int32(len(g.SlotArray.Indexes))
	return r, nil
}

func (g *GenericPage) Read(r SlotIdx) (Tuple, error) {
	return g.SlotArray.Read(r)
}

func (g *GenericPage) Put(r SlotIdx, t Tuple) error {
	if err := g.SlotArray.Put(r, t.Serialize()); err != nil {
		return err
	}
	g.Header.SlotArraySize = int32(len(g.SlotArray.Indexes))
	return nil
}

func DeserializeGenericHeader(r io.Reader) (*GenericPageHeader, error) {
	header, err := DeserializeStruct[GenericPageHeader](r,
		DeserWithInt("page type", func(t *GenericPageHeader, i *int32) { t.PageTyp = PageType(*i) }),
		DeserWithInt("next page", func(t *GenericPageHeader, i *int32) { t.NextPage = PageID(*i) }),
		DeserWithInt("slot array size", func(t *GenericPageHeader, i *int32) { t.SlotArraySize = *i }),
	)
	if err != nil {
		return nil, fmt.Errorf("error deserializing page header: %w", err)
	}
	return header, nil
}

func (g *GenericPage) Serialize() []byte {
	got := SerializeStruct(g,
		WithInt(func(g *GenericPage) int32 { return int32(g.Header.PageTyp) }),
		WithInt(func(g *GenericPage) int32 { return int32(g.Header.NextPage) }),
		WithInt(func(g *GenericPage) int32 { return int32(g.Header.SlotArraySize) }),
		func(g *GenericPage, b *bytes.Buffer) {
			b.Write(g.SlotArray.Serialize())
		},
	)

	debugAssert(len(got) == PageSize, "generic page size should be consistent")
	return got
}

func DeserializeGenericPage(header *GenericPageHeader, r io.Reader) (*GenericPage, error) {
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

func DeserializeOverflowPage(header *GenericPageHeader, r io.Reader) (*OverflowPage, error) {
	buf := make([]byte, PageSize-4-4-4) //12 bytes for header
	got, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error reading overflow page: %w", err)
	} else if got != len(buf) {
		return nil, fmt.Errorf("error reading overflow page got %d, expected %d", got, len(buf))
	}

	return &OverflowPage{
		Header: *header,
		Data:   buf,
	}, nil
}

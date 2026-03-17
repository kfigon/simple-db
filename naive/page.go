package naive

import (
	"bytes"
	"fmt"
	"io"
	"iter"
)

type PageType int32

const (
	RootPageType PageType = iota
	DataPageType
	SchemaPageType // todo: do we need it? Should be just generic data page, starting from the root pointer
	OverflowPageType
	LogPageType
)

type PageID int32
type PageOffset int32

type SlotIdx int

type GenericPageHeader struct {
	PageTyp       PageType
	NextPage      PageID
	SlotArraySize int32 // int32 might be too big, leave for now
}

type GenericPage struct {
	Header GenericPageHeader

	// slots for tuples
	Indexes    []PageOffset // RowId -> offset within page
	CellData   []byte
	lastOffset PageOffset
}

func NewPage(pageType PageType, pageSize int) *GenericPage {
	slotsSize := pageSize - 4 - 4 - 4 // generic size header

	return &GenericPage{
		Header: GenericPageHeader{
			PageTyp: pageType,
			// 0s for rest
		},
		lastOffset: PageOffset(slotsSize),
		CellData:   make([]byte, slotsSize),
	}
}

var errNoSpace = fmt.Errorf("no space in slot array")

const rowIdSize = 4

func (g *GenericPage) Add(t Tuple) (SlotIdx, error) {
	buf := t.Serialize()
	bytesWithHeader := SerializeBytes(buf)
	ln := len(bytesWithHeader)

	if !g.hasSpace(ln) {
		return 0, errNoSpace
	}
	copy(g.CellData[int(g.lastOffset)-ln:], bytesWithHeader)
	g.lastOffset -= PageOffset(ln)

	g.Indexes = append(g.Indexes, g.lastOffset)

	idx := SlotIdx(len(g.Indexes) - 1)

	g.Header.SlotArraySize = int32(len(g.Indexes))
	return idx, nil
}

func (g *GenericPage) Read(idx SlotIdx) (*Tuple, error) {
	if int(idx) >= len(g.Indexes) {
		return nil, fmt.Errorf("invalid idx %d, got only %d", idx, len(g.Indexes))
	}

	offset := g.Indexes[idx]
	rawBytes, err := DeserializeBytes(BytesWithHeader(g.CellData[offset:]))
	if err != nil {
		return nil, err
	}
	return DeserializeTuple(rawBytes)
}

func (g *GenericPage) Put(id SlotIdx, t Tuple) error {
	if int(id) >= len(g.Indexes) {
		return fmt.Errorf("invalid idx %d, got only %d", id, len(g.Indexes))
	}

	offset := g.Indexes[id]
	existing, err := DeserializeBytes(BytesWithHeader(g.CellData[offset:]))
	if err != nil {
		return err
	}
	buf := t.Serialize()
	if len(buf) <= len(existing) {
		bytesWithHeader := SerializeBytes(buf)
		offset := g.Indexes[id]
		copy(g.CellData[offset:], bytesWithHeader)

		return nil
	}

	newRowId, err := g.Add(t)
	if err != nil {
		return err
	}
	g.Indexes[id] = g.Indexes[newRowId]
	g.Indexes[newRowId] = -1 // tombstone value

	g.Header.SlotArraySize = int32(len(g.Indexes))
	return nil
	// todo: reclaim page space
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
			for _, id := range g.Indexes {
				b.Write(SerializeInt(int32(id)))
			}

			paddingLen := int(g.lastOffset) - len(g.Indexes)*rowIdSize

			b.Write(make([]byte, paddingLen))
			b.Write(g.CellData[g.lastOffset:])
		},
	)

	debugAssert(len(got) == PageSize, "generic page size should be consistent")
	return got
}

func DeserializeGenericPage(header *GenericPageHeader, r io.Reader) (*GenericPage, error) {
	p := NewPage(header.PageTyp, PageSize)

	lastOffset := int(p.lastOffset)

	for range header.SlotArraySize {
		i, err := ReadInt(r)
		if err != nil {
			return nil, fmt.Errorf("error deserializing slot array: %w", err)
		}
		ithPageOffset := PageOffset(i)
		p.Indexes = append(p.Indexes, ithPageOffset)

		lastOffset = min(lastOffset, int(ithPageOffset))
	}
	p.lastOffset = PageOffset(lastOffset)
	slotArrayAreaSize := len(p.Indexes) * rowIdSize
	freeSpaceAreaSize := lastOffset - slotArrayAreaSize
	r.Read(make([]byte, freeSpaceAreaSize)) // discard
	r.Read(p.CellData[lastOffset:])

	return p, nil
}

func (g *GenericPage) hasSpace(newData int) bool {
	return int(g.lastOffset)-newData-(len(g.Indexes)*rowIdSize) > 0
}

type TupleIteratorz iter.Seq[Tuple]

func (g *GenericPage) Iterator() TupleIteratorz {
	return func(yield func(Tuple) bool) {
		for slotId := 0; slotId < len(g.Indexes); slotId++ {
			d := must(g.Read(SlotIdx(slotId)))
			if !yield(*d) {
				return
			}
		}
	}
}

package naive

import (
	"bytes"
	"fmt"
	"io"
)

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

const MagicNumber int32 = 0xc0de

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
			b.Write(g.SlotArray.Serialize())
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

// ---------------------
// new and improved, based on sqlite. Remove directory pages, replace shcema with this
type SchemaTuple struct {
	PageTyp        PageType // what's the type of data described by schema	- data, index, etc
	Name           string
	StartingPageID PageID
	SqlStatement   string // sql stmt used to create this. Will be parsed on boot and cached
}

func (s SchemaTuple) Serialize() []byte {
	return SerializeStruct(
		&s,
		WithInt(func(t *SchemaTuple) int32 { return int32(t.PageTyp) }),
		WithString(func(t *SchemaTuple) string { return t.Name }),
		WithInt(func(t *SchemaTuple) int32 { return int32(t.StartingPageID) }),
		WithString(func(t *SchemaTuple) string { return t.SqlStatement }),
	)
}

func DeserializeSchemaTuple(b []byte) (*SchemaTuple, error) {
	return DeserializeStruct[SchemaTuple](bytes.NewBuffer(b),
		DeserWithInt("PageType", func(t *SchemaTuple, i *int32) { t.PageTyp = PageType(*i) }),
		DeserWithStr("Name", func(t *SchemaTuple, i *string) { t.Name = *i }),
		DeserWithInt("StartingPageID", func(t *SchemaTuple, i *int32) { t.StartingPageID = PageID(*i) }),
		DeserWithStr("SqlStatement", func(t *SchemaTuple, i *string) { t.SqlStatement = *i }),
	)
}

// todo: make iterator to extract this from slotted
type Tuple struct {
	NumberOfFields int32
	ColumnTypes    []ColumnType
	ColumnDatas    [][]byte
}

func (t Tuple) Serialize() []byte {
	return SerializeStruct(&t,
		WithInt(func(t *Tuple) int32 { return t.NumberOfFields }),
		func(t *Tuple, b *bytes.Buffer) {
			for _, v := range t.ColumnTypes {
				b.Write(SerializeInt(int32(v)))
			}
		},
		func(t *Tuple, b *bytes.Buffer) {
			for _, v := range t.ColumnDatas {
				b.Write(v)
			}
		},
	)
}

func DeserializeTuple(b []byte) (*Tuple, error) {
	return DeserializeStruct[Tuple](bytes.NewBuffer(b),
		DeserWithInt("NumberOfFields", func(t *Tuple, i *int32) { t.NumberOfFields = *i }),
		func(t *Tuple, r io.Reader) error {
			for i := range t.NumberOfFields {
				iVal, err := ReadInt(r)
				if err != nil {
					return fmt.Errorf("error deserializing tuple, column %d/%d: %w", i, t.NumberOfFields, err)
				}
				v, err := ColumnTypeFromInt(iVal)
				if err != nil {
					return fmt.Errorf("error deserializing tuple, column %d/%d: %w", i, t.NumberOfFields, err)
				}
				t.ColumnTypes = append(t.ColumnTypes, v)
			}
			return nil
		},
		func(t *Tuple, r io.Reader) error {
			for i := range t.NumberOfFields {
				typ := t.ColumnTypes[i]

				// todo: inefficient, but with data validation
				switch typ {
				case NullField:
					t.ColumnDatas = append(t.ColumnDatas, nil)
				case BooleanField:
					b, err := ReadBool(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, bool column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, SerializeBool(b))
				case IntField:
					v, err := ReadInt(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, int column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, SerializeInt(v))
				case StringField:
					v, err := ReadString(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, string column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, SerializeString(v))
				case OverflowField:
					ln, err := ReadInt(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, overflow length column %d/%d: %w", i, t.NumberOfFields, err)
					}
					pageID, err := ReadInt(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, overflow pageID column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, append(SerializeInt(ln), SerializeInt(pageID)...))
				default:
					return fmt.Errorf("error deserializing tuple, column %d/%d: unknown column type %d", i, t.NumberOfFields, typ)
				}
			}
			return nil
		},
	)
}

type ColumnType int32

const (
	// fixed size
	NullField    ColumnType = iota // size 0
	BooleanField                   // size 1
	IntField                       // size 4

	// var size
	StringField   // size 4 + len
	OverflowField // size 4 + 4 (PageID)
)

func ColumnTypeFromInt(i int32) (ColumnType, error) {
	switch i {
	case 0:
		return NullField, nil
	case 1:
		return BooleanField, nil
	case 2:
		return IntField, nil
	case 3:
		return StringField, nil
	case 4:
		return OverflowField, nil
	default:
		return 0, fmt.Errorf("invalid column type: %d", i)
	}
}

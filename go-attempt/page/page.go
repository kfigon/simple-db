package page

type FieldType byte
const (
	I8Type FieldType = iota
	I16Type
	I64Type
	StringType
	BinaryBlobType
)

const PageSize = 8*512

type PageId I32 // 0 base-indexed, but 0 value is reserved for the root page
type PageOffset I16 // offset within the single page
type SlotIdx I16 // slot number
type RecordID struct { // internal "primary key". Where to find given tuple
	PageID PageId
	SlotID SlotIdx
}

type PageType byte // always first byte in a page
const (
	RootPageType PageType = iota + 1
	DirectoryPageType
	SchemaPageType
	DataPageType
	OverflowPageType
	IndexPageType
)

// ============= root page. Always first page (1)
// entry point for all metadata
type RootPage struct {
	PageTyp PageType
	MagicNumber I32
	DirectoryPageRootID PageId
	SchemaPageRootID PageId
	LastFreePage PageId
}

func NewRootPage() RootPage{
	n := 0xDEADBEEF
	return RootPage{
		PageTyp: RootPageType,
		MagicNumber: I32(n),
		DirectoryPageRootID: 0,
		SchemaPageRootID: 0,
		LastFreePage: 0,
	}
}

func (r *RootPage) Serialize() []byte {
	length := 1 + 4 + 4 + 4 +4
	out := make([]byte, length)
	var offset int

	Write(out, &offset, Byte(r.PageTyp))
	Write(out, &offset, r.MagicNumber)
	Write(out, &offset, I32(r.DirectoryPageRootID))
	Write(out, &offset, I32(r.SchemaPageRootID))
	Write(out, &offset, I32(r.LastFreePage))

	return out
}

// =================== directory. 
// contains info about the content of the db
// where to find all pages and lookup what table's inside
type DirectoryEntry struct {
	DataRootPageID PageId //
	SchemaRootRecord RecordID // schema - only for data pages
	ObjectType PageType // what kind of page is it - index, data etc.
	ObjectName String
}

func NewDirectoryPage() GenericPage[DirectoryEntry] {
	return NewPage[DirectoryEntry](DirectoryPageType)
}

// ============ Schema 
type SchemaEntry struct {
	FieldTyp FieldType
	IsNull bool // todo - make it bitfield for more efficiency
	FieldName string
	Next RecordID
}

func NewSchemaPage() GenericPage[SchemaEntry] {
	return NewPage[SchemaEntry](SchemaPageType)
}

// ============== Data - just slot array with binary data
// todo: need table name/tableid? Or put associate schema with data in catalog?


// ============ generic page definiton
type GenericPage[T any] struct { // todo: use that generic in slotted page
	BaseHeader
	SlottedPageHeader

	data SlottedPage
}

func NewPage[T any](pageType PageType) GenericPage[T] {
	return GenericPage[T] {
		BaseHeader: BaseHeader{
			PageTyp: pageType,
			NextPage: 0,
		},
		SlottedPageHeader: SlottedPageHeader{
			SlotArrayLen:        0,
			SlotArrayLastOffset: 0,
		},
	}
}

func (g *GenericPage[T]) Serialize() []byte {
	length := 1+ 4 + 3
	out := make([]byte, length)
	offset := 0
	
	Write(out, &offset, Byte(g.PageTyp))
	Write(out, &offset, I32(g.NextPage))
	Write(out, &offset, g.SlottedPageHeader)

	return out
}

type BaseHeader struct{
	PageTyp PageType
	NextPage PageId
}

type SlottedPageHeader struct {
	SlotArrayLen Byte
	SlotArrayLastOffset PageOffset
}

func (s SlottedPageHeader) Serialize() []byte {
	length := 1 + 2
	out := make([]byte, length)
	offset := 0
	
	Write(out, &offset, s.SlotArrayLen)
	Write(out, &offset, I16(s.SlotArrayLastOffset))
	
	return out
}

type NextPage[T any] interface {
	Next() (T, bool)
}
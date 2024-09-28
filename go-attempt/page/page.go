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

type PageId int // 0 base-indexed, but 0 value is reserved for the root page
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
}

func NewRootPage() *RootPage{
	n := 0xDEADBEEF
	return &RootPage{
		PageTyp: RootPageType,
		MagicNumber: I32(n),
		DirectoryPageRootID: 0,
		SchemaPageRootID: 0,
	}
}

// =================== directory. 
// contains info about the content of the db
// where to find all pages and lookup what table's inside
type DirectoryPage struct {
	Header struct {
		PageTyp PageType
		NextPage PageId
	}
	PagesData []PageCatalog // todo: slotarray
}

type PageCatalog struct {
	StartPageID PageId
	SchemaRootRecord RecordID // for data pages
	ObjectType PageType // what kind of page is it - index, data etc.
	ObjectName String
}

func NewDirectoryPage() *DirectoryPage {
	return &DirectoryPage{
		Header:    struct{PageTyp PageType; NextPage PageId}{
			PageTyp: DirectoryPageType,
			NextPage: 0,
		},
		PagesData: nil,
	}
}

// ============ Schema 
type SchemaPage struct {
	Header struct {
		PageTyp PageType
		NextPage PageId
	}
	Schemas []SchemaData // todo: slot array
}

type SchemaData struct {
	FieldTyp FieldType
	IsNull bool // todo - make it bitfield for more efficiency
	FieldName string
	Next RecordID
}

// ============== Data
type DataPage struct {
	Header struct { // todo: need table name/tableid? Or put associate schema with data in catalog?
		PageTyp PageType
		NextPage PageId
		SlotArrayLen Byte
		SlotArrayLastOffset PageOffset
	}
	Slots []byte
	Cells [][]byte
}

// ============ generic page definiton
// todo: convert all pages to this type, probably a pattern will emerge. Delegate details to storage manager
type GenericPage struct {
	BaseHeader
	SlottedPageHeader
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
	out := make([]byte, s.Length())
	offset := 0
	
	a := s.SlotArrayLen.Serialize()
	copy(out[offset:], a)
	offset += len(a)
	
	b := I16(s.SlotArrayLastOffset).Serialize()
	copy(out[offset:], b)
	offset += len(b)

	return out
}

func (s SlottedPageHeader) Length() int {
	return 1 + 2
}

type NextPage[T any] interface {
	Next() (T, bool)
}
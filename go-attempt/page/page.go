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

type PageId int // 1 base-indexed
type PageOffset int
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
		PageDataLen I16
	}
	PagesData []PageCatalog
}

func NewDirectoryPage() *DirectoryPage {
	return &DirectoryPage{
		Header:    struct{PageTyp PageType; NextPage PageId; PageDataLen I16}{
			PageTyp: DirectoryPageType,
			NextPage: 0,
			PageDataLen: 0,
		},
		PagesData: nil,
	}
}

type PageCatalog struct {
	StartPageID PageId
	ObjectType PageType // what kind of page is it
	ObjectName String
}

// ============ Schema 
type SchemaPage struct {
	Header struct {
		PageTyp PageType
		NextPage PageId
		SchemaLength int16
	}
	Schemas []SchemaData
}

type SchemaData struct {
	FieldTyp FieldType
	IsNull bool // todo - make it bitfield for more efficiency
	FieldName string
}

// ============== Data
type DataPage struct {
	Header struct {
		PageTyp PageType
		NextPage PageId
		SlotArrayLen Byte
	}
	Slots []byte
	TupleData []byte
}
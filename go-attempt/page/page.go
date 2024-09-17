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
type PageType byte
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
	MagicNumber I32
	PageTyp PageType
	DirectoryPageRootID PageId
	SchemaPageRootID PageId
}

// =================== directory. 
// contains info about the content of the db
// where to find all pages and lookup what table's inside
type DirectoryPage struct {
	Header struct {
		PageTyp PageType
		PageDataLen I16
	}
	PagesData []PageCatalog
}

type PageCatalog struct {
	StartPageID PageId
	ObjectName String 
}

// ============ Schema 
type SchemaPage struct {
	Header struct {
		PageTyp PageType
		SchemaLength int16
	}
	Schemas []SchemaData
}

type SchemaData struct {
	FieldTyp FieldType
	IsNull bool // todo - make it bitfield for more efficiency
	FieldName string
}
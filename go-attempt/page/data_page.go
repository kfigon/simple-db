package page

type DataPageHeader struct {
	PageTyp PageType
	SchemaLength int32
}

type SchemaData struct {
	FieldName string
	FieldTyp FieldType
}

type DataPage struct {
	Header DataPageHeader
	Schema []SchemaData
	Data []byte
}
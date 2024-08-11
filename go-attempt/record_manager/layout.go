package recordmanager

type FieldType byte
const (
	I32 FieldType = iota
	I16
	Byte
	Bool
	String
	Blob
)

type Layout struct {
	TableName string	// make it fixed len string
	FieldName string	// make it fixed len string
	FieldTyp FieldType
}


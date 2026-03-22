package naive

import (
	"fmt"
	"simple-db/sql"
	"strconv"
)

// top level utils types

const PageSize = 4 * 1024
const schemaName = "catalog_schema"
const assertionsEnabled = true

type FieldType int32

const (
	Null FieldType = iota
	Int32
	String
	Boolean
	Float
)

func (f FieldType) String() string {
	return [...]string{
		"Null",
		"Int32",
		"String",
		"Boolean",
		"Float",
	}[f]
}

func FieldTypeFromString(s string) (FieldType, error) {
	switch s {
	case "null":
		return Null, nil
	case "int":
		return Int32, nil
	case "string":
		return String, nil
	case "boolean":
		return Boolean, nil
	case "float":
		return Float, nil
	default:
		return 0, fmt.Errorf("invalid type %v", s)
	}
}

func ParseExpressionValueToType(providedValue sql.Expression, fieldTyp FieldType) (any, error) {
	switch v := providedValue.(type) {
	case sql.ValueLiteral:
		switch fieldTyp {
		case Null:
			return nil, nil
		case Int32:
			v, err := strconv.ParseInt(v.Tok.Lexeme, 10, 32)
			return int32(v), err
		case String:
			return v.Tok.Lexeme, nil
		case Boolean:
			return strconv.ParseBool(v.Tok.Lexeme)
		case Float:
			return strconv.ParseFloat(v.Tok.Lexeme, 64)
		default:
			return nil, fmt.Errorf("invalid data type %v", fieldTyp)
		}
	case sql.NullLiteral:
		return nil, nil
	default:
		panic(fmt.Sprintf("todo expression type for insert: %T", v))
	}
}

type ColumnData struct {
	Typ  FieldType
	Data any
}

type TableName string
type FieldName string

type TableSchema struct {
	FieldsTypes []FieldType
	FieldNames  []FieldName
	StartPage   PageID
	PageTyp     PageType
}
type Schema map[TableName]TableSchema

type Row map[FieldName]ColumnData

func must[T any](v T, err error) T {
	debugAsserErr(err, "expected no error")
	return v
}

func debugAssert(expectTrue bool, format string, args ...any) {
	if assertionsEnabled && !expectTrue {
		panic(fmt.Sprintf(format, args...))
	}
}

func debugAsserErr(err error, format string, args ...any) {
	errStr := func() string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	debugAssert(err == nil, format+": "+errStr(), args...)
}

package naive

import (
	"fmt"
	"strconv"
)


type FieldName string

type ColumnData struct {
	Typ  FieldType
	Data any
}

type FieldType int32

const (
	Int32 FieldType = iota
	String
	Boolean
	Float
)

func (f FieldType) String() string {
	return [...]string{
		"Int32",
		"String",
		"Boolean",
		"Float",
	}[f]
}

func FieldTypeFromString(s string) (FieldType, error) {
	switch s {
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

func parseType(v string, typ FieldType) (any, error) {
	switch typ {
	case Int32:
		v, err := strconv.ParseInt(v, 10, 32)
		return int32(v), err
	case String:
		return v, nil
	case Boolean:
		return strconv.ParseBool(v)
	case Float:
		return strconv.ParseFloat(v, 64)
	default:
		return nil, fmt.Errorf("invalid data type %v", typ)
	}
}
package naive

import (
	"fmt"
	"simple-db/sql"
)

type FieldType int
const (
	Int32 FieldType = iota
	String
	Boolean
	Float
)

func FieldTypeFromString(s string) (FieldType, error) {
	switch s {
	case "int": return Int32, nil
	case "string": return String, nil
	case "boolean": return Boolean, nil
	case "float": return Float, nil
	default:  return 0, fmt.Errorf("invalid type %v", s)
	}
}

type TableName string
type FieldName string

type ColumnData struct {
	Typ FieldType
	Data any
}

type TableSchema map[FieldName]FieldType
type Schema map[TableName]TableSchema

type TableData map[FieldName]ColumnData
type Data map[TableName][]TableData


type Storage struct {
	SchemaMetadata Schema
	AllData Data
}

func NewStorage() *Storage {
	return &Storage{
		SchemaMetadata: map[TableName]TableSchema{},
		AllData: map[TableName][]TableData{},
	}
}

func (s *Storage) CreateTable(stmt sql.CreateStatement) error {
	if _, ok := s.SchemaMetadata[TableName(stmt.Table)]; ok {
		return fmt.Errorf("table %v already present", stmt.Table)
	}

	table := TableSchema{}
	for _, v := range stmt.Columns {
		f, err := FieldTypeFromString(v.Typ)
		if err != nil {
			return err
		}
		table[FieldName(v.Name)] = f
	}
	s.SchemaMetadata[TableName(stmt.Table)] = table
	return nil
}

func (s *Storage) Insert(stmt sql.InsertStatement) error {
	if _, ok := s.SchemaMetadata[TableName(stmt.Table)]; !ok {
		return fmt.Errorf("table %v does not exist", stmt.Table)
	}

	tables, ok := s.AllData[TableName(stmt.Table)]
	if !ok {
		tables = []TableData{}
		defer func () {
			s.AllData[TableName(stmt.Table)] = tables
		}()
	}

	table := TableData{}
	for i := 0; i < len(stmt.Columns); i++ {
		col := stmt.Columns[i]
		val := stmt.Values[i]
		table[FieldName(col)] = ColumnFromString(val, col)
	}
	tables = append(tables, table)
	return nil
}

func ColumnFromString(data string, typ string) ColumnData {
	return ColumnData{
		Typ: String,
		Data: "oopsie",
	}
}
package naive

import (
	"fmt"
	"simple-db/sql"
	"strconv"
)

const PageSize = 4*1096

type FieldType int32
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

	root RootPage
	allPages []GenericPage // pageId - just an index here. Page 0 is root, so noop
}

func NewStorage() *Storage {
	return &Storage{
		SchemaMetadata: Schema{},
		AllData: Data{},

	}
}

func (s *Storage) allocatePage(pageTyp PageType) (PageID, *GenericPage) {
	p := NewPage(pageTyp, PageSize)
	s.allPages = append(s.allPages, *p)
	newPageID := PageID(len(s.allPages))

	var lastPage *GenericPage
	for _, i := range NewPageIterator(s, pageTyp) {
		lastPage = i
	}

	if lastPage != nil {
		lastPage.Header.NextPage = newPageID
	}

	return newPageID, p
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
	schema, ok := s.SchemaMetadata[TableName(stmt.Table)]; 
	if !ok {
		return fmt.Errorf("table %v does not exist", stmt.Table)
	}

	tables, ok := s.AllData[TableName(stmt.Table)]
	if !ok {
		tables = []TableData{}
	}

	table := TableData{}
	for i := 0; i < len(stmt.Columns); i++ {
		col := stmt.Columns[i]
		val := stmt.Values[i]
		
		fieldType, ok := schema[FieldName(col)]
		if !ok {
			return fmt.Errorf("invalid column %v, not defined in schema for table %v", col, stmt.Table)
		}

		parsed, err := parseType(val, fieldType)
		if err != nil {
			return err
		}

		table[FieldName(col)] = ColumnData{
			Typ: fieldType,
			Data: parsed,
		}
	}

	tables = append(tables, table)
	s.AllData[TableName(stmt.Table)] = tables
	return nil
}

func parseType(v string, typ FieldType) (any, error) {
	switch typ {
	case Int32: 
		v, err := strconv.ParseInt(v, 10, 32)
		return int32(v), err
	case String: return v, nil
	case Boolean: return strconv.ParseBool(v)
	case Float: return strconv.ParseFloat(v, 64)
	default: return nil, fmt.Errorf("invalid data type %v", typ)
	}
}

type QueryResult struct {
	Header []string
	Values [][]string
}

func (s *Storage) Select(stmt sql.SelectStatement) (QueryResult, error) {
	schema, ok := s.SchemaMetadata[TableName(stmt.Table)]
	if !ok {
		return QueryResult{}, fmt.Errorf("unknown table %v", stmt.Table)
	}

	columnsToQuery, err := colsToQuery(stmt, schema)
	if err != nil {
		return QueryResult{}, err
	}

	out := QueryResult{
		Header: columnsToQuery,
	}

	rows := s.AllData[TableName(stmt.Table)]
	for _, row := range rows {
		vals := []string{}
		for _, col := range columnsToQuery {
			vals = append(vals, fmt.Sprint(row[FieldName(col)].Data))
		}
		out.Values = append(out.Values, vals)
	}

	return out, nil
}

func colsToQuery(stmt sql.SelectStatement, schema TableSchema) ([]string, error) {
	if stmt.HasWildcard {
		cols := []string{}
		for name := range schema {
			cols = append(cols, string(name))
		}
		return cols, nil
	}
	
	out := []string{}
	for _, v := range stmt.Columns {
		if _, ok := schema[FieldName(v)]; !ok {
			return nil, fmt.Errorf("unknown column %v in table %v", v, stmt.Table)
		}

		out = append(out, v)
	}
	return out, nil
}
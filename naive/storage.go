package naive

import (
	"bytes"
	"fmt"
	"io"
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

func SerializeSchema(s Schema) []byte {
	type catalogHeader struct {
		numberOfTables int32
	}

	type schemaHeader struct {
		name            TableName
		numberOfColumns int32
		columnMetadata  []struct {
			typ  FieldType
			name FieldName
		}
	}

	var buf bytes.Buffer
	buf.Write(SerializeInt(int32(len(s))))
	for table, data := range s {
		buf.Write(SerializeString(string(table)))
		buf.Write(SerializeInt(int32(len(data))))
		for name, typ := range data {
			buf.Write(SerializeInt(int32(typ)))
			buf.Write(SerializeString(string(name)))
		}
	}
	return buf.Bytes()
}

// todo: use scanner later
func DeserializeSchema(r io.Reader) (Schema, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	tableCount, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read num of tables: %w", err)
	}

	schema := Schema{}
	for i := range tableCount {
		name, err := DeserializeStringAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read %v table name: %w", i, err)
		}

		columnCount, err := DeserializeIntAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read num of columns for table %v: %w", name, err)
		}

		tab := TableSchema{}
		for j := range columnCount {
			typ, err := DeserializeIntAndEat(&bytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read column typ %v for %v: %w", j, name, err)
			}

			colName, err := DeserializeStringAndEat(&bytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read column name %v for %v: %w", j, name, err)
			}
			tab[FieldName(colName)] = FieldType(typ)
		}
		schema[TableName(name)] = tab
	}

	return schema, nil
}

func SerializeData(d Data) []byte {
	// array like that
	type dataHeader struct {
		name         TableName
		numberOfRows int32
		rowData      []struct {
			name  FieldName
			value any
		}
	}

	var buf bytes.Buffer
	for tableName, rows := range d {
		buf.Write(SerializeString(string(tableName)))
		buf.Write(SerializeInt(int32(len(rows))))
		for _, row := range rows {
			for fieldName, val := range row {
				buf.Write(SerializeString(string(fieldName)))
				var serializedColumn []byte
				switch val.Typ {
				case Int32:
					serializedColumn = SerializeInt(val.Data.(int32))
				case String:
					serializedColumn = SerializeString(val.Data.(string))
				case Boolean:
					serializedColumn = SerializeBool(val.Data.(bool))
				case Float:
					// todo: impl float
				default:
					panic(fmt.Sprintf("unknown column typ %v", val.Typ)) // data corruption, fail now
				}
				buf.Write(serializedColumn)
			}
		}
	}

	return buf.Bytes()
}

func DeserializeData(r io.Reader, s Schema) (Data, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read row data: %w", err)
	}

	d := Data{}
	for i := range len(s) {
		tableName, err := DeserializeStringAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read table data for %v table: %w", i, err)
		}
		schema, ok := s[TableName(tableName)]
		if !ok {
			return nil, fmt.Errorf("unknown table %v", tableName)
		}

		numRows, err := DeserializeIntAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read num of rows for %v table: %w", tableName, err)
		}
		tableData := make([]TableData, 0, numRows)
		for range numRows {
			td := TableData{}

			for k := range len(schema) {
				columnName, err := DeserializeStringAndEat(&bytes)
				if err != nil {
					return nil, fmt.Errorf("failed to read %v column name for %v table: %w", k, tableName, err)
				}

				typ, ok := schema[FieldName(columnName)]
				if !ok {
					return nil, fmt.Errorf("unknown column %v for table %v", columnName, tableName)
				}

				var v any
				switch typ {
				case Int32:
					if v, err = DeserializeIntAndEat(&bytes); err != nil {
						return nil, fmt.Errorf("failed to read %v for %v: %w", columnName, tableName, err)
					}
				case String:
					if v, err = DeserializeStringAndEat(&bytes); err != nil {
						return nil, fmt.Errorf("failed to read %v for %v: %w", columnName, tableName, err)
					}
				case Boolean:
					// todo: impl bool
				case Float:
					// todo: impl float
				default:
					panic(fmt.Sprintf("unknown column typ %v in table %v", typ, tableName)) // data corruption, fail now
				}

				td[FieldName(columnName)] = ColumnData{
					Typ:  typ,
					Data: v,
				}
			}

			tableData = append(tableData, td)
		}

		d[TableName(tableName)] = tableData
	}

	return d, nil
}

func SerializeDb(s *Storage) []byte {
	serializedSchema := SerializeSchema(s.SchemaMetadata)
	serializedData := SerializeData(s.AllData)

	return SerializeAll(SerializeInt(int32(len(serializedSchema))),
		SerializeInt(int32(len(serializedData))),
		serializedSchema,
		serializedData)
}

func DeserializeDb(r io.Reader) (*Storage, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	schemaLen, err := DeserializeIntAndEat(&data)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema length: %w", err)
	}

	_, err = DeserializeIntAndEat(&data)
	if err != nil {
		return nil, fmt.Errorf("failed to read data length: %w", err)
	}

	schema, err := DeserializeSchema(bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	// NewBuffer takes ownership of the byte array. Here Im reusing it, so split it here
	dataContent, err := DeserializeData(bytes.NewBuffer(data[schemaLen:]), schema)
	if err != nil {
		return nil, err
	}

	return &Storage{
		SchemaMetadata: schema,
		AllData:        dataContent,
	}, nil
}
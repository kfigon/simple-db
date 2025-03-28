package naive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"simple-db/sql"
	"strconv"
)

const PageSize = 4*1096
const newPageSchema = true
const directoryName =  "catalog_directory"

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
	s := &Storage{
		SchemaMetadata: Schema{},
		AllData: Data{},
		allPages: []GenericPage{GenericPage{}}, // empty 'root' page in the beginning
	}

	dirID, _ := s.allocatePage(DirectoryPageType, directoryName)
	s.root = NewRootPage(dirID)

	return s
}

func (s *Storage) allocatePage(pageTyp PageType, name string) (PageID, *GenericPage) {
	p := NewPage(pageTyp, PageSize)
	newPageID := PageID(len(s.allPages))
	s.allPages = append(s.allPages, *p)

	// link last page to the new one
	if startId, ok := FindStartingPageForEntity(s, pageTyp, name); ok {
		var lastId PageID
		for id := range NewPageIterator(s, startId){
			lastId = id
		}
		s.allPages[lastId].Header.NextPage = newPageID
	}

	return newPageID, p
}

func (s *Storage) CreateTable(stmt sql.CreateStatement) error {
	if newPageSchema {
		return s.CreateTable2(stmt)
	}
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

func (s *Storage) CreateTable2(stmt sql.CreateStatement) error {
	if _, ok := FindStartingPageForEntity(s, SchemaPageType, stmt.Table); ok {
		return fmt.Errorf("table %v already present", stmt.Table)
	}

	schemaEntries := []SchemaTuple{}
	for _, v := range stmt.Columns {
		f, err := FieldTypeFromString(v.Typ)
		if err != nil {
			return err
		}
		schemaEntries = append(schemaEntries, SchemaTuple{
			FieldNameV: FieldName(v.Name),
			FieldTypeV: f,
		})
	}

	if len(schemaEntries) == 0 {
		return nil
	}

	// need to add schema first, as inside we're looking into directory - but there's no directory entry yet
	// but to create directory entry, we need first page id
	firstPageIDForSchema := s.AddTuple(SchemaPageType, stmt.Table, schemaEntries[0].Serialize())

	s.AddDirectoryTuple(DirectoryTuple{
		PageTyp: SchemaPageType,
		StartingPage: firstPageIDForSchema,
		Name: stmt.Table,
	})

	for _, v := range schemaEntries[1:] {
		s.AddTuple(SchemaPageType, stmt.Table, v.Serialize())
	}

	// add empty data page
	dataPageID, _ := s.allocatePage(DataPageType, stmt.Table)

	s.AddDirectoryTuple(DirectoryTuple{
		PageTyp: DataPageType,
		StartingPage: dataPageID,
		Name: stmt.Table,
	})
	return nil
}

func (s *Storage) AddTuple(pageType PageType, name string, b []byte) PageID {
	var lastPage *GenericPage
	var lastPageID PageID

	if startPage, ok := FindStartingPageForEntity(s, pageType, name); !ok {
		// allocatePage also links it
		pageID, newPage := s.allocatePage(pageType, name) 
		lastPage = newPage
		lastPageID = pageID
	} else {
		for pageID, p := range NewPageIterator(s, startPage){
			lastPage = p
			lastPageID = pageID
		}
	}
	if _, err := lastPage.Add(b); err != nil {
		panic("overflow")
	}
	return lastPageID
}

func (s *Storage) AddDirectoryTuple(dir DirectoryTuple) {
	var lastPage *GenericPage
	for _, p := range directoryPages(s){ 
		lastPage = p
	}

	if lastPage == nil {
		_, newPage := s.allocatePage(DirectoryPageType, directoryName)
		lastPage = newPage
	}

	d := dir.Serialize()
	_, err := lastPage.Add(d)
	// retry if end of space
	if errors.Is(err, errNoSpace) {
		newPageID, p := s.allocatePage(DirectoryPageType, directoryName)
		must(p.Add(d))
		lastPage.Header.NextPage = newPageID
	}
}

func (s *Storage) Insert(stmt sql.InsertStatement) error {
	if newPageSchema {
		return s.Insert2(stmt)
	}
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

func (s *Storage) Insert2(stmt sql.InsertStatement) error {
	schema := []FieldName{}
	schemaLookup := map[FieldName]FieldType{} 
	for d := range NewEntityIterator(s, SchemaPageType, stmt.Table) {
		data, err := DeserializeSchemaTuple(d)
		if err != nil {
			return err
		}
		schemaLookup[data.FieldNameV] = data.FieldTypeV
		schema = append(schema, data.FieldNameV)
	}

	if len(schema) == 0 {
		return fmt.Errorf("table %v does not exist", stmt.Table)
	}
	
	inputLookup := map[FieldName]ColumnData{}
	for i := 0; i < len(stmt.Columns); i++ {
		col := stmt.Columns[i]
		val := stmt.Values[i]
		
		fieldType, ok := schemaLookup[FieldName(col)]
		if !ok {
			return fmt.Errorf("invalid column %v, not defined in schema for table %v", col, stmt.Table)
		}

		parsed, err := parseType(val, fieldType)
		if err != nil {
			return err
		}
		inputLookup[FieldName(col)] = ColumnData{
			Typ: fieldType,
			Data: parsed,
		}
	}

	inputData := []byte{}
	for _, col := range schema {
		d := inputLookup[col]	

		switch d.Typ {
		case Int32: 
			inputData = append(inputData, SerializeInt(d.Data.(int32))...)
		case String:
			inputData = append(inputData, SerializeString(d.Data.(string))...)
		case Boolean:
			inputData = append(inputData, SerializeBool(d.Data.(bool))...)
		}
	}

	s.AddTuple(DataPageType, stmt.Table, inputData)
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
	if newPageSchema {
		return s.Select2(stmt)
	}
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

func (s *Storage) Select2(stmt sql.SelectStatement) (QueryResult, error) {
	schema := []FieldName{}
	schemaLookup := map[FieldName]FieldType{} 
	var zero QueryResult

	for d := range NewEntityIterator(s, SchemaPageType, stmt.Table) {
		data, err := DeserializeSchemaTuple(d)
		if err != nil {
			return zero, err
		}
		schemaLookup[data.FieldNameV] = data.FieldTypeV
		schema = append(schema, data.FieldNameV)
	}

	if len(schema) == 0 {
		return zero, fmt.Errorf("table %v does not exist", stmt.Table)
	}

	columnsToQuery, err := colsToQuery(stmt, schemaLookup)
	if err != nil {
		return zero, err
	}

	out := QueryResult{
		Header: columnsToQuery,
	}

	for d := range NewEntityIterator(s, DataPageType, stmt.Table) {
		row := parseToRow(d, schema, schemaLookup)

		vals := []string{}
		for _, col := range columnsToQuery {
			vals = append(vals, fmt.Sprint(row[FieldName(col)].Data))
		}
		out.Values = append(out.Values, vals)
	}

	return out, nil
}

func (s *Storage) allSchema() Schema {
	if !newPageSchema {
		return s.SchemaMetadata
	}

	schema := Schema{}
	for dir := range DirectoryEntriesIterator(s) {
		t := TableSchema{}
		for sch := range NewEntityIterator(s, SchemaPageType, dir.Name) {
			entry := must(DeserializeSchemaTuple(sch))
			t[entry.FieldNameV] = entry.FieldTypeV	
		}
		if len(t) != 0 {
			schema[TableName(dir.Name)] = t
		}
	}

	return schema
}

func parseToRow(bytes []byte, schema []FieldName, lookup map[FieldName]FieldType) map[FieldName]ColumnData {
	out := map[FieldName]ColumnData{}
	for _, f := range schema {
		typ := lookup[f]
		cd := ColumnData{Typ: typ}
		switch typ {
			case Int32:
				cd.Data = must(DeserializeIntAndEat(&bytes))
			case String:
				cd.Data = must(DeserializeStringAndEat(&bytes))
			case Boolean:
				cd.Data = must(DeserializeBoolAndEat(&bytes))
			case Float: fallthrough
			default:
				panic("data corruption on parsing")
		}
		out[f] = cd
	}
	return out
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
	if !newPageSchema {
		serializedSchema := SerializeSchema(s.SchemaMetadata)
		serializedData := SerializeData(s.AllData)

		return SerializeAll(SerializeInt(int32(len(serializedSchema))),
			SerializeInt(int32(len(serializedData))),
			serializedSchema,
			serializedData)
	}
	var out bytes.Buffer
	out.Write(s.root.Serialize())
	for _, v := range s.allPages[1:] {
		out.Write(v.Serialize())
	}
	return out.Bytes()
}

func DeserializeDb(r io.Reader) (*Storage, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if newPageSchema {
		pages := []GenericPage{{}}
		root, err := DeserializeRootPage(bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		data = data[:PageSize]
		for len(data) != 0 {
			p, err := Deserialize(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			data = data[:PageSize]
			pages = append(pages, *p)
		}
		return &Storage{root: *root, allPages: pages}, nil
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

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
package naive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"simple-db/sql"
	"strconv"
)

const PageSize = 4*1024
const directoryName =  "catalog_directory"
const assertionsEnabled = true

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

type Storage struct {
	root RootPage
	allPages []GenericPage // pageId - just an index here. Page 0 is root, so noop
}

func NewStorage() *Storage {
	s := &Storage{
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
	debugAssert(len(schemaEntries) != 0, "no schema entries found")

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

	_, err := lastPage.Add(b)
	if errors.Is(err, errNoSpace) {
		newPageID, p := s.allocatePage(pageType, name)
		must(p.Add(b))
		lastPage.Header.NextPage = newPageID
		lastPageID = newPageID
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
			default:
				debugAssert(false, "data corruption on parsing, unknown type %v", typ)
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

func SerializeDb(s *Storage) []byte {
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
	debugAssert(err == nil, format, args...)
}
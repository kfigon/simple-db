package naive

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"simple-db/sql"
	"strconv"
)

const PageSize = 4 * 1024
const directoryName = "catalog_directory"
const schemaName = "catalog_schema"
const assertionsEnabled = true

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

type TableName string
type FieldName string

type ColumnData struct {
	Typ  FieldType
	Data any
}

type TableSchema map[FieldName]FieldType
type Schema map[TableName]TableSchema
type Row map[FieldName]ColumnData

type Storage struct {
	root     RootPage
	allPages []*GenericPage // pageId - just an index here. Page 0 is root, so noop
}

func NewStorage() *Storage {
	s := &Storage{
		allPages: []*GenericPage{&GenericPage{}}, // empty 'root' page in the beginning
	}

	dirID, _ := s.allocatePage(DirectoryPageType, directoryName)
	schemaID, _ := s.allocatePage(SchemaPageType, schemaName)
	s.root = NewRootPage(dirID, schemaID)

	return s
}

func (s *Storage) allocatePage(pageTyp PageType, name string) (PageID, *GenericPage) {
	p := NewPage(pageTyp, PageSize)
	newPageID := PageID(len(s.allPages))
	s.allPages = append(s.allPages, p)

	// link last page to the new one
	if startId, ok := s.iter().FindStartingPageForEntity(pageTyp, name); ok {
		var lastId PageID
		for id := range s.iter().NewPageIterator(startId) {
			lastId = id
		}
		s.allPages[lastId].Header.NextPage = newPageID
	}

	return newPageID, p
}

func (s *Storage) doesTableExists(table TableName) bool {
	for range s.iter().SchemaForTable(table) {
		return true
	}
	return false
}

func (s *Storage) CreateTable(stmt sql.CreateStatement) error {
	if s.doesTableExists(TableName(stmt.Table)) {
		return fmt.Errorf("table %v already present", stmt.Table)
	}

	schemaEntries := []SchemaTuple{}
	for _, v := range stmt.Columns {
		f, err := FieldTypeFromString(v.Typ)
		if err != nil {
			return err
		}
		schemaEntries = append(schemaEntries, SchemaTuple{
			TableNameV: TableName(stmt.Table),
			FieldNameV: FieldName(v.Name),
			FieldTypeV: f,
		})
	}
	if len(schemaEntries) == 0 {
		return fmt.Errorf("empty schema provided")
	}

	for _, v := range schemaEntries {
		s.AddSchemaTuple(v)
	}

	// add empty data page
	dataPageID, _ := s.allocatePage(DataPageType, stmt.Table)

	s.AddDirectoryTuple(DirectoryTuple{
		PageTyp:      DataPageType,
		StartingPage: dataPageID,
		Name:         stmt.Table,
	})
	return nil
}

func (s *Storage) AddTuple(pageType PageType, name string, b []byte) PageID {
	var lastPage *GenericPage
	var lastPageID PageID

	if startPage, ok := s.iter().FindStartingPageForEntity(pageType, name); !ok {
		// allocatePage also links it
		pageID, newPage := s.allocatePage(pageType, name)
		lastPage = newPage
		lastPageID = pageID
	} else {
		for pageID, p := range s.iter().NewPageIterator(startPage) {
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
	} else {
		debugAsserErr(err, "unknown error when adding tuple")
	}

	return lastPageID
}

func (s *Storage) AddDirectoryTuple(dir DirectoryTuple) {
	var lastPage *GenericPage
	for _, p := range s.iter().directoryPages() {
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
	} else {
		debugAsserErr(err, "unknown error when adding dir tuple")
	}
}

func (s *Storage) AddSchemaTuple(sch SchemaTuple) {
	var lastPage *GenericPage
	for _, p := range s.iter().schemaPages() {
		lastPage = p
	}

	if lastPage == nil {
		_, newPage := s.allocatePage(SchemaPageType, schemaName)
		lastPage = newPage
	}

	d := sch.Serialize()
	_, err := lastPage.Add(d)
	// retry if end of space
	if errors.Is(err, errNoSpace) {
		newPageID, p := s.allocatePage(SchemaPageType, schemaName)
		must(p.Add(d))
		lastPage.Header.NextPage = newPageID
	} else {
		debugAsserErr(err, "unknown error when adding schema tuple")
	}
}

func (s *Storage) iter() pageIterators {
	return pageIterators{s}
}

func (s *Storage) schemaForTable(tableName TableName) (schema []FieldName, schemaLookup map[FieldName]FieldType) {
	schemaLookup = map[FieldName]FieldType{}
	for data := range s.iter().SchemaForTable(tableName) {
		schemaLookup[data.FieldNameV] = data.FieldTypeV
		schema = append(schema, data.FieldNameV)
	}
	return
}

func (s *Storage) Insert(stmt sql.InsertStatement) error {
	schema, schemaLookup := s.schemaForTable(TableName(stmt.Table))
	if len(schema) == 0 {
		return fmt.Errorf("table %v does not exist", stmt.Table)
	}

	inputLookup := Row{}
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
			Typ:  fieldType,
			Data: parsed,
		}
	}

	inputData := bytes.NewBuffer(nil)
	for _, col := range schema {
		d := inputLookup[col]

		switch d.Typ {
		case Int32:
			inputData.Write(SerializeInt(d.Data.(int32)))
		case String:
			inputData.Write(SerializeString(d.Data.(string)))
		case Boolean:
			inputData.Write(SerializeBool(d.Data.(bool)))
		}
	}

	s.AddTuple(DataPageType, stmt.Table, inputData.Bytes())
	return nil
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

type QueryResult struct {
	Header []FieldName
	Values [][]string
}

func (s *Storage) Select(stmt sql.SelectStatement) (QueryResult, error) {
	var zero QueryResult
	schema, schemaLookup := s.schemaForTable(TableName(stmt.Table))

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

	rowIt := s.iter().RowIterator(stmt.Table, schema, schemaLookup)
	if stmt.Where != nil {
		rowIt = Select(rowIt, buildPredicate(stmt.Where.Predicate))
	}
	projection := Project(rowIt, columnsToQuery)

	for row := range projection {
		vals := make([]string, 0, len(columnsToQuery))
		for _, col := range columnsToQuery {
			vals = append(vals, fmt.Sprint(row[FieldName(col)].Data))
		}
		out.Values = append(out.Values, vals)
	}

	return out, nil
}

// todo: add error handling
func buildPredicate(pred sql.BoolExpression) func(Row) bool {
	return func(r Row) bool {
		col := predBuilder(pred, r)
		debugAssert(col.Typ == Boolean, "boolean predicate required, got %v", col.Typ)
		return col.Data.(bool)
	}
}

func castAndBinaryOp[T any](left, right any, op func(T, T) bool) bool {
	lV := left.(T)
	rV := right.(T)
	return op(lV, rV)
}

func eq[T comparable](a, b T) bool   { return a == b }
func neq[T comparable](a, b T) bool  { return a != b }
func or(a, b bool) bool              { return a || b }
func and(a, b bool) bool             { return a && b }
func gt[T cmp.Ordered](a, b T) bool  { return a > b }
func geq[T cmp.Ordered](a, b T) bool { return a >= b }
func lt[T cmp.Ordered](a, b T) bool  { return a < b }
func leq[T cmp.Ordered](a, b T) bool { return a <= b }

func buildCastAndOperand[T any](left, right ColumnData, fn func(a, b T) bool) func() bool {
	return func() bool {
		return castAndBinaryOp(left.Data, right.Data, fn)
	}
}

func predBuilder(pred sql.BoolExpression, r Row) ColumnData {
	switch v := pred.(type) {
	case *sql.InfixExpression:
		left := predBuilder(v.Left, r)
		right := predBuilder(v.Right, r)

		if v.Operator.Lexeme == "and" {
			return ColumnData{Boolean, castAndBinaryOp(left.Data, right.Data, and)}
		} else if v.Operator.Lexeme == "or" {
			return ColumnData{Boolean, castAndBinaryOp(left.Data, right.Data, or)}
		}

		op := map[string]map[FieldType]func() bool{
			"=": {
				String:  buildCastAndOperand(left, right, eq[string]),
				Int32:   buildCastAndOperand(left, right, eq[int32]),
				Boolean: buildCastAndOperand(left, right, eq[bool]),
			},
			"!=": {
				String:  buildCastAndOperand(left, right, neq[string]),
				Int32:   buildCastAndOperand(left, right, neq[int32]),
				Boolean: buildCastAndOperand(left, right, neq[bool]),
			},
			">":  {Int32: buildCastAndOperand(left, right, gt[int32])},
			">=": {Int32: buildCastAndOperand(left, right, geq[int32])},
			"<":  {Int32: buildCastAndOperand(left, right, lt[int32])},
			"<=": {Int32: buildCastAndOperand(left, right, leq[int32])},
		}

		ops, ok := op[v.Operator.Lexeme]
		debugAssert(ok, "unsupported op: %v", v.Operator)
		debugAssert(left.Typ == right.Typ, "incompatible types %v %v", left.Typ, right.Typ)

		fn, ok := ops[left.Typ]
		debugAssert(ok, "unknown type %v", left.Typ)
		return ColumnData{Boolean, fn()}
	case sql.ValueLiteral:
		if v.Tok.Typ == sql.Number {
			return ColumnData{Int32, int32(must(strconv.Atoi(v.Tok.Lexeme)))}
		} else if v.Tok.Typ == sql.String {
			return ColumnData{String, v.Tok.Lexeme}
		} else if v.Tok.Typ == sql.Boolean {
			return ColumnData{Boolean, must(strconv.ParseBool(v.Tok.Lexeme))}
		}
		debugAssert(false, "unsupported type %v", v)
	case sql.ColumnLiteral:
		return r[FieldName(v.Name.Lexeme)]
	}

	debugAssert(false, "unknown predicate type received %T", pred)
	panic("")
}

func (s *Storage) AllSchema() Schema {
	schema := Schema{}
	for sch := range s.iter().SchemaEntriesIterator() {
		v, ok := schema[sch.TableNameV]
		if !ok {
			v = TableSchema{}
		}

		v[sch.FieldNameV] = sch.FieldTypeV
		schema[sch.TableNameV] = v
	}

	return schema
}

func parseToRow(bytez []byte, schema []FieldName, lookup map[FieldName]FieldType) Row {
	out := Row{}
	buf := bytes.NewReader(bytez)
	for _, f := range schema {
		typ := lookup[f]
		cd := ColumnData{Typ: typ}
		switch typ {
		case Int32:
			cd.Data = must(ReadInt(buf))
		case String:
			cd.Data = must(ReadString(buf))
		case Boolean:
			cd.Data = must(ReadBool(buf))
		default:
			debugAssert(false, "data corruption on parsing, unknown type %v", typ)
		}
		out[f] = cd
	}
	return out
}

func colsToQuery(stmt sql.SelectStatement, schema TableSchema) ([]FieldName, error) {
	out := []FieldName{}

	if stmt.HasWildcard {
		for name := range schema {
			out = append(out, FieldName(name))
		}
		return out, nil
	}

	for _, v := range stmt.Columns {
		if _, ok := schema[FieldName(v)]; !ok {
			return nil, fmt.Errorf("unknown column %v in table %v", v, stmt.Table)
		}
		out = append(out, FieldName(v))
	}
	return out, nil
}

func SerializeDb(s *Storage) []byte {
	var out bytes.Buffer
	out.Write(s.root.Serialize())
	for _, v := range s.allPages[1:] {
		out.Write(v.Serialize())
	}
	res := out.Bytes()
	debugAssert(len(res)%PageSize == 0, "serialized database should be multiplication of page size")
	return res
}

func DeserializeDb(r io.Reader) (*Storage, error) {
	// todo: rework this readall
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if len(data)%PageSize != 0 {
		return nil, fmt.Errorf("read data should be multiplication of page size, got %d", len(data))
	}

	pages := []*GenericPage{{}}
	root, err := DeserializeRootPage(bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	data = data[PageSize:]
	for len(data) != 0 {
		p, err := Deserialize(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		data = data[PageSize:]
		pages = append(pages, p)
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

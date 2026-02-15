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

func ParseFieldTypeToData(v string, typ FieldType) (any, error) {
	switch typ {
	case Null:
		return nil, nil
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

type ColumnData struct {
	Typ  FieldType
	Data any
}

type TableName string
type FieldName string

type TableSchema map[FieldName]FieldType
type Schema map[TableName]TableSchema

type Row map[FieldName]ColumnData

type Storage struct {
	root          RootPage // page 0. cached root for ease of access
	allPagesBytes []byte
}

func NewStorage() *Storage {
	s := &Storage{
		allPagesBytes: make([]byte, 20*PageSize),
	}

	s.root = NewRootPage()
	schemaID, _ := s.allocatePage(SchemaPageType, schemaName)

	s.root.SchemaPageStart = schemaID
	// todo: optimise this, root persist is done also in dir and schema allocations, but misses setting dir and schema ids
	s.persistPage(0, s.root.Serialize())

	return s
}

func (s *Storage) allocatePage(pageTyp PageType, name string) (PageID, *GenericPage) {
	p := NewPage(pageTyp, PageSize)
	newPageID := PageID(s.root.NumberOfPages)

	// link last page to the new one
	if startId, ok := s.iter().FindStartingPageForEntity(pageTyp, name); ok {
		var lastId PageID
		for id := range s.iter().NewPageIterator(startId) {
			lastId = id
		}
		lastPage := s.getPage(lastId)

		lastPage.Header.NextPage = newPageID
		s.persistPage(lastId, lastPage.Serialize())
	}

	s.root.NumberOfPages++
	s.persistPage(0, s.root.Serialize())
	s.persistPage(newPageID, p.Serialize())

	return newPageID, p
}

func (s *Storage) allocateOverflowPages(bigColumn []byte) PageID {
	firstPageID := PageID(s.root.NumberOfPages)

	type pair struct {
		pid  PageID
		page *OverflowPage
	}

	overFlowPages := make([]*pair, 0)
	idx := 0
	for {
		newPage, rest := NewOverflowPage(PageSize, bigColumn)
		newPageID := PageID(s.root.NumberOfPages)

		overFlowPages = append(overFlowPages, &pair{newPageID, newPage})

		if idx > 0 {
			overFlowPages[idx-1].page.Header.NextPage = newPageID
		}

		s.root.NumberOfPages++

		if len(rest) == 0 {
			break
		}
		idx++
	}

	for _, p := range overFlowPages {
		s.persistPage(p.pid, p.page.Serialize())
	}

	s.persistPage(0, s.root.Serialize())
	return firstPageID
}

func byteOffsetFromPageID(p PageID) int {
	return int(p) * PageSize
}

func (s *Storage) getPage(id PageID) *GenericPage {
	// todo: guard

	offset := byteOffsetFromPageID(id)
	pageBytes := s.allPagesBytes[offset : offset+PageSize]
	buf := bytes.NewBuffer(pageBytes)
	header := must(DeserializeGenericHeader(buf))
	// todo: overflow pages?
	page := must(DeserializeGenericPage(header, buf))
	return page
}

func (s *Storage) persistPage(id PageID, pageBytes []byte) {
	debugAssert(len(pageBytes) == PageSize, "enforcing page size")
	offset := byteOffsetFromPageID(id)

	// realloc if needed
	if offset+len(pageBytes) >= len(s.allPagesBytes) {
		newBytes := make([]byte, PageSize*2*s.root.NumberOfPages)
		copy(newBytes, s.allPagesBytes)
		s.allPagesBytes = newBytes
	}
	copy(s.allPagesBytes[offset:offset+len(pageBytes)], pageBytes)
}

func (s *Storage) findSchemaTuple(table TableName) (*SchemaTuple, bool) {
	for s := range s.iter().SchemaEntriesIterator() {
		if s.Name == string(table) {
			return &s, true
		}
	}
	return nil, false
}

func (s *Storage) CreateTable(stmt sql.CreateStatement) error {
	if _, ok := s.findSchemaTuple(TableName(stmt.Table)); ok {
		return fmt.Errorf("table %v already present", stmt.Table)
	}

	if len(stmt.Columns) == 0 {
		return fmt.Errorf("empty schema provided")
	}

	// add empty data page
	dataPageID, dataPage := s.allocatePage(DataPageType, stmt.Table)
	s.persistPage(dataPageID, dataPage.Serialize())

	sch := SchemaTuple{
		PageTyp:        DataPageType,
		Name:           stmt.Table,
		StartingPageID: dataPageID,
		SqlStatement:   stmt.String(), // todo: do it better - don't generate statement again
	}

	s.AddSchemaTuple(sch)
	return nil
}

func (s *Storage) AddTuple(pageType PageType, name string, t Tuple) PageID {
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

	serialized := t.Serialize()
	_, err := lastPage.Add(serialized)
	if errors.Is(err, errNoSpace) {
		newPageID, p := s.allocatePage(pageType, name)
		must(p.Add(serialized))
		lastPage.Header.NextPage = newPageID
		lastPageID = newPageID

		s.persistPage(lastPageID, lastPage.Serialize())
		s.persistPage(newPageID, p.Serialize())
	} else {
		s.persistPage(lastPageID, lastPage.Serialize())
		debugAsserErr(err, "unknown error when adding tuple")
	}

	return lastPageID
}

func (s *Storage) AddSchemaTuple(sch SchemaTuple) {
	var lastPage *GenericPage
	var lastPageID PageID
	for pageID, p := range s.iter().schemaPages() {
		lastPage = p
		lastPageID = pageID
	}

	if lastPage == nil {
		newPageID, newPage := s.allocatePage(SchemaPageType, schemaName)
		lastPage = newPage
		lastPageID = newPageID
	}

	d := sch.Serialize()
	_, err := lastPage.Add(d)
	// retry if end of space
	if errors.Is(err, errNoSpace) {
		newPageID, p := s.allocatePage(SchemaPageType, schemaName)
		must(p.Add(d))
		lastPage.Header.NextPage = newPageID

		s.persistPage(lastPageID, lastPage.Serialize())
		s.persistPage(newPageID, p.Serialize())
	} else {
		s.persistPage(lastPageID, lastPage.Serialize())
		debugAsserErr(err, "unknown error when adding schema tuple")
	}
}

func (s *Storage) iter() pageIterators {
	return pageIterators{s}
}

func (s *Storage) schemaForTable(tableName TableName) (schema []FieldName, schemaLookup map[FieldName]FieldType, ok bool) {
	sch, ok := s.findSchemaTuple(tableName)
	if !ok {
		return
	}
	return extractSchema(*sch)
}

func extractSchema(sch SchemaTuple) (schema []FieldName, schemaLookup map[FieldName]FieldType, ok bool) {
	got, err := sql.Parse(sql.Lex(sch.SqlStatement))
	debugAsserErr(err, "schema corruption, invalid sql statement for table: %s", sch.Name)
	createStmt, ok := got.(*sql.CreateStatement)
	if !ok {
		return
	}

	schemaLookup = map[FieldName]FieldType{}
	for _, data := range createStmt.Columns {
		f, err := FieldTypeFromString(data.Typ)
		debugAsserErr(err, "schema corruption, invalid type for table %s: ", sch.Name)

		schemaLookup[FieldName(data.Name)] = f
		schema = append(schema, FieldName(data.Name))
	}

	ok = true
	return
}

func (s *Storage) Insert(stmt sql.InsertStatement) error {
	schema, schemaLookup, ok := s.schemaForTable(TableName(stmt.Table))
	if !ok {
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

		parsed, err := ParseFieldTypeToData(val, fieldType)
		if err != nil {
			return err
		}
		inputLookup[FieldName(col)] = ColumnData{
			Typ:  fieldType,
			Data: parsed,
		}
	}

	tuple := Tuple{
		NumberOfFields: int32(len(schema)),
	}
	for _, col := range schema {
		d := inputLookup[col]

		switch d.Typ {
		case Int32:
			tuple.ColumnDatas = append(tuple.ColumnDatas, SerializeInt(d.Data.(int32)))
			tuple.ColumnTypes = append(tuple.ColumnTypes, IntField)
		case String:
			strVal := d.Data.(string)
			if len(strVal) >= PageSize/2 {
				overFlowPageStartID := s.allocateOverflowPages([]byte(strVal))
				first := SerializeInt(int32(len(strVal)))
				second := SerializeInt(int32(overFlowPageStartID))
				serializedData := make([]byte, 0, 4+4)
				serializedData = append(serializedData, first...)
				serializedData = append(serializedData, second...)
				tuple.ColumnDatas = append(tuple.ColumnDatas, serializedData)
				tuple.ColumnTypes = append(tuple.ColumnTypes, OverflowField)
			} else {
				tuple.ColumnDatas = append(tuple.ColumnDatas, SerializeString(strVal))
				tuple.ColumnTypes = append(tuple.ColumnTypes, StringField)
			}
		case Boolean:
			tuple.ColumnDatas = append(tuple.ColumnDatas, SerializeBool(d.Data.(bool)))
			tuple.ColumnTypes = append(tuple.ColumnTypes, BooleanField)
		}
	}

	s.AddTuple(DataPageType, stmt.Table, tuple)
	return nil
}

type QueryResult struct {
	Header []FieldName
	Values [][]string
}

func (s *Storage) Select(stmt sql.SelectStatement) (QueryResult, error) {
	var zero QueryResult
	schema, schemaLookup, ok := s.schemaForTable(TableName(stmt.Table))

	if !ok {
		return zero, fmt.Errorf("table %v does not exist", stmt.Table)
	}

	columnsToQuery, err := colsToQuery(stmt, schemaLookup)
	if err != nil {
		return zero, err
	}

	out := QueryResult{
		Header: columnsToQuery,
	}

	rowIt := s.iter().RowIterator(stmt.Table, schema)
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
		_, lookup, _ := extractSchema(sch)
		schema[TableName(sch.Name)] = lookup
	}

	return schema
}

func parseTupleToRow(t Tuple, schema []FieldName) Row {
	out := Row{}
	for i := range t.NumberOfFields {
		data := t.ColumnDatas[i]
		typ := t.ColumnTypes[i]
		fieldName := schema[i]
		buf := bytes.NewBuffer(data)

		var columnData *ColumnData
		switch typ {
		case NullField:
			columnData = &ColumnData{Null, nil}
		case BooleanField:
			columnData = &ColumnData{Boolean, must(ReadBool(buf))}
		case IntField:
			columnData = &ColumnData{Int32, must(ReadInt(buf))}
		case StringField:
			columnData = &ColumnData{String, must(ReadString(buf))}
		case OverflowField:
			// todo: handle overflows
			debugAssert(false, "overflow todo - pass storage and follow pointers")
		default:
			debugAssert(false, "unexpected field type: %d", typ)
		}
		out[fieldName] = *columnData
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
	// out.Write(s.root.Serialize())
	// for _, v := range s.iter().AllPages(1) {
	// 	out.Write(v.Serialize())
	// }
	for i := range s.root.NumberOfPages {
		p := s.allPagesBytes[i*PageSize : (i+1)*PageSize]
		out.Write(p)
	}
	res := out.Bytes()
	debugAssert(len(res)%PageSize == 0, "serialized database should be multiplication of page size")
	return res
}

func DeserializeDb(r io.Reader) (*Storage, error) {
	allBytes := bytes.NewBuffer(nil)
	root, err := DeserializeRootPage(r)
	if err != nil {
		return nil, err
	}
	allBytes.Write(root.Serialize())

	// deserialize and validate

	numOfPages := 1

	// -1, because of root page
	for i := range root.NumberOfPages - 1 {
		header, err := DeserializeGenericHeader(r)
		if err != nil {
			return nil, fmt.Errorf("header corruption on page %d: %w", i, err)
		}

		if header.PageTyp == OverflowPageType {
			p, err := DeserializeOverflowPage(header, r)
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("unexpected end of data, expected %d pages, failed at %d", root.NumberOfPages, i)
			} else if err != nil {
				return nil, err
			}
			numOfPages++
			allBytes.Write(p.Serialize())
		} else {
			p, err := DeserializeGenericPage(header, r)
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("unexpected end of data, expected %d pages, failed at %d", root.NumberOfPages, i)
			} else if err != nil {
				return nil, err
			}
			numOfPages++
			allBytes.Write(p.Serialize())
		}
	}

	if numOfPages != int(root.NumberOfPages) {
		return nil, fmt.Errorf("corrupted metadata. Expected %d pages, deserialized %d", root.NumberOfPages, numOfPages)
	}
	return &Storage{root: *root, allPagesBytes: allBytes.Bytes()}, nil
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

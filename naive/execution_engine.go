package naive

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"simple-db/sql"
	"strconv"
)

type Database struct {
	*ExecutionEngine
}

func (d *Database) Serialize() []byte {
	var out bytes.Buffer
	for i := range d.storage.root.NumberOfPages {
		p := d.storage.allPages[byteOffsetFromPageID(PageID(i)):byteOffsetFromPageID(PageID(i+1))]
		out.Write(p)
	}
	res := out.Bytes()
	debugAssert(len(res)%PageSize == 0, "serialized database should be multiplication of page size")
	return res
}

func NewDatabaseFromBytes(r io.Reader) (*Database, error) {
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

	return NewDatabaseWithStorage(NewStorageEngineWithData(root, allBytes.Bytes())), nil
}

func NewDatabase() *Database {
	return &Database{NewExecutionEngine(NewStorageEngine())}
}

func NewDatabaseWithStorage(storage *StorageEngine) *Database {
	return &Database{NewExecutionEngine(storage)}
}

func (d *Database) Execute(sqlStatement string) (any, error) {
	stmt, err := sql.Parse(sql.Lex(sqlStatement))
	if err != nil {
		return nil, err
	}

	switch stmt := stmt.(type) {
	case *sql.CreateStatement:
		return nil, d.CreateTable(*stmt)
	case *sql.InsertStatement:
		return nil, d.Insert(*stmt)
	case *sql.SelectStatement:
		return d.Select(*stmt)
	default:
		return nil, fmt.Errorf("unknown statement type %T", stmt)
	}
}

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

type TableSchema struct {
	FieldsTypes []FieldType
	FieldNames  []FieldName
	StartPage   PageID
	PageTyp     PageType
}
type Schema map[TableName]TableSchema

type Row map[FieldName]ColumnData

type ExecutionEngine struct {
	storage *StorageEngine
}

func NewExecutionEngine(storage *StorageEngine) *ExecutionEngine {
	return &ExecutionEngine{
		storage: storage,
	}
}

func (e *ExecutionEngine) Schema() Schema {
	return e.storage.GetSchema()
}

func (e *ExecutionEngine) CreateTable(stmt sql.CreateStatement) error {
	_, schemaFound := FindStartingPage(e.Schema(), string(stmt.Table))
	if schemaFound {
		return fmt.Errorf("table %v already present", stmt.Table)
	} else if len(stmt.Columns) == 0 {
		return fmt.Errorf("empty table definition provided")
	}

	// empty data page
	dataPageID, _ := e.storage.AllocatePage(DataPageType, stmt.Table)

	sch := SchemaTuple{
		PageTyp:        DataPageType,
		StartingPageID: dataPageID,
		Name:           stmt.Table,
		SqlStatement:   stmt.String(),
	}

	_, _, err := e.storage.AddTuple(schemaName, sch.ToTuple())
	return err
}

func (e *ExecutionEngine) Insert(stmt sql.InsertStatement) error {
	schema, schemaFound := e.storage.GetSchema()[TableName(stmt.Table)]
	if !schemaFound {
		return fmt.Errorf("table %v not found", stmt.Table)
	}

	lookup := map[FieldName]FieldType{}
	for i := 0; i < len(schema.FieldNames); i++ {
		lookup[schema.FieldNames[i]] = schema.FieldsTypes[i]
	}

	inputLookup := Row{}
	for i := 0; i < len(stmt.Columns); i++ {
		col := stmt.Columns[i]
		val := stmt.Values[i]

		typ, ok := lookup[FieldName(col)]
		if !ok {
			return fmt.Errorf("unknown column %q for %v", col, stmt.Table)
		}
		parsed, err := ParseFieldTypeToData(val, typ)
		if err != nil {
			return fmt.Errorf("type mismatch for column %q for table %v, expected %v", col, stmt.Table, typ)
		}

		inputLookup[FieldName(col)] = ColumnData{
			Typ:  typ,
			Data: parsed,
		}
	}

	tuple := Tuple{
		NumberOfFields: int32(len(schema.FieldsTypes)),
	}

	for _, col := range schema.FieldNames {
		d, ok := inputLookup[col]
		if !ok {
			return fmt.Errorf("column %q not provided for %v", col, stmt.Table)
		}

		switch d.Typ {
		case Int32:
			tuple.ColumnDatas = append(tuple.ColumnDatas, SerializeInt(d.Data.(int32)))
			tuple.ColumnTypes = append(tuple.ColumnTypes, IntField)
		case String:
			tuple.ColumnDatas = append(tuple.ColumnDatas, SerializeString(d.Data.(string)))
			tuple.ColumnTypes = append(tuple.ColumnTypes, StringField)
		case Boolean:
			tuple.ColumnDatas = append(tuple.ColumnDatas, SerializeBool(d.Data.(bool)))
			tuple.ColumnTypes = append(tuple.ColumnTypes, BooleanField)
		}
	}

	_, _, err := e.storage.AddTuple(stmt.Table, tuple)
	return err
}

func (e *ExecutionEngine) Select(stmt sql.SelectStatement) (QueryResult, error) {
	// todo: better structure, currently it's not lazy
	var zero QueryResult
	schema, ok := e.storage.GetSchema()[TableName(stmt.Table)]
	if !ok {
		return zero, fmt.Errorf("table %v does not exist", stmt.Table)
	}

	columnsToQuery, err := colsToQuery(stmt, schema)
	if err != nil {
		return zero, err
	}

	out := QueryResult{
		Header: columnsToQuery,
	}

	// todo: row iterator. Should I use regular tuples here and late materialize?
	rowIt := e.rowIteratorzz(schema)
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

func (e *ExecutionEngine) rowIteratorzz(tableSchema TableSchema) RowIter {
	return func(yield func(Row) bool) {
		for tup := range e.storage.Tuples(tableSchema.StartPage) {
			row := e.parseTupleToRow(tup, tableSchema.FieldNames)
			if !yield(row) {
				return
			}
		}
	}
}

type QueryResult struct {
	Header []FieldName
	Values [][]string
}

func (e *ExecutionEngine) parseTupleToRow(t Tuple, schema []FieldName) Row {
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
			length := must(ReadInt(buf))
			firstPageID := must(ReadInt(buf))
			parsedData := e.followOverflowChain(int(length), PageID(firstPageID))
			columnData = &ColumnData{String, string(parsedData)}
		default:
			debugAssert(false, "unexpected field type: %d", typ)
		}
		out[fieldName] = *columnData
	}
	return out
}

func (e *ExecutionEngine) followOverflowChain(dataLen int, firstPage PageID) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, dataLen))
	remainingDataLen := dataLen

	for _, page := range e.storage.ReadPages(firstPage) {
		overflowPage := must(DeserializeOverflowPage(&page.GenericPageHeader, bytes.NewBuffer(page.data)))

		// todo: check for one off errors
		howMuchToRead := len(overflowPage.Data)
		if remainingDataLen < howMuchToRead {
			howMuchToRead = remainingDataLen
		}
		buf.Write(overflowPage.Data[:howMuchToRead])
	}
	return buf.Bytes()
}

func colsToQuery(stmt sql.SelectStatement, schema TableSchema) ([]FieldName, error) {
	out := []FieldName{}

	if stmt.HasWildcard {
		for _, name := range schema.FieldNames {
			out = append(out, FieldName(name))
		}
		return out, nil
	}
	allNames := map[FieldName]bool{}
	for _, v := range schema.FieldNames {
		allNames[v] = true
	}

	for _, v := range stmt.Columns {
		if _, ok := allNames[FieldName(v)]; !ok {
			return nil, fmt.Errorf("unknown column %v in table %v", v, stmt.Table)
		}
		out = append(out, FieldName(v))
	}
	return out, nil
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
	errStr := func() string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	debugAssert(err == nil, format+": "+errStr(), args...)
}

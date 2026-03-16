package naive

import (
	"errors"
	"fmt"
	"io"
	"simple-db/sql"
)

type Database struct {
	*ExecutionEngine
}

func (d *Database) Serialize() []byte {
	// todo
	return nil
}

func NewDatabaseFromBytes(r io.Reader) (*Database, error) {
	// todo
	return nil, nil
}

func NewDatabase() *Database {
	return &Database{NewExecutionEngine()}
}

type ExecutionEngine struct {
	storage *StorageEngine
}

func NewExecutionEngine() *ExecutionEngine {
	return &ExecutionEngine{
		storage: NewStorageEngine(),
	}
}

func (e *ExecutionEngine) CreateTable(stmt sql.CreateStatement) error {
	_, schemaFound := FindStartingPage(e.storage.GetSchema2(), DataPageType, string(stmt.Table))
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
	return e.addTupleAndAllocIfFull(schemaName, SchemaPageType, sch.ToTuple())
}

func (e *ExecutionEngine) addTupleAndAllocIfFull(name string, pageTyp PageType, t Tuple) error {
	page, id, err := e.storage.AddTuple(pageTyp, name, t)

	if err == nil {
		e.storage.persistPage(id, page.Serialize())
	} else if errors.Is(err, errNoSpace) {
		id, page := e.storage.AllocatePage(pageTyp, name)
		_, err = page.Add(t)
		if err != nil {
			return fmt.Errorf("error during realloc and add: %w", err)
		}
		e.storage.persistPage(id, page.Serialize())
	}
	return err
}

func (e *ExecutionEngine) Insert(stmt sql.InsertStatement) error {
	schema, schemaFound := e.storage.GetSchema2()[TableName(stmt.Table)]
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
			strVal := d.Data.(string)
			if len(strVal) >= PageSize/2 {
				overFlowPageStartID := e.storage.AllocateOverflowPage([]byte(strVal))
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

	return e.addTupleAndAllocIfFull(stmt.Table, DataPageType, tuple)
}

func (e *ExecutionEngine) Select(stmt sql.SelectStatement) (QueryResult, error) {
	// todo: better structure, currently it's not lazy
	var zero QueryResult
	schema, ok := e.storage.GetSchema2()[TableName(stmt.Table)]
	if !ok {
		return zero, fmt.Errorf("table %v does not exist", stmt.Table)
	}

	schemaLookup := map[FieldName]FieldType{}
	for i := 0; i < len(schema.FieldsTypes); i++ {
		schemaLookup[schema.FieldNames[i]] = schema.FieldsTypes[i]
	}

	columnsToQuery, err := colsToQuery(stmt, schemaLookup)
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

// todo: move to proper place
func (e *ExecutionEngine) rowIteratorzz(tableSchema TableSchema2) RowIter {
	return func(yield func(Row) bool) {
		for tup := range e.storage.Tuples(tableSchema.StartPage) {
			row := parseTupleToRow(tup, tableSchema.FieldNames)
			if !yield(row) {
				return
			}
		}
	}
}

func (e *ExecutionEngine) AllSchema() Schema {
	// make schema fit in regular generic pages, just add typed deserialized materialized entries
	// schema of schema tuple is known. Make it an iterator and filter required table
	return Schema{}
}

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
	if !schemaFound {
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
	_, err := e.storage.AddTuple(pageTyp, name, t)
	if errors.Is(err, errNoSpace) {
		// todo: realloc page
		debugAssert(false, "todo: realloc if full")
	}
	return err
}

func (e *ExecutionEngine) Insert(stmt sql.InsertStatement) error {
	// validate
	// serialize to tuple
	// allocate overflows if required
	// find free page or allocate new
	return nil
}

func (e *ExecutionEngine) Select(stmt sql.SelectStatement) (QueryResult, error) {
	// validate
	// prepare plan, build operators
	// execute plan
	// materialize to query result
	return QueryResult{}, nil
}

func (e *ExecutionEngine) AllSchema() Schema {
	// make schema fit in regular generic pages, just add typed deserialized materialized entries
	// schema of schema tuple is known. Make it an iterator and filter required table
	return Schema{}
}

package naive

import (
	"io"
	"simple-db/sql"
)

type ExecutionEngine struct {
}

func NewExecutionEngine() *ExecutionEngine {
	return &ExecutionEngine{}
}

func (e *ExecutionEngine) CreateTable(stmt sql.CreateStatement) error {
	// validate
	// allocate page
	// add schema tuple
	return nil
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
	// schema of schema tuple is known
	return Schema{}
}

// probably should be done on top DB class
func (e *ExecutionEngine) Serialize() []byte {
	return nil
}

func BuildFromBytes(r io.Reader) (*ExecutionEngine, error) {
	return nil, nil
}

package sql

type Statement interface{
	statementTag()
}

type SelectStatement struct {
	Columns []string
	HasWildcard bool
	Table string
}
func (*SelectStatement) statementTag(){}

type InsertStatement struct {
	Columns []string
	Values []string
	Table string
}
func (*InsertStatement) statementTag(){}

type CreateStatement struct {
	Columns []ColumnDefinition
	Table string
}
type ColumnDefinition struct {
	Name string
	Typ  string
}

func (*CreateStatement) statementTag(){}

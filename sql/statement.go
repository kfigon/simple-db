package sql

type Statement interface {
	statementTag()
}

type WhereStatement struct {
	Predicate BoolExpression
}

type SelectStatement struct {
	Columns     []string
	HasWildcard bool
	Table       string
	Where       *WhereStatement
}

func (*SelectStatement) statementTag() {}

type InsertStatement struct {
	Columns []string
	Values  []string
	Table   string
}

func (*InsertStatement) statementTag() {}

type CreateStatement struct {
	Columns []ColumnDefinition
	Table   string
}
type ColumnDefinition struct {
	Name string
	Typ  string
}

func (*CreateStatement) statementTag() {}

type BoolExpression interface {
	expressionTag()
}

type InfixExpression struct {
	Operator Token
	Left     BoolExpression
	Right    BoolExpression
}

func (*InfixExpression) expressionTag() {}

type ValueLiteral struct {
	Tok Token
}

func (ValueLiteral) expressionTag() {}

type ColumnLiteral struct {
	Name Token
}

func (ColumnLiteral) expressionTag() {}

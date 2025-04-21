package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	testCases := []struct {
		desc	string
		input	string
		expected Statement
	}{
		{
			desc: "simple select wildcard",
			input: "select * from foobar",
			expected: &SelectStatement{
				HasWildcard: true,
				Table: "foobar",
			},
		},
		{
			desc: "select with columns",
			input: "select a,asdf , bar from foobar",
			expected: &SelectStatement{
				Columns: []string{"a","asdf", "bar"},
				HasWildcard: false,
				Table: "foobar",
			},
		},
		{
			desc: "select with column where",
			input: "select * from foobar where a = 4",
			expected: &SelectStatement{
				HasWildcard: true,
				Table: "foobar",
				Where: &WhereStatement{BinaryBoolExpression{
					Operator: Token{Operator, "=", 1},
					Left: ColumnLiteral{Token{Identifier, "a", 1}},
					Right: ValueLiteral{Token{Number, "4", 1}},
				}},
			},
		},
		{
			desc: "select with where literal",
			input: "select * from foobar where true",
			expected: &SelectStatement{
				HasWildcard: true,
				Table: "foobar",
				Where: &WhereStatement{ValueLiteral{Token{Boolean, "true", 1}}}},
		},
		{
			desc: "select with where boolean",
			input: "select * from foobar where a = true",
			expected: &SelectStatement{
				HasWildcard: true,
				Table: "foobar",
				Where: &WhereStatement{BinaryBoolExpression{
					Operator:  Token{Operator, "=", 1},
					Left: ColumnLiteral{Token{Identifier, "a", 1}},
					Right: ValueLiteral{Token{Boolean, "true", 1}},
				}}},
		},
		{
			desc: "select with more predicates",
			input: `select * from foobar where a = 4 and b = "asdf"`,
			expected: &SelectStatement{
				HasWildcard: true,
				Table: "foobar",
				Where: &WhereStatement{BinaryBoolExpression{
					Operator: Token{Operator, "and",1},
					Left: BinaryBoolExpression{
						Operator: Token{Operator, "=", 1},
						Left: ColumnLiteral{Token{Identifier, "a", 1}},
						Right: ValueLiteral{Token{Number, "4", 1}},
					},
					Right: BinaryBoolExpression{
						Operator: Token{Operator, "=", 1},
						Left: ColumnLiteral{Token{Identifier, "b", 1}},
						Right: ValueLiteral{Token{String, "asdf", 1}},
					},
				}}},
		},
		{
			desc: "select with 3 predicates",
			input: `select * from foobar where a = 4 and b = "asdf" and c = true`,
			expected: &SelectStatement{
				HasWildcard: true,
				Table: "foobar",
				Where: &WhereStatement{BinaryBoolExpression{
					Operator: Token{Operator, "and",1},
					Left: BinaryBoolExpression{
						Operator: Token{Operator, "=", 1},
						Left: ColumnLiteral{Token{Identifier, "a", 1}},
						Right: ValueLiteral{Token{Number, "4", 1}},
					},
					Right: BinaryBoolExpression{
						Operator: Token{Operator, "and", 1},
						Left: BinaryBoolExpression{
							Operator: Token{Operator, "=", 1},
							Left: ColumnLiteral{Token{Identifier, "b", 1}},
							Right: ValueLiteral{Token{String, "asdf", 1}},
						},
						Right: BinaryBoolExpression{
							Operator: Token{Operator, "=", 1},
							Left: ColumnLiteral{Token{Identifier, "c", 1}},
							Right: ValueLiteral{Token{Boolean, "true", 1}},
						},
					},
				}},
			},
		},
		{
			desc: "create 1",
			input: `create table foobar(
				abc int,
				foobarz varchar,
				asdf boolean,
			)`,
			expected: &CreateStatement{
				Columns: []ColumnDefinition{
					{"abc", "int"},
					{"foobarz", "varchar"},
					{"asdf", "boolean"},
				},
				Table: "foobar",
			},
		},
		{
			desc: "create 2",
			input: `create table foobar(abc int, asdf boolean)`,
			expected: &CreateStatement{
				Columns: []ColumnDefinition{
					{"abc", "int"},
					{"asdf", "boolean"},
				},
				Table: "foobar",
			},
		},
		{
			desc: "insert1",
			input: `INSERT INTO foobar (colA, colB, colC)
					VALUES (true, 1234, "asfg")`,
			expected: &InsertStatement{
				Table: "foobar",
				Columns: []string{"colA", "colB", "colC"},
				Values: []string{"true", "1234", "asfg"},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got, err := Parse(Lex(tC.input))
			assert.NoError(t, err)
			assert.Equal(t, tC.expected, got)
		})
	}
}
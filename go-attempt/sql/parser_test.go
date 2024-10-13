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
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got, err := Parse(Lex(tC.input))
			assert.NoError(t, err)
			assert.Equal(t, tC.expected, got)
		})
	}
}
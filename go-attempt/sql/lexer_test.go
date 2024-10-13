package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLex(t *testing.T) {
	testCases := []struct {
		desc	string
		input 	string
		expected []Token
	}{
		{
			desc: "tokens1",
			input: "select i 3 adf 123 from . x",
			expected: []Token{
				{Select, "select", 1},
				{Identifier, "i", 1},
				{Number, "3", 1},
				{Identifier, "adf", 1},
				{Number, "123", 1},
				{From, "from", 1},
				{Dot, ".", 1},
				{Identifier, "x", 1},
				{EOF, "", 1},
			},
		},
		{
			desc: "create1",
			input: `create table foobar(
				abc int,
				asdf boolean,
			)`,
			expected: []Token{
				{Create, "create", 1},
				{Table, "table", 1},
				{Identifier, "foobar", 1},
				{OpenParen, "(", 1},
				
				{Identifier, "abc", 2},
				{Identifier, "int", 2},
				{Comma, ",", 2},
				{Identifier, "asdf", 3},
				{Identifier, "boolean", 3},
				{Comma, ",", 3},
				
				{CloseParen, ")", 4},
				{EOF, "", 4},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got := Lex(tC.input)
			assert.Equal(t, tC.expected, got)
		})
	}
}
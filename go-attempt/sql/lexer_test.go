package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLex(t *testing.T) {
	// todo: more
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
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got := Lex(tC.input)
			assert.Equal(t, tC.expected, got)
		})
	}
}
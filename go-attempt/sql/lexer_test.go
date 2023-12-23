package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLex(t *testing.T) {
	t.Skip("todo")

	testCases := []struct {
		desc	string
		input 	string
		expected []Token
	}{
		{
			desc: "",
			
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			
		})
	}
}

func TestLexer2(t *testing.T) {
	res := Lex2("select i 3 adf 123 from . x")
	exp := []Token{
		{Select, "select", 1},
		{Identifier, "i", 1},
		{Number, "3", 1},
		{Identifier, "adf", 1},
		{Number, "123", 1},
		{From, "from", 1},
		{Dot, ".", 1},
		{Identifier, "x", 1},
		{EOF, "", 1},
	}
	assert.Equal(t, exp, res)
}
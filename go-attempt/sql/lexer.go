package sql

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int
const (
	Select TokenType = iota
	Identifier
	Number
	Comma
	Dot
	Wildcard
	From
	Where
	Join
	Left
	Right
	Outer
	Operator
	OpenParen
	CloseParen
	Having
	EOF
)

func (t TokenType) String() string {
	return [...]string {
		"Select",
		"Identifier",
		"Number",
		"Comma",
		"Dot",
		"Wildcard",
		"From",
		"Where",
		"Join",
		"Left",
		"Right",
		"Outer",
		"Operator",
		"OpenParen",
		"CloseParen",
		"Having",
		"EOF",
	}[int(t)]
}

type Token struct {
	Typ TokenType
	Lexeme string
	Line int
}

func (t Token) String() string {
	return fmt.Sprintf("<%v; %v>", t.Typ, t.Lexeme)
}

func Lex(in string) []Token {
	var out []Token
	it := &strIter{strings.NewReader(in)}

	singleCharTokens := map[rune]TokenType {
		'.': Dot,
		',': Comma,
		'*': Wildcard,
		'=': Operator,
		'(': OpenParen,
		')': CloseParen,
	}

	keyword := map[string]TokenType {
		"select": Select,
		"from": From,
		"having": Having,
		"where": Where,
		"join": Join,
		"left": Left,
		"right": Right,
		"outer": Outer,
	}
	stringToType := func(w string) TokenType {
		if t, ok := keyword[w]; ok {
			return t
		}
		return Identifier
	}

	line := 1
	for c, ok := it.next(); ok; c, ok = it.next() {
		if c == '\n' {
			line++
		} else if unicode.IsSpace(c) {
			continue
		} else if c == '!' || c == '<' || c == '>' {
			if next, ok := it.peek(); ok && next == '=' {
				it.next()
				out = append(out, emit(Operator, string(c+next), line))
			} else {
				out = append(out, emit(Operator, string(c), line))
			}
		} else if typ, ok := singleCharTokens[c]; ok {
			out = append(out, emit(typ, string(c), line))
		} else if unicode.IsDigit(c) {
			dig := readUntil(c, it, unicode.IsDigit)
			out = append(out, emit(Number, dig, line))
		} else {
			word := readUntil(c, it, unicode.IsLetter)
			out = append(out, emit(stringToType(word), word, line))
		}
	}

	out = append(out, emit(EOF, "", line))
	return out
}

func readUntil(first rune, si *strIter, fn func(rune)bool) string {
	out := string(first)
	for next, ok := si.peek(); ok; next,ok = si.peek() {
		if fn(next) {
			si.next()
			out += string(next)
		} else {
			break
		}
	}
	return out
}

type strIter struct {
	*strings.Reader
}

func(l *strIter) next() (rune, bool) {
	r, _, err := l.ReadRune()
	if err != nil {
		return r, false
	}
	return r, true
}

func(l *strIter) peek() (rune, bool) {
	r, ok := l.next()
	l.UnreadRune()
	return r, ok
}

func emit(typ TokenType, lexeme string, line int) Token {
	return Token{typ, lexeme, line}
}
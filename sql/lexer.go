package sql

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int
const (
	EOF TokenType = iota
	Select
	Insert
	Create
	Table
	Identifier
	Number
	Boolean
	String
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
	Values
	Into
)

func (t TokenType) String() string {
	return [...]string {
		"EOF",
		"Select",
		"Insert",
		"Create",
		"Table",
		"Identifier",
		"Number",
		"Boolean",
		"String",
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
		"Values",
		"Into",
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
	it := &strIter{strings.NewReader(in), 1}

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
		"insert": Insert,
		"table": Table,
		"create": Create,
		"values": Values,
		"into": Into,
	}
	stringToType := func(w string) TokenType {
		lower := strings.ToLower(w)
		if t, ok := keyword[lower]; ok {
			return t
		} else if lower == "true" || lower == "false" {
			return Boolean
		} else if lower == "and" || lower == "or" {
			return Operator
		}
		return Identifier
	}

	for c, ok := it.next(); ok; c, ok = it.next() {
		if unicode.IsSpace(c) {
			continue
		} else if c == '!' || c == '<' || c == '>' {
			if next, ok := it.peek(); ok && next == '=' {
				it.next()
				out = append(out, emit(Operator, string(c+next), it.line))
			} else {
				out = append(out, emit(Operator, string(c), it.line))
			}
		} else if typ, ok := singleCharTokens[c]; ok {
			out = append(out, emit(typ, string(c), it.line))
		} else if unicode.IsDigit(c) {
			dig := readUntil(c, it, unicode.IsDigit)
			out = append(out, emit(Number, dig, it.line))
		} else if c == '"' {
			word := readUntil(c, it, func(r rune) bool { return r != '"'})
			it.next() // consume trailing "
			out = append(out, emit(String, word[1:], it.line))
		} else {
			word := readUntil(c, it, unicode.IsLetter)
			out = append(out, emit(stringToType(word), word, it.line))
		}
	}

	out = append(out, emit(EOF, "", it.line))
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
	line int
}

func(l *strIter) next() (rune, bool) {
	r, _, err := l.ReadRune()
	if err != nil {
		return r, false
	}
	if r == '\n' {
		l.line++
	}
	return r, true
}

func(l *strIter) peek() (rune, bool) {
	r, ok := l.next()
	if r == '\n' {
		l.line--
	}
	l.UnreadRune()
	return r, ok
}

func emit(typ TokenType, lexeme string, line int) Token {
	return Token{typ, lexeme, line}
}
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

// todo: is this better approach to lexing?
// it's functional, stateless, supports unicode
// but it's verbose and difficult to trace
func Lex2(in string) []Token {
	var out []Token
	l := newStrIter(in)
	var state stateFn = parse()
	
	for state != nil {
		nextFn, tok := state(l)
		state = nextFn

		if tok != nil {
			out = append(out, *tok)
		}
	}
	return out
}


type stateFn func(*strIter) (stateFn, *Token)

func parse() stateFn {
	return func(si *strIter) (stateFn, *Token) {
		singleDigitTokens := map[rune]TokenType {
			'.': Dot,
			',': Comma,
			'(': OpenParen,
			')': CloseParen,
			'+': Operator,
			'-': Operator,
		}

		r, ok := si.next()

		if !ok {
			return nil, si.emit(EOF, "")
		} else if unicode.IsSpace(r) {
			return parse(), nil
		} else if typ, ok := singleDigitTokens[r]; ok {
			return parse(), si.emit(typ, string(r))
		} else if unicode.IsDigit(r) {
			return digit(r), nil
		} else if r == '!' {
			return bang(), nil
		}

		return stringFn(r), nil
	}
}

func digit(first rune) stateFn {
	return func(si *strIter) (stateFn, *Token) {
		digits := readUntil(first, si, unicode.IsDigit)
		return parse(), si.emit(Number, digits)
	}
}

func bang() stateFn {
	return func(si *strIter) (stateFn, *Token) {
		next, ok := si.peek()
		if !ok {
			return nil, si.emit(EOF, "")
		} else if next == '=' {
			si.next()
			return parse(), si.emit(Operator, "!=")
		}
		return parse(), si.emit(Operator, "!")
	}
}

func stringFn(first rune) stateFn {
	return func(si *strIter) (stateFn, *Token) {
		out := readUntil(first, si, unicode.IsLetter)

		if out == "select" {
			return parse(), si.emit(Select, out)
		} else if out == "from" {
			return parse(), si.emit(From, out)
		} else if out == "where" {
			return parse(), si.emit(Where, out)
		} else if out == "having" {
			return parse(), si.emit(Having, out)
		}
		return parse(), si.emit(Identifier, out)
	}
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

func newStrIter(in string) *strIter {
	return &strIter{strings.NewReader(in), 1}
}

func(l *strIter) next() (rune, bool) {
	r, _, err := l.ReadRune()
	if err != nil {
		return r, false
	} else if r == '\n' {
		l.line++
	}
	return r, true
}

func(l *strIter) peek() (rune, bool) {
	r, ok := l.next()
	l.UnreadRune()
	return r, ok
}

func (l *strIter) emit(typ TokenType, lexeme string) *Token {
	return &Token{typ, lexeme, l.line}
}
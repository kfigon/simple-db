package sql

import "fmt"

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
	Values  []string
	Table string
}
func (*InsertStatement) statementTag(){}

func Parse(tokens []Token) (Statement, error) {
	p := &parser{toks: tokens}
	return p.parse()
}

type parser struct {
	toks []Token
	currentIdx int
}

func (p *parser) parse() (Statement, error) {
	t := p.next()
	switch t.Typ {
	case Select: return p.parseSelectStatement()
	case Insert: return p.parseInsertStatement()
	}
	return nil, fmt.Errorf("unknown token type: %v", t)
}

func (p *parser) parseInsertStatement() (Statement, error) {
	panic("todo")
}

func (p *parser) parseSelectStatement() (Statement, error) {
	columns := []string{}
	hasWildcard := false
	var t Token
	for t = p.next(); !eof(t); t = p.next() {
		if t.Typ == Identifier {
			columns = append(columns, t.Lexeme)
			if next := p.peek().Typ; next == Comma {
				p.next()
			}
		} else if t.Typ == Wildcard {
			if len(columns) != 0 {
				return nil, fmt.Errorf("sql error: found select wildcard and other columns")
			}
			hasWildcard = true
			t = p.next()
			break
		}

		return nil, fmt.Errorf("error parsing Select statement, unknown token when parsing columns: %v", t)
	}
	
	if t.Typ == From {
		if t = p.next(); t.Typ == Identifier {
			return &SelectStatement{columns, hasWildcard, t.Lexeme}, nil
		}
	}
	return nil, fmt.Errorf("error parsing Select statement, unknown token when parsing columns: %v", t)
}

func (p *parser) next() Token {
	if p.currentIdx >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	out := p.toks[p.currentIdx]
	p.currentIdx++
	return out
}

func (p *parser) peek() Token {
	if p.currentIdx >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	return p.toks[p.currentIdx]
}

func eof(t Token) bool {
	return t.Typ == EOF
}
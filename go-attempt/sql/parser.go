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


type CreateStatement struct {
	Columns []ColumnDefinition
	Table string
}
type ColumnDefinition struct {
	Name string
	Typ  string
}

func (*CreateStatement) statementTag(){}

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
	case Create: return p.parseCreateStatement()
	}
	return nil, fmt.Errorf("unknown token type: %v", t)
}

func (p *parser) parseSelectStatement() (Statement, error) {
	var columns []string
	hasWildcard := false
	var t Token
	for t = p.next(); !eof(t) && t.Typ != From; t = p.next() {
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
		} else {
			return nil, fmt.Errorf("error parsing Select statement, unknown token when parsing columns: %v", t)
		}
	}
	
	if t.Typ == From {
		if t = p.next(); t.Typ == Identifier {
			return &SelectStatement{columns, hasWildcard, t.Lexeme}, nil
		}
	}
	return nil, fmt.Errorf("error parsing Select statement, unknown token: %v", t)
}

func (p *parser) parseCreateStatement() (*CreateStatement, error) {
	if next := p.next(); next.Typ != Table {
		return nil, fmt.Errorf("create table: expected 'table' after 'create' token, got: %v", next)
	}
	
	identifier := p.next()
	if identifier.Typ != Identifier {
		return nil, fmt.Errorf("create table: expected identifier after 'create table' token, got: %v", identifier)
	}

	if open := p.next(); open.Typ != OpenParen {
		return nil, fmt.Errorf("create table: expected open param after 'create table identifier' tokens, got: %v", open)
	}

	var t Token
	var columns []ColumnDefinition
	for t = p.next(); !eof(t) && t.Typ != CloseParen; t = p.next() {
		if t.Typ == Identifier {
			next := p.next()
			comma := p.next()
			if next.Typ == Identifier && comma.Typ == Comma {
				columns = append(columns, ColumnDefinition{
					Name: t.Lexeme,
					Typ: next.Lexeme,
				})
			} else {
				return nil, fmt.Errorf("create table: unknown token when defining column. Expected id, id and comma, got %v, %v, %v", t, next, comma)
			}
		} else {
			return nil, fmt.Errorf("create table: unknown token when defining column. Expected identifier, got %v", t)
		}
	}

	if t.Typ == CloseParen {
		return &CreateStatement{Columns: columns, Table: identifier.Lexeme}, nil
	}
	return nil, fmt.Errorf("create table: unknown token at the end of column definition: %v", t)
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
package sql

import "fmt"

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
	case Insert: return p.parseInsertStatement()
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
			columns = append(columns, ColumnDefinition{
				Name: t.Lexeme,
				Typ: next.Lexeme,
			})

			if comma := p.peek(); comma.Typ == Comma {
				p.next()	
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

func (p *parser) parseInsertStatement() (*InsertStatement, error) {
	if next := p.next(); next.Typ != Into {
		return nil, fmt.Errorf("insert table: expected 'into' after 'insert' token, got: %v", next)
	}
	
	identifier := p.next()
	if identifier.Typ != Identifier {
		return nil, fmt.Errorf("insert table: expected identifier after 'insert into' token, got: %v", identifier)
	}
	if open := p.next(); open.Typ != OpenParen {
		return nil, fmt.Errorf("insert table: expected open param after 'insert into identifier' tokens, got: %v", open)
	}

	var columns []string
	for t := p.next(); !eof(t) && t.Typ != CloseParen; t = p.next() {
		if t.Typ == Identifier {
			columns = append(columns, t.Lexeme)
			if next := p.peek(); next.Typ == Comma {
				p.next()
			}
		} else {
			return nil, fmt.Errorf("insert table: unknown token when defining columns. Expected identifier, got %v", t)
		}
	}

	if values := p.next(); values.Typ != Values {
		return nil, fmt.Errorf("insert table: expected 'values' after defining columns, got: %v", values)
	}
	if open := p.next(); open.Typ != OpenParen {
		return nil, fmt.Errorf("insert table: expected open param 'values', got: %v", open)
	}

	var t Token
	var vals []string
	for t = p.next(); !eof(t) && t.Typ != CloseParen; t = p.next() {
		if t.Typ == Identifier || t.Typ == Number || t.Typ == Boolean || t.Typ == String {
			vals = append(vals, t.Lexeme)
			
			if next := p.peek(); next.Typ == Comma {
				p.next()
			}
		} else {
			return nil, fmt.Errorf("insert table: unknown token when defining values. Expected identifier, boolean, string or number, got %v", t)
		}
	}
	if t.Typ == CloseParen {
		if len(vals) != len(columns) {
			return nil, fmt.Errorf("insert table: mismatched number of columns and values: %v, %v", vals, columns)		
		}
		return &InsertStatement{Columns: columns, Values: vals, Table: identifier.Lexeme}, nil
	}
	return nil, fmt.Errorf("insert table: unknown token at the end of values: %v", t)
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
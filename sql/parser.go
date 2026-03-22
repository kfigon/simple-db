package sql

import "fmt"

func Parse(tokens []Token) (Statement, error) {
	p := &parser{toks: tokens}
	return p.parse()
}

type parser struct {
	toks       []Token
	currentIdx int
}

func (p *parser) parse() (Statement, error) {
	t := p.next()
	switch t.Typ {
	case Select:
		return p.parseSelectStatement()
	case Create:
		return p.parseCreateStatement()
	case Insert:
		return p.parseInsertStatement()
	}
	return nil, fmt.Errorf("unknown token type: %v", t)
}

func (p *parser) parseSelectStatement() (Statement, error) {
	var columns []string
	hasWildcard := false
	for t := p.next(); !eof(t); t = p.next() {
		next := p.next()

		if t.Typ == Identifier && next.Typ == Comma {
			columns = append(columns, t.Lexeme)
		} else if t.Typ == Identifier && next.Typ == From {
			columns = append(columns, t.Lexeme)
			break
		} else if t.Typ == Wildcard && next.Typ == From {
			if len(columns) != 0 {
				return nil, fmt.Errorf("sql error: found select wildcard and other columns")
			}
			hasWildcard = true
			break
		} else {
			return nil, fmt.Errorf("error parsing Select statement, unknown token when parsing columns: %v", t)
		}
	}
	t := p.next()
	tableName := ""
	if t.Typ == Identifier {
		tableName = t.Lexeme
	} else {
		return nil, fmt.Errorf("expected table name, got %v", t)
	}

	var where *WhereStatement
	if p.peek().Typ == Where {
		p.next()
		whreSt, err := p.parseWhere()
		if err != nil {
			return nil, fmt.Errorf("error parsing where statement: %w", err)
		}
		where = whreSt
	}

	return &SelectStatement{columns, hasWildcard, tableName, where}, nil
}

func (p *parser) parseWhere() (*WhereStatement, error) {
	pred, err := p.parsePredicate(Lowest)
	if err != nil {
		return nil, fmt.Errorf("error parsing predicate: %w", err)
	}

	return &WhereStatement{pred}, nil
}

func (p *parser) parsePredicate(precedence Precedence) (Expression, error) {
	left, err := p.parseSingleExpr()
	if err != nil {
		return nil, err
	}

	for !eof(p.peek()) && precedence < precedenceForToken(p.peek()) {
		newExpr, err := p.parseInfixExpression(left)
		if err != nil {
			return nil, err
		} else if newExpr == nil {
			break
		}
		left = newExpr
	}
	return left, nil
}

func (p *parser) parseSingleExpr() (Expression, error) {
	t := p.next()
	if t.Typ == Number || t.Typ == Boolean || t.Typ == String {
		return ValueLiteral{t}, nil
	} else if t.Typ == Identifier {
		return ColumnLiteral{t}, nil
	}
	return nil, fmt.Errorf("invalid expression token: %v", t)
}

func (p *parser) parseInfixExpression(left Expression) (*InfixExpression, error) {
	t := p.next()
	// todo: restrict only to infix token types
	op := t
	right, err := p.parsePredicate(precedenceForToken(op))
	if err != nil {
		return nil, err
	}

	return &InfixExpression{
		Operator: op,
		Left:     left,
		Right:    right,
	}, nil
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

	var columns []ColumnDefinition
	for t := p.next(); !eof(t); t = p.next() {
		if t.Typ == CloseParen {
			return &CreateStatement{Columns: columns, Table: identifier.Lexeme}, nil
		}

		next := p.next()
		maybeComma := p.peek()

		if t.Typ == Identifier && next.Typ == Identifier {
			columns = append(columns, ColumnDefinition{
				Name: t.Lexeme,
				Typ:  next.Lexeme,
			})

			if maybeComma.Typ == Comma {
				p.next()
			} else if maybeComma.Typ == CloseParen {
				return &CreateStatement{Columns: columns, Table: identifier.Lexeme}, nil
			} else {
				return nil, fmt.Errorf("create table: unknown token when defining column. Expected comma or close paren, got %v", maybeComma)
			}
		} else {
			return nil, fmt.Errorf("create table: unknown token when defining column. Expected 2 identifiers, got %v and %v", t, next)
		}
	}
	return nil, fmt.Errorf("create table: unexpected od of tokens at the end of column definition")
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
	for t := p.next(); !eof(t); t = p.next() {
		next := p.peek()

		if t.Typ == Identifier && next.Typ == Comma {
			p.next()
			columns = append(columns, t.Lexeme)
		} else if t.Typ == Identifier && next.Typ == CloseParen {
			p.next()
			columns = append(columns, t.Lexeme)
			break
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

	var vals []Expression
	for t := p.next(); !eof(t); t = p.next() {
		next := p.next()

		if t.Typ == Identifier || t.Typ == Number || t.Typ == Boolean || t.Typ == String || t.Typ == Null {
			vals = append(vals, toExpression(t))

			if next.Typ == CloseParen {
				if len(vals) != len(columns) {
					return nil, fmt.Errorf("insert table: mismatched number of columns and values: %v, %v", vals, columns)
				}
				return &InsertStatement{Columns: columns, Values: vals, Table: identifier.Lexeme}, nil
			} else if next.Typ != Comma {
				return nil, fmt.Errorf("insert table: values should be separated by commas, got %v", next)
			}
		} else {
			return nil, fmt.Errorf("insert table: unknown token when defining values. Expected identifier, boolean, string, null or number, got %v", t)
		}
	}

	return nil, fmt.Errorf("insert table: unexpected end of tokens when defining values")
}

func toExpression(t Token) Expression {
	switch t.Typ {
	case Identifier:
		return ColumnLiteral{t}
	case Null:
		return NullLiteral{}
	case Number:
		return ValueLiteral{t}
	case Boolean:
		return ValueLiteral{t}
	case String:
		return ValueLiteral{t}
	}
	panic(fmt.Sprintf("unsupported type for expression %v", t.Typ))
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

// binding power - how an operator pulls left and right operands to itself or not
type Precedence int

const (
	Lowest Precedence = iota + 1
	Logical
	Equals
	LessGreater
	Sum
	Product
	Prefix
)

func precedenceForToken(tok Token) Precedence {
	if tok.Typ != Operator {
		return Lowest
	}

	switch tok.Lexeme {
	case "!=", "=":
		return Equals
	case ">", "<", ">=", "<=":
		return LessGreater
	case "and", "or":
		return Logical
	// case "+", "-": return Sum
	// case Asterisk, Slash: return Product
	// case LParen: return Call
	default:
		return Lowest
	}
}

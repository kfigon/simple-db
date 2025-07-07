package naive

import (
	"cmp"
	"simple-db/sql"
	"strconv"
)

// todo: add error handling
func buildPredicate(pred sql.BoolExpression) func(Row) bool {
	return func(r Row) bool {
		col := predBuilder(pred, r)
		debugAssert(col.Typ == Boolean, "boolean predicate required, got %v", col.Typ)
		return col.Data.(bool)
	}
}

func castAndBinaryOp[T any](left, right any, op func(T, T) bool) bool {
	lV := left.(T)
	rV := right.(T)
	return op(lV, rV)
}

func eq[T comparable](a, b T) bool   { return a == b }
func neq[T comparable](a, b T) bool  { return a != b }
func or(a, b bool) bool              { return a || b }
func and(a, b bool) bool             { return a && b }
func gt[T cmp.Ordered](a, b T) bool  { return a > b }
func geq[T cmp.Ordered](a, b T) bool { return a >= b }
func lt[T cmp.Ordered](a, b T) bool  { return a < b }
func leq[T cmp.Ordered](a, b T) bool { return a <= b }

func buildCastAndOperand[T any](left, right ColumnData, fn func(a, b T) bool) func() bool {
	return func() bool {
		return castAndBinaryOp(left.Data, right.Data, fn)
	}
}

func predBuilder(pred sql.BoolExpression, r Row) ColumnData {
	switch v := pred.(type) {
	case *sql.InfixExpression:
		left := predBuilder(v.Left, r)
		right := predBuilder(v.Right, r)

		if v.Operator.Lexeme == "and" {
			return ColumnData{Boolean, castAndBinaryOp(left.Data, right.Data, and)}
		} else if v.Operator.Lexeme == "or" {
			return ColumnData{Boolean, castAndBinaryOp(left.Data, right.Data, or)}
		}

		op := map[string]map[FieldType]func() bool{
			"=": {
				String:  buildCastAndOperand(left, right, eq[string]),
				Int32:   buildCastAndOperand(left, right, eq[int32]),
				Boolean: buildCastAndOperand(left, right, eq[bool]),
			},
			"!=": {
				String:  buildCastAndOperand(left, right, neq[string]),
				Int32:   buildCastAndOperand(left, right, neq[int32]),
				Boolean: buildCastAndOperand(left, right, neq[bool]),
			},
			">":  {Int32: buildCastAndOperand(left, right, gt[int32])},
			">=": {Int32: buildCastAndOperand(left, right, geq[int32])},
			"<":  {Int32: buildCastAndOperand(left, right, lt[int32])},
			"<=": {Int32: buildCastAndOperand(left, right, leq[int32])},
		}

		ops, ok := op[v.Operator.Lexeme]
		debugAssert(ok, "unsupported op: %v", v.Operator)
		debugAssert(left.Typ == right.Typ, "incompatible types %v %v", left.Typ, right.Typ)

		fn, ok := ops[left.Typ]
		debugAssert(ok, "unknown type %v", left.Typ)
		return ColumnData{Boolean, fn()}
	case sql.ValueLiteral:
		if v.Tok.Typ == sql.Number {
			return ColumnData{Int32, int32(must(strconv.Atoi(v.Tok.Lexeme)))}
		} else if v.Tok.Typ == sql.String {
			return ColumnData{String, v.Tok.Lexeme}
		} else if v.Tok.Typ == sql.Boolean {
			return ColumnData{Boolean, must(strconv.ParseBool(v.Tok.Lexeme))}
		}
		debugAssert(false, "unsupported type %v", v)
	case sql.ColumnLiteral:
		return r[FieldName(v.Name.Lexeme)]
	}

	debugAssert(false, "unknown predicate type received %T", pred)
	panic("")
}
package naive

import (
	"simple-db/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNaiveStorage(t *testing.T) {
	t.Run("basic create", func(t *testing.T) {
		s := NewStorage()
		sql := `create table foobar(abc int, asdf boolean, xxx string)`
		assert.NoError(t, execute(t, s, sql))

		assert.Empty(t, s.AllData)
		assert.Len(t, s.SchemaMetadata, 1)
		assert.Equal(t, s.SchemaMetadata["foobar"], TableSchema{
			"abc":  Int32,
			"asdf": Boolean,
			"xxx":  String,
		})
	})

	t.Run("create already present", func(t *testing.T) {
		s := NewStorage()
		
		assert.NoError(t, execute(t, s, `create table foobar(abc int, asdf boolean, xxx string)`))
		assert.Error(t, execute(t, s, `create table foobar(opps int)`))
	})

	t.Run("basic insert", func(t *testing.T) {
		s := NewStorage()

		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))

		assert.Equal(t, s.SchemaMetadata["foobar"], TableSchema{
			"id":  Int32,
			"name": String,
		})

		assert.Equal(t, s.AllData["foobar"], []TableData{
			{
				"id": {
					Typ: Int32,
					Data: 123,
				},
				"name":{
					Typ: String,
					Data: "asdfx",
				},
			},
			{
				"id": {
					Typ: Int32,
					Data: 456,
				},
				"name":{
					Typ: String,
					Data: "baz",
				},
			},
		})
	})
}

func execute(t *testing.T, s *Storage, statement string) error {
	t.Helper()
	stmt, err := sql.Parse(sql.Lex(statement))

	assert.NoError(t, err)

	switch stmt := stmt.(type) {
	case *sql.CreateStatement:
		return s.CreateTable(*stmt)
	case *sql.InsertStatement:
		return s.Insert(*stmt)
		// case *sql.SelectStatement:
	}
	assert.Fail(t, "unreachable, invalid statement")
	return nil
}

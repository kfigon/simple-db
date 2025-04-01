package naive

import (
	"bytes"
	"simple-db/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNaiveStorage(t *testing.T) {
	t.Run("basic create", func(t *testing.T) {
		s := NewStorage()
		sql := `create table foobar(abc int, asdf boolean, xxx string)`
		assert.NoError(t, execute(t, s, sql))

		assert.Equal(t, s.allSchema(), Schema{
			"foobar": TableSchema{
			"abc":  Int32,
			"asdf": Boolean,
			"xxx":  String,
		}})
	})

	t.Run("create already present", func(t *testing.T) {
		s := NewStorage()
		
		assert.NoError(t, execute(t, s, `create table foobar(abc int, asdf boolean, xxx string)`))
		assert.Error(t, execute(t, s, `create table foobar(opps int)`))
	})

	t.Run("select for nonexisting table", func(t *testing.T) {
		s := NewStorage()
		
		_, err := query(t, s, "select * from foobar")
		assert.Error(t, err)
	})

	t.Run("unknown field in select", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		
		_, err := query(t, s, "select oops from foobar")
		assert.Error(t, err)
	})

	t.Run("empty select", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		
		res, err := query(t, s, "select * from foobar")
		assert.NoError(t, err)

		assert.ElementsMatch(t, []string{"id", "name"}, res.Header)
		assert.Empty(t, res.Values)
	})

	t.Run("basic select", func(t *testing.T) {
		s := NewStorage() 
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))

		res, err := query(t, s, "select * from foobar")
		assert.NoError(t, err)

		assert.ElementsMatch(t, []string{"id", "name"}, res.Header)
		assert.Len(t, res.Values, 2)

		assert.ElementsMatch(t, []string{"123", "asdf"}, res.Values[0])
		assert.ElementsMatch(t, []string{"456", "baz"}, res.Values[1])
	})

	t.Run("basic select with specified columns", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))

		res, err := query(t, s, "select name, id from foobar")
		assert.NoError(t, err)

		assert.ElementsMatch(t, []string{"name", "id"}, res.Header)
		assert.Len(t, res.Values, 2)
		assert.ElementsMatch(t, []string{"123", "asdf"}, res.Values[0])
		assert.ElementsMatch(t, []string{"456", "baz"}, res.Values[1])
	})
}

func TestSerializeStorage(t *testing.T) {
	t.Run("serialize empty", func(t *testing.T) {
		s := NewStorage()
		data := SerializeDb(s)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.allSchema(), recoveredDb.allSchema())
		assert.Equal(t, len(s.allPages), len(recoveredDb.allPages))
		assert.Equal(t, len(s.allPages), 2) // root and directory
	})

	t.Run("single table", func(t *testing.T) {
		s := NewStorage()
		sql := `create table foobar(abc int, asdf boolean, xxx string)`
		assert.NoError(t, execute(t, s, sql))

		data := SerializeDb(s)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.allSchema(), recoveredDb.allSchema())
		assert.Equal(t, len(s.allPages), len(recoveredDb.allPages), "length of old and recovered")
		assert.Equal(t, 1+1+1+1, len(s.allPages)) // root, dir and schema and empty data
	})

	t.Run("whole db state", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `create table xxx(email string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))
		assert.NoError(t, execute(t, s, `insert into xxx(email) VALUES ("john@doe.com")`))

		data := SerializeDb(s)

		assert.Equal(t, len(s.allPages), len(data) / PageSize)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, len(s.allPages), len(recoveredDb.allPages))
		assert.Equal(t, len(s.allPages), 1+1+2 +2) // root + directory + 2x schema + 2x data
		
		for i := 0; i < len(s.allPages); i++ {
			assert.Equal(t, s.allPages[i].Header, recoveredDb.allPages[i].Header, "header on page %d", i)		
			assert.Equal(t, s.allPages[i].SlotArray, recoveredDb.allPages[i].SlotArray, "slot array on page %d", i)		
		}
		assert.Equal(t, s.allPages, recoveredDb.allPages)
		assert.Equal(t, s.AllSchema(), recoveredDb.AllSchema())
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
	}

	assert.Fail(t, "unreachable, invalid statement")
	return nil
}

func query(t *testing.T, s *Storage, statement string) (QueryResult, error) {
	t.Helper()

	stmt, err := sql.Parse(sql.Lex(statement))
	assert.NoError(t, err)
	assert.IsType(t, &sql.SelectStatement{}, stmt)
	
	return s.Select(*stmt.(*sql.SelectStatement))
}
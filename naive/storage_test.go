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

		assert.Equal(t, s.AllSchema(), Schema{
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

		assert.ElementsMatch(t, []FieldName{"id", "name"}, res.Header)
		assert.Empty(t, res.Values)
	})

	t.Run("basic select", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))

		res, err := query(t, s, "select * from foobar")
		assert.NoError(t, err)

		assert.ElementsMatch(t, []FieldName{"id", "name"}, res.Header)
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

		assert.ElementsMatch(t, []FieldName{"name", "id"}, res.Header)
		assert.Len(t, res.Values, 2)
		assert.ElementsMatch(t, []string{"123", "asdf"}, res.Values[0])
		assert.ElementsMatch(t, []string{"456", "baz"}, res.Values[1])
	})

	t.Run("basic select with specified columns and filter", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (1, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (2, "baz")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (3, "baz")`))

		res, err := query(t, s, `select name, id from foobar where name = "baz"`)
		assert.NoError(t, err)

		assert.ElementsMatch(t, []FieldName{"name", "id"}, res.Header)
		assert.Len(t, res.Values, 2)
		assert.ElementsMatch(t, []string{"2", "baz"}, res.Values[0])
		assert.ElementsMatch(t, []string{"3", "baz"}, res.Values[1])
	})

	t.Run("select with complex filter", func(t *testing.T) {
		vs := []string{
			`create table foobar(id int, name string, age int)`,
			`insert into foobar(id, name, age) VALUES (1, "asdf", 20)`,
			`insert into foobar(id, name, age) VALUES (2, "baz", 30)`,
			`insert into foobar(id, name, age) VALUES (3, "baz", 20)`,
			`insert into foobar(id, name, age) VALUES (4, "four", 40)`}

		testSelect(t, vs, `select name, id from foobar where name = "baz" and age = 20`, QueryResult{
			[]FieldName{"name", "id"},
			[][]string{{"baz", "3"}},
		})
	})

	t.Run("select with 3 conditions", func(t *testing.T) {
		vs := []string{
			`create table foobar(id int, name string, age int)`,
			`insert into foobar(id, name, age) VALUES (1, "asdf", 20)`,
			`insert into foobar(id, name, age) VALUES (2, "baz", 30)`,
			`insert into foobar(id, name, age) VALUES (3, "baz", 20)`,
			`insert into foobar(id, name, age) VALUES (4, "four", 40)`}

		testSelect(t, vs, `select name, id from foobar where name = "baz" and age = 20 and id = 3`, QueryResult{
			[]FieldName{"name", "id"},
			[][]string{{"baz", "3"}},
		})

		testSelect(t, vs, `select name, id from foobar where name = "baz" and age = 20 and id = 4`, QueryResult{
			[]FieldName{"name", "id"},
			[][]string{},
		})
	})
}

func TestSerializeStorage(t *testing.T) {
	t.Run("serialize empty", func(t *testing.T) {
		s := NewStorage()
		data := SerializeDb(s)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.AllSchema(), recoveredDb.AllSchema())
		assert.Equal(t, s.root.NumberOfPages, recoveredDb.root.NumberOfPages)
		assert.EqualValues(t, recoveredDb.root.NumberOfPages, 2) // root schema
	})

	t.Run("single table", func(t *testing.T) {
		s := NewStorage()
		sql := `create table foobar(abc int, asdf boolean, xxx string)`
		assert.NoError(t, execute(t, s, sql))

		data := SerializeDb(s)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.AllSchema(), recoveredDb.AllSchema())
		assert.Equal(t, s.root.NumberOfPages, recoveredDb.root.NumberOfPages)
		assert.EqualValues(t, 1+1+1, recoveredDb.root.NumberOfPages) // root schema and empty data
	})

	t.Run("whole db state", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `create table xxx(email string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))
		assert.NoError(t, execute(t, s, `insert into xxx(email) VALUES ("john@doe.com")`))

		data := SerializeDb(s)

		assert.EqualValues(t, s.root.NumberOfPages, len(data)/PageSize)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.root.NumberOfPages, recoveredDb.root.NumberOfPages)

		assert.Equal(t, s.root.NumberOfPages, recoveredDb.root.NumberOfPages)
		assert.EqualValues(t, recoveredDb.root.NumberOfPages, 1+1+2) // root + schema + 2x data

		assert.Equal(t, s.root, recoveredDb.root)
		for i := 1; i < int(s.root.NumberOfPages); i++ {
			assert.Equal(t, s.getPage(PageID(i)).Header, recoveredDb.getPage(PageID(i)).Header, "header on page %d", i)
			assert.Equal(t, s.getPage(PageID(i)).SlotArray, recoveredDb.getPage(PageID(i)).SlotArray, "slot array on page %d", i)
		}
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

func testSelect(t *testing.T, prep []string, queryStr string, exp QueryResult) {
	t.Helper()

	s := NewStorage()
	for _, v := range prep {
		assert.NoError(t, execute(t, s, v))
	}

	res, err := query(t, s, queryStr)
	assert.NoError(t, err)

	assert.ElementsMatch(t, exp.Header, res.Header)
	assert.Len(t, res.Values, len(res.Values))
	assert.ElementsMatch(t, exp.Values, res.Values)
}

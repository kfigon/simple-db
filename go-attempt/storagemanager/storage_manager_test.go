package storagemanager

import (
	"simple-db/page"
	"simple-db/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	storage := NewEmptyStorageManager()

	v := parseSql[*sql.CreateStatement](t, "create table foo(id int, name string)")

	assert.NoError(t, storage.CreateTable(v))
	assert.Len(t, storage.data, 1)
	assert.Len(t, storage.dirEntries, 1)
	assert.Len(t, storage.schemaEntries, 2)

	assert.Equal(t, storage.dirEntries[0], page.DirectoryEntry{
		DataRootPageID:   0,
		SchemaRootRecord: page.RecordID{1, 0},
		ObjectType:       page.DataPageType,
		ObjectName:       "foo",
	})

	assert.Equal(t, storage.schemaEntries[0], page.SchemaEntry{
		Next:      page.RecordID{1, 1},
		FieldTyp:  page.I32Type,
		IsNull:    false,
		FieldName: "id",
	})

	assert.Equal(t, storage.schemaEntries[1], page.SchemaEntry{
		Next:      page.RecordID{},
		FieldTyp:  page.StringType,
		IsNull:    false,
		FieldName: "name",
	})
}

func TestInsert(t *testing.T) {
	storage := NewEmptyStorageManager()

	v := parseSql[*sql.CreateStatement](t, "create table foo(id int, name string)")
	assert.NoError(t, storage.CreateTable(v))

	ins := parseSql[*sql.InsertStatement](t, `insert into foo(id, name) VALUES (123, "foobar")`)
	rec, err := storage.Insert(ins)

	assert.NoError(t, err)
	_ = rec
	assert.Fail(t, "todo")
}

func parseSql[T sql.Statement](t *testing.T, input string) T {
	t.Helper()

	s, err := sql.Parse(sql.Lex(input))
	assert.NoError(t, err)
	v, ok := s.(T)
	assert.True(t, ok)
	
	return v
}
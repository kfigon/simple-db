package storagemanager

import (
	"simple-db/page"
	"simple-db/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	st := parseSql(t, "create table foo(id int, name string)")
	v, ok := st.(*sql.CreateStatement)
	assert.True(t, ok)

	storage := NewEmptyStorageManager()
	err := storage.CreateTable(v)

	assert.NoError(t, err)
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

func parseSql(t *testing.T, input string) sql.Statement {
	t.Helper()

	s, err := sql.Parse(sql.Lex(input))
	assert.NoError(t, err)
	return s
}
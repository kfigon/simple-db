package storagemanager

import (
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
	assert.Fail(t, "assert result")
}

func parseSql(t *testing.T, input string) sql.Statement {
	t.Helper()

	s, err := sql.Parse(sql.Lex(input))
	assert.NoError(t, err)
	return s
}
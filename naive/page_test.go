package naive

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	t.Run("bool true", func(t *testing.T) {
		bytes := []byte{1}
		assert.Equal(t, bytes, SerializeBool(true))

		got, err := DeserializeBool(bytes)
		assert.NoError(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("bool false", func(t *testing.T) {
		bytes := []byte{0}
		assert.Equal(t, bytes, SerializeBool(false))

		got, err := DeserializeBool(bytes)
		assert.NoError(t, err)
		assert.Equal(t, false, got)
	})

	t.Run("int", func(t *testing.T) {
		bytes := []byte{0, 255, 18, 52}
		assert.Equal(t, bytes, SerializeInt(0xff1234))

		got, err := DeserializeInt(bytes)
		assert.NoError(t, err)
		assert.Equal(t, int32(0xff1234), got)
	})

	t.Run("string", func(t *testing.T) {
		bytes := []byte{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}
		assert.Equal(t, bytes, SerializeString("hello world"))

		got, err := DeserializeString(bytes)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", got)
	})

	t.Run("bytes", func(t *testing.T) {
		input := []byte("hello world")
		expected := []byte{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}

		assert.Equal(t, expected, SerializeBytes(input))

		got, err := DeserializeBytes(expected)
		assert.NoError(t, err)
		assert.Equal(t, input, got)
	})
}

func TestSerializeStorage(t *testing.T) {
	t.Run("schema", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `create table asdf(email string)`))

		data := SerializeSchema(s.SchemaMetadata)
		assert.Len(t, data, 65)

		schema, err := DeserializeSchema(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.SchemaMetadata, schema)
	})

	t.Run("data", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))

		data := SerializeData(s.AllData)
		assert.Len(t, data, 65)

		dbData, err := DeserializeData(bytes.NewReader(data), s.SchemaMetadata)
		assert.NoError(t, err)
		assert.Equal(t, s.AllData, dbData)
	})

	t.Run("whole db state", func(t *testing.T) {
		s := NewStorage()
		assert.NoError(t, execute(t, s, `create table foobar(id int, name string)`))
		assert.NoError(t, execute(t, s, `create table xxx(email string)`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (123, "asdf")`))
		assert.NoError(t, execute(t, s, `insert into foobar(id, name) VALUES (456, "baz")`))
		assert.NoError(t, execute(t, s, `insert into xxx(email) VALUES ("john@doe.com")`))

		data := SerializeDb(s)
		assert.Len(t, data, 173)

		recoveredDb, err := DeserializeDb(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, s.AllData, recoveredDb.AllData)
		assert.Equal(t, s.SchemaMetadata, recoveredDb.SchemaMetadata)
	})
}

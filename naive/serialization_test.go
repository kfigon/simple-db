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
		expected := BytesWithHeader([]byte{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'})

		assert.Equal(t, expected, SerializeBytes(input))

		got, err := DeserializeBytes(expected)
		assert.NoError(t, err)
		assert.Equal(t, input, got)
	})
}

func TestSerializeGeneric(t *testing.T) {
	type data struct {
		Str  string
		Int  int32
		Vals []int32
	}

	bytes := []byte{
		0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd',
		0, 0, 0, 3,
		0, 0, 0, 10, 0, 0, 0, 0xff, 0, 0, 0, 5}

	funs := []deserializeFn[data, []byte]{
		compose("Str", func(t *data, s string) { t.Str = s}, DeserializeStringAndEat),
		compose("Int", func(t *data, v int32) { t.Int = v}, DeserializeIntAndEat),
		func(d *data, b *[]byte) error {
			for range d.Int {
				got, err := DeserializeIntAndEat(b)
				if err != nil {
					return err
				}
				d.Vals = append(d.Vals, got)
			}
			return nil
		},
	}

	got, err := DeserializeAll(bytes, funs...)
	assert.NoError(t, err)
	assert.Equal(t, got, &data{
		Str:  "hello world",
		Int:  3,
		Vals: []int32{10, 0xff, 5},
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
		assert.Equal(t, s.allSchema(), recoveredDb.allSchema())
	})
}

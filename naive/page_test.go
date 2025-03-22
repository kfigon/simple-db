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

func TestSerializeReflection(t *testing.T) {
	t.Run("serialize basic struct with int", func(t *testing.T) {
		type data struct {
			Integer int32 `bin:""`
		}
		expectedBytes := []byte{0, 255, 18, 52}
		d := data{0xff1234}
		assert.Equal(t, expectedBytes, SerializeReflection(d))

		got, err := DeserializeReflection[data](expectedBytes)
		assert.NoError(t, err)
		assert.Equal(t, data{0xff1234}, got)
	})

	t.Run("string", func(t *testing.T) {
		type data struct {
			Str string `bin:""`
		}
		bytes := []byte{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}
		d := data{"hello world"}
		assert.Equal(t, bytes, SerializeReflection(d))

		got, err := DeserializeReflection[data](bytes)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})

	t.Run("complex struct", func(t *testing.T) {
		type data struct {
			Str string `bin:""`
			Int int32 `bin:""`
			i int32 // don't serialize this one 
		}
		bytes := []byte{0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd',0, 255, 18, 52}
		d := data{"hello world", 0xff1234, 88}
		expected := data{"hello world", 0xff1234, 0}
		assert.Equal(t, bytes, SerializeReflection(d))

		got, err := DeserializeReflection[data](bytes)
		assert.NoError(t, err)
		assert.Equal(t, expected, got)
	})
	t.Run("with array inside", func(t *testing.T) {
		type data struct {
			Str string `bin:""`
			Int int32 `bin:""`
			Vals []int32 `bin:""`
			i int32 // don't serialize this one 
		}
		bytes := []byte{
			0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd',
			0,0,0,10,   0,0,0,0xff,  0,0,0, 5,
			0, 255, 18, 52}
		d := data{"hello world", 0xff1234, []int32{10,0xff, 5}, 88}
		expected := data{"hello world", 0xff1234, []int32{10,0xff, 5}, 0}
		assert.Equal(t, bytes, SerializeReflection(d))

		got, err := DeserializeReflection[data](bytes)
		assert.NoError(t, err)
		assert.Equal(t, expected, got)
	})
	t.Run("generic solution", func(t *testing.T) {
		type data struct {
			Str string `bin:""`
			Int int32 `bin:""`
			Vals []int32 `bin:""`
		}

		bytes := []byte{
			0, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd',
			0,0,0,3,
			0,0,0,10,   0,0,0,0xff,  0,0,0,5 }
		dem := Demapper[data]{
			funs: []demapper[data]{
				compose(
					func(t *data) *string {return &t.Str},
					func(d *data, v *string, b *[]byte) error {
						got, err := DeserializeStringAndEat(b)
						if err != nil {
							return err
						}
						*v = got
						return nil
					}),
				compose(
					func(t *data) *int32 {return &t.Int},
					func(d *data, v *int32, b *[]byte) error {
						got, err := DeserializeIntAndEat(b)
						if err != nil {
							return err
						}
						*v = got
						return nil
					}),
				compose(
					func(t *data) *[]int32 {return &t.Vals},
					func(d *data, v *[]int32, b *[]byte) error {
						for range d.Int {
							got, err := DeserializeIntAndEat(b)
							if err != nil {
								return err
							}
							*v = append(*v, got)
						}
						return nil 
					}),
			},
		}
		got, err := DeserializeIt(dem, bytes)
		assert.NoError(t, err)
		assert.Equal(t, got, &data{
			Str: "hello world",
			Int: 3,
			Vals: []int32{10,0xff,5},
		})
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

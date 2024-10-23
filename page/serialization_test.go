package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerialization(t *testing.T) {
	t.Run("byte", func(t *testing.T) {
		d := Byte(123)
		serialized := d.Serialize()
		got, err := Byte(0).Deserialize(serialized)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})

	t.Run("i16", func(t *testing.T) {
		d := I16(12345)
		serialized := d.Serialize()
		got, err := I16(0).Deserialize(serialized)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})


	t.Run("i64", func(t *testing.T) {
		d := I64(0xdeadbeefdeadbee)
		serialized := d.Serialize()
		got, err := I64(0).Deserialize(serialized)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})

	t.Run("i32", func(t *testing.T) {
		v := 0xdeadbeef
		d := I32(v)
		serialized := d.Serialize()
		got, err := I32(0).Deserialize(serialized)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})

	t.Run("bytes", func(t *testing.T) {
		d := Bytes([]byte{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15})
		serialized := d.Serialize()
		got, err := Bytes(nil).Deserialize(serialized)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})

	t.Run("string", func(t *testing.T) {
		d := String("foobar asdf 123 óą")
		serialized := d.Serialize()
		got, err := String("").Deserialize(serialized)
		assert.NoError(t, err)
		assert.Equal(t, d, got)
	})
}
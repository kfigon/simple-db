package naive

import (
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
		bytes := []byte{0,255, 18,52}
		assert.Equal(t, bytes, SerializeInt(0xff1234))

		got, err := DeserializeInt(bytes)
		assert.NoError(t, err)
		assert.Equal(t, int32(0xff1234), got)
	})

	t.Run("string", func(t *testing.T) {
		bytes := []byte{0,0,0,11, 'h', 'e','l','l','o',' ','w','o','r','l','d'}
		assert.Equal(t, bytes, SerializeString("hello world"))

		got, err := DeserializeString(bytes)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", got)
	})
}
package sciore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPage(t *testing.T) {
	offset := PageOffset(123)

	t.Run("read single byte", func(t *testing.T) {
		p := NewPage()		
		assert.NoError(t, p.StoreByte(offset, 0xff))
		
		got, err := p.ReadByte(offset)
		assert.NoError(t, err)
		assert.EqualValues(t, 0xff, got)
	})

	t.Run("read many bytes", func(t *testing.T) {
		p := NewPage()		
		assert.NoError(t, p.StoreBytes(offset, []byte{1,2,3,4}))
		
		got, err := p.ReadBytes(offset)
		assert.NoError(t, err)
		assert.EqualValues(t, []byte{1,2,3,4}, got)
	})
}
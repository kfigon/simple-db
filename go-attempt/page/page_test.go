package page

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPageSerialization(t *testing.T) {
	t.Run("can't fit i32 in page", func(t *testing.T) {
		p := NewPage()
		err := p.StoreInt(PageSize - 4, int32(123))
		assert.ErrorIs(t, err, ErrCantFitInPage)
	})

	t.Run("can't fit string page", func(t *testing.T) {
		p := NewPage()
		s := "hello world"
		err := p.StoreString(PageOffset(PageSize - len(s)), s)
		assert.ErrorIs(t, err, ErrCantFitInPage)
	})

	t.Run("store string", func(t *testing.T) {
		p := NewPage()
		s := "hello world"
		err := p.StoreString(0, s)
		assert.NoError(t, err)
		assert.Equal(t, p.takenSpace, 1 + len(s))
	})

	t.Run("read int", func(t *testing.T) {
		p := NewPage()
		exp := int32(123456)
		err := p.StoreInt(0, exp)
		assert.NoError(t, err)
		
		got := p.ReadInt(0)
		assert.Equal(t, exp, got)
	})

	t.Run("read string", func(t *testing.T) {
		p := NewPage()
		s := "hello worldó!"
		err := p.StoreString(0, s)
		assert.NoError(t, err)
		
		got := p.ReadString(0)
		assert.Equal(t, s, got)
	})
}
package naive

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlotted(t *testing.T) {
	t.Run("basic insert", func(t *testing.T) {
		p := NewSlotted(50)
		inserted := initWithData(t, p)
		
		assert.Len(t, inserted, 3)
		for id, expStr := range inserted {
			got, err := p.Read(id)
			assert.NoError(t, err)
	
			assert.Equal(t, []byte(expStr), got)
		}
	})
	
	t.Run("put", func(t *testing.T) {
		t.Skip("not implemented")
	})
}

func TestSerialization(t *testing.T) {
	t.Run("serialization", func(t *testing.T) {
		p := NewSlotted(50)
		initWithData(t, p)
		
		data := p.Serialize()
		assert.Len(t, data, 50)
	})

	t.Run("empty", func(t *testing.T) {
		data := NewSlotted(50).Serialize()
		assert.Equal(t, make([]byte, 50), data)
	})

	t.Run("deserialize", func(t *testing.T) {
		p := NewSlotted(50)
		inserted := initWithData(t, p)
		data := p.Serialize()
		
		newP, err := DeserializeSlotted(data, len(inserted))
		assert.NoError(t, err)

		assert.Len(t, newP.Indexes, 3)
		for id, expStr := range inserted {
			got, err := p.Read(id)
			assert.NoError(t, err)
	
			assert.Equal(t, []byte(expStr), got)
		}
		assert.Equal(t, p, newP)
	})
}

func initWithData(t *testing.T, p *Slotted) map[RowId]string {
	inserted := map[RowId]string{}

	for _, inStr := range []string{"hello", "world", "foobar"} {
		id, err := p.Add([]byte(inStr))
		assert.NoError(t, err)
		inserted[id] = inStr
	}
	return inserted
}
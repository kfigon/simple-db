package naive

import (
	"fmt"
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
		assert.Fail(t, "not implemented")
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
		
		newP, err := DeserializeSlotted(data)
		assert.NoError(t, err)

		assert.Len(t, newP.Indexes, 3)
		for id, expStr := range inserted {
			got, err := p.Read(id)
			assert.NoError(t, err)
	
			assert.Equal(t, []byte(expStr), got)
		}
	})
}

// id within the slotpage
type RowId int

type Slotted struct {
	Indexes []int // RowId -> offset within page
	CellData []byte
	lastOffset int
}

func NewSlotted(slottedPageSize int) *Slotted {
	return &Slotted{
		lastOffset: slottedPageSize,
		CellData: make([]byte, slottedPageSize),
	}
}

var errNoSpace = fmt.Errorf("no space in slot array")

func (s *Slotted) Add(buf []byte) (RowId, error) {
	bytesWithHeader := SerializeBytes(buf)
	ln := len(bytesWithHeader)

	if !s.hasSpace(ln) {
		return 0, errNoSpace
	}
	copy(s.CellData[s.lastOffset - ln:], bytesWithHeader)
	s.lastOffset -= ln

	s.Indexes = append(s.Indexes, s.lastOffset)

	return RowId(len(s.Indexes) - 1), nil
}

func (s *Slotted) Put(id RowId, buf []byte) (RowId, error) {
	// todo
	return 0, nil
}

func (s *Slotted) Read(idx RowId) ([]byte, error) {
	if int(idx) >= len(s.Indexes) {
		return nil, fmt.Errorf("invalid idx %d, got only %d", idx, len(s.Indexes))
	}

	offset := s.Indexes[idx]
	return DeserializeBytes(BytesWithHeader(s.CellData[offset:]))
}

func (s *Slotted) hasSpace(newData int) bool {
	const rowIdSize = 2
	return s.lastOffset - newData - (len(s.Indexes) * rowIdSize)  > 0
}

func (s *Slotted) Serialize() []byte {
	// indexes, padding, cells
	return nil
}

func DeserializeSlotted(b []byte) (*Slotted, error) {
	return nil, nil
}
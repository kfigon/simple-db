package naive

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlotted(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		inserted := map[RowId]string{}
		inputs := []string{"hello", "world", "foobar"} 
		p := NewSlotted(50)

		for _, inStr := range inputs{
			id, err := p.Add([]byte(inStr))
			assert.NoError(t, err)

			inserted[id] = inStr
		}
		
		assert.Len(t, inserted, len(inputs))
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
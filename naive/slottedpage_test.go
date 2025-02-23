package naive

import (
	"fmt"
	"testing"
)

func TestSlotted(t *testing.T) {
	t.Fatal("not implemented")
}

type Slotted struct {
	Indexes []int
	CellData []byte
	lastOffset int
}

func NewSlotted(pageSize int) *Slotted {
	return &Slotted{
		lastOffset: pageSize,
		CellData: make([]byte, pageSize),
	}
}

var errNoSpace = fmt.Errorf("no space in slot array")

func (s *Slotted) Add(buf []byte) (int, error) {
	if !s.hasSpace(len(buf) + 2) {
		return 0, errNoSpace
	}
	copy(s.CellData[s.lastOffset - 2 - len(buf):], []byte{byte(len(buf)), 0})
	copy(s.CellData[s.lastOffset - len(buf):], buf)
	s.lastOffset -= (2+len(buf))

	s.Indexes = append(s.Indexes, s.lastOffset)

	return len(s.Indexes) - 1, nil
}

func (s *Slotted) Read(idx int) ([]byte, error) {
	if idx >= len(s.Indexes) {
		return nil, fmt.Errorf("invalid idx %d, got only %d", idx, len(s.Indexes))
	}

	offset := s.Indexes[idx]
	ln := int(s.CellData[offset] + 0) // todo: marshall

	return s.CellData[offset+2: offset+2+ln], nil // todo: check boundaries
}

func (s *Slotted) hasSpace(newData int) bool {
	return s.lastOffset - newData - (len(s.Indexes) * 2)  > 0
}

func (s *Slotted) Serialize() []byte {
	// indexes, padding, cells
	return nil
}
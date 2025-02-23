package naive

import (
	"bytes"
	"fmt"
)

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
		CellData: make([]byte, slottedPageSize), // redundant, as not counting slot array size
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

func (s *Slotted) Put(id RowId, buf []byte) error {
	existing, err := s.Read(id)
	if err != nil {
		return err
	}
	if len(buf) <= len(existing) {
		bytesWithHeader := SerializeBytes(buf)
		offset := s.Indexes[id]
		copy(s.CellData[offset:], bytesWithHeader)

		return nil
	} 
	newRowId, err := s.Add(buf)
	if err != nil {
		return err
	}
	s.Indexes[id] = s.Indexes[newRowId]
	s.Indexes[newRowId] = -1 // tombstone value
	return nil
	// todo: reclaim page space
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

// todo: when serializing wrapping page - remember to add size of slot array
func (s *Slotted) Serialize() []byte {
	var buf bytes.Buffer
	for _, id := range s.Indexes {
		buf.Write(SerializeInt(int32(id)))
	}

	paddingLen := s.lastOffset - len(s.Indexes)*4

	buf.Write(make([]byte, paddingLen))
	buf.Write(s.CellData[s.lastOffset:])

	return buf.Bytes()
}

func DeserializeSlotted(b []byte, slotArrayLen int) (*Slotted, error) {
	originalSlice := b

	p := NewSlotted(len(b))
	var lastOffset *int
	
	for range slotArrayLen {
		i, err := DeserializeIntAndEat(&b)
		if err != nil {
			return nil, fmt.Errorf("error deserializing slot array: %w", err)
		}
		p.Indexes = append(p.Indexes, i)

		if lastOffset == nil || *lastOffset > i { // just min
			lastOffset = &i
		}
	}
	p.lastOffset = *lastOffset
	copy(p.CellData[*lastOffset:], originalSlice[*lastOffset:])

	return p, nil
}
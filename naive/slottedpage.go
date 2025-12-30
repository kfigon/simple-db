package naive

import (
	"bytes"
	"fmt"
	"io"
	"iter"
)

// id within the slotpage
type SlotIdx int

type Slotted struct {
	Indexes    []PageOffset // RowId -> offset within page
	CellData   []byte
	lastOffset PageOffset
}

// pagesize - generic headers
func NewSlotted(slottedPageSize int) *Slotted {
	return &Slotted{
		lastOffset: PageOffset(slottedPageSize),
		CellData:   make([]byte, slottedPageSize), // redundant, as not counting slot array size
	}
}

var errNoSpace = fmt.Errorf("no space in slot array")

func (s *Slotted) Add(buf []byte) (SlotIdx, error) {
	bytesWithHeader := SerializeBytes(buf)
	ln := len(bytesWithHeader)

	if !s.hasSpace(ln) {
		return 0, errNoSpace
	}
	copy(s.CellData[int(s.lastOffset)-ln:], bytesWithHeader)
	s.lastOffset -= PageOffset(ln)

	s.Indexes = append(s.Indexes, s.lastOffset)

	return SlotIdx(len(s.Indexes) - 1), nil
}

func (s *Slotted) Put(id SlotIdx, buf []byte) error {
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

func (s *Slotted) Read(idx SlotIdx) ([]byte, error) {
	if int(idx) >= len(s.Indexes) {
		return nil, fmt.Errorf("invalid idx %d, got only %d", idx, len(s.Indexes))
	}

	offset := s.Indexes[idx]
	return DeserializeBytes(BytesWithHeader(s.CellData[offset:]))
}

func (s *Slotted) hasSpace(newData int) bool {
	const rowIdSize = 2
	return int(s.lastOffset)-newData-(len(s.Indexes)*rowIdSize) > 0
}

func (s *Slotted) Serialize() []byte {
	var buf bytes.Buffer
	for _, id := range s.Indexes {
		buf.Write(SerializeInt(int32(id)))
	}

	paddingLen := int(s.lastOffset) - len(s.Indexes)*4

	buf.Write(make([]byte, paddingLen))
	buf.Write(s.CellData[s.lastOffset:])

	return buf.Bytes()
}

func DeserializeSlotted(r io.Reader, slotArrayLen int) (*Slotted, error) {
	p := NewSlotted(PageSize - 4*3)
	lastOffset := PageSize

	for range slotArrayLen {
		i, err := ReadInt(r)
		if err != nil {
			return nil, fmt.Errorf("error deserializing slot array: %w", err)
		}
		ithPageOffset := PageOffset(i)
		p.Indexes = append(p.Indexes, ithPageOffset)

		lastOffset = min(lastOffset, int(ithPageOffset))
	}
	p.lastOffset = PageOffset(lastOffset)
	// copy(p.CellData[lastOffset:], all[lastOffset:])
	r.Read(p.CellData)

	return p, nil
}

func (s *Slotted) SlotIdxIterator() iter.Seq[SlotIdx] {
	return func(yield func(SlotIdx) bool) {
		for slotIds := 0; slotIds < len(s.Indexes); slotIds++ {
			if !yield(SlotIdx(slotIds)) {
				return
			}
		}
	}
}

type CellIterator iter.Seq[[]byte]

func (s *Slotted) Iterator() CellIterator {
	return func(yield func([]byte) bool) {
		for slotId := range s.SlotIdxIterator() {
			d := must(s.Read(slotId))
			if !yield(d) {
				return
			}
		}
	}
}

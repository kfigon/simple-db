package page


type SlottedPage struct {
	baseHeaderSize int
	lastOffset PageOffset

	slots SlotArray
	cells []byte
}

const slotEntrySize = 2 // PageOffset = I16 -> 2

type SlotArray []PageOffset

func NewEmptySlottedPage(baseHeaderSize int) *SlottedPage {
	return &SlottedPage{
		baseHeaderSize: baseHeaderSize,
		lastOffset: PageSize,
	}
}

func (s *SlottedPage) Header() SlottedPageHeader {
	return SlottedPageHeader{
		SlotArrayLen: Byte(len(s.slots)),
		SlotArrayLastOffset: s.lastOffset,
	}
}

func (s *SlottedPage) Serialize() []byte {
	length := PageSize - s.baseHeaderSize
	out := make([]byte, length)
	offset := 0
	
	d := s.Header().Serialize()
	copy(out[offset:], d)
	offset += len(d)

	for _, slot := range s.slots {
		d = I16(slot).Serialize()
		copy(out[offset:], d)
		offset += len(d)
	}

	cellsStart := len(out) - len(s.cells)

	copy(out[cellsStart:], s.cells)
	return out
}

// todo: overflow checks including both headers
func (s *SlottedPage) AppendCell(cell Bytes) SlotIdx {
	payload := cell.Serialize()
	s.lastOffset -= PageOffset(len(payload))
	if int(s.lastOffset) < s.baseHeaderSize {
		panic("data page overflow")
	}

	s.cells = append(s.cells, payload...)
	s.slots = append(s.slots, s.lastOffset)
	return SlotIdx(len(s.slots) - 1)
}

func (s *SlottedPage) ReadCell(id SlotIdx) (Bytes, error) {
	offset := s.slots[id]
	var b Bytes
	return b.Deserialize(s.cells[offset:])
}


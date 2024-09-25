package page

type SlottedPage struct {
	headerLen int
	lastOffset PageOffset

	slots SlotArray
	cells []byte
}

const slotEntrySize = 2 // PageOffset = I16 -> 2

type SlotArray []PageOffset
type Cell Bytes // len + data

// lastoffset default - page size
func NewSlottedPage(headerLen int, lastOffset PageOffset) *SlottedPage {
	return &SlottedPage{
		headerLen: headerLen,
		lastOffset: lastOffset,
	}
}

func (s *SlottedPage) Header() SlottedPageHeader {
	return SlottedPageHeader{
		SlotArrayLen: Byte(len(s.slots)),
		SlotArrayLastOffset: s.lastOffset,
	}
}

// todo: overflow checks including header
func (s *SlottedPage) AppendCell(cell Bytes) SlotIdx {
	payload := cell.Serialize()
	s.lastOffset -= PageOffset(len(payload))
	if s.lastOffset < 0 {
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

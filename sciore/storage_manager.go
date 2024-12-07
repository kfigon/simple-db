package sciore

import (
	"fmt"
	"simple-db/page"
)

type PageOffset int

type Page struct {
	data []byte 
}

const PageSize = 4*1024

func NewPage() *Page {
	return &Page{
		data: make([]byte, PageSize),
	}
}

func (p *Page) StoreByte(offset PageOffset, b byte) error {
	if int(offset) + 1 > len(p.data) {
		return fmt.Errorf("too big offset %d", offset)
	}

	p.data[offset] = b
	return nil
}

func (p *Page) ReadByte(offset PageOffset) (byte, error) {
	if int(offset) > len(p.data) {
		return 0, fmt.Errorf("too big offset %d", offset)
	}

	return p.data[offset],nil
}

func (p *Page) StoreBytes(offset PageOffset, b []byte) error {
	serialized := page.Bytes(b).Serialize()
	if int(offset) + len(serialized) > len(p.data) {
		return fmt.Errorf("too much data (%d) at offset %d", len(serialized), offset)
	}
	copy(p.data[offset:], serialized)
	return nil
}

func (p *Page) ReadBytes(offset PageOffset) ([]byte, error) {
	if int(offset) > len(p.data) {
		return nil, fmt.Errorf("too big offset %d", offset)
	}
	return page.Bytes(nil).Deserialize(p.data[offset:])
}
package page

import (
	"encoding/binary"
	"fmt"
)

const PageSize = 8*512 // 4KB

type PageOffset int
type PageID int

type Page struct {
	data []byte
	takenSpace int
}

func NewPage() *Page {
	return &Page{
		data: make([]byte, PageSize),
		takenSpace: 0,
	}
}
// todo: impl overflow pages for big data

// todo: generalize pager? Accept a byte array and store anything? Delegate serialization to concrete wrapper types?

func (p *Page) tooBig(offset PageOffset, dataSize int) bool {
	return int(offset) + dataSize >= len(p.data)
}

var ErrCantFitInPage = fmt.Errorf("can't fit it in page")
func (p *Page) StoreInt(offset PageOffset, data int32) error {
	if p.tooBig(offset, 4) {
		return ErrCantFitInPage
	}
	
	binary.BigEndian.PutUint32(p.data[offset:], uint32(data))
	p.takenSpace += 4
	return nil
}

func (p *Page) StoreByte(offset PageOffset, data byte) error {
	if p.tooBig(offset, 1) {
		return ErrCantFitInPage
	}
	
	p.data[offset] = data
	p.takenSpace += 1
	return nil
}

func (p *Page) StoreI16(offset PageOffset, data int16) error {
	if p.tooBig(offset, 2) {
		return ErrCantFitInPage
	}
	
	binary.BigEndian.PutUint16(p.data[offset:], uint16(data))
	p.takenSpace += 2
	return nil
}

func (p *Page) StoreString(offset PageOffset, data string) error {
	return p.StoreBlob(offset, []byte(data))
}

func (p *Page) StoreBlob(offset PageOffset, data []byte) error {
	if p.tooBig(offset, 2 + len(data)) {
		return ErrCantFitInPage
	}

	binary.BigEndian.PutUint16(p.data[offset:], uint16(len(data)))
	ret := copy(p.data[offset+2:], data)
	p.takenSpace += 2 + len(data)

	if ret != len(data) {
		return fmt.Errorf("invalid number of bytes written, got %v, exp %v", ret, len(data))
	}
	return nil
}

func (p *Page) ReadInt(offset PageOffset) int32 {
	v := binary.BigEndian.Uint32(p.data[offset:])
	return int32(v)
}

func (p *Page) ReadByte(offset PageOffset) byte {
	return p.data[offset]
}

func (p *Page) ReadInt16(offset PageOffset) int16 {
	v := binary.BigEndian.Uint16(p.data[offset:])
	return int16(v)
}

func (p *Page) ReadString(offset PageOffset) string {
	return string(p.ReadBlob(offset))
}

func (p *Page) ReadBlob(offset PageOffset) []byte {
	howMany := binary.BigEndian.Uint16(p.data[offset:])
	
	data := make([]byte, howMany)
	copy(data, p.data[offset+2:])
	return data
}
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

var ErrCantFitInPage = fmt.Errorf("can't fit it in page")
func (p *Page) StoreInt(offset PageOffset, data int32) error {
	if int(offset) + 4 >= len(p.data) {
		return ErrCantFitInPage
	}
	
	binary.BigEndian.PutUint32(p.data[offset:], uint32(data))
	p.takenSpace += 4
	return nil
}

func (p *Page) StoreString(offset PageOffset, data string) error {
	if int(offset) + 1 + len(data) >= len(p.data) {
		return ErrCantFitInPage
	} else if len(data) > 255 {
		return fmt.Errorf("cant store more than 255 now")
	}

	p.data[offset] = byte(len(data))
	ret := copy(p.data[offset+1:], data)
	p.takenSpace += 1 + len(data)

	if ret != len(data) {
		return fmt.Errorf("invalid number of bytes written, got %v, exp %v", ret, len(data))
	}
	return nil
}

func (p *Page) ReadInt(offset PageOffset) int32 {
	v := binary.BigEndian.Uint32(p.data[offset:])
	return int32(v)
}

func (p *Page) ReadString(offset PageOffset) string {
	howMany := p.data[offset]
	data := make([]byte, howMany)
	copy(data, p.data[offset+1:])
	return string(data)
}
package sciore

import (
	"encoding/binary"
	"strings"
)

var endian = binary.LittleEndian

type PageID int

type Page struct {
	Data []byte
}

func NewPage() *Page {
	return &Page{Data: make([]byte, PageSize)}
}

func (p *Page) ReadInt(offset int) int {
	assert(offset < len(p.Data), "size mismatch %d != %d", offset, len(p.Data))
	got := endian.Uint64(p.Data[offset:])
	return int(got)
}

func (p *Page) WriteInt(offset int, value int) {
	assert(offset+8 < len(p.Data), "size mismatch %d != %d", offset+8, len(p.Data))
	endian.PutUint64(p.Data[offset:], uint64(value))
}

func (p *Page) ReadString(offset int) string {
	offsetPlusHeader := offset + 8
	assert(offsetPlusHeader < len(p.Data), "size mismatch %d != %d", offsetPlusHeader, len(p.Data))
	size := p.ReadInt(offset)
	requiredSize := offsetPlusHeader + size
	assert(requiredSize < len(p.Data), "size mismatch %d != %d", requiredSize, len(p.Data))

	b := strings.Builder{}
	b.Write(p.Data[offsetPlusHeader:requiredSize])
	return b.String()
}

func (p *Page) WriteString(offset int, s string) {
	strLen := len(s)
	sizeLen := 8
	requiredLen := strLen + sizeLen + offset
	assert(requiredLen < len(p.Data), "size mismatch %d != %d", requiredLen, len(p.Data))

	p.WriteInt(offset, strLen)
	_ = copy(p.Data[offset+8:], []byte(s))
}

package sciore

import (
	"encoding/binary"
	"fmt"
)

var endian = binary.LittleEndian

type PageID int

type Page struct {
	Data []byte
}

func NewPage() *Page {
	return &Page{Data: make([]byte, PageSize)}
}

var ErrCantFit = fmt.Errorf("cant fit data to the page")

func (p *Page) ReadInt(offset int) (int, error) {
	if offset < len(p.Data) {
		return 0, fmt.Errorf("size mismatch %d != %d, %w", offset, len(p.Data), ErrCantFit)
	}
	got := endian.Uint64(p.Data[offset:])
	return int(got), nil
}

func (p *Page) WriteInt(offset int, value int) error {
	if offset+8 < len(p.Data) {
		return fmt.Errorf("size mismatch %d != %d %w", offset+8, len(p.Data), ErrCantFit)
	}
	endian.PutUint64(p.Data[offset:], uint64(value))
	return nil
}

func (p *Page) ReadString(offset int) (string, error) {
	b, err := p.ReadBytes(offset)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Page) ReadBytes(offset int) ([]byte, error) {
	offsetPlusHeader := offset + 8
	if offsetPlusHeader < len(p.Data) {
		return nil, fmt.Errorf("size mismatch %d != %d, %w", offsetPlusHeader, len(p.Data), ErrCantFit)
	}
	size, err := p.ReadInt(offset)
	if err != nil {
		return nil, err
	}
	requiredSize := offsetPlusHeader + size
	if requiredSize < len(p.Data) {
		return nil, fmt.Errorf("size mismatch %d != %d, %w", requiredSize, len(p.Data), ErrCantFit)
	}

	b := make([]byte, requiredSize)
	copy(b, p.Data[offsetPlusHeader:requiredSize])
	return b, nil
}

func (p *Page) WriteString(offset int, s string) error {
	return p.WriteBytes(offset, []byte(s))
}

func (p *Page) WriteBytes(offset int, b []byte) error {
	strLen := len(b)
	sizeLen := 8
	requiredLen := strLen + sizeLen + offset
	if requiredLen < len(p.Data) {
		return fmt.Errorf("size mismatch %d != %d, %w", requiredLen, len(p.Data), ErrCantFit)
	}

	err := p.WriteInt(offset, strLen)
	if err != nil {
		return err
	}
	_ = copy(p.Data[offset+8:], b)
	return nil
}

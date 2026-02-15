package naive

import (
	"bytes"
	"fmt"
	"io"
)

type OverflowPage struct {
	Header GenericPageHeader
	Data   []byte
}

func NewOverflowPage(pageSize int, data []byte) (page *OverflowPage, rest []byte) {
	page = &OverflowPage{
		Header: GenericPageHeader{
			PageTyp:       OverflowPageType,
			NextPage:      0, // next pageID
			SlotArraySize: 0, // not used
		},
		Data: make([]byte, pageSize-4-4-4), //12 bytes for header
	}

	if len(data) >= len(page.Data) {
		copy(page.Data, data[:len(page.Data)])
		rest = data[len(page.Data):]
	} else {
		copy(page.Data, data)
		rest = nil
	}
	return page, rest
}

func (o *OverflowPage) Serialize() []byte {
	got := SerializeStruct(o,
		WithInt(func(g *OverflowPage) int32 { return int32(g.Header.PageTyp) }),
		WithInt(func(g *OverflowPage) int32 { return int32(g.Header.NextPage) }),
		WithInt(func(g *OverflowPage) int32 { return int32(g.Header.SlotArraySize) }),
		func(g *OverflowPage, b *bytes.Buffer) {
			b.Write(g.Data)
		},
	)

	debugAssert(len(got) == PageSize, "overflow page size should be consistent")
	return got
}

func DeserializeOverflowPage(header *GenericPageHeader, r io.Reader) (*OverflowPage, error) {
	buf := make([]byte, PageSize-4-4-4) //12 bytes for header
	got, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error reading overflow page: %w", err)
	} else if got != len(buf) {
		return nil, fmt.Errorf("error reading overflow page got %d, expected %d", got, len(buf))
	}

	return &OverflowPage{
		Header: *header,
		Data:   buf,
	}, nil
}

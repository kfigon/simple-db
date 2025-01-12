package naive

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var endinanness = binary.BigEndian

func SerializeBool(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func SerializeInt(i int32) []byte {
	return endinanness.AppendUint32([]byte{}, uint32(i))
}

func SerializeString(s string) []byte {
	header := SerializeInt(int32(len(s)))
	return append(header, []byte(s)...)
}

func DeserializeBool(b []byte) (bool, error) {
	switch {
	case len(b) < 1: return false, deserializationErr(len(b), 1, "bool")
	case b[0] == 0: return false, nil
	case b[0] == 1: return true, nil
	default: return false, fmt.Errorf("invalid boolean, expected 0 or 1, got %v", b[0])
	}
}

func DeserializeInt(b []byte) (int32, error) {
	exp := 4
	got := len(b)
	if got < exp {
		return 0, deserializationErr(got, exp, "i32")
	}
	return int32(endinanness.Uint32(b)), nil
}

func DeserializeString(b []byte) (string, error) {
	exp := 4
	got := len(b)
	if got < exp {
		return "", deserializationErr(got, exp, "string header")
	}
	return string(b[4:]), nil
}

func deserializationErr(got, exp int, typ string) error {
	return fmt.Errorf("cant deserialize into %v, expected lenght %v, got %v", typ, exp, got)
}

type PageType int32
const (
	RootPage PageType = iota
	DataPage
	SchemaPage
)

type PageID int32
type PageOffset int32

type Page struct {
	Header struct {
		PageTyp PageType
		NextPage PageID
		Size int32
	}
	PageData []byte
}

func SerializeSchema(s Schema) []byte {
	var buf bytes.Buffer
	for table, data := range s {

	}
	return buf.Bytes()
}

func SerializeData(d Data) []byte {
	return nil
}
package naive

import (
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
	if len(b) < 1 {
		return false, deserializationErr(len(b), 1, "bool")
	} else if b[0] == 0 {
		return false, nil
	} else if b[0] == 1 {
		return true, nil
	}
	return false, fmt.Errorf("invalid boolean, expected 0 or 1, got %v", b[0])
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

type Page struct {
	
}
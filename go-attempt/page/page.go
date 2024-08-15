package page

import (
	"encoding/binary"
	"fmt"
)

type PageType byte
const (
	DirectoryPageType PageType = iota + 1
	DataPageType
)

type FieldType byte
const (
	I8Type FieldType = iota
	I16Type
	I32Type
	I64Type
	StringType
	BinaryBlobType
)

type Serializable[T any] interface {
	Serialize() []byte
	Deserialize([]byte) (T, error)
	Length() int
}

var ErrTooShortBuffer = fmt.Errorf("too short buffer")

type Byte byte
func(b Byte) Serialize() []byte{
	return []byte{byte(b)}
}

func(b Byte) Deserialize(data []byte) (Byte, error) {
	if len(data) < b.Length() {
		return 0, ErrTooShortBuffer
	}
	return Byte(data[0]), nil
}

func (b Byte) Length() int {
	return 1
}

type I16 int16
func(i I16) Serialize() []byte{
	out := make([]byte, i.Length())
	binary.BigEndian.PutUint16(out, uint16(i))
	return out
}

func(i I16) Deserialize(data []byte) (I16, error) {
	if len(data) < i.Length() {
		return 0, ErrTooShortBuffer
	}
	
	return I16(binary.BigEndian.Uint16(data)), nil
}

func(i I16) Length() int {
	return 2
}

type String string
func(s String) Serialize() []byte {
	return Bytes([]byte(s)).Serialize()
}

func(String) Deserialize(data []byte) (String, error){
	out, err := Bytes(nil).Deserialize(data)
	if err != nil {
		return "", err
	}
	return String(out), nil
}


type Bytes []byte
func(b Bytes) Serialize() []byte{
	out := make([]byte, 0, b.Length())
	out = append(out, I16(len(b)).Serialize()...)
	out = append(out, b...)
	return out
}

func(Bytes) Deserialize(data []byte) (Bytes, error){
	howMany, err := I16(0).Deserialize(data)
	headerLen := I16(0).Length()

	if err != nil {
		return nil, err
	} else if int(howMany) + headerLen > len(data) {
		return nil, ErrTooShortBuffer
	}
	out := data[headerLen:int(howMany)+headerLen]
	return Bytes(out),nil
}

func(b Bytes) Length() int {
	return I16(0).Length() + len(b)
}
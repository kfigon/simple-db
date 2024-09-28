package page

import (
	"encoding/binary"
	"fmt"
)

// todo: these are very similar. Make it an std interface like Reader and Writer?
type Serializable[T any] interface {
	Serialize() []byte
	Deserialize([]byte) (T, error)
}

var ErrTooShortBuffer = fmt.Errorf("too short buffer")

var endian = binary.BigEndian

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
	endian.PutUint16(out, uint16(i))
	return out
}

func(i I16) Deserialize(data []byte) (I16, error) {
	if len(data) < i.Length() {
		return 0, ErrTooShortBuffer
	}
	
	return I16(endian.Uint16(data)), nil
}

func(i I16) Length() int {
	return 2
}

type I64 int64
func(i I64) Serialize() []byte{
	out := make([]byte, i.Length())
	endian.PutUint64(out, uint64(i))
	return out
}

func(i I64) Deserialize(data []byte) (I64, error) {
	if len(data) < i.Length() {
		return 0, ErrTooShortBuffer
	}
	
	return I64(endian.Uint64(data)), nil
}

func(i I64) Length() int {
	return 8
}

type I32 int32
func(i I32) Serialize() []byte{
	out := make([]byte, i.Length())
	endian.PutUint32(out, uint32(i))
	return out
}

func(i I32) Deserialize(data []byte) (I32, error) {
	if len(data) < i.Length() {
		return 0, ErrTooShortBuffer
	}
	
	return I32(endian.Uint32(data)), nil
}

func(i I32) Length() int {
	return 4
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
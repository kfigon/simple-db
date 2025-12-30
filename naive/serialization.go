package naive

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type setter[T any, V any] func(*T, V)
type deserializeFn[T any, V any] func(*T, *V) error
type mapper[T any, V any] func(*T) (V, error)

func compose[T any, V any](fieldName string, setFn setter[T, V], m mapper[[]byte, V]) deserializeFn[T, []byte] {
	return func(t *T, b *[]byte) error {
		v, err := m(b)
		if err != nil {
			return fmt.Errorf("error deserializing %s: %w", fieldName, err)
		}
		setFn(t, v)
		return nil
	}
}

// -----------------

type serializationFn[T any] func(*T, *bytes.Buffer)

func SerializeStruct[T any](v *T, funs ...serializationFn[T]) []byte {
	buf := bytes.NewBuffer(nil)
	for _, fn := range funs {
		fn(v, buf)
	}
	return buf.Bytes()
}

type getterFn[T any, K any] func(*T) K

func WithInt[T any](get getterFn[T, int32]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		i := get(t)
		serInt(i, b)
	}
}

func WithBool[T any](get getterFn[T, bool]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		boolV := get(t)
		if boolV {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
	}
}

func WithString[T any](get getterFn[T, string]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		str := get(t)
		serInt(int32(len(str)), b)
		b.WriteString(str)
	}
}

func WithBytes[T any](get getterFn[T, []byte]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		bytez := get(t)
		serInt(int32(len(bytez)), b)
		b.Write(bytez)
	}
}

func serInt(i int32, b *bytes.Buffer) {
	b.Write(endinanness.AppendUint32([]byte{}, uint32(i)))
}

type deserializeFn2[T any] func(*T, *bytes.Reader) error

func Deserialize2[T any](buf *bytes.Reader, funs ...deserializeFn2[T]) (*T, error) {
	var out T
	for _, fn := range funs {
		if err := fn(&out, buf); err != nil {
			return nil, fmt.Errorf("deserialization error on %T: %w", out, err)
		}
	}
	return &out, nil
}

type setterFn[T any, K any] func(*T, *K)

func DeserWithInt[T any](fieldName string, set setterFn[T, int32]) deserializeFn2[T] {
	return func(t *T, r *bytes.Reader) error {
		i, err := ReadInt(r)
		if err != nil {
			return fmt.Errorf("error deserializing %q int: %w", fieldName, err)
		}
		set(t, &i)
		return nil
	}
}

func DeserWithStr[T any](fieldname string, set setterFn[T, string]) deserializeFn2[T] {
	return func(t *T, r *bytes.Reader) error {
		i, err := ReadInt(r)
		if err != nil {
			return fmt.Errorf("error deserializing lenght of the %q string: %w", fieldname, err)
		}
		buf := make([]byte, i)
		got, err := r.Read(buf)
		if err != nil {
			return fmt.Errorf("error deserializing %q string: %w", fieldname, err)
		} else if got != int(i) {
			return fmt.Errorf("error deserializing %q string, expected %d bytes, got %d", fieldname, i, got)
		}
		data := string(buf)
		set(t, &data)
		return nil
	}
}

func DeserWithBool[T any](fieldName string, set setterFn[T, bool]) deserializeFn2[T] {
	return func(t *T, r *bytes.Reader) error {
		got, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("error deserializing %q bool: %w", fieldName, err)
		} else if got == 0 {
			v := false
			set(t, &v)
		} else if got == 1 {
			v := true
			set(t, &v)
		} else {
			return fmt.Errorf("error deserializing %q bool, unexpected bool value: %b", fieldName, got)
		}
		return nil
	}
}

func ReadInt(r *bytes.Reader) (int32, error) {
	buf := make([]byte, 4)
	got, err := r.Read(buf)
	if err != nil {
		return 0, err
	} else if got != 4 {
		return 0, fmt.Errorf("failed to read int, expected 4 bytes, got %d", got)
	}

	return int32(endinanness.Uint32(buf)), nil
}

func ReaderToBytesReader(r io.Reader) (*bytes.Reader, error) {
	got, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(got), nil
}

func DeserializeAll[T any](b []byte, funs ...deserializeFn[T, []byte]) (*T, error) {
	var out T
	for _, v := range funs {
		if err := v(&out, &b); err != nil {
			return nil, err
		}
	}
	return &out, nil
}

var endinanness = binary.BigEndian

type BytesWithHeader []byte

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
	return SerializeBytes([]byte(s))
}

func SerializeBytes(b []byte) BytesWithHeader {
	header := SerializeInt(int32(len(b)))
	return append(header, b...)
}

func DeserializeBool(b []byte) (bool, error) {
	switch {
	case len(b) < 1:
		return false, deserializationErr(len(b), 1, "bool")
	case b[0] == 0:
		return false, nil
	case b[0] == 1:
		return true, nil
	default:
		return false, fmt.Errorf("invalid boolean, expected 0 or 1, got %v", b[0])
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
	v, err := DeserializeBytes(b)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func DeserializeBytes(b BytesWithHeader) ([]byte, error) {
	exp := 4
	got := len(b)
	if got < exp {
		return nil, deserializationErr(got, exp, "array header")
	}
	howMany, err := DeserializeInt(b[:4])
	if err != nil {
		return nil, fmt.Errorf("error deserializing header length: %w", err)
	}
	return b[4 : 4+howMany], nil
}

func deserializationErr(got, exp int, typ string) error {
	return fmt.Errorf("cant deserialize into %v, expected lenght %v, got %v", typ, exp, got)
}

func DeserializeStringAndEat(b *[]byte) (string, error) {
	v, err := DeserializeString(*b)
	if err != nil {
		return "", err
	}
	advanceBuf(b, 4+len(v))
	return v, nil
}

func DeserializeBoolAndEat(b *[]byte) (bool, error) {
	v, err := DeserializeBool(*b)
	if err != nil {
		return false, err
	}
	advanceBuf(b, 1)
	return v, nil
}

func DeserializeIntAndEat(b *[]byte) (int32, error) {
	v, err := DeserializeInt(*b)
	if err != nil {
		return 0, err
	}
	advanceBuf(b, 4)
	return v, nil
}

func advanceBuf(b *[]byte, howMany int) {
	*b = (*b)[howMany:]
}

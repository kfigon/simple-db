package naive

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	return SerializeBytes([]byte(s))
}

func SerializeBytes(b []byte) BytesWithHeader {
	header := SerializeInt(int32(len(b)))
	return append(header, b...)
}

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
		b.Write(SerializeInt(int32(i)))
	}
}

func WithBool[T any](get getterFn[T, bool]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		boolV := get(t)
		b.Write(SerializeBool(boolV))
	}
}

func WithString[T any](get getterFn[T, string]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		str := get(t)
		b.Write(SerializeInt(int32(len(str))))
		b.WriteString(str)
	}
}

func WithBytes[T any](get getterFn[T, []byte]) serializationFn[T] {
	return func(t *T, b *bytes.Buffer) {
		bytez := get(t)
		b.Write(SerializeInt(int32(len(bytez))))
		b.Write(bytez)
	}
}

type deserializeFn2[T any] func(*T, io.Reader) error

func DeserializeStruct[T any](buf io.Reader, funs ...deserializeFn2[T]) (*T, error) {
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
	return func(t *T, r io.Reader) error {
		i, err := ReadInt(r)
		if err != nil {
			return fmt.Errorf("error deserializing %q int: %w", fieldName, err)
		}
		set(t, &i)
		return nil
	}
}

func DeserWithStr[T any](fieldname string, set setterFn[T, string]) deserializeFn2[T] {
	return func(t *T, r io.Reader) error {
		got, err := ReadString(r)
		if err != nil {
			return fmt.Errorf("error deserializing %q string: %w", fieldname, err)
		}
		set(t, &got)
		return nil
	}
}

func DeserWithBytes[T any](fieldname string, set setterFn[T, []byte]) deserializeFn2[T] {
	return func(t *T, r io.Reader) error {
		got, err := ReadBytes(r)
		if err != nil {
			return fmt.Errorf("error deserializing %q byte array: %w", fieldname, err)
		}
		set(t, &got)
		return nil
	}
}

func DeserWithBool[T any](fieldName string, set setterFn[T, bool]) deserializeFn2[T] {
	return func(t *T, r io.Reader) error {
		got, err := ReadBool(r)
		if err != nil {
			return fmt.Errorf("error deserializing %q bool: %w", fieldName, err)
		}
		set(t, &got)
		return nil
	}
}

func ReadInt(r io.Reader) (int32, error) {
	buf := make([]byte, 4)
	got, err := r.Read(buf)
	if err != nil {
		return 0, err
	} else if got != 4 {
		return 0, fmt.Errorf("failed to read int, expected 4 bytes, got %d", got)
	}

	return int32(endinanness.Uint32(buf)), nil
}

func ReadString(r io.Reader) (string, error) {
	i, err := ReadInt(r)
	if err != nil {
		return "", fmt.Errorf("error deserializing lenght of the string: %w", err)
	}
	buf := make([]byte, i)
	got, err := r.Read(buf)
	if err != nil {
		return "", fmt.Errorf("error deserializing string: %w", err)
	} else if got != int(i) {
		return "", fmt.Errorf("error deserializing string, expected %d bytes, got %d", i, got)
	}
	return string(buf), nil
}

func ReadBytes(r io.Reader) ([]byte, error) {
	i, err := ReadInt(r)
	if err != nil {
		return nil, fmt.Errorf("error deserializing lenght of byte array: %w", err)
	}
	buf := make([]byte, i)
	got, err := r.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("error deserializing byte array: %w", err)
	} else if got != int(i) {
		return nil, fmt.Errorf("error deserializing byte array, expected %d bytes, got %d", i, got)
	}
	return buf, nil
}

func ReadBool(r io.Reader) (bool, error) {
	d := make([]byte, 1)
	got, err := r.Read(d)
	if err != nil {
		return false, fmt.Errorf("error deserializing bool: %w", err)
	} else if got != 1 {
		return false, fmt.Errorf("error deserializing bool, expected 1 byte, got %d", got)
	} else if d[0] == 0 {
		return false, nil
	} else if d[0] == 1 {
		return true, nil
	}
	return false, fmt.Errorf("error deserializing bool, unexpected bool value: %b", d[0])
}

type BytesWithHeader []byte

func DeserializeBytes(b BytesWithHeader) ([]byte, error) {
	exp := 4
	got := len(b)
	if got < exp {
		return nil, fmt.Errorf("cant deserialize into %v, expected lenght %v, got %v", "array header", exp, got)
	}
	howMany := endinanness.Uint32(b[:4])
	return b[4 : 4+howMany], nil
}

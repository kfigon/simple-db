package naive

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func SerializeAll(chunks ...[]byte) []byte {
	var buf bytes.Buffer
	for _, c := range chunks {
		buf.Write(c)
	}
	return buf.Bytes()
}

type setter[T any]func(*T, *[]byte) error

// sometimes you need also the parent for some context
type setterWithParent[T any, V any]func(*T, *V, *[]byte) error
type extract[T any, V any]func(*T)*V

func compose[T any,V any](fieldName string, ex extract[T,V], setFn setter[V]) setter[T] {
	return func(t *T, b *[]byte) error {
		v := ex(t)
		err := setFn(v, b)
		if err != nil {
			return fmt.Errorf("error deserializing %s: %w", fieldName, err)
		}
		return nil
	}
}

func composeWithParent[T any,V any](fieldName string, ex extract[T,V], setFn setterWithParent[T, V]) setter[T] {
	return func(t *T, b *[]byte) error {
		v := ex(t)
		err := setFn(t, v, b)
		if err != nil {
			return fmt.Errorf("error deserializing %s: %w", fieldName, err)
		}
		return nil
	}
}
func intDeser(v *int32, b *[]byte) error {
	got, err := DeserializeIntAndEat(b)
	if err != nil {
		return err
	}
	*v = got
	return nil
}

func strDeser(v *string, b *[]byte) error {
	got, err := DeserializeStringAndEat(b)
	if err != nil {
		return err
	}
	*v = got
	return nil
}

func DeserializeAll[T any](b []byte, funs ...setter[T]) (*T, error) {
	var out T
	for _, v := range funs{
		if err := v(&out, &b); err != nil{
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

package naive

import (
	"bytes"
	"fmt"
)

func SerializeAll(chunks ...[]byte) []byte {
	var buf bytes.Buffer
	for _, c := range chunks {
		buf.Write(c)
	}
	return buf.Bytes()
}

type demapper[T any]func(*T, *[]byte) error

// sometimes you need also the parent for some context
type demapperWithParent[T any, V any]func(*T, *V, *[]byte) error
type extract[T any, V any]func(*T)*V

type Demapper[T any] struct {
	funs []demapper[T]	
}

func compose[T any,V any](fieldName string, ex extract[T,V], dem demapper[V]) demapper[T] {
	return func(t *T, b *[]byte) error {
		v := ex(t)
		err := dem(v, b)
		if err != nil {
			return fmt.Errorf("error deserializing %s: %w", fieldName, err)
		}
		return nil
	}
}

func composeWithParent[T any,V any](fieldName string, ex extract[T,V], dem demapperWithParent[T, V]) demapper[T] {
	return func(t *T, b *[]byte) error {
		v := ex(t)
		err := dem(t, v, b)
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

func DeserializeIt[T any](d Demapper[T], b []byte) (*T, error) {
	var out T
	for _, v := range d.funs {
		if err := v(&out, &b); err != nil{
			return nil, err
		}	
	}
	return &out, nil
}

func DeserializeAll[T any](b []byte, funs ...demapper[T]) (*T, error) {
	var out T
	for _, v := range funs{
		if err := v(&out, &b); err != nil{
			return nil, err
		}	
	}
	return &out, nil
}
package naive

import (
	"fmt"
	"reflect"
)

func SerializeReflection[T any](obj T) []byte {
	out := []byte{}
	val := reflect.ValueOf(obj)
	typ := reflect.TypeOf(obj)

	if val.Kind() != reflect.Struct {
		return out
	}

	// Iterate over the fields
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		if _, ok := field.Tag.Lookup("bin"); !ok {
			continue
		}

		reflectedValue := val.Field(i).Interface()
		switch v := reflectedValue.(type) {
		case bool:
			out = append(out, SerializeBool(v)...)
		case int32:
			out = append(out, SerializeInt(v)...)
		case string:
			out = append(out, SerializeString(v)...)
		case []byte:
			out = append(out, SerializeBytes(v)...)
		}
	}
	
	return out
}

func DeserializeReflection[T any](bytes []byte) (T, error) {
	var obj T
	val := reflect.ValueOf(&obj)
	typ := reflect.TypeOf(obj)

	elem := val.Elem()

	// Iterate over the fields
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if _, ok := field.Tag.Lookup("bin"); !ok {
			continue
		}
		f := elem.FieldByName(field.Name)
		fieldType := typ.Field(i).Type.Kind()
		switch fieldType {
		case reflect.Bool:
			b, err := DeserializeBoolAndEat(&bytes)
			if err != nil {
				return obj, fmt.Errorf("error deserializing bool on field %s", field.Name)
			}
			f.Set(reflect.ValueOf(b))
		case reflect.Int32:
			i, err := DeserializeIntAndEat(&bytes)
			if err != nil {
				return obj, fmt.Errorf("error deserializing int on field %s", field.Name)
			}
			f.Set(reflect.ValueOf(i))
		case reflect.String:
			s, err := DeserializeStringAndEat(&bytes)
			if err != nil {
				return obj, fmt.Errorf("error deserializing string on field %s", field.Name)
			}
			f.Set(reflect.ValueOf(s))
		}
	}
	return obj, nil
}
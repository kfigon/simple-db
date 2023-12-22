package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)


func main() {
	fmt.Println("hello")

	d, err := serialize(Row{
		Id:    1,
		Name:  "foo",
		Email: "asdf@fad.com",
	})

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(d)
}


type Row struct {
	Id int
	Name string
	Email string
}

func serialize[T any](row T) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(row)
	if err != nil {
		return nil, fmt.Errorf("error serializing entry: %w", err)
	}
	return buf.Bytes(), nil
}
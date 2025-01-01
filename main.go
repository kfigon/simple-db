package main

import (
	"bufio"
	"fmt"
	"os"
	"simple-db/naive"
	"simple-db/sql"
	"strings"
)

func main() {
	fmt.Println("hello, type commands, 'quit' to stop")
	storage := naive.NewStorage()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		
		s, err := reader.ReadString('\n')
		s = strings.TrimSpace(s)
		if err != nil {
			fmt.Println(err)
			return
		} else if s == "quit" || s == "exit" {
			fmt.Println("auf wiedersehen!")
			break
		}

		stmt, err := sql.Parse(sql.Lex(s))
		if err != nil {
			fmt.Println("error:", err)
			continue
		}

		switch stmt := stmt.(type) {
		case *sql.CreateStatement:
			if err = storage.CreateTable(*stmt); err != nil {
				fmt.Println(err)
			}
		case *sql.InsertStatement:
			if err = storage.Insert(*stmt); err != nil {
				fmt.Println(err)
			}
		case *sql.SelectStatement:
			res, err := storage.Select(*stmt)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(fmtQueryRes(res))
			}
		}
	}
}

func fmtQueryRes(r naive.QueryResult) string {
	var out string
	out += strings.Join(r.Header, "\t")
	out += "\n"
	out += "------------------"
	out += "\n"
	for _, v := range r.Values {
		out += strings.Join(v, "\t")
		out += "\n"
	}
	out += fmt.Sprintf("%d lines found", len(r.Values))
	return out
}
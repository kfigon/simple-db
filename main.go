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
	fmt.Println("hello, type commands:")
	fmt.Println("'quit' or 'exit' to stop")
	fmt.Println("'dump <filename>' to dump current db to <filename>")
	fmt.Println("'load <filename>' to load <filename> to db")

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
		} else if strings.HasPrefix(s, "dump ") {
			fileName := strings.TrimPrefix(s, "dump ")
			if err = dumpDbToFile(storage, fileName); err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("db wrote to %q\n", fileName)
			}
			continue
		} else if strings.HasPrefix(s, "load ") {
			fileName := strings.TrimPrefix(s, "load ")
			if err = loadFile(&storage, fileName); err != nil {
				fmt.Printf("failed to load file: %q. Current db is not changed", fileName)
			} else {
				fmt.Printf("db refreshed from %q\n", fileName)
			}
			continue
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

func dumpDbToFile(s *naive.Storage, fileName string) error {
	if fileName == "" {
		return fmt.Errorf("please provide file name for 'dump' command")
	}
	f, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error writing a file: %w", err)
	}
	defer f.Close()
	
	_, err = f.Write(naive.SerializeDb(s))
	if err != nil {
		return fmt.Errorf("error writing to file %v: %w", fileName, err)
	}

	return nil
}

func loadFile(s **naive.Storage, fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("error reading the file: %w", err)
	}
	defer f.Close()
	newDb, err := naive.DeserializeDb(f)
	if err != nil {
		return fmt.Errorf("error deserializing the file %q: %w", fileName, err)
	}

	*s = newDb
	return nil
}

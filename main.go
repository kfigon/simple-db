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
	fmt.Println("or type <sql statement> to execute")

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
			return
		} else if ok, fileName := hasPrefixAndTrim(s, "dump "); ok {
			if err = dumpDbToFile(storage, fileName); err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("db wrote to %q\n", fileName)
			}
		} else if ok, fileName := hasPrefixAndTrim(s, "load "); ok {
			if err = loadFile(&storage, fileName); err != nil {
				fmt.Printf("failed to load file: %q. Current db is not changed\n", fileName)
			} else {
				fmt.Printf("db refreshed from %q\n", fileName)
			}
		} else {
			if err := handleSql(storage, s); err != nil {
				fmt.Println("error:", err)
			}
		}
	}
}

func handleSql(storage *naive.Storage, s string) error {
	stmt, err := sql.Parse(sql.Lex(s))
	if err != nil {
		return fmt.Errorf("error parsing sql: %w", err)
	}

	switch stmt := stmt.(type) {
	case *sql.CreateStatement:
		if err = storage.CreateTable(*stmt); err != nil {
			return fmt.Errorf("statement error: %w", err)
		}
	case *sql.InsertStatement:
		if err = storage.Insert(*stmt); err != nil {
			return fmt.Errorf("statement error: %w", err)
		}
	case *sql.SelectStatement:
		res, err := storage.Select(*stmt)
		if err != nil {
			return fmt.Errorf("statement error: %w", err)
		}
		fmt.Println(fmtQueryRes(res))
	default:
		return fmt.Errorf("invalid statement for sql: %q", s)
	}
	return nil
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

func hasPrefixAndTrim(s string, prefix string) (bool, string) {
	if strings.HasPrefix(s, prefix) {
		return true, strings.TrimPrefix(s, prefix)
	}
	return false, s
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

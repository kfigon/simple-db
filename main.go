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
	fmt.Println("'dump_db <filename>' to dump current db to <filename>")
	fmt.Println("'load_db <filename>' to load <filename> to db")
	fmt.Println("'schema' - to print tables and schema")
	fmt.Println("'load_sql <filename>' - execute sql file, statements separated by newlines")
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
		} else if ok, fileName := hasPrefixAndTrim(s, "dump_db "); ok {
			if err = dumpDbToFile(storage, fileName); err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("db wrote to %q\n", fileName)
			}
		} else if ok, fileName := hasPrefixAndTrim(s, "load_db "); ok {
			if err = loadFile(storage, fileName); err != nil {
				fmt.Printf("failed to load file: %q. Current db is not changed\n", fileName)
			} else {
				fmt.Printf("db refreshed from %q\n", fileName)
			}
		} else if ok, _ := hasPrefixAndTrim(s, "schema"); ok {
			schema := storage.AllSchema()
			fmt.Println()
			if len(schema) == 0 {
				fmt.Println("empty schema")
				continue
			}
			for tableName, tableSchema := range schema{
				fmt.Println(tableName+":")
				q := schemaToQuery(tableSchema)
				fmt.Println(fmtQueryRes(q))
				fmt.Println()
			}
		} else if ok, fileName := hasPrefixAndTrim(s, "load_sql "); ok {
			if err = executeCommandsFromFile(storage, fileName); err != nil {
				fmt.Println("error while processing file: ", err)
			} else {
				fmt.Println("file processed")
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
		fmt.Println("created table", stmt.Table)
	case *sql.InsertStatement:
		if err = storage.Insert(*stmt); err != nil {
			return fmt.Errorf("statement error: %w", err)
		}
		fmt.Println("inserted into table", stmt.Table)
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
	header := make([]string, 0, len(r.Header))
	for _, v := range r.Header {
		header = append(header, string(v))
	}
	
	var out strings.Builder
	out.WriteString(strings.Join(header, "\t"))
	out.WriteString("\n")
	out.WriteString("------------------")
	out.WriteString("\n")
	for _, v := range r.Values {
		out.WriteString(strings.Join(v, "\t"))
		out.WriteString("\n")
	}
	out.WriteString(fmt.Sprintf("%d lines found", len(r.Values)))
	return out.String()
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

func executeCommandsFromFile(s *naive.Storage, fileName string) error {
	f, err := os.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("error reading the file %v: %w", fileName, err)
	}
	data := strings.Replace(string(f), "\r", "", -1)
	for _, line := range strings.Split(data, "\n") {
		if err := handleSql(s, line); err != nil {
			return err
		}
	}
	return nil
}

func loadFile(s *naive.Storage, fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("error reading the file: %w", err)
	}
	defer f.Close()
	newDb, err := naive.DeserializeDb(f)
	if err != nil {
		return fmt.Errorf("error deserializing the file %q: %w", fileName, err)
	}

	*s = *newDb
	return nil
}

func schemaToQuery(sch naive.TableSchema) naive.QueryResult {
	columns := [][]string{}
	for fieldName, fieldType := range sch {
		columns = append(columns, []string{ string(fieldName), fieldType.String() })
	}
	return naive.QueryResult{
		Header: []naive.FieldName{"column name", "column type"},
		Values: columns,
	}
}
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
	header := SerializeInt(int32(len(s)))
	return append(header, []byte(s)...)
}

func DeserializeBool(b []byte) (bool, error) {
	switch {
	case len(b) < 1: return false, deserializationErr(len(b), 1, "bool")
	case b[0] == 0: return false, nil
	case b[0] == 1: return true, nil
	default: return false, fmt.Errorf("invalid boolean, expected 0 or 1, got %v", b[0])
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
	exp := 4
	got := len(b)
	if got < exp {
		return "", deserializationErr(got, exp, "string header")
	}
	howMany, err := DeserializeInt(b[:4])
	if err != nil {
		return "", fmt.Errorf("error deserializing string length: %w", err)
	}
	return string(b[4:4+howMany]), nil
}

func deserializationErr(got, exp int, typ string) error {
	return fmt.Errorf("cant deserialize into %v, expected lenght %v, got %v", typ, exp, got)
}

// type PageType int32
// const (
// 	RootPage PageType = iota
// 	DataPage
// 	SchemaPage
// )

// type PageID int32
// type PageOffset int32

// type Page struct {
// 	Header struct {
// 		PageTyp PageType
// 		NextPage PageID
// 		Size int32
// 	}
// 	PageData []byte
// }

func DeserializeStringAndEat(b *[]byte) (string, error) {
	v, err := DeserializeString(*b)
	if err != nil {
		return "", err
	}
	advanceBuf(b, 4 + len(v))
	return v, nil
}

func DeserializeIntAndEat(b *[]byte) (int, error) {
	v, err := DeserializeInt(*b)
	if err != nil {
		return 0, err
	}
	advanceBuf(b, 4)
	return int(v), nil
}

func advanceBuf(b *[]byte, howMany int) {
	*b = (*b)[howMany:]
}


func SerializeSchema(s Schema) []byte {
	type catalogHeader struct {
		numberOfTables int32
	}

	type schemaHeader struct {
		name TableName
		numberOfColumns int32
		columnMetadata []struct {
			typ FieldType
			name FieldName
		}
	}

	var buf bytes.Buffer
	buf.Write(SerializeInt(int32(len(s))))
	for table, data := range s {
		buf.Write(SerializeString(string(table)))
		buf.Write(SerializeInt(int32(len(data))))
		for name,typ := range data {
			buf.Write(SerializeInt(int32(typ)))
			buf.Write(SerializeString(string(name)))
		}
	}
	return buf.Bytes()
}

// todo: use scanner later
func DeserializeSchema(r io.Reader) (Schema, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	tableCount, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read num of tables: %w", err)
	}

	schema := Schema{}
	for i := range tableCount {
		name, err := DeserializeStringAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read %v table name: %w", i, err)
		}
		
		columnCount, err := DeserializeIntAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read num of columns for table %v: %w", name, err)
		}

		tab := TableSchema{}
		for j := range columnCount {
			typ, err := DeserializeIntAndEat(&bytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read column typ %v for %v: %w", j, name, err)
			}

			colName, err := DeserializeStringAndEat(&bytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read column name %v for %v: %w", j, name, err)
			}
			tab[FieldName(colName)] = FieldType(typ)
		}
		schema[TableName(name)] = tab
	}

	return schema, nil
}

func SerializeData(d Data) []byte {
	// array like that
	type dataHeader struct {
		name TableName
		numberOfRows int32
		rowData []struct {
			name FieldName
			value any
		}
	}

	var buf bytes.Buffer
	for tableName, rows := range d {
		buf.Write(SerializeString(string(tableName)))
		buf.Write(SerializeInt(int32(len(rows))))
		for _, row := range rows {
			for fieldName, val := range row {
				buf.Write(SerializeString(string(fieldName)))				
				var serializedColumn []byte
				switch val.Typ {
				case Int32: 
					serializedColumn = SerializeInt(int32(val.Data.(int)))
				case String:
					serializedColumn = SerializeString(val.Data.(string))
				case Boolean:
					serializedColumn = SerializeBool(val.Data.(bool))
				case Float:
					// todo: impl float
				default: panic(fmt.Sprintf("unknown column typ %v", val.Typ)) // data corruption, fail now
				}
				buf.Write(serializedColumn)
			}
		}
	}

	return buf.Bytes()
}

func DeserializeData(r io.Reader, s Schema) (Data, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read row data: %w", err)
	}
	
	d := Data{}
	for i := range len(s) {
		tableName, err := DeserializeStringAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read table data for %v table: %w", i, err)
		}
		schema, ok := s[TableName(tableName)]
		if !ok {
			return nil, fmt.Errorf("unknown table %v", tableName)
		}

		numRows, err := DeserializeIntAndEat(&bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read num of rows for %v table: %w", tableName, err)
		}
		tableData := make([]TableData, 0, numRows)
		for range numRows {
			td := TableData{}

			for k := range len(schema) {
				columnName, err := DeserializeStringAndEat(&bytes)
				if err != nil {
					return nil, fmt.Errorf("failed to read %v column name for %v table: %w", k, tableName, err)
				}

				typ, ok := schema[FieldName(columnName)]
				if !ok {
					return nil, fmt.Errorf("unknown column %v for table %v", columnName, tableName)
				}
			
				var v any
				switch typ {
				case Int32: 
					if v, err = DeserializeIntAndEat(&bytes); err != nil {
						return nil, fmt.Errorf("failed to read %v for %v: %w", columnName, tableName, err)
					}
				case String:
					if v, err = DeserializeStringAndEat(&bytes); err != nil {
						return nil, fmt.Errorf("failed to read %v for %v: %w", columnName, tableName, err)
					}
				case Boolean:
					// todo: impl bool
				case Float:
					// todo: impl float
				default: panic(fmt.Sprintf("unknown column typ %v in table %v", typ, tableName)) // data corruption, fail now
				}
	
				td[FieldName(columnName)] = ColumnData{
					Typ: typ,
					Data: v,
				}
			}

			tableData = append(tableData, td)
		}

		d[TableName(tableName)] = tableData
	}

	return d, nil
}

func SerializeDb(s Storage) []byte {

}

func DeserializeDb(r io.Reader) (*Storage, error) {

}
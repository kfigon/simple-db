package naive

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
)

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

func SerializeSchema(s Schema) []byte {
	type catalogHeader struct {
		numberOfTables int32
	}

	type schemaHeader struct {
		name            TableName
		numberOfColumns int32
		columnMetadata  []struct {
			typ  FieldType
			name FieldName
		}
	}

	var buf bytes.Buffer
	buf.Write(SerializeInt(int32(len(s))))
	for table, data := range s {
		buf.Write(SerializeString(string(table)))
		buf.Write(SerializeInt(int32(len(data))))
		for name, typ := range data {
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
		name         TableName
		numberOfRows int32
		rowData      []struct {
			name  FieldName
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
					serializedColumn = SerializeInt(val.Data.(int32))
				case String:
					serializedColumn = SerializeString(val.Data.(string))
				case Boolean:
					serializedColumn = SerializeBool(val.Data.(bool))
				case Float:
					// todo: impl float
				default:
					panic(fmt.Sprintf("unknown column typ %v", val.Typ)) // data corruption, fail now
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
				default:
					panic(fmt.Sprintf("unknown column typ %v in table %v", typ, tableName)) // data corruption, fail now
				}

				td[FieldName(columnName)] = ColumnData{
					Typ:  typ,
					Data: v,
				}
			}

			tableData = append(tableData, td)
		}

		d[TableName(tableName)] = tableData
	}

	return d, nil
}

func SerializeDb(s *Storage) []byte {
	serializedSchema := SerializeSchema(s.SchemaMetadata)
	serializedData := SerializeData(s.AllData)

	return SerializeAll(SerializeInt(int32(len(serializedSchema))),
		SerializeInt(int32(len(serializedData))),
		serializedSchema,
		serializedData)
}

func DeserializeDb(r io.Reader) (*Storage, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	schemaLen, err := DeserializeIntAndEat(&data)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema length: %w", err)
	}

	_, err = DeserializeIntAndEat(&data)
	if err != nil {
		return nil, fmt.Errorf("failed to read data length: %w", err)
	}

	schema, err := DeserializeSchema(bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	// NewBuffer takes ownership of the byte array. Here Im reusing it, so split it here
	dataContent, err := DeserializeData(bytes.NewBuffer(data[schemaLen:]), schema)
	if err != nil {
		return nil, err
	}

	return &Storage{
		SchemaMetadata: schema,
		AllData:        dataContent,
	}, nil
}

type RootPage struct {
	MagicNumber     int32
	PageSize        int32
	SchemaStartPage PageID
	DataPageStart   PageID
}

const MagicNumber = 0xdeadc0de

func (r *RootPage) Serialize() []byte {
	return SerializeAll(
		SerializeInt(r.MagicNumber),
		SerializeInt(r.PageSize),
		SerializeInt(int32(r.SchemaStartPage)),
		SerializeInt(int32(r.DataPageStart)))
}

func DeserializeRootPage(r io.Reader) (*RootPage, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("error reading root page: %w", err)
	}
	magicNum, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing magic num: %w", err)
	}

	if int(magicNum) != MagicNumber {
		return nil, fmt.Errorf("invalid magic num, got: %x", magicNum)
	}

	pageSize, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing page size: %w", err)
	}

	schemaStart, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing schema start: %w", err)
	}

	dataStart, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing data start: %w", err)
	}
	return &RootPage{
		MagicNumber:     int32(magicNum),
		PageSize:        int32(pageSize),
		SchemaStartPage: PageID(schemaStart),
		DataPageStart:   PageID(dataStart),
	}, nil
}

type PageType int32

const (
	RootPageType PageType = iota
	DataPageType
	SchemaPageType
)

type PageID int32
type PageOffset int32

type GenericPageHeader struct {
	PageTyp       PageType
	NextPage      PageID
	SlotArraySize int32 // might be too big, leave for now
}

type GenericPage struct {
	Header    GenericPageHeader
	SlotArray *Slotted
}

func NewPage(pageType PageType, pageSize int) *GenericPage {
	return &GenericPage{
		Header: GenericPageHeader{
			PageTyp: pageType,
		},
		SlotArray: NewSlotted(pageSize - 4 - 4 - 4), //12 bytes for header
	}
}

func (g *GenericPage) Add(b []byte) (SlotIdx, error) {
	r, err := g.SlotArray.Add(b)
	if err != nil {
		return 0, err
	}
	g.Header.SlotArraySize = int32(len(g.SlotArray.Indexes))
	return r, nil
}

func (g *GenericPage) Read(r SlotIdx) ([]byte, error) {
	return g.SlotArray.Read(r)
}

func (g *GenericPage) Put(r SlotIdx, b []byte) error {
	if err := g.SlotArray.Put(r, b); err != nil {
		return err
	}
	g.Header.SlotArraySize = int32(len(g.SlotArray.Indexes))
	return nil
}

func (g *GenericPage) Serialize() []byte {
	return SerializeAll(
		SerializeInt(int32(g.Header.PageTyp)),
		SerializeInt(int32(g.Header.NextPage)),
		SerializeInt(int32(g.Header.SlotArraySize)),
		g.SlotArray.Serialize())
}

func Deserialize(r io.Reader) (*GenericPage, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	pageType, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read page type: %w", err)
	}

	nextPage, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read next page id: %w", err)
	}

	slotArraySize, err := DeserializeIntAndEat(&bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read slot array size: %w", err)
	}

	slotted, err := DeserializeSlotted(bytes, int(slotArraySize))
	if err != nil {
		return nil, err
	}

	return &GenericPage{
		Header: GenericPageHeader{
			PageTyp:       PageType(pageType),
			NextPage:      PageID(nextPage),
			SlotArraySize: int32(slotArraySize),
		},
		SlotArray: slotted,
	}, nil
}

type PageIterator iter.Seq2[PageID, *GenericPage]

func NewPageIterator(storage *Storage, pageType PageType) PageIterator {
	var currentPageId PageID
	if pageType == DataPageType {
		currentPageId = storage.root.DataPageStart
	} else if pageType == SchemaPageType {
		currentPageId = storage.root.SchemaStartPage
	} // else: currentPageId == 0 -> empty iter

	return func(yield func(PageID, *GenericPage) bool) {
		for currentPageId != 0 {
			currentPage := &storage.allPages[currentPageId]
			if !yield(currentPageId, currentPage) {
				break
			}
			currentPageId = currentPage.Header.NextPage
		}
	}
}

type PageTupleIterator iter.Seq[[]byte]

func NewTupleIterator(storage *Storage, pageType PageType) PageTupleIterator {
	return func(yield func([]byte) bool) {
		for _, thisPage := range NewPageIterator(storage, pageType) {
			for tuple := range thisPage.SlotArray.Iterator() {
				if !yield(tuple) {
					return
				}
			}
		}
	}
}

type DirectoryTuple struct {
	PageTyp PageType
	StartingPage PageID
	Name string
}

func (d DirectoryTuple) Serialize() []byte {
	return SerializeAll(
		SerializeInt(int32(d.PageTyp)),
		SerializeInt(int32(d.StartingPage)),
		SerializeString(d.Name))
}

func DeserializeDirectoryTuple(b []byte) (*DirectoryTuple, error) {
	pageType, err := DeserializeIntAndEat(&b)
	if err != nil {
		return nil, fmt.Errorf("error deserializing page type: %w", err)
	}

	startingPage, err := DeserializeIntAndEat(&b)
	if err != nil {
		return nil, fmt.Errorf("error deserializing starting page id: %w", err)
	}

	name, err := DeserializeStringAndEat(&b)
	if err != nil {
		return nil, fmt.Errorf("error deserializing name: %w", err)
	}
	return &DirectoryTuple{
		PageTyp: PageType(pageType),
		StartingPage: PageID(startingPage),
		Name: name,
	}, nil
}

type SchemaTuple struct {
	TableNameV TableName
	FieldNameV FieldName
	FieldTypeV FieldType
}

func (s SchemaTuple) Serialize() []byte {
	return SerializeAll(
		SerializeString(string(s.TableNameV)),
		SerializeString(string(s.FieldNameV)),
		SerializeInt(int32(s.FieldTypeV)))
}

func DeserializeSchemaTuple(b []byte) (*SchemaTuple, error) {
	tableName, err := DeserializeStringAndEat(&b)
	if err != nil {
		return nil, fmt.Errorf("error deserializing table name: %w", err)
	}

	fieldName, err := DeserializeStringAndEat(&b)
	if err != nil {
		return nil, fmt.Errorf("error deserializing field name: %w", err)
	}

	fieldType, err := DeserializeIntAndEat(&b)
	if err != nil {
		return nil, fmt.Errorf("error deserializing field type: %w", err)
	}

	return &SchemaTuple{
		TableNameV: TableName(tableName),
		FieldNameV: FieldName(fieldName),
		FieldTypeV: FieldType(fieldType),
	}, nil
}
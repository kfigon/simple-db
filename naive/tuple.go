package naive

import (
	"bytes"
	"fmt"
	"io"
)

type ColumnType int32

const (
	// fixed size
	NullField    ColumnType = iota // size 0
	BooleanField                   // size 1
	IntField                       // size 4

	// var size
	StringField   // size 4 + len
	OverflowField // size 4 + 4 (PageID)
)

func ColumnTypeFromInt(i int32) (ColumnType, error) {
	switch i {
	case 0:
		return NullField, nil
	case 1:
		return BooleanField, nil
	case 2:
		return IntField, nil
	case 3:
		return StringField, nil
	case 4:
		return OverflowField, nil
	default:
		return 0, fmt.Errorf("invalid column type: %d", i)
	}
}

type Tuple struct {
	NumberOfFields int32
	ColumnTypes    []ColumnType
	ColumnDatas    [][]byte
}

func (t Tuple) Serialize() []byte {
	return SerializeStruct(&t,
		WithInt(func(t *Tuple) int32 { return t.NumberOfFields }),
		func(t *Tuple, b *bytes.Buffer) {
			for _, v := range t.ColumnTypes {
				b.Write(SerializeInt(int32(v)))
			}
		},
		func(t *Tuple, b *bytes.Buffer) {
			for _, v := range t.ColumnDatas {
				b.Write(v)
			}
		},
	)
}

func DeserializeTuple(b []byte) (*Tuple, error) {
	return DeserializeStruct[Tuple](bytes.NewBuffer(b),
		DeserWithInt("NumberOfFields", func(t *Tuple, i *int32) { t.NumberOfFields = *i }),
		func(t *Tuple, r io.Reader) error {
			for i := range t.NumberOfFields {
				iVal, err := ReadInt(r)
				if err != nil {
					return fmt.Errorf("error deserializing tuple, column %d/%d: %w", i, t.NumberOfFields, err)
				}
				v, err := ColumnTypeFromInt(iVal)
				if err != nil {
					return fmt.Errorf("error deserializing tuple, column %d/%d: %w", i, t.NumberOfFields, err)
				}
				t.ColumnTypes = append(t.ColumnTypes, v)
			}
			return nil
		},
		func(t *Tuple, r io.Reader) error {
			for i := range t.NumberOfFields {
				typ := t.ColumnTypes[i]

				// todo: inefficient, but with data validation
				switch typ {
				case NullField:
					t.ColumnDatas = append(t.ColumnDatas, nil)
				case BooleanField:
					b, err := ReadBool(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, bool column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, SerializeBool(b))
				case IntField:
					v, err := ReadInt(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, int column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, SerializeInt(v))
				case StringField:
					v, err := ReadString(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, string column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, SerializeString(v))
				case OverflowField:
					ln, err := ReadInt(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, overflow length column %d/%d: %w", i, t.NumberOfFields, err)
					}
					pageID, err := ReadInt(r)
					if err != nil {
						return fmt.Errorf("error deserializing tuple, overflow pageID column %d/%d: %w", i, t.NumberOfFields, err)
					}
					t.ColumnDatas = append(t.ColumnDatas, append(SerializeInt(ln), SerializeInt(pageID)...))
				default:
					return fmt.Errorf("error deserializing tuple, column %d/%d: unknown column type %d", i, t.NumberOfFields, typ)
				}
			}
			return nil
		},
	)
}

// based on sqlite. Remove directory pages, replace shcema with this
type SchemaTuple struct {
	PageTyp        PageType // what's the type of data described by schema	- data, index, etc
	StartingPageID PageID
	Name           string
	SqlStatement   string // sql stmt used to create this. Will be parsed on boot and cached
}

func (s SchemaTuple) ToTuple() Tuple {
	return Tuple{
		NumberOfFields: 4,
		ColumnTypes: []ColumnType{
			IntField, IntField, StringField, StringField,
		},
		ColumnDatas: [][]byte{
			SerializeInt(int32(s.PageTyp)),
			SerializeInt(int32(s.StartingPageID)),
			SerializeString(s.Name),
			SerializeString(s.SqlStatement),
		},
	}
}

func SchemaTupleFromTuple(t Tuple) (*SchemaTuple, error) {
	if t.NumberOfFields != 4 {
		return nil, fmt.Errorf("invalid number of fields for schema tuple, got %d", t.NumberOfFields)
	}

	// todo: more elaborated validation of tuple...

	pageTyp, err := ReadInt(bytes.NewBuffer(t.ColumnDatas[0]))
	if err != nil {
		return nil, fmt.Errorf("error deserializing schema tuple, page type: %w", err)
	}
	startPage, err := ReadInt(bytes.NewBuffer(t.ColumnDatas[1]))
	if err != nil {
		return nil, fmt.Errorf("error deserializing schema tuple, starting page: %w", err)
	}
	name, err := ReadString(bytes.NewBuffer(t.ColumnDatas[2]))
	if err != nil {
		return nil, fmt.Errorf("error deserializing schema tuple, name field: %w", err)
	}
	stmt, err := ReadString(bytes.NewBuffer(t.ColumnDatas[3]))
	if err != nil {
		return nil, fmt.Errorf("error deserializing schema tuple, stmt field: %w", err)
	}
	return &SchemaTuple{
		PageTyp:        PageType(pageTyp),
		StartingPageID: PageID(startPage),
		Name:           name,
		SqlStatement:   stmt,
	}, nil
}

package naive

import (
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAlgebraSelect(t *testing.T) {
	testCases := []struct {
		desc	string
		input []Row
		expected []Row
		filter func(Row)bool
	}{
		{
			desc: "no filter",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
				{"foo": col(t, 2), "bar": col(t, "two")},
			},
			expected: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
				{"foo": col(t, 2), "bar": col(t, "two")},
			},
			filter: func(r Row) bool {return true},
		},
		{
			desc: "filter everything",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
				{"foo": col(t, 2), "bar": col(t, "two")},
			},
			expected: nil,
			filter: func(r Row) bool {return false},
		},
		{
			desc: "filter single condition",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
				{"foo": col(t, 2), "bar": col(t, "two")},
				{"foo": col(t, 3), "bar": col(t, "three")},
			},
			expected: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
			},
			filter: func(r Row) bool {return r["foo"].Data == 1},
		},
		{
			desc: "multiple conditions",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
				{"foo": col(t, 2), "bar": col(t, "two")},
				{"foo": col(t, 3), "bar": col(t, "three")},
			},
			expected: []Row{
				{"foo": col(t,1), "bar": col(t, "one")},
				{"foo": col(t, 2), "bar": col(t, "two")},
			},
			filter: func(r Row) bool {return r["foo"].Data == 1 || r["bar"].Data == "two"},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got := Select(RowIter(slices.Values(tC.input)), tC.filter)	
			result := slices.Collect(iter.Seq[Row](got))

			assert.Equal(t, tC.expected, result)
		})
	}
}

func col[T int | string | bool](t *testing.T, v T) ColumnData {
	t.Helper()
	switch a := any(v).(type) {
	case int: return ColumnData{Int32, a}
	case string: return ColumnData{String, a}
	case bool: return ColumnData{Boolean, a}
	default:
		assert.Fail(t, "insupported type %T", v)
	}
	return ColumnData{}
}

func TestAlgebraProject(t *testing.T) {
	testCases := []struct {
		desc	string
		input []Row
		expected []Row
		projection []FieldName
	}{
		{
			desc: "no projection",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			expected: []Row{ {}, {}, {}, },
			projection: nil,
		},
		{
			desc: "select single row",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			expected: []Row{
				{"bar": col(t, "one")},
				{"bar": col(t, "two")},
				{"bar": col(t, "three")},
			},
			projection: []FieldName{"bar"},
		},
		{
			desc: "select all",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			expected: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			projection: []FieldName{"bar", "foo", "xax"},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got := Project(RowIter(slices.Values(tC.input)), tC.projection)	
			result := slices.Collect(iter.Seq[Row](got))

			assert.Equal(t, tC.expected, result)
		})
	}
}
func TestAlgebraProduct(t *testing.T) {
	testCases := []struct {
		desc	string
		input []Row
		input2 []Row
		expected []Row
	}{
		{
			desc: "nothing in second",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			input2: []Row{},
			expected: []Row{},
		},
		{
			desc: "nothing in first",
			input2: []Row{},
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			expected: []Row{},
		},
		{
			desc: "combine both",
			input: []Row{
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
			},
			input2: []Row{
				{"data": col(t, 123)},
				{"data": col(t, 456)},
			},
			expected: []Row{	
				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true), "data": col(t, 123)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false), "data": col(t, 123)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true), "data": col(t, 123)},

				{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true), "data": col(t, 456)},
				{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false), "data": col(t, 456)},
				{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true), "data": col(t, 456)},
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got := Product(RowIter(slices.Values(tC.input)), RowIter(slices.Values(tC.input2)))
			result := slices.Collect(iter.Seq[Row](got))

			assert.ElementsMatch(t, tC.expected, result)
		})
	}
}

func TestAlgebraCombined(t *testing.T) {
	t.Run("filter and project", func(t *testing.T) {
		input := RowIter(slices.Values([]Row{
			{"foo": col(t,1), "bar": col(t, "one"), "xax": col(t, true)},
			{"foo": col(t, 2), "bar": col(t, "two"), "xax": col(t, false)},
			{"foo": col(t, 3), "bar": col(t, "three"), "xax": col(t, true)},
		}))

		got := Project(
			Select(input, func(r Row) bool { return r["foo"].Data == 2 }), 
			[]FieldName{"foo"},
		)

		expected := []Row{{"foo": col(t,2)}}
		assert.Equal(t, expected, slices.Collect(iter.Seq[Row](got)))
	})
}
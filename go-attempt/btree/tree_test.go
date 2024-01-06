package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDebugTree(t *testing.T) {
	// todo: remove when legit inserting works
	bt := NewBtree(3)

	bt.root = &node{[]int{5}, false, []*node{
		{
			[]int{3}, false, []*node{
				{[]int{1,2}, true, nil}, {[]int{4}, true, nil},
			},
		},
		{
			[]int{7,11}, false, []*node{
				{[]int{6}, true, nil}, {[]int{9}, true, nil}, {[]int{14, 18}, true, nil},
			},
		},
	}}

	expected := `[5]
	[3]
		L:[1,2]
		L:[4]
	[7,11]
		L:[6]
		L:[9]
		L:[14,18]`
	assert.Equal(t, bt.String(), expected)
}

func TestInserting(t *testing.T) {
	tdt := []struct {
		name string
		input []int
		expected string
	} {
		{
			"simple 1", 
			[]int{3,5,8,1,2,6,9,4,7,11,10},
`[8]
	[3 5]
		L:[1,2]
		L:[4]
		L:[6,7]
	[11]
		L:[9,10]
		L:[12]`,
		},
		{
			"simple 2",
			[]int{3, 5, 1, 2, 6, 4, 7, 18, 9, 11, 14},
`[5]
	[3]
		L:[1,2]
		L:[4]
	[7,11]
		L:[6]
		L:[9]
		L:[14,18]`,
		},
		{
			"single entries",
			[]int{3,1},
			`L:[1,3]`,
		},
		{
			"single split",
			[]int{3,5,1},
`[3]
	L:[1]
	L:[5]`,
		},
	}

	for _, tc := range tdt {
		t.Run(tc.name, func(t *testing.T) {
			bt := NewBtree(3)
			for _, v := range tc.input {
				bt.insert(v)
			}
			
			assert.Equal(t, tc.expected, bt.String())
		})
	}
}
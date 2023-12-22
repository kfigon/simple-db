package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDebugTree(t *testing.T) {
	// todo: do legit inserts, rework to table test, add more

	bt := NewBtree(3)
	// for _, v := range []int{3, 5, 1, 2, 6, 4, 7, 18, 9, 11, 14} {
	// 	bt.insert(v)
	// }

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
	assert.Equal(t, bt.asStr(), expected)
}
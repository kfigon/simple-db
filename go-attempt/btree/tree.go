package btree

import (
	"slices"
	"strconv"
	"strings"
)

// [K cmp.Ordered, V any]
type node struct{
	keys []int
	
	isLeaf bool
	children []*node
}


type BTree struct{
	order int // number of children
	root *node
}

func NewBtree(order int) *BTree {
	return &BTree {
		order: order,
	}
}

func (b *BTree)insert(key int) {
	if b.root == nil {
		b.root = &node{
			keys:      nil,
			isLeaf:    true,
			children:  []*node{},
		}
	}
	
	if len(b.root.keys) < (b.order-1) {
		b.root.keys = append(b.root.keys, key)
		slices.Sort(b.root.keys)
		return
	}

	nextNode := b.root
	for nextNode != nil {
		break
	}
}

func (b *BTree)search(key int) (int, bool) {
	return 0, false
}

func (b *BTree)delete(key int) bool {
	return false
}

func (b *BTree) asStr() string {
	out := strings.Builder{}
	
	var fn func(*node,int)
	fn = func(n *node, depth int) {
		writeLine := func(s string) {
			// ignore length, always nil err
			if out.Len() != 0 {
				out.WriteString("\n")
			}

			for i := 0; i < depth; i++ {
				out.WriteString("\t")
			}
			out.WriteString(s)
		}

		if n == nil {
			return
		}

		keys := make([]string, 0, len(n.keys))
		for _, k := range n.keys {
			keys = append(keys, strconv.Itoa(k))
		}

		if len(keys) != 0 {
			s := "[" + strings.Join(keys, ",") + "]"
			if n.isLeaf {
				writeLine("L:" +s)
			} else {
				writeLine(s)
			}
		}

		for _, child := range n.children {
			fn(child, depth+1)
		}
	}
	
	fn(b.root, 0)
	return out.String()
}
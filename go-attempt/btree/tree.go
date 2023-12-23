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
		root: &node{
			keys:      nil,
			isLeaf:    true,
			children:  nil,
		},
	}
}

// https://iq.opengenus.org/b-tree-in-python/
// https://gist.github.com/mateor/885eb950df7231f178a5
// https://algs4.cs.princeton.edu/code/edu/princeton/cs/algs4/BTree.java.html
func (b *BTree)insert(key int) {	
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

func (b *BTree) split(parent *node, fullChildIdx int) {
	// todo: not finished, check ranges
	
	fullChild := parent.children[fullChildIdx]

	newNode := &node{ isLeaf: fullChild.isLeaf }
	// todo: sort both
	parent.children = append(parent.children, newNode)

	// insert median
	parent.keys = append(parent.keys, fullChild.keys[len(fullChild.keys)/2])

	newNode.keys = fullChild.keys[1+len(fullChild.keys)/2:]
	fullChild.keys = fullChild.keys[:len(fullChild.keys)/2]

	if !fullChild.isLeaf {
		newNode.children = fullChild.children[1+len(fullChild.children)/2:]
		fullChild.children = fullChild.children[:len(fullChild.children)/2]
	}
}

func (b *BTree)search(key int) (int, bool) {
	var fn func(*node) (int, bool)
	fn = func(n *node) (int, bool) {
		i := 0
		for i < len(n.keys) && key > n.keys[i] {
			i += 1
		}
		if i < len(n.keys) && key == n.keys[i] {
			return n.keys[i], true
		} else if n.isLeaf {
			return 0, false
		}
		return fn(b.root.children[i])
	}
	return fn(b.root)
}

func (b *BTree)delete(key int) bool {
	panic("todo")
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

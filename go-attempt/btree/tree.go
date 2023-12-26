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

func (b *BTree) full(n *node) bool {
	return len(n.keys) >= b.order-1
}

// https://iq.opengenus.org/b-tree-in-python/
// https://gist.github.com/mateor/885eb950df7231f178a5
// https://algs4.cs.princeton.edu/code/edu/princeton/cs/algs4/BTree.java.html

// https://www.youtube.com/watch?v=tT2DT9Z4H-0&list=PL9xmBV_5YoZNFPPv98DjTdD9X6UI9KMHz&index=5
func (b *BTree)insert(key int) {
	if b.full(b.root) {
		oldRoot := b.root
		b.root = &node{}
		b.root.children = append(b.root.children, oldRoot)
		b.split(b.root, 0)
		b.insertNonFull(b.root, key)
		return
	}
	b.insertNonFull(b.root, key)
	
}

func (b *BTree) insertNonFull(n *node, key int) {
	if n.isLeaf {
		n.keys = append(n.keys, key)
		slices.Sort(n.keys)
		return
	}

	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}
	if b.full(n) {
		b.split(n, i)
		if key > n.keys[i] {
			i++
		}
	}
	b.insertNonFull(n.children[i], key)
}

func (b *BTree) split(parent *node, fullChildIdx int) {
	// todo: not finished, check ranges
	fullChild := parent.children[fullChildIdx]
	medianId := len(fullChild.keys)/2


	newNode := &node{ isLeaf: fullChild.isLeaf }
	// todo: sort both
	parent.children = append(parent.children, newNode)

	// insert median
	parent.keys = append(parent.keys, fullChild.keys[medianId])

	newNode.keys = fullChild.keys[1+medianId:]
	fullChild.keys = fullChild.keys[:medianId]

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
			i++
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

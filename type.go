package skiplist

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"
)

type elementNode struct {
	list *SkipList
	next []unsafe.Pointer
}

func (n *elementNode) Next() *Element {
	return n.NextAt(0)
}

func (n *elementNode) NextAt(i int) *Element {
	return (*Element)(atomic.LoadPointer(&n.next[i]))
}

type Element struct {
	elementNode
	key   []byte
	value interface{}
}

// Key allows retrieval of the key for a given Element
func (e *Element) Key() []byte {
	return e.key
}

// Value allows retrieval of the value for a given Element
func (e *Element) Value() interface{} {
	return e.value
}

// Next returns the following Element or nil if we're at the end of the list.
// Only operates on the bottom level of the skip list (a fully linked list).
func (element *Element) Next() *Element {
	return element.elementNode.Next()
}

type SkipList struct {
	elementNode
	maxLevel       int
	Length         int
	randSource     rand.Source
	probability    float64
	probTable      []float64
	mutex          sync.RWMutex
	prevNodesCache []*elementNode
}

package skiplist

import (
	"bytes"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	DefaultMaxLevel    int     = 18
	DefaultProbability float64 = 1 / math.E
)

// Front returns the head node of the list.
func (list *SkipList) Front() *Element {
	return list.elementNode.Next()
}

// Set inserts a value in the list with the specified key, ordered by the key.
// If the key exists, it updates the value in the existing node.
// Returns a pointer to the new element.
// Locking is optimistic and happens only after searching.
func (list *SkipList) Set(key []byte, value interface{}) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	var element *Element
	prevs := list.getPrevElementNodes(key)

	if element = prevs[0].Next(); element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	element = &Element{
		elementNode: elementNode{
			list: list,
			next: make([]unsafe.Pointer, list.randLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		atomic.StorePointer(&element.next[i], prevs[i].next[i])
		atomic.StorePointer(&prevs[i].next[i], unsafe.Pointer(element))
	}

	list.Length++
	return element
}

// Get finds an element by key. It returns element pointer if found, nil if not found.
// Locking is optimistic and happens only after searching with a fast check for deletion after locking.
func (list *SkipList) Get(key []byte) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	var prev *elementNode = &list.elementNode
	var next *Element

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.NextAt(i)

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.elementNode
			next = next.NextAt(i)
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

// Remove deletes an element from the list.
// Returns removed element pointer if found, nil if not found.
// Locking is optimistic and happens only after searching with a fast check on adjacent nodes after locking.
func (list *SkipList) Remove(key []byte) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	prevs := list.getPrevElementNodes(key)

	// found the element, remove it
	if element := prevs[0].Next(); element != nil && bytes.Compare(element.key, key) <= 0 {
		for k := range element.next {
			atomic.StorePointer(&prevs[k].next[k], atomic.LoadPointer(&element.next[k]))
		}

		list.Length--
		return element
	}

	return nil
}

// getPrevElementNodes is the private search mechanism that other functions use.
// Finds the previous nodes on each level relative to the current Element and
// caches them. This approach is similar to a "search finger" as described by Pugh:
// http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.17.524
func (list *SkipList) getPrevElementNodes(key []byte) []*elementNode {
	var prev *elementNode = &list.elementNode
	var next *Element

	prevs := list.prevNodesCache

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.NextAt(i)

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.elementNode
			next = next.NextAt(i)
		}

		prevs[i] = prev
	}

	return prevs
}

// SetProbability changes the current P value of the list.
// It doesn't alter any existing data, only changes how future insert heights are calculated.
func (list *SkipList) SetProbability(newProbability float64) {
	list.probability = newProbability
	list.probTable = probabilityTable(list.probability, list.maxLevel)
}

func (list *SkipList) randLevel() (level int) {
	// Our random number source only has Int63(), so we have to produce a float64 from it
	// Reference: https://golang.org/src/math/rand/rand.go#L150
	r := float64(list.randSource.Int63()) / (1 << 63)

	level = 1
	for level < list.maxLevel && r < list.probTable[level] {
		level++
	}
	return
}

// probabilityTable calculates in advance the probability of a new node having a given level.
// probability is in [0, 1], MaxLevel is (0, 64]
// Returns a table of floating point probabilities that each level should be included during an insert.
func probabilityTable(probability float64, MaxLevel int) (table []float64) {
	for i := 1; i <= MaxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}

// NewWithMaxLevel creates a new skip list with MaxLevel set to the provided number.
// Returns a pointer to the new list.
func NewWithMaxLevel(maxLevel int) *SkipList {
	if maxLevel < 1 || maxLevel > 64 {
		panic("maxLevel for a SkipList must be a positive integer <= 64")
	}

	return &SkipList{
		elementNode:    elementNode{next: make([]unsafe.Pointer, DefaultMaxLevel)},
		prevNodesCache: make([]*elementNode, DefaultMaxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    DefaultProbability,
		probTable:      probabilityTable(DefaultProbability, DefaultMaxLevel),
	}
}

// New creates a new skip list with default parameters. Returns a pointer to the new list.
func New() *SkipList {
	return NewWithMaxLevel(DefaultMaxLevel)
}

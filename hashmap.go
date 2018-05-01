package hashmap

import (
	"sync"
	"hash"
	"hash/crc64"
)

// HashMap is a thread safe string, string map
type HashMap struct {
	nodes []*node
	hasher sync.Pool
	size uint64

	rwMutex sync.RWMutex
}

// New creates a new HashMap that uses crc64 as hash function. Size contains the size of the lookup table
func New(size uint64) *HashMap {
	return NewHashMap(func() hash.Hash64 { return crc64.New(crc64.MakeTable(crc64.ECMA)) }, size)
}

// NewHashMap lets you define what hash func to use and how big shall the lookup table be
func NewHashMap(hashFn func() hash.Hash64, size uint64) *HashMap {
	hm := &HashMap{
		nodes: make([]*node, size),
		hasher: sync.Pool{
			New:func() interface{} {
				return hashFn()
			},
		},

		size: size,
	}
	for i := range hm.nodes {
		hm.nodes[i] = &node{}
	}
	return hm
}

// getIndex returns the index of the key in the lookup table
func (hm *HashMap) getIndex(key string) uint64 {
	hasher := hm.hasher.Get().(hash.Hash64)
	hasher.Reset()
	hasher.Write([]byte(key))
	index := hasher.Sum64() % hm.size
	hm.hasher.Put(hasher)
	return index
}

// Put puts a new value in the map. It overwrites values of existing keys
func (hm *HashMap) Put(key, value string) {
	index := hm.getIndex(key)
	hm.rwMutex.Lock()
	defer hm.rwMutex.Unlock()
	has, _ := hm.getNode(key)
	if has != nil {
		has.value = value
		return
	}
	hm.nodes[index].append(&node{key: key, value:value})
}

// getNode returns the node that contains the element and it's parent or nil
func (hm *HashMap) getNode(key string) (*node, *node) {
	index := hm.getIndex(key)
	elem := hm.nodes[index]
	for elem.next != nil {
		parent := elem
		elem = elem.next
		if elem.key == key {
			return elem, parent
		}
	}
	return nil, nil
}

// Get returns the value for a given key and true, or "" and false if the key is not in the hash map
func (hm *HashMap) Get(key string) (string, bool) {
	hm.rwMutex.RLock()
	defer hm.rwMutex.RUnlock()
	n, _ := hm.getNode(key)
	if n == nil {
		return "", false
	}
	return n.value, true
}

// Delete deletes the value associated with the key, if it exists
func (hm *HashMap) Delete(key string) {
	hm.rwMutex.Lock()
	defer hm.rwMutex.Unlock()
	n, parent := hm.getNode(key)
	if n == nil {
		return
	}
	parent.next = n.next
}

package main

import "bytes"

const (
	mapSize   = 131072 // 2^17, power of two
	mapMask   = mapSize - 1
	maxLoad   = mapSize / 2 // panic if more than half full, we know there's no more 10k station names
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

type entry struct {
	key   []byte
	value *Aggregation
}

type customMap struct {
	buckets [mapSize]*entry
	size    int
}

func newCustomMap() *customMap {
	return &customMap{}
}

// hash implements FNV-1a hash algorithm
func hash(data []byte) uint64 {
	hash := uint64(fnvOffset)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= uint64(fnvPrime)
	}
	return hash
}

func (m *customMap) set(key []byte, value *Aggregation) {
	bucket := hash(key) & mapMask

	// Linear probing: find the first available slot
	for {
		e := m.buckets[bucket]

		if e == nil {
			// Empty slot, insert here
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			m.buckets[bucket] = &entry{
				key:   keyCopy,
				value: value,
			}
			m.size++
			if m.size >= maxLoad {
				panic("custom map exceeded maximum load factor")
			}
			return
		}

		if bytes.Equal(e.key, key) {
			// Key already exists, update value
			e.value = value
			return
		}

		// Move to next bucket (linear probing)
		bucket++
	}
}

func (m *customMap) get(key []byte) (*Aggregation, bool) {
	bucket := hash(key) & mapMask

	// Linear probing: search for the key
	for {
		entry := m.buckets[bucket]

		if entry == nil {
			// Empty slot means key doesn't exist
			return nil, false
		}

		if bytes.Equal(entry.key, key) {
			return entry.value, true
		}

		// Move to next bucket (linear probing)
		bucket = bucket + 1
	}
}

// forEach iterates over all key-value pairs in the map
func (m *customMap) forEach(fn func(key []byte, value *Aggregation)) {
	for i := range mapSize {
		entry := m.buckets[i]
		if entry != nil {
			fn(entry.key, entry.value)
		}
	}
}

// toMap converts the custom map to a regular Go map
func (m *customMap) toMap() map[string]*Aggregation {
	result := make(map[string]*Aggregation, m.size)
	m.forEach(func(key []byte, value *Aggregation) {
		result[string(key)] = value
	})
	return result
}

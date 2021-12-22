package common

import (
	"hash"
	"hash/fnv"
)

type BloomFilter struct {
	bitField []byte
}

// Creates a new instance of a BloomFilter.
// Accepts the size, in bytes, of the bitfield.
func NewBloomFilter(size uint32) *BloomFilter {
	return &BloomFilter{bitField: make([]byte, size/8+1)}
}

// Insert a new entry into the set of entries.
func (bf *BloomFilter) Insert(bytes []byte) {
	for _, h := range bf.newHashes() {
		h.Write(bytes)
		out := h.Sum32() % (uint32)(len(bf.bitField)*8)
		bf.bitField[out/8] |= (byte)(1 << (out % 8))
	}
}

// Test whether an entry exists within the set of entries. The return value is
// probabilistic and only indicates whether an entry is definitively not present (false)
// or may be present (true).
func (bf *BloomFilter) Test(bytes []byte) bool {
	for _, h := range bf.newHashes() {
		h.Write(bytes)
		out := h.Sum32() % (uint32)(len(bf.bitField)*8)
		if bf.bitField[out/8]&((byte)(1<<(out%8))) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) newHashes() []hash.Hash32 {
	return []hash.Hash32{fnv.New32(), fnv.New32a()}
}

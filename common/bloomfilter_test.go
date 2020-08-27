package common

import "testing"

func TestEmptyBloomFilterDoesNotContainValues(t *testing.T) {
	bf := NewBloomFilter(100)
	if bf.Test([]byte{1, 0, 1}) {
		t.Error("Expected empty bloom filter to not contain an entry, but did")
	}
}

func TestBloomFilterContainsValue(t *testing.T) {
	bf := NewBloomFilter(100)
	bf.Insert([]byte{1, 0, 1})
	if !bf.Test([]byte{1, 0, 1}) {
		t.Error("Expected bloom filter to contain written entry, but did not")
	}
}

func TestBloomFilterDoesNotContainsValue(t *testing.T) {
	bf := NewBloomFilter(100)
	bf.Insert([]byte{1, 0, 1})
	bf.Insert([]byte{0, 1, 0})
	if bf.Test([]byte{1, 1, 1}) {
		t.Error("Expected bloom filter to not contain entry, but did")
	}
}

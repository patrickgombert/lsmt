package cache

import (
	"testing"

	c "github.com/patrickgombert/lsmt/comparator"
)

type sharded struct {
	shard int
}

func (s *sharded) Shard(numShards int) int {
	return s.shard
}

func TestGetNewGeneratedValue(t *testing.T) {
	lru := NewShardedLRUCache(1, 100)
	key := &sharded{shard: 0}
	value, _ := lru.Get(key, staticProvider([]byte{1, 2}))
	if c.Compare([]byte{1, 2}, value) != c.EQUAL {
		t.Errorf("Expected Get() to return %q, but got %q", []byte{1, 2}, value)
	}
}

func TestEvictEmptyCacheNoError(t *testing.T) {
	lru := NewShardedLRUCache(1, 100)
	err := lru.Evict(&sharded{shard: 0})
	if err != nil {
		t.Error("Expected empty cache to Evict() without an error, but did not")
	}
}

func TestEvict(t *testing.T) {
	lru := NewShardedLRUCache(1, 100)
	key := &sharded{shard: 0}
	lru.Get(key, staticProvider([]byte{1, 2}))
	lru.Evict(key)
	value, _ := lru.Get(key, staticProvider([]byte{2, 1}))
	if c.Compare([]byte{2, 1}, value) != c.EQUAL {
		t.Errorf("Expected Get() to return %q, but got %q", []byte{2, 1}, value)
	}
}

func TestReadIgnoresProviderIfPresent(t *testing.T) {
	lru := NewShardedLRUCache(1, 100)
	key := &sharded{shard: 0}
	lru.Get(key, staticProvider([]byte{1, 2}))
	value, _ := lru.Get(key, staticProvider([]byte{2, 1}))
	if c.Compare([]byte{1, 2}, value) != c.EQUAL {
		t.Errorf("Expected Get() to return %q, but got %q", []byte{1, 2}, value)
	}
}

func TestTwoShardsNoEvictions(t *testing.T) {
	lru := NewShardedLRUCache(2, 100)
	key0 := &sharded{shard: 0}
	key1 := &sharded{shard: 1}
	lru.Get(key0, staticProvider([]byte{0}))
	lru.Get(key1, staticProvider([]byte{1}))

	v0, _ := lru.Get(key0, staticProvider([]byte{2}))
	if c.Compare([]byte{0}, v0) != c.EQUAL {
		t.Errorf("Expected Get() to return %q, but got %q", []byte{0}, v0)
	}
	v1, _ := lru.Get(key1, staticProvider([]byte{3}))
	if c.Compare([]byte{1}, v1) != c.EQUAL {
		t.Errorf("Expected Get() to return %q, but got %q", []byte{1}, v0)
	}
}

func TestSizeEviction(t *testing.T) {
	lru := NewShardedLRUCache(1, 1)
	k1 := &sharded{shard: 0}
	k2 := &sharded{shard: 0}
	lru.Get(k1, staticProvider([]byte{1}))
	value, _ := lru.Get(k2, staticProvider([]byte{2}))
	if c.Compare([]byte{2}, value) != c.EQUAL {
		t.Errorf("Expected Get() to return %q, but got %q", []byte{2}, value)
	}
	if lru.shards[0].size != 1 {
		t.Errorf("Expected shard to hit max size 1, but got %d", lru.shards[0].size)
	}
}

func staticProvider(value []byte) func(Shardable) ([]byte, error) {
	return func(Shardable) ([]byte, error) {
		return value, nil
	}
}

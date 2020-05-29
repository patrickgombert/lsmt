package cache

import "testing"

func TestGetNewGeneratedValue(t *testing.T) {
	lru := NewShardedLRUCache(1, 1, staticSharder(0))
	value, _ := lru.Get("foo", staticProvider("bar", 1))
	if "bar" != value {
		t.Errorf("Expected Get() to return \"bar\", but got %q", value)
	}
}

func TestEvictEmptyCacheNoError(t *testing.T) {
	lru := NewShardedLRUCache(1, 1, staticSharder(0))
	err := lru.Evict("foo")
	if err != nil {
		t.Error("Expected empty cache to Evict() without an error, but did not")
	}
}

func TestEvict(t *testing.T) {
	lru := NewShardedLRUCache(1, 1, staticSharder(0))
	lru.Get("foo", staticProvider("bar", 1))
	lru.Evict("foo")
	value, _ := lru.Get("foo", staticProvider("baz", 1))
	if value != "baz" {
		t.Errorf("Expected Get() to return \"baz\", but got %q", value)
	}
}

func TestReadIgnoresProviderIfPresent(t *testing.T) {
	lru := NewShardedLRUCache(1, 1, staticSharder(0))
	lru.Get("foo", staticProvider("bar", 1))
	value, _ := lru.Get("foo", staticProvider("baz", 1))
	if value != "bar" {
		t.Errorf("Expected Get() to return \"bar\", but got %q", value)
	}
}

func TestTwoShardsNoEvictions(t *testing.T) {
	lru := NewShardedLRUCache(2, 2, func(key interface{}) int {
		if key == "foo0" {
			return 0
		} else {
			return 1
		}
	})
	lru.Get("foo0", staticProvider("bar0", 1))
	lru.Get("foo1", staticProvider("bar1", 1))

	v0, _ := lru.Get("foo0", staticProvider("reload0", 1))
	if v0 != "bar0" {
		t.Errorf("Expected Get() to return \"bar0\", but got %q", v0)
	}
	v1, _ := lru.Get("foo1", staticProvider("reload1", 1))
	if v1 != "bar1" {
		t.Errorf("Expected Get() to return \"bar1\", but got %q", v0)
	}
}

func TestSizeEviction(t *testing.T) {
	lru := NewShardedLRUCache(1, 1, staticSharder(0))
	lru.Get("foo", staticProvider("bar", 1))
	value, _ := lru.Get("baz", staticProvider("bang", 1))
	if value != "bang" {
		t.Errorf("Expected Get() to return \"bang\", but got %q", value)
	}
	if lru.shards[0].size != 1 {
		t.Errorf("Expected shard to hit max size 1, but got %d", lru.shards[0].size)
	}
}

func staticSharder(shard int) func(interface{}) int {
	return func(interface{}) int {
		return shard
	}
}

func staticProvider(value interface{}, size int) func(interface{}) (interface{}, int) {
	return func(interface{}) (interface{}, int) {
		return value, size
	}
}

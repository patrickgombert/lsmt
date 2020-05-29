package cache

import (
	"container/list"
	"fmt"
	"sync"
)

// A single cache entry to be contained in a list.Element
type e struct {
	key   interface{}
	value interface{}
	size  int
}

// A shard contained within the larger ShardedLRUCache.
// Each shard contains its own read-write lock. An ordering list is maintained so that
// the shard knows what value to evict the least recently used element(s).
type shard struct {
	lock     sync.RWMutex
	size     int64
	entries  map[interface{}]*list.Element
	ordering *list.List
}

// A Cache implementation which provides shards in order to minimize lock contention.
// All shards are of equal size as defined by shardMaxSize. A hashing function is also
// stored in order to route new incoming elements to the correct shard.
type ShardedLRUCache struct {
	shardMaxSize int64
	shards       []*shard
	shardKey     func(interface{}) int
}

// Generates a new instance of a ShardedLRUCache for the number of shards provided.
// Each shard is of equal size.
func NewShardedLRUCache(numShards int, size int64, shardKey func(interface{}) int) *ShardedLRUCache {
	shards := make([]*shard, numShards)
	shardSize := size / int64(numShards)
	for i := range shards {
		entries := make(map[interface{}]*list.Element)
		shards[i] = &shard{size: 0, entries: entries, ordering: list.New()}
	}

	return &ShardedLRUCache{shardMaxSize: shardSize, shards: shards, shardKey: shardKey}
}

// Get the value for a given key, falling back to the provider function if it does not
// exist. The provider function must provide both the value for the associated key and
// the size of the generated value.
func (lru *ShardedLRUCache) Get(key interface{}, provider func(interface{}) (interface{}, int)) (interface{}, error) {
	shard, err := lru.getShard(key)
	if err != nil {
		return nil, err
	}

	shard.lock.RLock()
	listElement, found := shard.entries[key]
	shard.lock.RUnlock()

	if found {
		shard.lock.Lock()
		shard.ordering.MoveToFront(listElement)
		shard.lock.Unlock()

		return listElement.Value.(*e).value, nil
	} else {
		value, size := provider(key)
		entry := &e{key: key, value: value, size: size}
		shard.lock.Lock()
		listElement = shard.ordering.PushFront(entry)
		shard.entries[key] = listElement
		shard.size += int64(size)
		for shard.size > lru.shardMaxSize {
			removed := shard.ordering.Back()
			if removed == nil {
				break
			}
			shard.remove(removed)
		}
		shard.lock.Unlock()

		return value, nil
	}
}

// Evict the entry for a given key from the cache.
func (lru *ShardedLRUCache) Evict(key interface{}) error {
	shard, err := lru.getShard(key)
	if err != nil {
		return err
	}

	shard.lock.RLock()
	listElement, found := shard.entries[key]
	shard.lock.RUnlock()

	if found {
		shard.lock.Lock()
		shard.remove(listElement)
		shard.lock.Unlock()
	}

	return nil
}

func (lru *ShardedLRUCache) getShard(key interface{}) (*shard, error) {
	shardNum := lru.shardKey(key)
	if shardNum < 0 || shardNum > len(lru.shards) {
		return nil, fmt.Errorf("shard key produced %d for number of shards %d with key %q", shardNum, len(lru.shards), key)
	}

	shard := lru.shards[shardNum]
	return shard, nil
}

func (s *shard) remove(listElement *list.Element) {
	entry := listElement.Value.(*e)
	delete(s.entries, entry.key)
	s.ordering.Remove(listElement)
	s.size -= int64(entry.size)
}

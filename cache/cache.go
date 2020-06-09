package cache

// Cache provides an interface for a write-through cache. Keys must be Shardable.
type Cache interface {
	Get(key Shardable, provider func(Shardable) ([]byte, error)) ([]byte, error)
	Evict(key Shardable) error
}

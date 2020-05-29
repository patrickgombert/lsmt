package cache

// Cache provides an interface for a write-through cache
type Cache interface {
	Get(key interface{}, provider func(interface{}) (interface{}, int)) (interface{}, error)
	Evict(key interface{}) error
}

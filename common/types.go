package common

// Container for a key/value pair
type Pair struct {
	Key   []byte
	Value []byte
}

// Iterators allow for the sequential movement through ordered data structures
// Next() must always be called before Get(). If Next() returns false then there are
// no more values to Get() and Close() should be invoked.
type Iterator interface {
	Next() (bool, error)
	Get() (*Pair, error)
	Close() error
}

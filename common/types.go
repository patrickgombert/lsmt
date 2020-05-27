package common

type Pair struct {
	Key   []byte
	Value []byte
}

type Iterator interface {
	Next() (bool, error)
	Get() (*Pair, error)
	Close() error
}

package common

type emptyIterator struct{}

var SINGLETON *emptyIterator = &emptyIterator{}

func EmptyIterator() *emptyIterator {
	return SINGLETON
}

func (iter *emptyIterator) Next() (bool, error) {
	return false, nil
}

func (iter *emptyIterator) Get() (*Pair, error) {
	return nil, nil
}

func (iter *emptyIterator) Close() error {
	return nil
}

package common

type placeholder struct{}
type Semaphore chan placeholder

// Creates a new semaphore allowing for a fixed size of locks.
func NewSemaphore(size int) Semaphore {
	s := make(Semaphore, size)
	for i := 0; i < size; i++ {
		s <- placeholder{}
	}
	return s
}

// Try to acquire a lock, returns true if the lock was acquired and false otherwise.
// It is assumed that all calls to TryLock which succeed will be responsible for their
// own call to Unlock.
func (s Semaphore) TryLock() bool {
	select {
	case <-s:
	default:
		return false
	}
	return true
}

// Unlocks a lock. This function assumes that the caller had previously called TryLock
// successfully. This function also assumed that Unlock will be called only once per
// unlock attempt.
func (s Semaphore) Unlock() {
	s <- placeholder{}
}

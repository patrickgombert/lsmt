package common

import "testing"

func TestTryLockReturnAndTryAgain(t *testing.T) {
	s := NewSemaphore(1)
	acquired := s.TryLock()
	if !acquired {
		t.Error("Expected semaphore to acquire lock, but did not")
	}
	s.Unlock()
	acquired = s.TryLock()
	if !acquired {
		t.Error("Expected semaphore to acquire lock, but did not")
	}
	s.Unlock()
}

func TestTryLockFull(t *testing.T) {
	s := NewSemaphore(1)
	s.TryLock()
	acquired := s.TryLock()
	if acquired {
		t.Error("Expected sempahore to not be acquire lock, but did")
	}
}

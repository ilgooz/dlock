// Package dlock is a distributed lock to enable synchronization in distributed environments.
// Its API is very similar to `sync.Mutex` and underlying `Store` can be implemented for
// any data storage by the consumers.
package dlock

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	// storePrefix used to prefix lock related keys in KV store.
	storePrefix = "dlock:"
)

const (
	// lockTTL is lock's expiry time.
	lockTTL = time.Second * 15

	// lockRefreshInterval used to determine how long to wait before refreshing
	// a lock's expiry time.
	lockRefreshInterval = time.Second

	// lockTryInterval used to wait before trying to obtain the lock again.
	lockTryInterval = time.Second
)

// Store used to keep locks state.
type Store interface {
	// Set sets value for the key with ttl.
	Set(key string, value interface{}, replace bool, ttl time.Duration) (isSet bool, err error)

	// Delete deletes value set for the key.
	Delete(key string) error
}

// Logger use to do logging.
type Logger interface {
	Error(a ...interface{})
}

// DLock is a distributed lock.
type DLock struct {
	store Store

	log Logger

	// key to lock for.
	key string

	// refreshCancel stops refreshing lock's TTL.
	refreshCancel context.CancelFunc

	// refreshWait is a waiter to make sure refreshing is finished.
	refreshWait *sync.WaitGroup
}

// New creates a new distributed lock for key on given store with options.
// think,
//   `dl := New("my-key", store, logger)`
// as an equivalent of,
//   `var m sync.Mutex`
// and use it in the same way.
func New(key string, store Store, logger Logger) *DLock {
	return &DLock{
		key:   buildKey(key),
		store: store,
		log:   logger,
	}
}

// Lock obtains a new lock.
// ctx provides a context to locking. when ctx is cancelled, Lock() will stop
// blocking and retries and return with error.
// use Lock() exactly like sync.Mutex.Lock(), avoid missuses like deadlocks.
func (d *DLock) Lock(ctx context.Context) error {
	for {
		isLockObtained, err := d.lock()
		if err != nil {
			return err
		}
		if isLockObtained {
			return nil
		}
		afterC := time.After(lockTryInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-afterC:
		}
	}
}

// TryLock tries to obtain the lock immediately.
// err is only filled on system failure.
func (d *DLock) TryLock() (isLockObtained bool, err error) {
	return d.lock()
}

// lock obtains a new lock and starts refreshing the lock until a call to
// Unlock() to not hit lock's TTL.
func (d *DLock) lock() (isLockObtained bool, err error) {
	isLockObtained, err = d.store.Set(d.key, true, false, lockTTL)
	if err != nil {
		return false, errors.Wrap(err, "cannot obtain a lock, Store.Set() returned with an error")
	}
	if isLockObtained {
		d.startRefreshLoop()
	}
	return isLockObtained, nil
}

// startRefreshLoop refreshes an obtained lock to not get caught by lock's TTL.
// TTL tends to hit and release the lock automatically when plugin terminates.
func (d *DLock) startRefreshLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(lockRefreshInterval)
		for {
			select {
			case <-t.C:
				_, err := d.store.Set(d.key, true, true, lockTTL)
				if err != nil {
					d.log.Error(errors.Wrap(err, "cannot refresh a lock, Store.Set() returned with an error"))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	d.refreshCancel = cancel
	d.refreshWait = &wg
}

// Unlock unlocks Lock().
// use Unlock() exactly like sync.Mutex.Unlock().
func (d *DLock) Unlock() error {
	d.refreshCancel()
	d.refreshWait.Wait()
	if err := d.store.Delete(d.key); err != nil {
		return errors.Wrap(err, "cannot release a lock, Store.Set() returned with an error")
	}
	return nil
}

// buildKey builds a lock key for KV store.
func buildKey(key string) string {
	return storePrefix + key
}

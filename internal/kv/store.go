package kv

import (
	"container/list"
	"errors"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrInvalidKey  = errors.New("invalid key")
	ErrInvalidValue = errors.New("invalid value")
	ErrStoreClosed = errors.New("store is closed")
)

/* ──────────────── public commands ──────────────── */
type SetCmd struct{ Key, Value string }
type DelCmd struct{ Key string }
type GetCmd struct{ Key string }

/* ──────────────── Store struct ─────────────────── */
type Store struct {
	db     *bolt.DB
	lru    *lruCache
	closed bool
	mu     sync.RWMutex
}

func New(db *bolt.DB, maxEntries int) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}
	if maxEntries <= 0 {
		return nil, fmt.Errorf("maxEntries must be positive")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket([]byte("kv"))
		_, err := tx.CreateBucket([]byte("kv"))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize store: %w", err)
	}

	return &Store{db: db, lru: newLRU(maxEntries)}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return ErrStoreClosed
	}
	
	s.closed = true
	return s.db.Close()
}

/* ─────────────────── API ───────────────────────── */

func (s *Store) Get(key string) (string, error) {
	if err := s.validateKey(key); err != nil {
		return "", err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return "", ErrStoreClosed
	}

	if v, ok := s.lru.get(key); ok {
		return v, nil
	}

	var value string
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("kv"))
		if b == nil {
			return fmt.Errorf("bucket not found")
		}
		v := b.Get([]byte(key))
		if v == nil {
			return ErrKeyNotFound
		}
		value = string(v)
		return nil
	})

	if err == nil {
		s.lru.add(key, value)
	}
	return value, err
}

func (s *Store) Apply(cmd any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	switch c := cmd.(type) {
	case SetCmd:
		if err := s.validateKey(c.Key); err != nil {
			return nil, err
		}
		if err := s.validateValue(c.Value); err != nil {
			return nil, err
		}

		err := s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("kv"))
			if b == nil {
				return fmt.Errorf("bucket not found")
			}
			return b.Put([]byte(c.Key), []byte(c.Value))
		})
		if err != nil {
			return nil, fmt.Errorf("failed to set key: %w", err)
		}
		s.lru.add(c.Key, c.Value)
		return nil, nil

	case DelCmd:
		if err := s.validateKey(c.Key); err != nil {
			return nil, err
		}

		err := s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("kv"))
			if b == nil {
				return fmt.Errorf("bucket not found")
			}
			return b.Delete([]byte(c.Key))
		})
		if err != nil {
			return nil, fmt.Errorf("failed to delete key: %w", err)
		}
		s.lru.remove(c.Key)
		return nil, nil

	case GetCmd:
		return s.Get(c.Key)

	default:
		return nil, fmt.Errorf("unknown command type: %T", cmd)
	}
}

func (s *Store) validateKey(key string) error {
	if key == "" {
		return ErrInvalidKey
	}
	return nil
}

func (s *Store) validateValue(value string) error {
	if value == "" {
		return ErrInvalidValue
	}
	return nil
}

/* ───────────── LRU cache ──────────────── */
type entry struct{ key, value string }

type lruCache struct {
	mu  sync.Mutex
	ll  *list.List
	tab map[string]*list.Element
	cap int
}

func newLRU(cap int) *lruCache {
	return &lruCache{ll: list.New(), tab: make(map[string]*list.Element), cap: cap}
}

func (c *lruCache) get(k string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.tab[k]
	if !ok {
		return "", false
	}
	return e.Value.(*entry).value, true
}

func (c *lruCache) add(k, v string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.tab[k]; ok {
		e.Value.(*entry).value = v
		return
	}
	e := c.ll.PushBack(&entry{k, v})
	c.tab[k] = e
	if c.ll.Len() >= c.cap {
		tail := c.ll.Back()
		c.ll.Remove(tail)
		delete(c.tab, tail.Value.(*entry).key)
	}
}

func (c *lruCache) remove(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.tab[k]; ok {
		c.ll.Remove(e)
		delete(c.tab, k)
	}
}

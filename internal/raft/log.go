package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrLogIndexOutOfRange = errors.New("log index out of range")
	ErrLogTruncateFailed  = errors.New("failed to truncate log")
	ErrLogAppendFailed    = errors.New("failed to append to log")
	ErrLogReadFailed      = errors.New("failed to read from log")
)

type LogEntry struct {
	Term    int
	Command any
}

type StableLog interface {
	Append(entries ...LogEntry) (int, error) // returns last index appended (1‑based)
	At(index int) (LogEntry, bool, error)    // returns false if index < firstIndex or > lastIndex
	LastIndexTerm() (int, int, error)
	LastIndex() (int, error)
	FirstIndex() int

	TruncateSuffix(idx int) error
	TruncateBefore(index int) error
}

// ---------------- util ---------------------
func u64ToKey(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}

func keyToU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// -------------- BoltLog --------------------
type boltLog struct {
	db   *bolt.DB
	mu   sync.Mutex
	base uint64 // first index = base
}

func NewBoltLog(db *bolt.DB) (StableLog, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}

	var base uint64 = 1
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("log"))
		if err != nil {
			return fmt.Errorf("failed to create log bucket: %w", err)
		}
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return fmt.Errorf("failed to create meta bucket: %w", err)
		}
		if v := meta.Get([]byte("firstIndex")); v != nil {
			base = binary.BigEndian.Uint64(v)
		} else {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], base)
			if err := meta.Put([]byte("firstIndex"), b[:]); err != nil {
				return fmt.Errorf("failed to set first index: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize log: %w", err)
	}
	return &boltLog{db: db, base: base}, nil
}

// --------------- StableLog interface -----------------------
func (l *boltLog) Append(entries ...LogEntry) (int, error) {
	if len(entries) == 0 {
		return 0, fmt.Errorf("no entries to append")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var last uint64
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return fmt.Errorf("log bucket not found")
		}

		for _, e := range entries {
			last = uint64(l.LastIndex() + 1)
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(e); err != nil {
				return fmt.Errorf("failed to encode entry: %w", err)
			}
			if err := b.Put(u64ToKey(last), buf.Bytes()); err != nil {
				return fmt.Errorf("failed to store entry: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrLogAppendFailed, err)
	}
	return int(last), nil
}

func (l *boltLog) At(idx int) (LogEntry, bool, error) {
	if idx < int(l.base) {
		return LogEntry{}, false, fmt.Errorf("%w: index %d < base %d", ErrLogIndexOutOfRange, idx, l.base)
	}

	var e LogEntry
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return fmt.Errorf("log bucket not found")
		}

		v := b.Get(u64ToKey(uint64(idx)))
		if v == nil {
			return ErrLogIndexOutOfRange
		}

		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&e); err != nil {
			return fmt.Errorf("failed to decode entry: %w", err)
		}
		return nil
	})

	if err != nil {
		return LogEntry{}, false, fmt.Errorf("%w: %v", ErrLogReadFailed, err)
	}
	return e, e.Term != 0 || e.Command != nil, nil
}

func (l *boltLog) LastIndexTerm() (int, int, error) {
	var idx, term int
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return fmt.Errorf("log bucket not found")
		}

		c := b.Cursor()
		k, v := c.Last()
		if k == nil {
			idx, term = int(l.base-1), 0
			return nil
		}

		idx = int(keyToU64(k))
		var e LogEntry
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&e); err != nil {
			return fmt.Errorf("failed to decode entry: %w", err)
		}
		term = e.Term
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrLogReadFailed, err)
	}
	return idx, term, nil
}

func (l *boltLog) LastIndex() (int, error) {
	i, _, err := l.LastIndexTerm()
	return i, err
}

func (l *boltLog) FirstIndex() int {
	return int(l.base)
}

// ------------ snapshot compaction ---------------
func (l *boltLog) TruncateBefore(index int) error {
	if index <= int(l.base) {
		return fmt.Errorf("cannot truncate before base index %d", l.base)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return fmt.Errorf("log bucket not found")
		}

		c := b.Cursor()
		for k, _ := c.First(); k != nil && keyToU64(k) < uint64(index); k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("failed to delete entry: %w", err)
			}
		}

		// Update base index
		meta := tx.Bucket([]byte("meta"))
		if meta == nil {
			return fmt.Errorf("meta bucket not found")
		}

		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(index))
		if err := meta.Put([]byte("firstIndex"), buf[:]); err != nil {
			return fmt.Errorf("failed to update base index: %w", err)
		}

		l.base = uint64(index)
		return nil
	})

	if err != nil {
		return fmt.Errorf("%w: %v", ErrLogTruncateFailed, err)
	}
	return nil
}

func (l *boltLog) TruncateSuffix(idx int) error {
	if idx < int(l.base) {
		return fmt.Errorf("%w: index %d < base %d", ErrLogIndexOutOfRange, idx, l.base)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return fmt.Errorf("log bucket not found")
		}

		c := b.Cursor()
		for k, _ := c.Seek(u64ToKey(uint64(idx))); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("failed to delete entry: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("%w: %v", ErrLogTruncateFailed, err)
	}
	return nil
}

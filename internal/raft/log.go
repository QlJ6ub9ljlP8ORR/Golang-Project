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
	// Error definitions
	ErrLogIndexOutOfRange = errors.New("log index out of range")
	ErrLogTruncateFailed  = errors.New("failed to truncate log")
	ErrLogAppendFailed    = errors.New("failed to append to log")
	ErrLogReadFailed      = errors.New("failed to read from log")
	ErrNilDatabase        = errors.New("database cannot be nil")
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrNoEntriesToAppend  = errors.New("no entries to append")
)

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int
	Command any
}

// StableLog defines the interface for persistent log storage
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

func keyToU64(b []byte) uint64 { 
	return binary.BigEndian.Uint64(b) 
}

// -------------- BoltLog --------------------
type boltLog struct {
	db   *bolt.DB
	mu   sync.Mutex
	base uint64 // first index = base
}

// NewBoltLog creates a new log backed by a BoltDB database
func NewBoltLog(db *bolt.DB) (StableLog, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}

	var base uint64 = 1
	err := db.Update(func(tx *bolt.Tx) error {
		logBucket, err := tx.CreateBucketIfNotExists([]byte("log"))
		if err != nil {
			return fmt.Errorf("failed to create log bucket: %w", err)
		}
		
		metaBucket, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return fmt.Errorf("failed to create meta bucket: %w", err)
		}
		
		if v := metaBucket.Get([]byte("firstIndex")); v != nil {
			base = keyToU64(v)
		} else {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], base)
			if err := metaBucket.Put([]byte("firstIndex"), b[:]); err != nil {
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

// Append adds one or more LogEntry objects to the log and returns the index of the last appended entry
func (l *boltLog) Append(entries ...LogEntry) (int, error) {
	if len(entries) == 0 {
		return 0, ErrNoEntriesToAppend
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var last uint64
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return ErrBucketNotFound
		}

		// Get current last index
		lastIndex, err := l.getLastIndexUnsafe(tx)
		if err != nil {
			return err
		}
		
		// Append entries
		for i := range entries {
			last = uint64(lastIndex + i + 1)
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(entries[i]); err != nil {
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

// At retrieves the LogEntry at the specified index
func (l *boltLog) At(idx int) (LogEntry, bool, error) {
	if idx < int(l.base) {
		return LogEntry{}, false, fmt.Errorf("%w: index %d < base %d", 
			ErrLogIndexOutOfRange, idx, l.base)
	}

	var e LogEntry
	var found bool
	
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return ErrBucketNotFound
		}

		v := b.Get(u64ToKey(uint64(idx)))
		if v == nil {
			return nil // Not found, but not an error
		}

		found = true
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&e); err != nil {
			return fmt.Errorf("failed to decode entry: %w", err)
		}
		return nil
	})

	if err != nil {
		return LogEntry{}, false, fmt.Errorf("%w: %v", ErrLogReadFailed, err)
	}
	
	return e, found, nil
}

// getLastIndexUnsafe is a helper function that must be called with the mutex held
// and within a transaction
func (l *boltLog) getLastIndexUnsafe(tx *bolt.Tx) (int, error) {
	b := tx.Bucket([]byte("log"))
	if b == nil {
		return 0, ErrBucketNotFound
	}

	c := b.Cursor()
	k, _ := c.Last()
	if k == nil {
		return int(l.base) - 1, nil
	}

	return int(keyToU64(k)), nil
}

// LastIndexTerm returns the index and term of the last entry in the log
func (l *boltLog) LastIndexTerm() (int, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	var idx, term int
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return ErrBucketNotFound
		}

		c := b.Cursor()
		k, v := c.Last()
		if k == nil {
			idx, term = int(l.base) - 1, 0
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

// LastIndex returns the index of the last entry in the log
func (l *boltLog) LastIndex() (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	var idx int
	err := l.db.View(func(tx *bolt.Tx) error {
		var err error
		idx, err = l.getLastIndexUnsafe(tx)
		return err
	})
	
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrLogReadFailed, err)
	}
	
	return idx, nil
}

// FirstIndex returns the index of the first entry in the log
func (l *boltLog) FirstIndex() int {
	return int(l.base)
}

// ------------ snapshot compaction ---------------

// TruncateBefore removes all entries with index less than the specified index
func (l *boltLog) TruncateBefore(index int) error {
	if index <= int(l.base) {
		return nil // Nothing to truncate
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return ErrBucketNotFound
		}

		// Delete all entries with index < specified index
		c := b.Cursor()
		for k, _ := c.First(); k != nil && keyToU64(k) < uint64(index); {
			key := make([]byte, len(k)) // Create a copy of the key
			copy(key, k)
			
			// Get next before deleting
			k, _ = c.Next()
			
			if err := b.Delete(key); err != nil {
				return fmt.Errorf("failed to delete entry: %w", err)
			}
		}

		// Update base index in meta bucket
		meta := tx.Bucket([]byte("meta"))
		if meta == nil {
			return ErrBucketNotFound
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

// TruncateSuffix removes all entries with index greater than or equal to the specified index
func (l *boltLog) TruncateSuffix(idx int) error {
	if idx < int(l.base) {
		return fmt.Errorf("%w: index %d < base %d", ErrLogIndexOutOfRange, idx, l.base)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		if b == nil {
			return ErrBucketNotFound
		}

		// Delete all entries with index >= specified index
		c := b.Cursor()
		for k, _ := c.Seek(u64ToKey(uint64(idx))); k != nil; {
			key := make([]byte, len(k)) // Create a copy of the key
			copy(key, k)
			
			// Get next before deleting
			k, _ = c.Next()
			
			if err := b.Delete(key); err != nil {
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
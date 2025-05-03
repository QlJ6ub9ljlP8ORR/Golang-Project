package raft

import (
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrStorageNotInitialized = errors.New("storage not initialized")
	ErrStorageOperationFailed = errors.New("storage operation failed")
	ErrInvalidTerm = errors.New("invalid term")
	ErrInvalidIndex = errors.New("invalid index")
)

// StableStore holds the Raft persistent metadata.
type StableStore interface {
	Term() (int, error)
	SetTerm(t int) error
	VotedFor() (string, error)
	SetVotedFor(id string) error
	LastApplied() (int, error)
	SetLastApplied(index int) error
}

// ------------------------------------------------------------
// Bolt-backed implementation
// ------------------------------------------------------------
const (
	bMeta       = "meta" // bucket name
	kTerm       = "term"
	kVotedFor   = "voted_for"
	LastApplied = "last_applied"
)

type boltStore struct{ db *bolt.DB }

func NewBoltStore(db *bolt.DB) (StableStore, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bMeta))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) Term() (int, error) {
	var t uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bMeta))
		if b == nil {
			return ErrStorageNotInitialized
		}
		if v := b.Get([]byte(kTerm)); v != nil {
			t = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return int(t), nil
}

func (s *boltStore) SetTerm(term int) error {
	if term < 0 {
		return ErrInvalidTerm
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(term))
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bMeta))
		if b == nil {
			return ErrStorageNotInitialized
		}
		return b.Put([]byte(kTerm), buf[:])
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

func (s *boltStore) VotedFor() (string, error) {
	var id string
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bMeta))
		if b == nil {
			return ErrStorageNotInitialized
		}
		if v := b.Get([]byte(kVotedFor)); v != nil {
			id = string(v)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return id, nil
}

func (s *boltStore) SetVotedFor(id string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bMeta))
		if b == nil {
			return ErrStorageNotInitialized
		}
		return b.Put([]byte(kVotedFor), []byte(id))
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

func (s *boltStore) LastApplied() (int, error) {
	var last uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bMeta))
		if b == nil {
			return ErrStorageNotInitialized
		}
		if v := b.Get([]byte(LastApplied)); v != nil {
			last = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return int(last), nil
}

func (s *boltStore) SetLastApplied(index int) error {
	if index < 0 {
		return ErrInvalidIndex
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bMeta))
		if b == nil {
			return ErrStorageNotInitialized
		}
		return b.Put([]byte(LastApplied), buf[:])
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorageOperationFailed, err)
	}
	return nil
}

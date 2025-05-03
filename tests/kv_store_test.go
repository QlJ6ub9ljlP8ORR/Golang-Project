package tests

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
)


// ---------------------------------------------------------
// 	helpers
// ---------------------------------------------------------
func newTempStore(t *testing.T, cacheSize int) (*kv.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "kv.bolt")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 0})
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	s, err := kv.New(db, cacheSize)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return s, func() { s.Close() }
}


// ---------------------------------------------------------
//  1. basic Set / Get / Del
// ---------------------------------------------------------
func TestStoreBasicOps(t *testing.T) {
	s, clean := newTempStore(t, 10)
	defer clean()

	// Test invalid key
	_, err := s.Get("")
	if err != kv.ErrInvalidKey {
		t.Errorf("expected ErrInvalidKey for empty key, got %v", err)
	}

	// Test invalid value
	_, err = s.Apply(kv.SetCmd{Key: "foo", Value: ""})
	if err != kv.ErrInvalidValue {
		t.Errorf("expected ErrInvalidValue for empty value, got %v", err)
	}

	// Test valid set
	_, err = s.Apply(kv.SetCmd{Key: "foo", Value: "bar"})
	if err != nil {
		t.Errorf("unexpected error on set: %v", err)
	}

	// Test get
	val, err := s.Get("foo")
	if err != nil {
		t.Errorf("unexpected error on get: %v", err)
	}
	if val != "bar" {
		t.Errorf("want bar, got %q", val)
	}

	// Test get non-existent
	val, err = s.Get("nonexistent")
	if err != kv.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	// Test delete
	_, err = s.Apply(kv.DelCmd{Key: "foo"})
	if err != nil {
		t.Errorf("unexpected error on delete: %v", err)
	}

	// Test get after delete
	val, err = s.Get("foo")
	if err != kv.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

// ---------------------------------------------------------
//  2. LRU cache hit / eviction
// ---------------------------------------------------------
func TestStoreLRUEviction(t *testing.T) {
	s, clean := newTempStore(t, 2) // cache holds only 2
	defer clean()

	// fill three keys
	_, err := s.Apply(kv.SetCmd{Key: "a", Value: "1"})
	if err != nil {
		t.Errorf("unexpected error on set a: %v", err)
	}
	_, err = s.Apply(kv.SetCmd{Key: "b", Value: "2"})
	if err != nil {
		t.Errorf("unexpected error on set b: %v", err)
	}
	_, err = s.Apply(kv.SetCmd{Key: "c", Value: "3"}) // evicts "a"
	if err != nil {
		t.Errorf("unexpected error on set c: %v", err)
	}

	// "a" must trigger Bolt hit (cache miss) but still return correct value
	val, err := s.Get("a")
	if err != nil {
		t.Errorf("unexpected error on get a: %v", err)
	}
	if val != "1" {
		t.Errorf("LRU eviction wrong, want 1 got %q", val)
	}
}

// ---------------------------------------------------------
//  3. persistence across reopen
// ---------------------------------------------------------
func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "kv.bolt")

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	store1, err := kv.New(db, 5)
	if err != nil {
		t.Fatalf("create store1: %v", err)
	}
	_, err = store1.Apply(kv.SetCmd{Key: "x", Value: "y"})
	if err != nil {
		t.Errorf("unexpected error on set: %v", err)
	}
	err = store1.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	// reopen same file
	db2, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open bolt2: %v", err)
	}
	store2, err := kv.New(db2, 5)
	if err != nil {
		t.Fatalf("create store2: %v", err)
	}
	val, err := store2.Get("x")
	if err != nil {
		t.Errorf("unexpected error on get: %v", err)
	}
	if val != "y" {
		t.Errorf("persistence lost: want y got %q", val)
	}
	err = store2.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

// ---------------------------------------------------------
//  4. concurrent writers & readers
// ---------------------------------------------------------
func TestStoreConcurrent(t *testing.T) {
	s, clean := newTempStore(t, 50)
	defer clean()

	var wg sync.WaitGroup
	for i := range 100 {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k" + string(rune(i))
			_, err := s.Apply(kv.SetCmd{Key: key, Value: "v"})
			if err != nil {
				t.Errorf("unexpected error on set: %v", err)
				return
			}
			val, err := s.Get(key)
			if err != nil {
				t.Errorf("unexpected error on get: %v", err)
				return
			}
			if val != "v" {
				t.Errorf("concurrent get failed for %s", key)
			}
			_, err = s.Apply(kv.DelCmd{Key: key})
			if err != nil {
				t.Errorf("unexpected error on delete: %v", err)
			}
		}()
	}
	wg.Wait()

	// small delay to ensure deletes flushed
	time.Sleep(50 * time.Millisecond)
	val, err := s.Get("k0")
	if err != kv.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
	if val != "" {
		t.Errorf("expected empty after delete, got %q", val)
	}
}

// ---------------------------------------------------------
//  5. store closed operations
// ---------------------------------------------------------
func TestStoreClosed(t *testing.T) {
	s, _ := newTempStore(t, 10)
	s.Close()

	// Test operations after close
	_, err := s.Get("key")
	if err != kv.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on get, got %v", err)
	}

	_, err = s.Apply(kv.SetCmd{Key: "key", Value: "value"})
	if err != kv.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on set, got %v", err)
	}

	_, err = s.Apply(kv.DelCmd{Key: "key"})
	if err != kv.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on delete, got %v", err)
	}

	// Test double close
	err = s.Close()
	if err != kv.ErrStoreClosed {
		t.Errorf("expected ErrStoreClosed on second close, got %v", err)
	}
}

// ---------------------------------------------------------
//  6. invalid store creation
// ---------------------------------------------------------
func TestStoreInvalidCreation(t *testing.T) {
	// Test nil database
	_, err := kv.New(nil, 10)
	if err == nil {
		t.Error("expected error for nil database")
	}

	// Test invalid cache size
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "kv.bolt")
	db, _ := bolt.Open(dbPath, 0600, nil)
	_, err = kv.New(db, 0)
	if err == nil {
		t.Error("expected error for zero cache size")
	}
	_, err = kv.New(db, -1)
	if err == nil {
		t.Error("expected error for negative cache size")
	}
}

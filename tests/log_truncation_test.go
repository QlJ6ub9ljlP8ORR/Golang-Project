package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/config"
	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

// ---------------------------------------------------------------------
//
//	UNIT: boltLog.TruncateBefore
//
// --------------------------------------------------------------------
func TestBoltLogTruncateBefore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "log.bolt")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	logStore, err := raft.NewBoltLog(db)
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	// Test invalid truncation before base
	err = logStore.TruncateBefore(0)
	if err == nil {
		t.Error("expected error when truncating before base")
	}

	// append 100 entries
	for i := 1; i <= 100; i++ {
		_, err := logStore.Append(raft.LogEntry{Term: 1, Command: kv.SetCmd{
			Key: fmt.Sprintf("k%02d", i), Value: "v"}})
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	lastIdx, err := logStore.LastIndex()
	if err != nil {
		t.Fatalf("get last index failed: %v", err)
	}
	if lastIdx != 100 {
		t.Fatalf("append failed: want last index 100, got %d", lastIdx)
	}

	// keep tail 40, prune before 61
	err = logStore.TruncateBefore(61)
	if err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	firstIdx := logStore.FirstIndex()
	if firstIdx != 61 {
		t.Fatalf("firstIndex want 61 got %d", firstIdx)
	}

	// Test accessing truncated entries
	_, ok, err := logStore.At(50)
	if err == nil {
		t.Errorf("expected error when accessing truncated entry")
	}
	if ok {
		t.Fatalf("expected entry 50 to be gone")
	}

	// Test accessing valid entries
	_, ok, err = logStore.At(80)
	if err != nil {
		t.Errorf("unexpected error accessing valid entry: %v", err)
	}
	if !ok {
		t.Fatalf("expected entry 80 to exist")
	}

	// close + reopen → FirstIndex must persist
	db.Close()
	db2, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("reopen bolt: %v", err)
	}
	reopen, err := raft.NewBoltLog(db2)
	if err != nil {
		t.Fatalf("recreate log store: %v", err)
	}
	if reopen.FirstIndex() != 61 {
		t.Fatalf("persisted firstIndex lost; want 61 got %d", reopen.FirstIndex())
	}
	db2.Close()
}

// ---------------------------------------------------------------------
//
//	INTEGRATION: manual pruning
//
// ---------------------------------------------------------------------
func TestClusterManualPrune(t *testing.T) {
	nodes, stop := buildCluster(t, 1)
	defer stop()
	time.Sleep(3 * time.Second) // elect

	// find leader
	var leader *raft.Node
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader")
	}

	/* write 3 000 entries */
	for i := 1; i <= 3000; i++ {
		_, ok, err := leader.Propose(kv.SetCmd{Key: fmt.Sprintf("x%04d", i), Value: "v"})
		if err != nil {
			t.Fatalf("propose error at %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("propose fail at %d", i)
		}
	}
	// wait leader apply
	for leader.LastApplied() < 3000 {
		time.Sleep(5 * time.Millisecond)
	}

	/* --- prune the first 2500 entries on the leader --- */
	err := leader.Log().TruncateBefore(2501)
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}

	// verify firstIndex reflects change
	if fi := leader.Log().FirstIndex(); fi != 2501 {
		t.Fatalf("prune failed: firstIndex=%d", fi)
	}

	/* followers should still replicate new command */
	_, ok, err := leader.Propose(kv.SetCmd{Key: "tail", Value: "ok"})
	if err != nil {
		t.Fatalf("post-prune propose error: %v", err)
	}
	if !ok {
		t.Fatalf("post-prune propose failed")
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, n := range nodes {
		for n.LastApplied() < 3001 {
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not catch up after prune", n.ID())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------
//
//	INTEGRATION: automatic pruning
//
// ---------------------------------------------------------------------
func TestClusterAutoPrune(t *testing.T) {
	// -------- build single-node cluster ----------
	nodes, stop := buildCluster(t, 1)
	defer stop()

	leader := nodes[0]
	time.Sleep(2 * time.Second) // self-elect

	// -------- flood just over pruneEvery ----------
	const total = 2100
	for i := 1; i <= total; i++ {
		_, ok, err := leader.Propose(kv.SetCmd{
			Key: fmt.Sprintf("k%04d", i), Value: "x"})
		if err != nil {
			t.Fatalf("propose error at %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("propose %d failed", i)
		}
	}
	for leader.LastApplied() < total {
		time.Sleep(5 * time.Millisecond)
	}

	// -------- auto-prune should have fired --------
	fi := leader.Log().FirstIndex()
	wantMin := int(total/config.PruneEvery)*config.PruneEvery - config.RetainTail + 1
	if fi < wantMin {
		t.Fatalf("auto prune did not advance firstIndex; got %d, want ≥ %d",
			fi, wantMin)
	}

	// -------- cluster still usable after prune ----
	_, ok, err := leader.Propose(kv.SetCmd{Key: "tail", Value: "ok"})
	if err != nil {
		t.Fatalf("post-prune propose error: %v", err)
	}
	if !ok {
		t.Fatalf("post-prune propose failed")
	}
	for leader.LastApplied() < total+1 {
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------
//
//	UNIT: log entry validation
//
// --------------------------------------------------------------------
func TestLogEntryValidation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "log.bolt")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	logStore, err := raft.NewBoltLog(db)
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	// Test appending nil command
	_, err = logStore.Append(raft.LogEntry{Term: 1, Command: nil})
	if err == nil {
		t.Error("expected error when appending nil command")
	}

	// Test appending with invalid term
	_, err = logStore.Append(raft.LogEntry{Term: -1, Command: kv.SetCmd{Key: "k", Value: "v"}})
	if err == nil {
		t.Error("expected error when appending with negative term")
	}

	// Test appending empty entries
	_, err = logStore.Append()
	if err == nil {
		t.Error("expected error when appending empty entries")
	}

	// Test truncating with invalid index
	err = logStore.TruncateSuffix(-1)
	if err == nil {
		t.Error("expected error when truncating with negative index")
	}

	// Test accessing invalid index
	_, _, err = logStore.At(-1)
	if err == nil {
		t.Error("expected error when accessing negative index")
	}
}

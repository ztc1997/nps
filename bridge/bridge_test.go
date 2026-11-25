package bridge

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"ehang.io/nps/lib/conn"
	"ehang.io/nps/lib/file"
	"ehang.io/nps/lib/nps_mux"
)

// newTestBridge 构造一个隔离的 Bridge 与内存 Db。
func newTestBridge(t *testing.T) (*Bridge, *file.DbUtils) {
	t.Helper()
	file.GetDb() // 触发 once，避免后续覆盖被初始化逻辑重新写回
	old := file.Db
	db := &file.DbUtils{JsonDb: file.NewJsonDb(t.TempDir())}
	file.Db = db
	t.Cleanup(func() { file.Db = old })
	return NewTunnel(0, "tcp", sync.Map{}, 60), db
}

// addGroupClient 向组中加入一个客户端记录。
func addGroupClient(db *file.DbUtils, id int, group string, status bool) {
	db.JsonDb.Clients.Store(id, &file.Client{Id: id, Group: group, Status: status, Cnf: &file.Config{}})
	db.MarkGroupIndexDirty()
}

// addOnlineClient 在 Bridge 中登记一个在线客户端（带有效 mux 和 signal）。
func addOnlineClient(t *testing.T, b *Bridge, id int) {
	t.Helper()
	left, right := net.Pipe()
	mux := nps_mux.NewMux(left, "tcp", 60)
	t.Cleanup(func() {
		_ = mux.Close()
		_ = left.Close()
		_ = right.Close()
	})
	b.Client.Store(id, NewClient(mux, nil, conn.NewConn(right), ""))
}

func TestSelectClientSkipsOfflineAndDisabled(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClient(db, 1, "office", true)
	addGroupClient(db, 2, "office", true) // 组内但不在线
	addGroupClient(db, 3, "office", true)
	addGroupClient(db, 4, "office", false) // 已停用
	addOnlineClient(t, b, 1)
	addOnlineClient(t, b, 3)

	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		id, err := b.SelectClient("office", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != 1 && id != 3 {
			t.Fatalf("selected offline/disabled client %d", id)
		}
		seen[id] = true
	}
	if !seen[1] || !seen[3] {
		t.Fatalf("expected both online clients to be selected, got %v", seen)
	}
}

func TestSelectClientAllOffline(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClient(db, 1, "office", true)
	addGroupClient(db, 2, "office", true)
	// Bridge 中没有任何在线连接
	if _, err := b.SelectClient("office", ""); err == nil {
		t.Fatal("expected error when all clients are offline")
	}
}

func TestSelectClientMissingGroup(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClient(db, 1, "office", true)
	addOnlineClient(t, b, 1)
	for _, group := range []string{"", "not-exist"} {
		if _, err := b.SelectClient(group, ""); err == nil {
			t.Fatalf("expected error for group %q", group)
		}
	}
}

func TestSelectClientConcurrent(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClient(db, 1, "office", true)
	addGroupClient(db, 2, "office", true)
	addOnlineClient(t, b, 1)
	addOnlineClient(t, b, 2)

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := b.SelectClient("office", "")
			if err != nil {
				errCh <- err
				return
			}
			if id != 1 && id != 2 {
				errCh <- errUnexpectedClient(id)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// addGroupClientWithLocal 向组中加入一个带上报内网 IP 的客户端记录。
func addGroupClientWithLocal(db *file.DbUtils, id int, group string, status bool, localAddr string) {
	db.JsonDb.Clients.Store(id, &file.Client{
		Id: id, Group: group, Status: status, LocalAddr: localAddr, Cnf: &file.Config{},
	})
	db.MarkGroupIndexDirty()
}

func TestSelectClientTargetIPAffinity(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClientWithLocal(db, 1, "office", true, "192.168.1.11")
	addGroupClientWithLocal(db, 2, "office", true, "192.168.1.12")
	addOnlineClient(t, b, 1)
	addOnlineClient(t, b, 2)

	for _, target := range []string{"192.168.1.12:3389", "192.168.1.12"} {
		id, err := b.SelectClient("office", target)
		if err != nil {
			t.Fatalf("target %s: unexpected error: %v", target, err)
		}
		if id != 2 {
			t.Fatalf("target %s: expected affinity client 2, got %d", target, id)
		}
	}
}

func TestSelectClientTargetIPAffinitySkipsOffline(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClientWithLocal(db, 1, "office", true, "192.168.1.11")
	addGroupClientWithLocal(db, 2, "office", true, "192.168.1.12") // 目标机不在线
	addOnlineClient(t, b, 1)

	id, err := b.SelectClient("office", "192.168.1.12:3389")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected fallback client 1, got %d", id)
	}
}

func TestSelectClientNonIPTargetFallsBackToRR(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClientWithLocal(db, 1, "office", true, "192.168.1.11")
	addGroupClientWithLocal(db, 2, "office", true, "192.168.1.12")
	addOnlineClient(t, b, 1)
	addOnlineClient(t, b, 2)

	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		id, err := b.SelectClient("office", "router.example.com:3389")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != 1 && id != 2 {
			t.Fatalf("unexpected client %d", id)
		}
		seen[id] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("expected both online clients to be selected, got %v", seen)
	}
}

func TestSelectClientMultiLocalIPAffinity(t *testing.T) {
	b, db := newTestBridge(t)
	addGroupClientWithLocal(db, 1, "office", true, "10.0.0.2, 192.168.50.9")
	addGroupClientWithLocal(db, 2, "office", true, "192.168.1.12")
	addOnlineClient(t, b, 1)
	addOnlineClient(t, b, 2)

	id, err := b.SelectClient("office", "192.168.50.9:443")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected multi-IP affinity client 1, got %d", id)
	}
}

func TestSelectClientRotationUsesStableIDOrder(t *testing.T) {
	b, db := newTestBridge(t)
	const group = "rotate-stable"
	addGroupClient(db, 3, group, true)
	addGroupClient(db, 1, group, true)
	addGroupClient(db, 7, group, true)
	addOnlineClient(t, b, 1)
	addOnlineClient(t, b, 3)
	addOnlineClient(t, b, 7)

	// 用独立组并重置计数器，确保序列可预测。
	groupSequences.Store(group, &atomic.Uint64{})
	want := []int{1, 3, 7, 1, 3, 7}
	for i, expected := range want {
		id, err := b.SelectClient(group, "")
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if id != expected {
			t.Fatalf("call %d: expected client %d, got %d", i, expected, id)
		}
	}
}

type errUnexpectedClient int

func (e errUnexpectedClient) Error() string {
	return "unexpected client id"
}

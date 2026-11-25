package file

import (
	"sync"
	"testing"
)

func newIndexTestDb(t *testing.T) *DbUtils {
	t.Helper()
	return &DbUtils{JsonDb: NewJsonDb(t.TempDir())}
}

func storeIndexedClient(db *DbUtils, id int, group string, status bool) *Client {
	c := &Client{Id: id, Group: group, Status: status, Cnf: &Config{}}
	db.JsonDb.Clients.Store(id, c)
	db.MarkGroupIndexDirty()
	return c
}

func indexedClientIDs(clients []*Client) []int {
	ids := make([]int, 0, len(clients))
	for _, c := range clients {
		ids = append(ids, c.Id)
	}
	return ids
}

func assertIndexedIDs(t *testing.T, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got ids %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got ids %v, want %v", got, want)
		}
	}
}

func TestGetClientsByGroupFiltersAndSorts(t *testing.T) {
	db := newIndexTestDb(t)
	storeIndexedClient(db, 3, "office", true)
	storeIndexedClient(db, 1, "office", true)
	storeIndexedClient(db, 2, "office", false) // 停用成员不应入选
	storeIndexedClient(db, 7, "office", true)
	storeIndexedClient(db, 4, "ops", true)
	storeIndexedClient(db, 5, "", true) // 无组客户端不进任何组

	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), []int{1, 3, 7})
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("ops")), []int{4})
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("")), nil)
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("missing")), nil)

	// 第二次读取命中缓存，结果必须一致。
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), []int{1, 3, 7})
}

func TestGroupIndexInvalidatesOnMembershipChanges(t *testing.T) {
	db := newIndexTestDb(t)
	a := storeIndexedClient(db, 1, "office", true)
	b := storeIndexedClient(db, 2, "office", true)
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), []int{1, 2})

	// Web 修改所属组。
	b.Lock()
	b.Group = "ops"
	b.Unlock()
	db.MarkGroupIndexDirty()
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), []int{1})
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("ops")), []int{2})

	// Web 停用客户端。
	a.Lock()
	a.Status = false
	a.Unlock()
	db.MarkGroupIndexDirty()
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), nil)

	// 重新启用后删除客户端。
	a.Lock()
	a.Status = true
	a.Unlock()
	db.MarkGroupIndexDirty()
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), []int{1})

	db.JsonDb.Clients.Delete(1)
	db.MarkGroupIndexDirty()
	assertIndexedIDs(t, indexedClientIDs(db.GetClientsByGroup("office")), nil)
}

func TestGroupIndexConcurrentAccess(t *testing.T) {
	db := newIndexTestDb(t)
	client := storeIndexedClient(db, 1, "office", true)
	for i := 2; i <= 8; i++ {
		storeIndexedClient(db, i, "office", true)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, c := range db.GetClientsByGroup("office") {
					c.RLock()
					_ = c.Id
					c.RUnlock()
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			client.Lock()
			client.Status = i%2 == 0
			client.Unlock()
			db.MarkGroupIndexDirty()
		}
		close(stop)
	}()
	wg.Wait()
}

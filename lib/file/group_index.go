package file

import (
	"sort"
	"sync"
	"sync/atomic"
)

// groupIndex 缓存“高可用组 -> 已启用成员”的只读快照。
//
// 只缓存静态资格（Group 与 Status），在线状态由调用方实时判定，
// 因此 Bridge 的连接上下线无需刷新索引。快照通过 atomic.Value 发布，
// 读路径无锁；成员资格变化时由 MarkGroupIndexDirty 递增版本，
// 下次读取时惰性重建。重建期间若版本再次变化则重试，避免漏失效。
type groupIndex struct {
	once         sync.Once
	built        atomic.Bool
	version      atomic.Uint64
	builtVersion atomic.Uint64
	published    atomic.Value // map[string][]*Client
}

// 写入极端频繁时限制重建重试次数，避免选路被无限拖住。
// 达到上限则跳过本次发布，builtVersion 保持旧值，下次读取会再试。
const maxGroupIndexRebuildRetries = 8

// MarkGroupIndexDirty 标记客户端成员资格（Group/Status）或成员集合发生变化。
// 索引将在下次 GetClientsByGroup 时惰性重建。
func (s *DbUtils) MarkGroupIndexDirty() {
	s.groups.version.Add(1)
}

// GetClientsByGroup 返回组内 Status=true 的成员，按 Id 升序。
// 返回的切片是内部快照，调用方必须视为只读，不得修改其元素或顺序。
func (s *DbUtils) GetClientsByGroup(group string) []*Client {
	if group == "" {
		return nil
	}
	s.groups.once.Do(func() {
		s.groups.published.Store(map[string][]*Client{})
	})
	if !s.groups.built.Load() || s.groups.builtVersion.Load() != s.groups.version.Load() {
		s.rebuildGroupIndex()
	}
	return s.groups.published.Load().(map[string][]*Client)[group]
}

// rebuildGroupIndex 扫描一次客户端表并发布新快照。
func (s *DbUtils) rebuildGroupIndex() {
	for attempt := 0; attempt < maxGroupIndexRebuildRetries; attempt++ {
		start := s.groups.version.Load()
		result := make(map[string][]*Client)
		s.JsonDb.Clients.Range(func(_, value interface{}) bool {
			c, ok := value.(*Client)
			if !ok || c == nil {
				return true
			}
			c.RLock()
			group, enabled := c.Group, c.Status
			c.RUnlock()
			if group != "" && enabled {
				result[group] = append(result[group], c)
			}
			return true
		})
		for _, members := range result {
			sort.Slice(members, func(i, j int) bool { return members[i].Id < members[j].Id })
		}
		if s.groups.version.Load() == start {
			// 先发布数据再标记版本，读方看到 built=true 时必能取到配套快照。
			s.groups.published.Store(result)
			s.groups.builtVersion.Store(start)
			s.groups.built.Store(true)
			return
		}
	}
}

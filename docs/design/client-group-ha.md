# NPS 客户端组高可用（方案 C）设计与交互方案

## 1. 目标与范围

**目标**：多台内网电脑各自运行 npc，公网 NPS 服务端维护一个「客户端组」。一条隧道只暴露一个公网入口，服务端按在线状态自动挑选组内成员转发；某台电脑关机/掉线时，新连接自动落到其他在线成员。

**范围**：

- 阶段一支持：`tcp` / `udp` / `socks5` 隧道。
- 阶段二可选：`http/https` 域名代理（`Host` 模式）、按实际转发成员限流。
- **客户端零改动**：npc 不需要升级，也不需要新增启动参数。

**为什么客户端零改动**：握手时 `bridge.cliProcess` 用 vKey 调 `GetIdByVerifyKey` 得到 client ID（`bridge/bridge.go:257`），服务端已持有完整的 `Client` 记录。组归属完全由服务端 Web 端配置即可，不需要客户端上报。

## 2. 现状约束（为什么原生做不到）

| 代码位置 | 事实 |
|---|---|
| `bridge/bridge.go:49-60` | `Bridge.Client` 是以 client ID 为 key 的 `sync.Map`，一个 ID 只有一个在线连接 |
| `bridge/bridge.go:312-323` | 同一 ID 再次连入会踢掉旧连接（`oldSignal.WriteClose()`） |
| `bridge/bridge.go:432-479` | `SendLinkInfo(clientId, ...)` 按 ID 建通道，找不到报 `the client %d is not connect` |
| `server/proxy/base.go:128` | 转发写死 `s.bridge.SendLinkInfo(client.Id, ...)` |
| `lib/file/obj.go:144-165` | `Tunnel.Client` 是单个 `*Client` 指针 |
| `lib/file/obj.go:212-235` | 原生「多目标」是一个客户端后挂多个内网地址轮询（`GetRandomTarget`），不是多客户端选一 |

⚠️ **绝不能让多台电脑共用一个 vKey**：后连者踢掉先连者，被踢的客户端 5 秒后重连（`client/client.go:112-143`），会陷入无限「抢椅子」，隧道持续抖动。每台电脑必须使用各自独立的 vKey。

## 3. 数据模型

在 `lib/file/obj.go` 增加三个字段：

```go
type Client struct {
    // ... 原有字段不变
    Group string `json:"Group"` // 高可用组名；空 = 普通客户端
}

type Tunnel struct {
    // ... 原有字段不变
    Client      *Client // 保留：代表客户端，用于持久化/列表过滤/兼容
    ClientGroup string  `json:"ClientGroup"`          // 非空时按组选路，优先于 Client
    GroupOnline int     `json:"GroupOnline,omitempty"` // 运行时展示字段，不参与业务
    GroupTotal  int     `json:"GroupTotal,omitempty"`
}
```

设计要点：

- `Client` 字段**不能删**：`LoadTaskFromJsonFile`（`lib/file/file.go:50`）用 `post.Client.Id` 恢复隧道，`GetTunnel`（`server/server.go:316`）用 `v.Client.Id` 过滤，非组模式的限流/黑名单也依赖它。
- 组模式下 `Client` 仍必填：Web 创建隧道时选一个「代表客户端」，仅作为持久化/列表过滤的兜底；**实际转发目标由 `ClientGroup` 决定**，流量、限流与并发数也按实际选中的成员统计。
- `GroupOnline/GroupTotal` 由 `GetTunnel` 每次请求时填充，供列表展示，不参与选路。

## 4. 选路设计

### 4.1 选路函数（`bridge/bridge.go`）

不建独立组索引，直接遍历 `JsonDb.Clients`。内网电脑数量很少，开销可忽略；好处是 Web 端改组后**立即生效**，无需在 `typeDeal` / `DelClient` 维护注册与注销钩子。

```go
var groupSequences sync.Map // map[string]*atomic.Uint64，每组独立轮询计数器

// targetIP 只解析 IP 字面量，不做 DNS。
// clientHasLocalIP 匹配客户端上报的 LocalAddr（兜底路径下可能是逗号分隔多 IP）。
// clientUsable 检查 Bridge.Client 中 signal 与 mux tunnel 是否可用。
func (s *Bridge) SelectClient(group, target string) (int, error) {
    if group == "" {
        return 0, errors.New("empty group")
    }
    candidates := make([]*file.Client, 0, 4)
    file.GetDb().JsonDb.Clients.Range(func(_, value any) bool {
        c := value.(*file.Client)
        c.RLock()
        match := c.Group == group && c.Status
        c.RUnlock()
        if match {
            candidates = append(candidates, c)
        }
        return true
    })
    if len(candidates) == 0 {
        return 0, fmt.Errorf("no client in group %q", group)
    }

    // 按 ID 排序：消除 sync.Map 顺序抖动，也作为亲和并列时的确定性兜底
    sort.Slice(candidates, func(i, j int) bool { return candidates[i].Id < candidates[j].Id })

    // 1) 目标 IP 亲和：target 的 IP 命中某成员 LocalAddr 时直接选它
    if ip := targetIP(target); ip != nil {
        for _, c := range candidates {
            if clientHasLocalIP(c, ip) && s.clientUsable(c.Id) {
                return c.Id, nil
            }
        }
    }

    // 2) 未命中则轮询：每组独立计数器
    start := int(groupSequence(group).Add(1)-1) % len(candidates)
    for i := range candidates {
        id := candidates[(start+i)%len(candidates)].Id
        if s.clientUsable(id) {
            return id, nil
        }
    }
    return 0, fmt.Errorf("all clients in group %q are offline", group)
}
```

并发自查（Go 专家检查清单）：

- `c.RLock()` 保护 `Group`/`Status`/`LocalAddr` 读；Web 写这些字段时必须 `c.Lock()`（见 5.1），否则 data race。
- `cl.mu` 保护 `signal`/`tunnel` 读；锁内不做 IO，`IsClose()` 放到锁外。
- `groupSequence` 用 `sync.Map` + `atomic.Uint64`，每组一个计数器，无锁竞争放大；无新增 goroutine、无 channel、无 context，不存在泄漏。
- 选择结果用 `logs.Trace` 打印 `group / target / selectedId / reason(affinity|rr)`，需要观察时调高日志级别。

### 4.2 选路前置与 `SendLinkInfo`

`server/proxy/base.go` 的 `NetBridge` 在原有 `SelectClient` / `SendLinkInfo` 之外新增 `ResolveClient`，用于返回选中的完整客户端记录：

```go
type NetBridge interface {
    SelectClient(group, target string) (int, error)
    ResolveClient(group, target string) (*file.Client, error)
    SendLinkInfo(clientId int, group string, link *conn.Link, t *file.Tunnel) (net.Conn, error)
}
```

组模式调用方先 `ResolveClient` 得到实际成员，再用该成员的 `Cnf` / `Rate` / `Flow` 建链和记账，最后以 `client.Id` + `group=""` 调用 `SendLinkInfo`。`bridge.SendLinkInfo` 保留 `group != ""` 的兼容选路分支，但正常组模式不会再走它，避免二次选路选到不同成员。

调用点改造清单：

| 文件 | 位置 | 处理方式 |
|---|---|---|
| `server/proxy/base.go` | `DealClient` | `ResolveClient` 后以 `group=""` 调 `SendLinkInfo`；流量/限流用实际成员 |
| `server/proxy/udp.go` | `runSession` | 同上，并把实际成员存入 `udpSession` 供后续包记账 |
| `server/proxy/socks5.go` | `doConnect` | 通过 `DealClient` 走同一套组模式选路与记账 |
| `server/proxy/http.go` | `:249` | `""`（Host 模式阶段一不支持） |
| `server/proxy/websocket.go` | `:123`、`:154` | `""` |
| `client/local.go` | `p2pBridge` 实现 + `:176` | 忽略组参数 |
| `server/proxy/tcp_test.go` | mock 实现 | 忽略组参数 |

### 4.3 运行时时序

1. 各电脑用**各自 vKey** 运行 npc → 服务端 `Bridge.Client` 登记，Web 显示在线。
2. 外网访问隧道端口 → `ResolveClient(group, target)` → 先做目标 IP 亲和（目标 IP 命中某成员上报的 `LocalAddr` 则直接选它），未命中再按组轮询找在线成员 → 用该成员建 Link、记账并调用 `SendLinkInfo(clientId, "", ...)` 建 mux 子连接 → 下发 link 信息 → 客户端 `handleChan` 连接内网目标。
3. 某电脑关机 → `ping()`（`bridge/bridge.go:482`）检测到 `tunnel.IsClose()` → `DelClient` 移除 → 下次新连接自动选其他成员。
4. 全部离线 → 返回明确错误，连接立即关闭；不 panic、不阻塞。

## 5. 后端改造清单

### 5.1 客户端增删改（`web/controllers/client.go`）

- `Add()`：构造 `file.Client` 时加 `Group: strings.TrimSpace(s.getEscapeString("group"))`。
- `Edit()`：写组字段必须加锁，与 `SelectClient` 的读锁配对：

```go
c.Lock()
c.Group = strings.TrimSpace(s.getEscapeString("group"))
c.Unlock()
```

- 新增 `Groups()` 方法，返回去重排序后的组名列表（`data` 保持字符串数组以兼容旧调用方），并在 `stats` 中附带每组的客户端总数与在线数，供下拉直接展示：

```go
func (s *ClientController) Groups() {
    stats := map[string]*haGroupStat{}
    file.GetDb().JsonDb.Clients.Range(func(_, v any) bool {
        c := v.(*file.Client)
        c.RLock()
        group, id, enabled := c.Group, c.Id, c.Status
        c.RUnlock()
        if group == "" {
            return true
        }
        st, ok := stats[group]
        if !ok {
            st = &haGroupStat{Name: group}
            stats[group] = st
        }
        st.Total++
        if enabled && server.Bridge != nil {
            if _, connected := server.Bridge.Client.Load(id); connected {
                st.Online++
            }
        }
        return true
    })
    names := make([]string, 0, len(stats))
    for name := range stats {
        names = append(names, name)
    }
    sort.Strings(names)
    detail := make([]haGroupStat, 0, len(names))
    for _, name := range names {
        detail = append(detail, *stats[name])
    }
    s.Data["json"] = map[string]any{"code": 1, "data": names, "stats": detail}
    s.ServeJSON()
}
```

路由无需改动：`web/routers/router.go:19/29` 已 `AutoRouter(&ClientController{})`，新方法自动映射为 `POST/GET /client/groups`。

### 5.2 隧道增删改（`web/controllers/index.go`）

- `Add()`（`:110`）：读取 `backend_type`（`single` / `group`）。group 模式校验组名非空且存在成员，写入 `t.ClientGroup`；`t.Client` 仍设为提交的 `client_id`（代表客户端），保证持久化兼容。
- `Edit()`（`:230`）：同样读写 `ClientGroup`；切换模式时清空另一字段。
- `Copy()`（`:171`）：`newTask.ClientGroup = oldTask.ClientGroup`，否则复制后丢组配置。
- `GetOneTunnel()`（`:218`）：已序列化整个 `Tunnel`，新字段自动带出，无需改。
- `GetTunnel()`（`server/server.go:310`）：填充 `GroupOnline/GroupTotal`，供列表展示。

### 5.3 校验（`web/controllers/validate.go`）

`validateTunnelInput` 增加：group 模式下 `group` 不能为空；组名限制 `^[A-Za-z0-9_-]{1,32}$`，避免特殊字符破坏前端渲染。

## 6. 前端改造清单

所有模板通过 `//go:embed views` / `//go:embed static` 打包（`web/embed.go:13-17`），**改完必须重新编译 nps 才会生效**。

### 6.1 客户端页

| 文件 | 改动 |
|---|---|
| `web/views/client/add.html` | 在「备注」后加「高可用组」下拉：默认「不加入组」，列出 `/client/groups` 返回的已有组（显示 `组名 (在线/总数)`），末项「＋ 新建组…」仅在建第一个成员时展开输入框。最终值写入隐藏字段 `name="group"` 供表单序列化 |
| `web/views/client/edit.html` | 同上，下拉预选当前组；当前组已无成员时也动态补回选项，避免保存时丢失配置 |
| `web/views/client/list.html` | 新增「组」列 `field:'Group'`；`detailFormatter` 显示组名（可选按组筛选） |

### 6.2 隧道页

| 文件 | 改动 |
|---|---|
| `web/views/index/add.html` | 在「客户端 ID」上方加「后端类型」单选（单客户端 / 客户端组）。选组时隐藏 `#client_id`、显示 `#client_group`（select，数据来自 `/client/groups`，同样显示 `组名 (在线/总数)`）。同步修改 `arr[o]` 映射：把 `client_id` 换成 `client_id_or_group`，在 `resetForm()` 中按后端类型切换可见性 |
| `web/views/index/edit.html` | 同上；`backend_type` 由 `{{.t.ClientGroup}}` 是否为空推导，组下拉预选原值 |
| `web/views/index/list.html` | 新增「后端」列：单客户端显示 `ID-备注`，组模式显示 `组名 (在线 x/y)`，数据取 `row.GroupOnline` / `row.GroupTotal` |

交互细节：

- 切换「后端类型」时立即重置表单可见性，避免提交残留字段导致误判。
- 组下拉为空时显示「请先在客户端管理中设置高可用组」并禁用提交。
- `client_id` 用隐藏字段保留提交（代表客户端 ID），确保 `t.Client` 非空、持久化兼容。
- 组模式下 `target` 填**所有成员都能访问到的同一内网地址**（如 `192.168.1.10:3389`），切换成员对访问方透明。

### 6.3 国际化（`web/static/page/languages.xml`）

新增 lang id（同时写 zh-CN / en-US）：

| id | 中文 | 英文 |
|---|---|---|
| `word-hagroup` | 高可用组 | HA group |
| `word-backendtype` | 后端类型 | Backend type |
| `word-backendsingle` | 单客户端 | Single client |
| `word-backendgroup` | 客户端组 | Client group |
| `info-hagroup` | 多台电脑填相同组名 | Same group name on multiple PCs |
| `word-grouponline` | 组内在线 | Online in group |

## 7. 用户交互流程（管理员视角）

1. **建客户端**：客户端管理 → 新增 3 个客户端，备注 PC-A / PC-B / PC-C，各自复制 vkey，「高可用组」统一填 `office-lan`。
2. **部署电脑**：每台电脑用**各自 vkey** 运行 npc，并装为服务开机自启。**不要共用 vkey**。
3. **建隧道**：隧道管理 → 新增 TCP 隧道，后端类型选「客户端组」= `office-lan`，端口 10000，目标 `192.168.1.10:3389`。
4. **观察状态**：隧道列表显示 `office-lan (在线 2/3)`；客户端列表可见各机在线状态。
5. **故障切换**：关掉一台电脑 → 新发起的连接自动落到在线成员；已建立的 RDP/SSH 会断，重连即可。
6. **扩容**：新增第 4 台电脑，建客户端填相同组名即可，无需改隧道。

## 8. 兼容性与边界

- `ClientGroup == ""` 完全走原逻辑，老配置/老隧道零影响。
- `clients.json` / `tasks.json` 新增字段默认空，旧文件可直接加载。
- 组不存在 / 全离线：返回明确错误，连接关闭；不 panic。
- 目标 IP 亲和只匹配 IP 字面量（不做 DNS 解析），依据是客户端上报的 `LocalAddr`；老客户端不上报时自动退化为纯轮询。亲和命中但目标机不在线时，回退到轮询选其他在线成员。
- `t.Client` 不能为 nil（`LoadTaskFromJsonFile` 依赖），所以组隧道仍需选代表客户端。
- 限流/黑名单：组模式在 `DealClient` / UDP `runSession` 中先调用 `ResolveClient` 选出实际成员，再按该成员执行流量上限、并发数和黑名单检查；非组模式仍沿用原有代表客户端判定路径。
- http/https 域名代理（Host 模式）阶段一不支持；阶段二给 `Host` 加 `ClientGroup` 并改 `http.go` / `websocket.go`。
- **已建立的 TCP 长连接不会迁移**（四层转发通病），切换只影响新连接。
- 多台电脑共用 vKey 会造成踢线抖动，Web 端应在组提示中明确警告。

## 9. 测试计划

- `bridge/bridge_test.go`（新增）：表驱动测试，构造 3 个客户端（A/C 在线、B 离线），断言轮询只落在 A/C；全离线返回错误；组名不存在返回错误；目标 IP 命中 `LocalAddr` 走亲和；命中但离线回退轮询；非 IP 目标跳过亲和；多 IP `LocalAddr` 任一命中；按 ID 排序的稳定轮转序列。
- `server/proxy`：组模式下 TCP/UDP/SOCKS5 流量只累加到实际转发成员，隧道流量正常累加，代表客户端不增长；确认 `SendLinkInfo` 不会二次选路。
- Web 层：`Groups()` 去重与排序；`Add`/`Edit` 组字段读写与切换清空。
- `go test -race ./bridge ./server/proxy ./lib/file ./web/...`：重点验证 `Client.Group` 读写无 data race。
- 手工验收：按第 7 节流程，关掉一台电脑后用**新连接**验证自动切换；确认旧隧道不受影响。

## 10. 分阶段实施

- **阶段一**：第 3~6 节全部改造，覆盖 tcp/udp/socks5，约 10 个文件。
- **阶段二（可选）**：Host 模式组支持、组内成员及当前选中状态的可视化展示。

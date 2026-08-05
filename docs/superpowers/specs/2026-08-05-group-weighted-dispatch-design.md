# 组内会话分流：余额加权 + 会话粘滞（`loadBalancingMode: weighted`）

日期：2026-08-05

## 1. 问题

组内多个账号无法分流，流量长期集中在单个账号上。

现状实测（`data/kiro_stats.json` + `data/kiro_balance_cache.json`，2026-08-05）。老竹组 7 个号，priority 1~7：

| id | priority | success_count | 余额已用 | last_used |
|---|---|---|---|---|
| 2 | 1 | 7323 | 26.25% | 08-05 07:14 |
| 12 | 2 | 0 | 0% | 08-03 |
| 10 | 3 | 0 | 0% | 从未 |
| 8 | 4 | 12 | 0.48% | 07-27 |
| 4 | 5 | 362 | 0.16% | 07-27 |
| 5 | 6 | 0 | 0.56% | 从未 |
| 11 | 7 | 0 | 0% | 从未 |

7 个号中 3 个从未被使用。

### 根因

生效配置为 `loadBalancingMode: "priority"`（`data/config.json:24`，亦为默认值）。该模式下：

1. `acquire_context_excluding`（`src/kiro/token_manager.rs:1648`）优先复用全局 `current_id`，只要它通过硬过滤就直接返回，不进入排序。
2. 未命中时 `select_next_credential_excluding`（`:1574`）取 `min_by_key(priority)`，恒定指向 priority 最小的号。

因此换号只由**故障**触发（402 quota / 401·403 连续失败 3 次 / 429 suspicious activity 冷却），从不由**余额**触发。

**余额数据完全不参与选路。** `BalanceSnapshot`（`src/admin/balance_store.rs:20`）只写入 DuckDB 与 admin 面板；选路代码从未读取。一个用了 99% 额度的号与一个 0% 的号，在选路器眼中权重相同。

### 现有 `balanced` 模式为何不能直接用

`balanced` 取 `min_by_key((success_count, priority))`。`success_count` 持久化在 `kiro_stats.json`，当前 id2=7323 而其余多为 0。直接切换会导致零计数账号被连续打满 7000+ 次才轮到 id2，属于冷启动踩踏。且 `success_count` 不按组隔离，跨组账号的排名会被其他组的流量污染。

此外「次数」不等于「额度」：id4 成功 362 次消耗 0.16%，id2 成功 7323 次消耗 26.25%，单次成本相差近一个量级。

## 2. 目标与非目标

### 目标

- 组内流量按**剩余额度**加权分流，使各账号额度消耗趋于同步。
- 同一会话的多轮请求尽量落在同一账号，保住上游 prompt cache。
- 默认行为零变化，可热切换、可热回退。

### 非目标

- 不修改 `MAX_TOTAL_RETRIES = 4`（`src/kiro/provider.rs:33`）。它与分流策略正交——被冷却或禁用的账号不进入候选集，加权分流不提高撞到坏号的概率。单独改动。
- 不改动 `priority` 与 `balanced` 两个既有模式的任何行为。

## 3. 决策记录

| 编号 | 决策 | 备选与否决理由 |
|---|---|---|
| D1 | 均衡口径按**剩余额度**加权 | 按请求次数均衡与实际额度消耗不成正比（实测差近一个量级）；按本地 token 计量需先验证与上游计费口径一致 |
| D2 | **启用会话粘滞** | 不粘滞会打散上游 prompt cache，与既有 cache metering 工作直接冲突 |
| D3 | 落地为**第三种 mode** `weighted` | 改造现有 `balanced` 会改变已有语义且无法 A/B 对比 |
| D4 | 粘滞 key 复用 `cache_metering::isolation_seed` 的三级降级链 | 该逻辑已在生产运行，且其第二级 `stable_cache_control_hash` 已规避「message 级断点每轮漂移」（`src/anthropic/cache_metering.rs:580`） |
| D5 | 加权算法用**平滑加权轮询 SWRR** | `argmax(remaining)` 在余额缓存冻结的 300s 内退化为单账号粘滞，等于换皮不治病；加权随机不可复现，线上倾斜时无法从状态复算原因 |
| D6 | 余额不可得时**分级降级**：全组不可得→等权；个别缺失→取组内已知余额中位数；过期→照用 | 缺失取 0 会形成死锁反馈环（不被选中→无余额数据→权重恒为 0）；取满额会让新号瞬间独吞流量 |
| D7 | 粘滞空闲过期 **30 分钟**，容量 **10000 条 + LRU**，**不持久化** | 重启后上游 cache 状态不可知，恢复粘滞是假精确；无容量上限则刷 UUID 的客户端可打爆内存 |
| D8 | 新逻辑抽出独立模块 `src/kiro/dispatch.rs` | `token_manager.rs` 已 5440 行；且 `:1725` 已有显式死锁警告注释，再加锁需重审全局锁序。独立模块可脱离 tokio 与凭据构造做纯单测 |

### D5 补充：为何 `argmax(remaining)` 不可行

`BALANCE_CACHE_TTL_SECS = 300`（`src/admin/service.rs:43`），后台刷新间隔同为 300s（`src/main.rs:521`）。两次刷新之间 `remaining` 是一组冻结常量，`argmax` 恒定指向同一账号，5 分钟内的全部请求集中于一个号——与当前病状相同。

SWRR 不受影响：权重不变只意味着分流**比例**不变，轮转照常进行。

## 4. 架构

### 4.1 `src/admin/balance_cache.rs`（新增）

将 `AdminService.balance_cache`（`src/admin/service.rs:180`）抽出为独立的 `Arc<BalanceCache>`：

- 内部仍为 `parking_lot::Mutex<HashMap<u64, CachedBalance>>`，启动时从 `data/kiro_balance_cache.json` 加载并按 TTL 过滤（沿用 `service.rs:2692` 的 `load_balance_cache_from`）。
- `main.rs` 中先于二者创建，同时注入 `AdminService`（写方）与 `GroupDispatcher`（读方）。
- **`start_balance_refresher` 从 `main.rs:521` 的 admin 分支中移出，无条件启动。** 否则未配置 `admin_api_key` 的部署权重永远为空。

模块位置说明：`src/kiro/` 已依赖 `src/admin/`（`token_manager.rs:19` 引 `trace_db`，`provider.rs:135` 持有 `admin::ops::SharedOpsRuntime`），故放在 `src/admin/` 不产生新的依赖方向。

### 4.2 `src/kiro/dispatch.rs`（新增）

```rust
pub struct Candidate { pub id: u64, pub priority: i32 }

pub struct GroupDispatcher {
    sticky:  Mutex<HashMap<String, StickyEntry>>,    // 上限 10000，见下方淘汰策略
    weights: Mutex<HashMap<(String, u64), i64>>,     // SWRR current_weight
    balance: Arc<BalanceCache>,
}

struct StickyEntry { cred_id: u64, last_seen: Instant }

impl GroupDispatcher {
    /// candidates 必须是调用方过完硬过滤的结果，且非空。本函数不做任何过滤。
    /// now 由调用方传入，便于测试推进时钟。
    pub fn pick(&self, group: Option<&str>, candidates: &[Candidate],
                sticky_key: Option<&str>, now: Instant) -> u64;
}
```

**淘汰策略不引入 `lru` 依赖**（当前 `Cargo.toml` 无此 crate）。插入时若 `len >= 10000`，先清除全部已超 30min idle 的条目；若仍满，再 O(n) 扫描淘汰 `last_seen` 最早的一条。正常负载下 30min 过期会使表远小于 10000，O(n) 路径基本不触发。以此换掉一个新依赖。

**SWRR 平局的确定性打破**：`argmax(current_weight)` 出现相同值时，依次按 `priority` 升序、`id` 升序取第一个。这是 `Candidate.priority` 字段的唯一用途——保证选路结果可复算，不依赖 HashMap 迭代顺序。

两处刻意的分组隔离，用于修复已确认的既有缺陷：

1. **SWRR 权重按 `(group, id)` 分桶**，而非按 `id`。现有 `success_count` 为全局计数，跨组账号排名会被其他组流量污染。
2. **粘滞 key 前缀带 group**，即 `format!("{}|{}", group.unwrap_or(""), seed)`。否则同一 session 经不同 client key 打到不同组时会反复覆写同一条记录。

### 4.3 `token_manager.rs` 改动面

限于 `select_next_credential_excluding`（`:1574`）末尾 `match mode` 增加一个分支，硬过滤链不动：

```rust
"weighted" => {
    let cands: Vec<Candidate> = available.iter().map(|e| Candidate {
        id: e.id,
        priority: e.credentials.priority.unwrap_or(i32::MAX),
    }).collect();
    let id = self.dispatcher.pick(group, &cands, sticky_key, now);
    // 按 id 取回对应 entry
}
```

`priority` 与 `balanced` 分支保持字节级不变——这是可回退的前提。

`weighted` 模式下全局 `current_id` 不参与选路，由粘滞表取代。粘滞表按 `(group, session)` 分桶，不存在现有 `current_id` 的跨组串扰问题。

## 5. 数据流

```
post_messages (src/anthropic/handlers.rs:923)     此处有 payload 与 key_ctx
  └─ sticky_key = dispatch_key(&payload, key_ctx.key_id)
       复用 isolation_seed 三级降级：
       "sess:<uuid>"  (metadata.user_id → extract_session_id)
         → "cc:<稳定断点 hash>"  (stable_cache_control_hash)
           → "key:<key_id>"
  └─ ResponseProcessingConfig { group, sticky_key, .. }   新增字段 (handlers.rs:166)
      └─ call_api_stream(body, tracer, group, sticky_key) 新增参数 (provider.rs:337)
          └─ call_api_with_retry(..., sticky_key)          照 pinned 参数既有先例
              └─ acquire_context_excluding(model, group, excluded, sticky_key)
                  └─ select_next_credential_excluding(...)
                      └─ [硬过滤: disabled / throttled / group / model / opus]  不变
                          └─ dispatcher.pick(group, &candidates, sticky_key, now)
```

`pick` 内部：

```
1. 查粘滞：key 命中 且 未超 30min idle 且 cred_id ∈ candidates
     → 刷新 last_seen，返回该 id                    （粘滞路径）
2. 否则 SWRR：
     for id in candidates: w[(g,id)] += weight(id)
     winner = argmax w
     w[(g,winner)] -= Σ weight(id)
   写入粘滞表，返回 winner                          （分流路径）
```

`weight(id)` 按 D6 降级：读余额取 `remaining`（过期照用）→ 缺失取组内已知余额的中位数 → 全组不可得或全为 0 则一律取 1（等权 SWRR）。

### 锁序约束

`pick` 在 `select_next_credential_excluding` 内被调用，此时已持有 `entries.lock()` 与 `credential_support.read()`（`token_manager.rs:1725` 有显式死锁警告注释）。因此：

- dispatcher 的两把锁必须是**最内层**；
- `pick` 全程不做 IO、不 `await`、不回调 token_manager。

`BalanceCache` 的读取为纯内存 `parking_lot` 加锁，满足该约束。

## 6. 错误处理与降级

| 情形 | 处理 | 理由 |
|---|---|---|
| `candidates` 为空 | `pick` 不会被调用（`:1610` 已 `return None`） | 保持前置不变量：调用方保证非空，函数内不做防御性兜底 |
| 粘滞命中的号已被排除/禁用/冷却 | 丢弃粘滞，走 SWRR 重选，并用新号覆写粘滞记录 | 会话应整体迁移到新号，而非每轮重试老号 |
| 同一请求内重试换号 | 老号在 `excluded` 中 → 落入上一行 → 自动迁移 | 无需为重试写特殊逻辑 |
| `sticky_key = None`（MCP 路径 `provider.rs:382`、admin 探针） | 不查表也不写表，纯 SWRR | 探针流量不应污染真实会话的粘滞表 |
| LRU 淘汰活跃会话 | 退化为重新分配，不报错 | 容量上限是防爆内存的兜底，非正常路径 |
| `loadBalancingMode` 值非法 | 落入既有 `_` 分支 = `priority` | 与现状一致，不新增失败模式 |
| 冷却结束后会话不迁回原号 | 接受，不迁回 | cache 已在新号建立，迁回等于再丢一次 |
| 组内所有 `remaining` 均为 0（月底耗尽） | 退化等权，不除零、不返回空集 | 请求照常发出，由上游返回 402 走既有 quota-exhausted 路径 |

## 7. 测试

`dispatch.rs` 不依赖 tokio、不依赖凭据构造，核心行为为纯单测。`pick` 接受 `now: Instant` 参数以便推进时钟，不引入时钟 trait。

1. **分流比例**：权重 7000/3000 的两个号跑 1000 次，命中比例为 7:3。SWRR 确定性，断言精确值。
2. **等权退化**：全组无余额数据，N 个号跑 N 次，每个恰好命中一次。
3. **中位数降级**：3 个号已知余额 + 1 个缺失，验证缺失号有效权重等于中位数，既非 0 亦非 max。
4. **全零不炸**：所有 `remaining = 0` 时不 panic，退化等权。
5. **粘滞命中**：同一 key 连续调用恒返回同一 id。
6. **粘滞失效**：目标 id 不在 `candidates` 时换号，且粘滞表更新为新号。
7. **idle 过期**：`now` 推进 31 分钟后同一 key 重新分配。
8. **LRU 容量**：插入 10001 条，最早的被淘汰。
9. **分组隔离**：同一 id 挂在两组，两组 `current_weight` 互不影响。
10. **`sticky_key = None` 不写表**。

回归：`token_manager` 现有测试全绿，并补断言——`priority` / `balanced` 模式下选择结果与改动前逐位一致。

## 8. 配置与上线

- `loadBalancingMode` 增加取值 `"weighted"`。**默认仍为 `priority`**，现有部署行为零变化。
- admin UI 下拉增加一项（`admin-ui/src/components/topbar-tools.tsx:236`）。
- 生效方式：`PUT /api/admin/load-balancing` 热切换，不重启（既有能力，`src/admin/service.rs:2404`）。
- 回退：切回 `priority`。内存中的粘滞表与权重表留存，不影响任何行为。

### 验证口径

`kiro_stats.json` 的 `success_count` 是**累计值**（id2 已 7323）。验证必须取切换前后的**增量分布**，直接看绝对值在数万次请求内仍会显示 id2 一枝独秀。

判定标准：

1. 切换后新增请求中，组内各号的增量份额与其余额份额的偏差在可解释范围内；
2. `kiro_balance_cache.json` 中各号的 `usagePercentage` 开始收敛而非继续发散。

## 9. 待实现阶段确认

`weight()` 函数的最终形态由本人编写，涉及运营判断而非技术选型：

- 中位数降级的具体计算；
- 全零退化的处理；
- 权重是否设下限——防止接近耗尽的账号被彻底饿死，连探活流量都拿不到。

实现时先搭好 `dispatch.rs` 骨架与函数签名并留 TODO。

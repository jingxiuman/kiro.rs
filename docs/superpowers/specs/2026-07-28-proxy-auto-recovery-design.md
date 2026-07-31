# 代理池自动恢复探针 —— 设计

日期：2026-07-28

## 1. 问题

代理被自动禁用后**永远不会自己回来**。`check_all`（`proxy_pool.rs:415`）筛选探测目标时用
`.filter(|e| e.enabled)`，而两条自动禁用路径都把 `enabled` 置为 `false`：

- 探测失败：连续 3 次（`MAX_PROXY_PROBE_FAILURES`）→ `proxy_pool.rs:373`
- 请求级失败：连续 5 次（`MAX_PROXY_REQUEST_FAILURES`）→ `proxy_pool.rs:492`

两条路径都会同时置 `auto_disabled = true`，这是区分「自动禁用」与「用户手动禁用」的唯一依据。

恢复目前只有人工一条路：管理端重新启用，`set_enabled(id, true)` 清零三个计数。

## 2. 目标与非目标

**目标**：被**自动**禁用的代理，在上游链路恢复后能自动放回可分配池。

**非目标**：
- 不恢复绑定拓扑。当初被 `ops.rs:632` `reassign_after_disable` 换绑走的凭据留在新代理上。
  恢复的是「可用性」，不是「谁绑在谁上」。
- 不自动放回**用户手动禁用**的代理。这是硬边界。
- 不引入配置项。沿用既有常量风格，等有实际调参需求再说。

## 3. 接法：并进主健康检查循环

在 `check_all` 里一次同时探「在线的」与「待恢复的」。两个集合天然不相交
（`enabled` vs `auto_disabled && !enabled`），共用一次 `join_all`、一次 `persist()`、
一把 `check_in_progress` 重入锁。

否决的替代方案：单开一条恢复探针 task。参数已定为与主检查同频（5 分钟），
独立 task 的唯一优势（独立节奏）不存在，却带来两个 task 并发写 `proxy_pool.json` 的问题。

## 4. 状态

`ProxyEntry` 已有 11 个字段。恢复逻辑自成一体，收进嵌套子结构，便于独立测试：

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RecoveryState {
    /// 连续探测成功次数。放回后清零；中途任何一次失败也清零。
    pub consecutive_successes: u32,
    /// 退避档位。探针放回后又被自动禁用则递增。
    pub backoff_level: u32,
    /// 下次允许探测的时间（RFC3339）。None = 立即可探。
    pub next_probe_at: Option<String>,
    /// 最近一次被探针放回的时间，用于判定「是不是同一场故障」。
    pub last_recovered_at: Option<String>,
}
```

挂到 `ProxyEntry` 上：`#[serde(default)] pub recovery: RecoveryState`。
`default` 保证现有 `proxy_pool.json`（无此字段）能原样加载。

## 5. 参数

| 参数 | 取值 | 常量名 |
|---|---|---|
| 探测间隔（基础） | 300 秒，与主检查同频 | `PROXY_RECOVERY_BASE_INTERVAL_SECS` |
| 放回阈值 | 连续成功 2 次 | `PROXY_RECOVERY_SUCCESSES` |
| 退避上限 | 4 小时 | `PROXY_RECOVERY_MAX_INTERVAL_SECS` |
| 同场故障判定窗口 | 24 小时 | `PROXY_RECOVERY_INCIDENT_WINDOW_SECS` |

退避间隔 = `min(300 × 2^backoff_level, 14400)`。
level 0→5m、1→10m、2→20m、3→40m、4→80m、5→160m、6 及以上→240m（封顶）。

## 6. 行为

### 6.1 探测目标筛选

每轮 `check_all`：

- 在线集合：`enabled`（不变）
- 恢复集合：`auto_disabled && !enabled` **且** `next_probe_at` 已到（`None` 视为已到）

两个集合合并成一次并发探测。`targets.is_empty()` 的早退判断要覆盖两个集合。

### 6.2 探测结果应用（恢复集合）

- 成功 → `consecutive_successes += 1`；未达 2 次则写 `next_probe_at = now + 退避间隔`。
- 失败 → `consecutive_successes = 0`，`next_probe_at = now + 退避间隔`。
- 达到 2 次 → **放回**，终态：
  - `enabled = true`、`auto_disabled = false`
  - `consecutive_failures = 0`
  - **`request_failures = 0`**（不清零的话，恢复后第一个真实请求失败就直接触及
    `MAX_PROXY_REQUEST_FAILURES = 5` 的旧计数）
  - `recovery.consecutive_successes = 0`、`next_probe_at = None`
  - `recovery.last_recovered_at = now`
  - `backoff_level` **保持不变**（下次再挂时才根据窗口决定升降）

### 6.3 退避档位推进

发生在**自动禁用**时（两条路径共用）：

- `last_recovered_at` 存在且距今 < 24 小时 → `backoff_level += 1`（上次恢复是假的，抖动还在）
- `last_recovered_at` 不存在，或距今 ≥ 24 小时 → `backoff_level = 0`（独立事件，重新开始数）

置 0 而不是 1：level 0 对应 5 分钟，即「与主检查同频」这个既定参数。若新故障从 level 1
起步，首次恢复探测要等 10 分钟，与参数表矛盾。

置 `next_probe_at = now + 新档位对应的间隔`。

### 6.4 人工介入即重置

`set_enabled(id, true)` 在既有的三个计数清零之外，把整个 `RecoveryState` 恢复默认值。

### 6.5 事件

`ops.rs` 新增 `handle_probe_auto_recover(proxy_id, url)`，与 `handle_probe_auto_disable`
对称：记 `PROXY_AUTO_RECOVER` 事件（level `info`）。**不碰凭据绑定。**

`CheckSummary` 新增 `newly_recovered: Vec<(u64, String)>`，`service.rs` 的健康检查循环
按这个列表调 ops，与既有 `newly_disabled` 的处理方式一致。

## 7. 落点

- `src/admin/proxy_pool.rs`：`RecoveryState`、常量、`recovery_backoff_interval()`、
  探测目标筛选、`apply_recovery_probe_result()`、自动禁用两处的档位推进、`set_enabled` 清理、
  `CheckSummary` 扩展。
- `src/admin/service.rs`：健康检查循环处理 `newly_recovered`。
- `src/admin/ops.rs`：`handle_probe_auto_recover` 与事件分类常量。

`GET /api/admin/proxy-pool` 的响应会自动多出 `recovery` 字段（`ProxyEntry` 本身就是
序列化返回的）。前端不改也不会坏。

## 8. 测试

纯逻辑，不打网络：

1. 用户手动禁用（`auto_disabled == false`）永远不进恢复集合。
2. 连续成功 1 次不放回，第 2 次才放回。
3. 中途失败清零 `consecutive_successes`。
4. 放回终态：`enabled` / `auto_disabled` / `consecutive_failures` / `request_failures` /
   `consecutive_successes` / `next_probe_at` / `last_recovered_at` 七项逐一断言。
5. 24 小时内再次自动禁用 → 档位 +1；超过 24 小时 → 档位重置为 1。
6. 退避间隔封顶 4 小时。
7. `next_probe_at` 未到时本轮跳过该条目。
8. `set_enabled(id, true)` 清空整个 `RecoveryState`。
9. 旧 `proxy_pool.json`（无 `recovery` 字段）能反序列化。

## 9. 风险

放回后的代理立刻进入 `assignable_urls()`，会被后续的凭据分配选中。若上游只是短暂恢复，
生产流量会打到一个不稳定的代理上——这正是退避存在的理由，但第一次放回无法避免。
`MAX_PROXY_REQUEST_FAILURES = 5` 是这条路径上的兜底。

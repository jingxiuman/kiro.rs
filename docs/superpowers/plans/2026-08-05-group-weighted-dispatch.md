# 组内会话分流（weighted 模式）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `loadBalancingMode: "weighted"`，组内按「有效剩余额度 = 缓存余额 − 本代次已消耗 credits」选号，并按会话粘滞以保住上游 prompt cache。

**Architecture:** 选号逻辑抽入新模块 `src/kiro/dispatch.rs`（`GroupDispatcher`），余额缓存从 `AdminService` 抽出为共享的 `src/admin/balance_cache.rs`（带 generation 原子发布）。`token_manager` 的硬过滤链不变，只在过滤后构造候选快照、释放锁、再调 `dispatcher.pick()`。请求结束后经 `UsageRecordHook` 回写实际消耗 credits，形成闭环。

**Tech Stack:** Rust 2024 edition、parking_lot、tokio、axum、DuckDB（仅统计侧，不在选路热路径）。

## Global Constraints

- **分支**：全部在当前分支 `feat/absorb-upstream-0.7.5` 上开发提交。
- **提交卫生（Task 1 已踩过一次，务必照做）**：每次提交必须 `git add` 显式列出本任务改动的文件，禁止 `git add -A` / `git add .`。

  **但这还不够。** `git add <file>` 是**文件粒度**的，无法排除同一文件里他人未提交的 hunk。Task 1 就因此把整个 RPM 限流特性的后端半截卷进了提交 `6497355`，导致该提交无法独立构建（`service.rs` import 了当时尚未提交的 `types.rs` 里的类型）。已由 `75339ad` 补齐收尾。

  因此**每个任务提交前必须先自检**：

  ```bash
  git add <本任务文件>
  git diff --cached --stat          # 文件清单是否与预期一致
  git diff --cached | grep -nE '<本任务无关的特征符号>'   # 应无输出
  ```

  发现混入他人改动时**停下来报告**，不要自行提交，也不要 revert 他人改动。

- **当前工作区剩余在制品**（截至 `75339ad`）：`src/kiro/provider.rs`、`src/anthropic/websearch_loop.rs`，内容是 WebSearch / MCP 的 `ensure_profile_arn` 修复。**Task 5 要改 `provider.rs`，会撞上它**——该任务开工前需先确认这批在制品的处置。
- **默认行为零变化**：`loadBalancingMode` 默认仍为 `"priority"`；`priority` 与 `balanced` 两个分支的选号结果必须与改动前逐位一致。
- **锁序**：`dispatcher.pick()` 内部只允许持有 `DispatchState` 一把锁，且调用它之前必须已释放 `entries` 与 `credential_support`。`pick` 内不得做 IO、不得 `await`、不得回调 `token_manager`。
- **余额单位**：`credits`，与 `BalanceResponse.remaining` / `usageLimit`（10000）同量纲，已由生产数据验证（误差 <2%）。
- **`MAX_STALE`**：3600 秒。**空闲过期**：30 分钟。**粘滞表容量**：10000。
- 不引入新的 crate 依赖（特别是不引入 `lru`）。
- 规格文档：`docs/superpowers/specs/2026-08-05-group-weighted-dispatch-design.md`。

## 文件结构

| 文件 | 职责 |
|---|---|
| `src/kiro/dispatch.rs`（新建） | `GroupDispatcher`：粘滞表 + 本代次消耗表 + 选号算法。不依赖 tokio、不依赖凭据构造，可纯单测 |
| `src/admin/balance_cache.rs`（新建） | `BalanceCache`：余额快照的共享只读视图，generation 原子发布，锁外写盘 |
| `src/kiro/token_manager.rs` | 模式枚举化；`weighted` 分支构造候选快照后委托 dispatcher |
| `src/kiro/provider.rs` | 透传 `sticky_key` |
| `src/anthropic/handlers.rs` | 产生 `sticky_key`；`UsageRecordHook` 回写消耗 |
| `src/admin/service.rs` | 改用共享 `BalanceCache`；模式白名单接受 `weighted` |
| `src/model/config.rs` | 无需改结构（`load_balancing_mode` 已是 String），仅文档注释 |
| `admin-ui/src/components/topbar-tools.tsx` | 下拉增加 `weighted` 选项 |

---

### Task 1: 模式枚举化 + `weighted` 接入（致命集成项）

当前 `weighted` 连开关都打不开，打开了也会被 `current_id` 快路径静默绕过。本任务只做接入，选号仍暂时复用 `balanced` 的逻辑，**目的是先让「weighted 不被 current_id 绕过」这条能被测试锁住**。

**Files:**
- Modify: `src/kiro/token_manager.rs`（`select_next_credential_excluding` 的 match、`acquire_context_excluding` 的 `is_balanced`、`set_load_balancing_mode` 的校验）
- Modify: `src/admin/service.rs`（`set_load_balancing_mode` 校验、`exposed_current_id`）
- Test: `src/kiro/token_manager.rs` 的 `mod tests`

**Interfaces:**
- Produces: `pub(crate) enum LoadBalancingMode { Priority, Balanced, Weighted }`，`impl LoadBalancingMode { fn parse(s: &str) -> Self; fn as_str(&self) -> &'static str; fn is_dynamic(&self) -> bool }`。`is_dynamic()` 对 `Balanced` 与 `Weighted` 返回 `true`，表示「每次请求重新选号，不固定 `current_id`」。

- [ ] **Step 1: 写失败的集成测试**

在 `src/kiro/token_manager.rs` 的 `mod tests` 中新增。参照同文件已有测试的凭据构造方式（搜索 `fn manager_with` 或既有多凭据测试用例，复用其 helper）：

```rust
#[tokio::test]
async fn weighted_mode_ignores_current_id_fast_path() {
    // 两个可用凭据，预设 current_id 指向第一个
    let manager = test_manager_with_two_credentials();
    manager.set_load_balancing_mode("weighted".to_string()).unwrap();
    let first = manager.acquire_context(None, None).await.unwrap().id;

    // weighted 下连续取号必须能选到另一个，而不是恒定返回 current_id
    let mut seen = std::collections::HashSet::new();
    for _ in 0..10 {
        seen.insert(manager.acquire_context(None, None).await.unwrap().id);
    }
    assert!(
        seen.len() > 1,
        "weighted 模式被 current_id 快路径绕过：10 次取号只出现了 {:?}",
        seen
    );
    let _ = first;
}

#[test]
fn weighted_is_accepted_by_mode_validation() {
    let manager = test_manager_with_two_credentials();
    assert!(manager.set_load_balancing_mode("weighted".to_string()).is_ok());
    assert_eq!(manager.get_load_balancing_mode(), "weighted");
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test weighted_mode_ignores_current_id_fast_path weighted_is_accepted -- --nocapture`
Expected: `weighted_is_accepted_by_mode_validation` FAIL（`set_load_balancing_mode` 返回 `无效的负载均衡模式: weighted`）；`weighted_mode_ignores_current_id_fast_path` FAIL（unwrap panic 或 `seen.len() == 1`）

- [ ] **Step 3: 新增模式枚举**

在 `src/kiro/token_manager.rs` 中（放在 `MultiTokenManager` 定义之前）：

```rust
/// 负载均衡模式。序列化仍用原字符串，兼容既有 config.json。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadBalancingMode {
    Priority,
    Balanced,
    Weighted,
}

impl LoadBalancingMode {
    /// 未知取值一律落到 Priority——与改动前 `match mode { .. _ => priority }` 的行为一致。
    pub(crate) fn parse(s: &str) -> Self {
        match s {
            "balanced" => Self::Balanced,
            "weighted" => Self::Weighted,
            _ => Self::Priority,
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Priority => "priority",
            Self::Balanced => "balanced",
            Self::Weighted => "weighted",
        }
    }

    /// 动态选号模式：每次请求重新选，不固定 current_id。
    pub(crate) fn is_dynamic(&self) -> bool {
        matches!(self, Self::Balanced | Self::Weighted)
    }
}
```

- [ ] **Step 4: 三处白名单接受 weighted**

`src/kiro/token_manager.rs` 的 `set_load_balancing_mode`（当前在 `:3729` 附近），把

```rust
if mode != "priority" && mode != "balanced" {
    anyhow::bail!("无效的负载均衡模式: {}", mode);
}
```

改为

```rust
if !matches!(mode.as_str(), "priority" | "balanced" | "weighted") {
    anyhow::bail!("无效的负载均衡模式: {}", mode);
}
```

`src/admin/service.rs` 的 `set_load_balancing_mode`（当前在 `:2417` 附近），把

```rust
if req.mode != "priority" && req.mode != "balanced" {
    return Err(AdminServiceError::InvalidCredential(
        "mode 必须是 'priority' 或 'balanced'".to_string(),
    ));
}
```

改为

```rust
if !matches!(req.mode.as_str(), "priority" | "balanced" | "weighted") {
    return Err(AdminServiceError::InvalidCredential(
        "mode 必须是 'priority'、'balanced' 或 'weighted'".to_string(),
    ));
}
```

- [ ] **Step 5: `current_id` 快路径对 weighted 也跳过**

`src/kiro/token_manager.rs` 的 `acquire_context_excluding`（当前在 `:1829` 附近），把

```rust
let is_balanced = self.load_balancing_mode.lock().as_str() == "balanced";

// balanced 模式：每次请求都重新均衡选择，不固定 current_id
// priority 模式：优先使用 current_id 指向的凭据
let current_hit = if is_balanced {
```

改为

```rust
let mode = LoadBalancingMode::parse(self.load_balancing_mode.lock().as_str());

// balanced / weighted：每次请求都重新选号，不固定 current_id
// priority：优先使用 current_id 指向的凭据
let current_hit = if mode.is_dynamic() {
```

- [ ] **Step 6: `weighted` 暂时复用 balanced 的选号**

`src/kiro/token_manager.rs` 的 `select_next_credential_excluding`（当前 `:1770` 附近）把

```rust
match mode {
    "balanced" => {
```

改为

```rust
match LoadBalancingMode::parse(mode) {
    // Weighted 在 Task 7 接入 dispatcher；在此之前与 Balanced 同行为，
    // 以便 Task 1 的集成测试能独立锁住「不被 current_id 绕过」这一条。
    LoadBalancingMode::Balanced | LoadBalancingMode::Weighted => {
```

并把下面的 `_ =>` 分支改为 `LoadBalancingMode::Priority =>`。

- [ ] **Step 7: admin 侧隐藏 current_id**

`src/admin/service.rs`（当前 `:657` 附近）把

```rust
let exposed_current_id = if self.token_manager.get_load_balancing_mode() == "balanced" {
```

改为

```rust
let exposed_current_id = if matches!(
    self.token_manager.get_load_balancing_mode().as_str(),
    "balanced" | "weighted"
) {
```

- [ ] **Step 8: 运行测试确认通过**

Run: `cargo test weighted_ -- --nocapture`
Expected: 两条新测试 PASS

- [ ] **Step 9: 回归**

Run: `cargo test load_balanc -- --nocapture && cargo test token_manager:: 2>&1 | tail -20`
Expected: 全部 PASS，无既有测试被破坏

- [ ] **Step 10: 提交**

```bash
git add src/kiro/token_manager.rs src/admin/service.rs
git commit -m "feat(dispatch): 模式枚举化并接入 weighted 取值

weighted 此前无法启用：三处白名单只认 priority/balanced，
且 current_id 快路径只对 balanced 短路，weighted 会被静默绕过。
本次仅做接入与枚举化，选号暂与 balanced 同行为。"
```

---

### Task 2: 抽出共享 `BalanceCache`（generation 原子发布）

**Files:**
- Create: `src/admin/balance_cache.rs`
- Modify: `src/admin/mod.rs`（挂载模块）
- Modify: `src/admin/service.rs`（`balance_cache` 字段改为 `Arc<BalanceCache>`；`refresh_all_balances` 改为整轮收集后原子发布；`save_balance_cache` 改为锁外写盘）
- Test: `src/admin/balance_cache.rs` 的 `mod tests`

**Interfaces:**
- Consumes: `BalanceResponse`（既有类型，见 `src/admin/types.rs`）
- Produces:
```rust
pub struct BalanceCache { /* .. */ }
pub struct BalanceSnapshotView { pub generation: u64, pub entries: HashMap<u64, CachedBalance> }
pub struct CachedBalance { pub cached_at: f64, pub data: BalanceResponse }  // 从 service.rs 移过来，改为 pub
impl BalanceCache {
    pub fn new(path: Option<PathBuf>) -> Self;              // 启动时按 TTL 过滤加载
    pub fn snapshot(&self) -> BalanceSnapshotView;          // 读快照，立即释放锁
    pub fn publish(&self, entries: HashMap<u64, CachedBalance>);  // 整轮原子发布，generation += 1
    pub fn upsert_one(&self, id: u64, entry: CachedBalance);      // 单点更新（get_balance 读时刷新用），不动 generation
}
pub type SharedBalanceCache = Arc<BalanceCache>;
```

- [ ] **Step 0: 给 `BalanceResponse` 加 `Default` 派生**

本任务的测试 helper 要用 `BalanceResponse { remaining, ..Default::default() }`，否则每处都得写全 10 个字段。`src/admin/types.rs:317` 当前是

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceResponse {
```

改为

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceResponse {
```

所有字段（`u64` / `f64` / `Option<_>`）都实现了 `Default`，纯附加改动。

Run: `cargo build 2>&1 | tail -5` → 应编译通过。

- [ ] **Step 1: 写失败的测试**

新建 `src/admin/balance_cache.rs`，先只写测试模块。注意 `next_reset_at` 字段的类型是 `Option<f64>`：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn cb(cached_at: f64, remaining: f64) -> CachedBalance {
        CachedBalance { cached_at, data: BalanceResponse { remaining, ..Default::default() } }
    }

    #[test]
    fn publish_bumps_generation_and_replaces_all() {
        let c = BalanceCache::new(None);
        assert_eq!(c.snapshot().generation, 0);

        c.publish(HashMap::from([(1, cb(100.0, 9000.0))]));
        let s1 = c.snapshot();
        assert_eq!(s1.generation, 1);
        assert_eq!(s1.entries.len(), 1);

        // 整轮发布是替换而非合并：上一轮的 id=1 不应残留
        c.publish(HashMap::from([(2, cb(200.0, 8000.0))]));
        let s2 = c.snapshot();
        assert_eq!(s2.generation, 2);
        assert!(s2.entries.contains_key(&2));
        assert!(!s2.entries.contains_key(&1), "整轮发布必须替换而非合并");
    }

    #[test]
    fn upsert_one_does_not_bump_generation() {
        let c = BalanceCache::new(None);
        c.publish(HashMap::from([(1, cb(100.0, 9000.0))]));
        let before = c.snapshot().generation;
        c.upsert_one(1, cb(150.0, 8500.0));
        let after = c.snapshot();
        assert_eq!(after.generation, before, "单点更新不得推进 generation");
        assert_eq!(after.entries[&1].data.remaining, 8500.0);
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test balance_cache:: -- --nocapture`
Expected: 编译失败（`BalanceCache` 未定义）

- [ ] **Step 3: 实现 `BalanceCache`**

```rust
//! 余额快照的共享只读视图。
//!
//! 从 AdminService 抽出的动机：选路需要读余额，而 AdminService 持有
//! Arc<MultiTokenManager>，所有权是单向的，token_manager 侧看不到它。
//! 抽成独立 Arc 后由 main.rs 双向注入，参照既有的 admin::ops 共享模式。
//!
//! generation 的动机：后台刷新是串行逐账号进行的（账号间 sleep 400ms），
//! 逐条写入会让调度器读到同一轮的新旧混合值。整轮收齐后一次性 publish，
//! 并推进 generation，调度侧据此判断「本代次」边界并清空本地消耗累计。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::admin::types::BalanceResponse;

/// 缓存条目 TTL（秒）。超过即视为 stale，但仍可在 MAX_STALE 内被调度使用。
pub const BALANCE_CACHE_TTL_SECS: i64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBalance {
    pub cached_at: f64,
    pub data: BalanceResponse,
}

pub struct BalanceSnapshotView {
    pub generation: u64,
    pub entries: HashMap<u64, CachedBalance>,
}

struct Inner {
    generation: u64,
    entries: HashMap<u64, CachedBalance>,
}

pub struct BalanceCache {
    inner: Mutex<Inner>,
    path: Option<PathBuf>,
}

pub type SharedBalanceCache = Arc<BalanceCache>;

impl BalanceCache {
    pub fn new(path: Option<PathBuf>) -> Self {
        let entries = Self::load_from(&path);
        Self {
            inner: Mutex::new(Inner { generation: 0, entries }),
            path,
        }
    }

    pub fn snapshot(&self) -> BalanceSnapshotView {
        let inner = self.inner.lock();
        BalanceSnapshotView {
            generation: inner.generation,
            entries: inner.entries.clone(),
        }
    }

    pub fn publish(&self, entries: HashMap<u64, CachedBalance>) {
        // 锁内只做替换与 clone，序列化和写盘在锁外——否则磁盘延迟会传播到选路。
        let to_persist = {
            let mut inner = self.inner.lock();
            inner.generation = inner.generation.saturating_add(1);
            inner.entries = entries;
            inner.entries.clone()
        };
        self.persist(&to_persist);
    }

    pub fn upsert_one(&self, id: u64, entry: CachedBalance) {
        let to_persist = {
            let mut inner = self.inner.lock();
            inner.entries.insert(id, entry);
            inner.entries.clone()
        };
        self.persist(&to_persist);
    }

    fn persist(&self, entries: &HashMap<u64, CachedBalance>) {
        let Some(path) = &self.path else { return };
        let keyed: HashMap<String, &CachedBalance> =
            entries.iter().map(|(k, v)| (k.to_string(), v)).collect();
        match serde_json::to_string_pretty(&keyed) {
            Ok(s) => {
                if let Err(e) = std::fs::write(path, s) {
                    tracing::warn!("余额缓存写盘失败: {}", e);
                }
            }
            Err(e) => tracing::warn!("余额缓存序列化失败: {}", e),
        }
    }

    /// 启动加载：丢弃超过 TTL 的条目（沿用既有语义）。
    fn load_from(path: &Option<PathBuf>) -> HashMap<u64, CachedBalance> {
        let Some(path) = path else { return HashMap::new() };
        let Ok(text) = std::fs::read_to_string(path) else { return HashMap::new() };
        let Ok(raw) = serde_json::from_str::<HashMap<String, CachedBalance>>(&text) else {
            return HashMap::new();
        };
        let now = chrono::Utc::now().timestamp() as f64;
        raw.into_iter()
            .filter(|(_, v)| (now - v.cached_at) < BALANCE_CACHE_TTL_SECS as f64)
            .filter_map(|(k, v)| k.parse::<u64>().ok().map(|id| (id, v)))
            .collect()
    }
}
```

在 `src/admin/mod.rs` 增加 `pub mod balance_cache;`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test balance_cache:: -- --nocapture`
Expected: 两条 PASS

- [ ] **Step 5: `AdminService` 改用共享缓存**

- 删除 `src/admin/service.rs` 里的 `struct CachedBalance` 与 `const BALANCE_CACHE_TTL_SECS`，改为 `use crate::admin::balance_cache::{BalanceCache, CachedBalance, SharedBalanceCache, BALANCE_CACHE_TTL_SECS};`
- 字段 `balance_cache: Mutex<HashMap<u64, CachedBalance>>` 改为 `balance_cache: SharedBalanceCache`
- `AdminService::new` 增加一个 `balance_cache: SharedBalanceCache` 入参，不再自行 `load_balance_cache_from`
- 所有 `self.balance_cache.lock()` 的读点改为 `self.balance_cache.snapshot().entries`
- `get_balance` 的读时刷新改为 `self.balance_cache.upsert_one(id, entry)`
- `refresh_all_balances` 改为：循环中把结果收集进本地 `HashMap<u64, CachedBalance>`，循环结束后 `self.balance_cache.publish(collected)`；**保留「失败条目沿用旧值」的语义**——收集前先 `let mut collected = self.balance_cache.snapshot().entries;`，成功的覆盖进去，失败的自然保留旧条目
- 删除 `save_balance_cache` / `load_balance_cache_from`（职责已移入 `BalanceCache`）

- [ ] **Step 6: `main.rs` 创建并注入**

在创建 `AdminService` 之前创建缓存，注入之：

```rust
let balance_cache: crate::admin::balance_cache::SharedBalanceCache =
    std::sync::Arc::new(crate::admin::balance_cache::BalanceCache::new(
        token_manager.cache_dir().map(|d| d.join("kiro_balance_cache.json")),
    ));
```

`AdminService::new(...)` 调用处补上 `balance_cache.clone()`。

- [ ] **Step 7: 全量回归**

Run: `cargo test 2>&1 | tail -30`
Expected: 全部 PASS。若有 admin 测试因 `AdminService::new` 签名变化编译失败，逐个补上 `Arc::new(BalanceCache::new(None))`。

- [ ] **Step 8: 提交**

```bash
git add src/admin/balance_cache.rs src/admin/mod.rs src/admin/service.rs src/main.rs
git commit -m "refactor(admin): 余额缓存抽为共享 BalanceCache

选路需要读余额，但 AdminService 单向持有 token_manager，
后者看不到它。抽成独立 Arc 由 main.rs 双向注入。
同时修正两个既有问题：串行逐条写入改为整轮原子发布（带 generation），
持锁序列化写盘改为锁外写盘。"
```

---

### Task 3: `dispatch.rs` 骨架与有效剩余选号（数值域）

**本任务包含一处留给本人编写的 TODO**（`effective_remaining` 的余额取值规则），见 Step 4。

**Files:**
- Create: `src/kiro/dispatch.rs`
- Modify: `src/kiro/mod.rs`（挂载模块）
- Test: `src/kiro/dispatch.rs` 的 `mod tests`

**Interfaces:**
- Consumes: `BalanceSnapshotView`、`CachedBalance`（Task 2）
- Produces:
```rust
pub struct Candidate { pub id: u64, pub priority: i32 }
pub enum ExclusionKind { Transient, Durable }
pub enum PickReason { StickyHit, StickyMigrated, TransientFallback, FreshSelect }
pub struct PickResult { pub cred_id: u64, pub reason: PickReason }
pub struct GroupDispatcher { /* .. */ }
impl GroupDispatcher {
    pub fn new(balance: SharedBalanceCache) -> Self;
    pub fn pick(&self, group: Option<&str>, candidates: &[Candidate],
                excluded: &HashMap<u64, ExclusionKind>,
                sticky_key: Option<&str>, now: Instant) -> PickResult;
    pub fn report_consumption(&self, group: Option<&str>, cred_id: u64, credits: f64);
}
```

- [ ] **Step 0: 确认 `BalanceResponse` 已有 `Default` 派生**

该派生已在 Task 2 Step 0 加入（本任务的测试同样依赖它）。跑一次确认：

Run: `grep -n 'derive.*Default.*Serialize' src/admin/types.rs`
Expected: 命中 `BalanceResponse` 上方的 derive 行。若未命中，按 Task 2 Step 0 补上。

- [ ] **Step 1: 写失败的测试（数值域部分）**

新建 `src/kiro/dispatch.rs`，先写测试模块。注意 `next_reset_at` 的类型是 `Option<f64>`：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::balance_cache::{BalanceCache, CachedBalance};
    use std::collections::HashMap;

    fn now_ts() -> f64 { chrono::Utc::now().timestamp() as f64 }

    fn disp(balances: &[(u64, f64)]) -> GroupDispatcher {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let mut m = HashMap::new();
        for (id, remaining) in balances {
            m.insert(*id, CachedBalance {
                cached_at: now_ts(),
                data: BalanceResponse { remaining: *remaining, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() },
            });
        }
        cache.publish(m);
        GroupDispatcher::new(cache)
    }

    fn cands(ids: &[u64]) -> Vec<Candidate> {
        ids.iter().enumerate()
            .map(|(i, id)| Candidate { id: *id, priority: i as i32 })
            .collect()
    }

    fn pick(d: &GroupDispatcher, c: &[Candidate]) -> u64 {
        d.pick(None, c, &HashMap::new(), None, Instant::now()).cred_id
    }

    #[test]
    fn picks_highest_effective_remaining() {
        let d = disp(&[(1, 7000.0), (2, 3000.0)]);
        let c = cands(&[1, 2]);
        assert_eq!(pick(&d, &c), 1);

        // 给 1 回写 5000 消耗后，有效剩余 2000 < 3000，应改选 2
        d.report_consumption(None, 1, 5000.0);
        assert_eq!(pick(&d, &c), 2);
    }

    #[test]
    fn rotates_while_balance_frozen() {
        // 余额固定不变（模拟 300s 缓存冻结），靠本地消耗驱动轮转
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let id = pick(&d, &c);
            seen.insert(id);
            d.report_consumption(None, id, 100.0);
        }
        assert_eq!(seen.len(), 2, "余额冻结期内必须仍然轮转，实际只用了 {:?}", seen);
    }

    #[test]
    fn unknown_balance_falls_back_to_median() {
        // 已知 1000/2000/9000，中位数 2000；id=4 无余额数据
        let d = disp(&[(1, 1000.0), (2, 2000.0), (3, 9000.0)]);
        let c = cands(&[1, 2, 3, 4]);
        // 缺失者有效值应为中位数 2000：既不是 0（会被饿死）也不是 max（会独吞）
        assert_eq!(pick(&d, &c), 3, "最大者仍应胜出");
        d.report_consumption(None, 3, 8000.0);   // 3 降到 1000
        assert_eq!(pick(&d, &c), 4, "缺失者取中位数 2000，此时应为组内最高");
    }

    #[test]
    fn all_unknown_degrades_to_least_consumed() {
        let d = disp(&[]);                       // 全组无余额数据
        let c = cands(&[1, 2, 3]);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let id = pick(&d, &c);
            seen.insert(id);
            d.report_consumption(None, id, 1.0);
        }
        assert_eq!(seen.len(), 3, "全部未知时应退化为最少消耗轮转，各命中一次");
    }

    #[test]
    fn negative_balance_sorts_last_but_not_starved_when_all_negative() {
        let d = disp(&[(1, -100.0), (2, -50.0)]);
        let c = cands(&[1, 2]);
        assert_eq!(pick(&d, &c), 2, "组内全负时仍应选出最不负者，不得 panic 或返回空");
    }

    #[test]
    fn stale_beyond_max_stale_is_unavailable() {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let old = now_ts() - 7200.0;             // 2 小时前 > MAX_STALE(3600)
        cache.publish(HashMap::from([
            (1, CachedBalance { cached_at: old, data: BalanceResponse { remaining: 9000.0, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() } }),
            (2, CachedBalance { cached_at: now_ts(), data: BalanceResponse { remaining: 100.0, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() } }),
        ]));
        let d = GroupDispatcher::new(cache);
        // id1 超期 → unavailable → 取已知中位数（只有 id2 已知，中位数 = 100）
        // 两者有效剩余相等，平局按 priority 升序 → id1 (priority 0)
        assert_eq!(pick(&d, &cands(&[1, 2])), 1);
    }

    #[test]
    fn past_next_reset_invalidates_snapshot() {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        cache.publish(HashMap::from([
            // cached_at 很新，但 next_reset_at 已过 → 必须判为 unavailable
            (1, CachedBalance { cached_at: now_ts(), data: BalanceResponse { remaining: 0.0, next_reset_at: Some(now_ts() - 10.0), ..Default::default() } }),
            (2, CachedBalance { cached_at: now_ts(), data: BalanceResponse { remaining: 500.0, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() } }),
        ]));
        let d = GroupDispatcher::new(cache);
        // id1 的 0 若被沿用会永不被选；正确行为是取中位数 500，与 id2 平局，按 priority 选 id1
        assert_eq!(pick(&d, &cands(&[1, 2])), 1);
    }

    #[test]
    fn generation_change_clears_consumed() {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let mk = |r: f64| CachedBalance {
            cached_at: now_ts(),
            data: BalanceResponse { remaining: r, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() },
        };
        cache.publish(HashMap::from([(1, mk(5000.0)), (2, mk(5000.0))]));
        let d = GroupDispatcher::new(cache.clone());
        let c = cands(&[1, 2]);
        d.report_consumption(None, 1, 4000.0);
        assert_eq!(pick(&d, &c), 2);

        // 新一轮余额发布：本地消耗必须清零，否则会重复扣减
        cache.publish(HashMap::from([(1, mk(1000.0)), (2, mk(5000.0))]));
        d.report_consumption(None, 2, 4500.0);   // 2 降到 500
        assert_eq!(pick(&d, &c), 1, "generation 切换后旧消耗不得残留");
    }

    #[test]
    fn ties_break_by_priority_then_id() {
        let d = disp(&[(7, 100.0), (3, 100.0)]);
        // 两者有效剩余相同；priority 由 cands() 按顺序赋 0,1
        let c = vec![
            Candidate { id: 7, priority: 5 },
            Candidate { id: 3, priority: 1 },
        ];
        assert_eq!(pick(&d, &c), 3, "平局应按 priority 升序");

        let c2 = vec![
            Candidate { id: 7, priority: 1 },
            Candidate { id: 3, priority: 1 },
        ];
        assert_eq!(pick(&d, &c2), 3, "priority 也相同则按 id 升序");
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test dispatch:: -- --nocapture`
Expected: 编译失败（`GroupDispatcher` 未定义）

- [ ] **Step 3: 实现骨架与选号（不含粘滞，粘滞在 Task 4）**

```rust
//! 组内选号：有效剩余额度调度。
//!
//! 有效剩余 = 缓存余额 − 本代次已消耗 credits。
//!
//! 为什么不是直接选 remaining 最大者：余额缓存 300s 才刷新一次，两次刷新之间
//! remaining 是一组冻结常量，argmax 会恒定指向同一账号，等于换皮的单账号粘滞。
//! 减去本地消耗后，冻结期内也会随消耗增长自然轮转。
//!
//! 为什么不是 SWRR：SWRR 只在粘滞 miss 时运行，粘滞命中不动权重，因此只能控制
//! 「新会话分配数」而控制不了额度消耗；且候选集变化时 current_weight 会冻结，
//! 恢复后携带旧信用或旧债务。

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;

use crate::admin::balance_cache::{SharedBalanceCache, BALANCE_CACHE_TTL_SECS};

/// 余额最长可被调度使用的陈旧期（秒）。超过即视为 unavailable。
/// 取 12 个刷新周期：外部消耗不进本地计数，陈旧期越长调度偏差越大。
pub const MAX_STALE_SECS: f64 = 3600.0;

/// 粘滞记录空闲过期时长。
pub const STICKY_IDLE_SECS: u64 = 30 * 60;

/// 粘滞表容量上限。防止刷 UUID 的客户端打爆内存，正常负载远用不满。
pub const STICKY_CAPACITY: usize = 10_000;

pub struct Candidate {
    pub id: u64,
    pub priority: i32,
}

/// 本次请求为何不可选。决定粘滞是否应当迁移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionKind {
    /// 并发门禁队满/超时、RPM 窗口竞争——短暂拥塞，不应毁掉会话的 prompt cache
    Transient,
    /// disabled / quota 耗尽 / 429 冷却——长期不可用，会话应整体迁移
    Durable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickReason {
    StickyHit,
    StickyMigrated,
    TransientFallback,
    FreshSelect,
}

pub struct PickResult {
    pub cred_id: u64,
    pub reason: PickReason,
}

struct StickyEntry {
    cred_id: u64,
    last_seen: Instant,
}

struct DispatchState {
    sticky: HashMap<String, StickyEntry>,
    consumed: HashMap<(String, u64), f64>,
    generation: u64,
}

pub struct GroupDispatcher {
    state: Mutex<DispatchState>,
    balance: SharedBalanceCache,
}

impl GroupDispatcher {
    pub fn new(balance: SharedBalanceCache) -> Self {
        Self {
            state: Mutex::new(DispatchState {
                sticky: HashMap::new(),
                consumed: HashMap::new(),
                generation: 0,
            }),
            balance,
        }
    }

    pub fn report_consumption(&self, group: Option<&str>, cred_id: u64, credits: f64) {
        if !credits.is_finite() || credits <= 0.0 {
            return;
        }
        let key = (group.unwrap_or("").to_string(), cred_id);
        let mut st = self.state.lock();
        let slot = st.consumed.entry(key).or_insert(0.0);
        *slot += credits;
    }

    /// candidates 非空。excluded 给出本次不可选者及其原因（Task 4 用于粘滞判定）。
    pub fn pick(
        &self,
        group: Option<&str>,
        candidates: &[Candidate],
        _excluded: &HashMap<u64, ExclusionKind>,
        _sticky_key: Option<&str>,
        _now: Instant,
    ) -> PickResult {
        debug_assert!(!candidates.is_empty(), "调用方须保证候选非空");
        let snap = self.balance.snapshot();
        let g = group.unwrap_or("").to_string();

        let mut st = self.state.lock();
        // 新一代余额发布：本地消耗归零，避免与新快照重复扣减
        if st.generation != snap.generation {
            st.generation = snap.generation;
            st.consumed.clear();
        }

        let now_ts = chrono::Utc::now().timestamp() as f64;
        let balances = resolve_balances(candidates, &snap.entries, now_ts);

        let winner = candidates
            .iter()
            .map(|c| {
                let consumed = st.consumed.get(&(g.clone(), c.id)).copied().unwrap_or(0.0);
                (c, balances[&c.id] - consumed)
            })
            .max_by(|(ca, ea), (cb, eb)| {
                // 有效剩余降序 → priority 升序 → id 升序，保证可复算
                ea.partial_cmp(eb)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| cb.priority.cmp(&ca.priority))
                    .then_with(|| cb.id.cmp(&ca.id))
            })
            .map(|(c, _)| c.id)
            .expect("candidates 非空");

        PickResult { cred_id: winner, reason: PickReason::FreshSelect }
    }
}
```

- [ ] **Step 4: 【留给本人编写】实现 `resolve_balances`**

在 `src/kiro/dispatch.rs` 中已备好签名与文档，函数体留 TODO：

```rust
/// 解析每个候选的「可用于调度的余额」，返回 id → 余额 的完整映射（候选必全覆盖）。
///
/// 三态判定（规格 §4.4）：
/// - fresh：`now_ts - cached_at < BALANCE_CACHE_TTL_SECS` → 用 `remaining` 原值
/// - stale：`< MAX_STALE_SECS` → 仍用 `remaining` 原值
/// - unavailable：超过 MAX_STALE_SECS / 条目缺失 / `remaining` 非有限 /
///   `next_reset_at` 为 `Some(t)` 且 `now_ts >= t`（跨月重置后旧值必然失效，
///   此条**优先于** TTL 判定）
///   → 取组内**已知**值的中位数
///
/// `next_reset_at` 为 `None`（上游未回报重置时间）时**不**据此判失效，
/// 仅按 TTL / MAX_STALE 判定——没有信息不等于信息为「已过期」。
///
/// 边界：
/// - 组内全部 unavailable 时中位数无定义，一律取 0.0。此时
///   `argmax(0 - consumed) = argmin(consumed)`，自动退化为最少消耗轮转。
/// - `remaining` 允许为负（开启超额后 `usageLimit - currentUsage < 0`，
///   见 src/admin/service.rs 的注释）。负值不做 clamp——超额账号排最后是
///   正确行为，且组内全负时仍会选出最不负者。
/// - 偶数个已知值时中位数的取法由实现决定（取中间两者均值或偏低者均可，
///   但必须确定性）。
fn resolve_balances(
    candidates: &[Candidate],
    entries: &HashMap<u64, crate::admin::balance_cache::CachedBalance>,
    now_ts: f64,
) -> HashMap<u64, f64> {
    todo!("由本人实现：见上方三态判定与边界")
}
```

**这一段由本人编写**，因为「余额多陈旧还算数」「缺失者取什么值」直接决定月底额度快耗尽时的行为形态，属运营判断。写完后继续 Step 5。

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test dispatch:: -- --nocapture`
Expected: 9 条测试全部 PASS。若 `stale_beyond_max_stale_is_unavailable` 或 `past_next_reset_invalidates_snapshot` 失败，检查 `resolve_balances` 的三态优先级——`next_reset_at` 判定必须优先于 TTL。

- [ ] **Step 6: 提交**

```bash
git add src/kiro/dispatch.rs src/kiro/mod.rs
git commit -m "feat(dispatch): 有效剩余额度选号

有效剩余 = 缓存余额 − 本代次已消耗 credits。
减去本地消耗是为了让 300s 缓存冻结期内仍能轮转——
直接 argmax(remaining) 会恒定指向同一账号，等于换皮的单账号粘滞。"
```

---

### Task 4: 会话粘滞

**Files:**
- Modify: `src/kiro/dispatch.rs`
- Test: `src/kiro/dispatch.rs` 的 `mod tests`

**Interfaces:**
- Consumes: Task 3 的 `GroupDispatcher::pick`、`ExclusionKind`、`PickReason`
- Produces: `pick` 的 `sticky_key` / `excluded` 参数正式生效；新增私有 `fn sticky_key_of(group: Option<&str>, seed: &str) -> String`

- [ ] **Step 1: 写失败的测试**

追加到 `src/kiro/dispatch.rs` 的 `mod tests`：

```rust
    fn pick_sticky(d: &GroupDispatcher, c: &[Candidate], key: &str, now: Instant) -> PickResult {
        d.pick(None, c, &HashMap::new(), Some(key), now)
    }

    #[test]
    fn sticky_key_returns_same_credential() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let first = pick_sticky(&d, &c, "sess-a", now).cred_id;
        for _ in 0..5 {
            let r = pick_sticky(&d, &c, "sess-a", now);
            assert_eq!(r.cred_id, first);
            assert_eq!(r.reason, PickReason::StickyHit);
        }
    }

    #[test]
    fn transient_exclusion_does_not_migrate_sticky() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let pinned = pick_sticky(&d, &c, "sess-a", now).cred_id;
        let other = if pinned == 1 { 2 } else { 1 };

        // 目标被临时排除（队满/RPM 竞争）：本次换号但不得改写粘滞
        let ex = HashMap::from([(pinned, ExclusionKind::Transient)]);
        let r = d.pick(None, &c, &ex, Some("sess-a"), now);
        assert_eq!(r.cred_id, other);
        assert_eq!(r.reason, PickReason::TransientFallback);

        // 排除解除后必须回到原号
        let back = pick_sticky(&d, &c, "sess-a", now);
        assert_eq!(back.cred_id, pinned, "临时拥塞不应永久迁移会话");
        assert_eq!(back.reason, PickReason::StickyHit);
    }

    #[test]
    fn durable_exclusion_migrates_sticky() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let pinned = pick_sticky(&d, &c, "sess-a", now).cred_id;
        let other = if pinned == 1 { 2 } else { 1 };

        let ex = HashMap::from([(pinned, ExclusionKind::Durable)]);
        let r = d.pick(None, &c, &ex, Some("sess-a"), now);
        assert_eq!(r.cred_id, other);
        assert_eq!(r.reason, PickReason::StickyMigrated);

        // 已迁移：即使原号恢复，也应留在新号上
        let after = pick_sticky(&d, &c, "sess-a", now);
        assert_eq!(after.cred_id, other);
    }

    #[test]
    fn sticky_expires_after_idle() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let t0 = Instant::now();
        let first = pick_sticky(&d, &c, "sess-a", t0).cred_id;
        // 把 first 的消耗打高，过期后重选必然换号
        d.report_consumption(None, first, 4000.0);

        let t1 = t0 + std::time::Duration::from_secs(STICKY_IDLE_SECS + 60);
        let r = pick_sticky(&d, &c, "sess-a", t1);
        assert_eq!(r.reason, PickReason::FreshSelect, "超过空闲期应重新分配");
        assert_ne!(r.cred_id, first);
    }

    #[test]
    fn sticky_table_is_capacity_bounded() {
        let d = disp(&[(1, 5000.0)]);
        let c = cands(&[1]);
        let now = Instant::now();
        for i in 0..(STICKY_CAPACITY + 100) {
            pick_sticky(&d, &c, &format!("sess-{i}"), now);
        }
        assert!(d.sticky_len() <= STICKY_CAPACITY, "粘滞表必须有界");
    }

    #[test]
    fn groups_have_isolated_consumption() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        d.report_consumption(Some("A"), 1, 4900.0);
        // B 组不受 A 组消耗影响：1 在 B 组仍是满额，与 2 平局按 priority 选 1
        assert_eq!(d.pick(Some("B"), &c, &HashMap::new(), None, Instant::now()).cred_id, 1);
        // A 组里 1 已被打低，应选 2
        assert_eq!(d.pick(Some("A"), &c, &HashMap::new(), None, Instant::now()).cred_id, 2);
    }

    #[test]
    fn distinct_sessions_do_not_share_entry() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let a = pick_sticky(&d, &c, "sess-a", now).cred_id;
        d.report_consumption(None, a, 4900.0);
        // 不同 session 必须独立分配，不得继承 sess-a 的粘滞
        let b = pick_sticky(&d, &c, "sess-b", now);
        assert_eq!(b.reason, PickReason::FreshSelect);
        assert_ne!(b.cred_id, a);
    }

    #[test]
    fn none_sticky_key_writes_nothing() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        d.pick(None, &c, &HashMap::new(), None, Instant::now());
        assert_eq!(d.sticky_len(), 0, "sticky_key=None 不得写表");
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test dispatch::tests -- --nocapture`
Expected: 编译失败（`sticky_len` 未定义）与断言失败（`reason` 恒为 `FreshSelect`）

- [ ] **Step 3: 实现粘滞**

把 Task 3 的 `pick` 替换为完整版：

```rust
    pub fn pick(
        &self,
        group: Option<&str>,
        candidates: &[Candidate],
        excluded: &HashMap<u64, ExclusionKind>,
        sticky_key: Option<&str>,
        now: Instant,
    ) -> PickResult {
        debug_assert!(!candidates.is_empty(), "调用方须保证候选非空");
        let snap = self.balance.snapshot();
        let g = group.unwrap_or("").to_string();
        let skey = sticky_key.map(|s| sticky_key_of(group, s));

        let mut st = self.state.lock();
        if st.generation != snap.generation {
            st.generation = snap.generation;
            st.consumed.clear();
        }

        // 1. 查粘滞
        if let Some(k) = &skey {
            let hit = st.sticky.get(k).and_then(|e| {
                if now.duration_since(e.last_seen).as_secs() > STICKY_IDLE_SECS {
                    None
                } else {
                    Some(e.cred_id)
                }
            });
            if let Some(cred_id) = hit {
                match excluded.get(&cred_id) {
                    // 1a. 目标可用：命中，刷新 last_seen
                    None if candidates.iter().any(|c| c.id == cred_id) => {
                        if let Some(e) = st.sticky.get_mut(k) {
                            e.last_seen = now;
                        }
                        return PickResult { cred_id, reason: PickReason::StickyHit };
                    }
                    // 1b. 临时排除：本次换号，但保留粘滞记录
                    Some(ExclusionKind::Transient) => {
                        let alt = select_by_effective_remaining(candidates, &st, &g, &snap);
                        return PickResult { cred_id: alt, reason: PickReason::TransientFallback };
                    }
                    // 1c. 长期排除或已不在候选池：迁移
                    _ => {
                        let alt = select_by_effective_remaining(candidates, &st, &g, &snap);
                        st.sticky.insert(k.clone(), StickyEntry { cred_id: alt, last_seen: now });
                        return PickResult { cred_id: alt, reason: PickReason::StickyMigrated };
                    }
                }
            }
        }

        // 2. 无粘滞或已过期：重新选号
        let winner = select_by_effective_remaining(candidates, &st, &g, &snap);
        if let Some(k) = skey {
            evict_if_full(&mut st.sticky, now);
            st.sticky.insert(k, StickyEntry { cred_id: winner, last_seen: now });
        }
        PickResult { cred_id: winner, reason: PickReason::FreshSelect }
    }

    #[cfg(test)]
    pub(crate) fn sticky_len(&self) -> usize {
        self.state.lock().sticky.len()
    }
```

配套的自由函数：

```rust
/// 粘滞 key 前缀带 group：同一 session 经不同 client key 打到不同组时，
/// 不应反复覆写同一条记录来回抖动。
fn sticky_key_of(group: Option<&str>, seed: &str) -> String {
    format!("{}|{}", group.unwrap_or(""), seed)
}

fn select_by_effective_remaining(
    candidates: &[Candidate],
    st: &DispatchState,
    g: &str,
    snap: &crate::admin::balance_cache::BalanceSnapshotView,
) -> u64 {
    let now_ts = chrono::Utc::now().timestamp() as f64;
    let balances = resolve_balances(candidates, &snap.entries, now_ts);
    candidates
        .iter()
        .map(|c| {
            let consumed = st.consumed.get(&(g.to_string(), c.id)).copied().unwrap_or(0.0);
            (c, balances[&c.id] - consumed)
        })
        .max_by(|(ca, ea), (cb, eb)| {
            ea.partial_cmp(eb)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| cb.priority.cmp(&ca.priority))
                .then_with(|| cb.id.cmp(&ca.id))
        })
        .map(|(c, _)| c.id)
        .expect("candidates 非空")
}

/// 到量时先清全部过期条目，仍满则 O(n) 淘汰 last_seen 最早的一条。
/// 不引入 lru 依赖：正常负载下 30min 过期会让表远小于上限，O(n) 基本不触发。
fn evict_if_full(sticky: &mut HashMap<String, StickyEntry>, now: Instant) {
    if sticky.len() < STICKY_CAPACITY {
        return;
    }
    sticky.retain(|_, e| now.duration_since(e.last_seen).as_secs() <= STICKY_IDLE_SECS);
    while sticky.len() >= STICKY_CAPACITY {
        let Some(oldest) = sticky
            .iter()
            .min_by_key(|(_, e)| e.last_seen)
            .map(|(k, _)| k.clone())
        else {
            break;
        };
        sticky.remove(&oldest);
    }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test dispatch:: -- --nocapture`
Expected: Task 3 的 9 条 + 本任务 8 条全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/kiro/dispatch.rs
git commit -m "feat(dispatch): 会话粘滞

临时排除（队满/RPM 竞争）只影响本次请求，不改写粘滞记录——
一次短暂拥塞不应永久毁掉会话的 prompt cache。
只有长期不可用（disabled/quota/冷却）才提交迁移。"
```

---

### Task 5: `sticky_key` 透传链

**Files:**
- Modify: `src/anthropic/handlers.rs`（`ResponseProcessingConfig` 加字段、3 处构造点、3 处 `call_api*` 调用点）
- Modify: `src/kiro/provider.rs`（`call_api`、`call_api_stream`、`call_api_with_retry` 加参数；构造 `ExclusionKind` 映射）
- Modify: `src/kiro/token_manager.rs`（`acquire_context_excluding` 加参数）
- Test: `src/anthropic/handlers.rs` 的 `mod tests`

**Interfaces:**
- Consumes: `crate::anthropic::metadata::extract_session_id`（既有，`src/anthropic/metadata.rs:9`）
- Produces:
  - `ResponseProcessingConfig.sticky_key: Option<String>`
  - `KiroProvider::call_api(&self, request_body: &str, sink: Option<&dyn TraceSink>, group: Option<&str>, sticky_key: Option<&str>)`
  - `call_api_stream` 同上
  - `MultiTokenManager::acquire_context_excluding(&self, model: Option<&str>, group: Option<&str>, excluded: &HashSet<u64>, sticky_key: Option<&str>)`
  - `fn dispatch_sticky_key(req: &MessagesRequest) -> Option<String>`（`handlers.rs`）

- [ ] **Step 1: 写失败的测试**

在 `src/anthropic/handlers.rs` 的 `mod tests` 中：

```rust
    #[test]
    fn dispatch_sticky_key_only_from_uuid_session() {
        // Claude Code 形态：metadata.user_id 内含 session uuid
        let with_session: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [{"role": "user", "content": "hi"}],
            "metadata": {"user_id": "{\"device_id\":\"d\",\"session_id\":\"0b4445e1-1111-4222-8333-444455556666\"}"}
        })).unwrap();
        assert_eq!(
            dispatch_sticky_key(&with_session).as_deref(),
            Some("0b4445e1-1111-4222-8333-444455556666")
        );

        // 无 metadata：必须返回 None 而不是降级到 key_id。
        // 降级会让同一 client key 下的所有会话共享一条粘滞记录被永久钉死。
        let without: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [{"role": "user", "content": "hi"}]
        })).unwrap();
        assert_eq!(dispatch_sticky_key(&without), None);

        // metadata 存在但不含合法 uuid：同样返回 None
        let bad: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [{"role": "user", "content": "hi"}],
            "metadata": {"user_id": "not-a-session"}
        })).unwrap();
        assert_eq!(dispatch_sticky_key(&bad), None);
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test dispatch_sticky_key -- --nocapture`
Expected: 编译失败（`dispatch_sticky_key` 未定义）

- [ ] **Step 3: 实现 key 提取**

在 `src/anthropic/handlers.rs` 中（紧邻既有的 `session_id_of`）：

```rust
/// 会话粘滞 key。**只在能解析出 UUID session 时启用**。
///
/// 刻意不复用 cache_metering::isolation_seed：那个函数只在 key_id == 0 时走
/// cc 级降级，普通 client key 直接返回 key:<key_id>，会让同一 client key 下
/// 的所有会话共享一条粘滞记录，流量被永久钉死在一个账号——正是本功能要修的病。
fn dispatch_sticky_key(req: &MessagesRequest) -> Option<String> {
    req.metadata
        .as_ref()
        .and_then(|m| m.user_id.as_deref())
        .and_then(super::metadata::extract_session_id)
}
```

- [ ] **Step 4: 加字段与参数**

1. `ResponseProcessingConfig`（`handlers.rs:166`）增加 `sticky_key: Option<String>,`
2. 3 处构造点（`:1107`、`:1139`、`:2222`/`:2254` 附近，以 `grep -n 'ResponseProcessingConfig {' src/anthropic/handlers.rs` 为准）各增加 `sticky_key: dispatch_sticky_key(&payload),`
3. 3 处解构点（`:1164`、`:1577`、`:2282`）把 `sticky_key` 加进模式
4. 3 处调用点改为 `provider.call_api_stream(request_body, Some(tracer.as_ref()), group.as_deref(), sticky_key.as_deref())`（非流式同理用 `call_api`）
5. `src/kiro/provider.rs`：`call_api` / `call_api_stream` 增加 `sticky_key: Option<&str>` 参数并透传给 `call_api_with_retry`；`call_api_with_retry` 增加同名参数
6. `src/kiro/provider.rs:382`（MCP 路径）的 `acquire_context(None, group)` 保持不变——该路径无会话概念，`sticky_key` 恒为 `None`
7. `src/kiro/token_manager.rs`：`acquire_context_excluding` 增加 `sticky_key: Option<&str>` 参数；`acquire_context` 传 `None`

- [ ] **Step 5: 构造 `ExclusionKind` 映射**

`src/kiro/provider.rs` 的 `call_api_with_retry` 里，把现有的 `queue_excluded: HashSet<u64>` 改为 `HashMap<u64, ExclusionKind>`，并入元素时一律标 `Transient`——该集合的来源只有并发门禁队满/超时（见 `:585`、`:649` 附近），本就是短暂拥塞：

```rust
let mut queue_excluded: std::collections::HashMap<u64, crate::kiro::dispatch::ExclusionKind> =
    std::collections::HashMap::new();
// 插入点改为：
queue_excluded.insert(id, crate::kiro::dispatch::ExclusionKind::Transient);
```

`token_manager` 侧硬过滤链里因 `disabled` / `throttled_until` / `rpm_exceeded` 被剔除的账号，在 Task 7 构造候选快照时标为 `Durable`（RPM 虽是短暂的，但它由 token_manager 自己的窗口判定，不是本请求的临时竞争——归 `Durable` 会触发迁移，故**应归 `Transient`**，见 Task 7 Step 3）。

- [ ] **Step 6: 编译与回归**

Run: `cargo test 2>&1 | tail -30`
Expected: 全部 PASS。所有 `call_api*` / `acquire_context_excluding` 的既有调用点（含测试）需补 `None`。

- [ ] **Step 7: 提交**

```bash
git add src/anthropic/handlers.rs src/kiro/provider.rs src/kiro/token_manager.rs
git commit -m "feat(dispatch): 透传会话粘滞 key

只在能解析出 UUID session 时启用粘滞。刻意不复用 isolation_seed:
它只在 key_id==0 时走 cc 降级，普通 client key 直接返回 key:<id>,
会让同一 key 下所有会话共享一条记录被永久钉死。"
```

---

### Task 6: 反向路径（消耗回写）

**Files:**
- Modify: `src/anthropic/handlers.rs`（`UsageRecordHook` 加字段与回写）
- Test: `src/kiro/dispatch.rs` 的 `mod tests`（回写语义）+ `src/anthropic/handlers.rs`

**Interfaces:**
- Consumes: `GroupDispatcher::report_consumption`（Task 3）
- Produces: `UsageRecordHook { dispatcher: Option<Arc<GroupDispatcher>>, group: Option<String>, .. }`

- [ ] **Step 1: 写失败的测试**

在 `src/kiro/dispatch.rs` 的 `mod tests`：

```rust
    #[test]
    fn report_consumption_ignores_non_positive_and_non_finite() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        // 这些都不应改变选择结果（平局按 priority 选 1）
        d.report_consumption(None, 1, 0.0);
        d.report_consumption(None, 1, -100.0);
        d.report_consumption(None, 1, f64::NAN);
        d.report_consumption(None, 1, f64::INFINITY);
        assert_eq!(pick(&d, &c), 1);

        // 正常值才生效
        d.report_consumption(None, 1, 4900.0);
        assert_eq!(pick(&d, &c), 2);
    }

    #[test]
    fn sticky_hit_consumption_still_counted() {
        // 长会话粘在一个号上，其消耗必须照样计入，否则控制不了额度倾斜
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let pinned = pick_sticky(&d, &c, "long", now).cred_id;
        for _ in 0..50 {
            let r = pick_sticky(&d, &c, "long", now);
            assert_eq!(r.cred_id, pinned);
            d.report_consumption(None, r.cred_id, 100.0);
        }
        // 新会话不应再流向被打爆的账号
        let fresh = pick_sticky(&d, &c, "new", now);
        assert_ne!(fresh.cred_id, pinned, "长会话的消耗必须影响新会话的分配");
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test dispatch::tests::report_consumption dispatch::tests::sticky_hit -- --nocapture`
Expected: `sticky_hit_consumption_still_counted` FAIL（若 Task 3/4 实现正确则可能直接 PASS——那也是有效结果，说明闭环已成立，继续 Step 3）

- [ ] **Step 3: 接入 `UsageRecordHook`**

`src/anthropic/handlers.rs` 的 `UsageRecordHook`（`:50`）增加两个字段：

```rust
pub(crate) struct UsageRecordHook {
    pub usage: Option<SharedUsageStore>,
    pub client_keys: Option<SharedClientKeyManager>,
    pub key_id: u64,
    pub model: String,
    pub started_at: Instant,
    /// 消耗回写目标。credits 与余额 remaining 同量纲（已由生产数据验证，误差 <2%）。
    pub dispatcher: Option<std::sync::Arc<crate::kiro::dispatch::GroupDispatcher>>,
    pub group: Option<String>,
}
```

`from_state` 改为 `from_state(state: &AppState, key_id: u64, model: String, group: Option<String>)`，内部填 `dispatcher: state.dispatcher.clone()`、`group`。所有调用点（`grep -n 'UsageRecordHook::from_state' src/anthropic/handlers.rs`）补上 `key_ctx.group.clone()`。

在 `record` 方法体末尾追加：

```rust
        // 反向路径：把本次实际消耗回写给调度器。
        // 粘滞命中与新分配都会经过这里，长会话的消耗因此照样计入——
        // 这是本设计能承诺「额度消耗趋同」而非仅「新会话数加权」的依据。
        if credential_id != 0
            && let Some(d) = &self.dispatcher
        {
            d.report_consumption(self.group.as_deref(), credential_id, credits);
        }
```

注意用**入参 `credits`** 而非 `rec.credits`——虽然二者在正常路径相同，但 `report_consumption` 内部已对非有限/非正值做了过滤，语义更直白。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test dispatch:: -- --nocapture && cargo test handlers:: 2>&1 | tail -20`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/anthropic/handlers.rs src/kiro/dispatch.rs
git commit -m "feat(dispatch): 消耗回写闭环

UsageRecordHook::record 已带 credential_id 与 credits，
且 credits 与余额 remaining 同量纲（生产数据验证误差 <2%），
直接回写即可，无需按 token 估算。"
```

---

### Task 7: `token_manager` 接入 dispatcher

**Files:**
- Modify: `src/kiro/token_manager.rs`（`MultiTokenManager` 加 `dispatcher` 字段；`select_next_credential_excluding` 的 `Weighted` 分支）
- Modify: `src/main.rs`（构造并注入）
- Test: `src/kiro/token_manager.rs` 的 `mod tests`

**Interfaces:**
- Consumes: `GroupDispatcher::pick`（Task 4）、`ExclusionKind`（Task 3）
- Produces: `MultiTokenManager::with_dispatcher(self, d: Arc<GroupDispatcher>) -> Self`

- [ ] **Step 1: 写失败的测试**

```rust
#[tokio::test]
async fn weighted_prefers_higher_remaining_balance() {
    let manager = test_manager_with_two_credentials();   // id 1、2
    let cache = std::sync::Arc::new(crate::admin::balance_cache::BalanceCache::new(None));
    let now_ts = chrono::Utc::now().timestamp() as f64;
    let mk = |r: f64| crate::admin::balance_cache::CachedBalance {
        cached_at: now_ts,
        data: crate::admin::types::BalanceResponse {
            remaining: r,
            next_reset_at: now_ts + 86400.0,
            ..Default::default()
        },
    };
    cache.publish(std::collections::HashMap::from([(1, mk(1000.0)), (2, mk(9000.0))]));

    let manager = manager.with_dispatcher(std::sync::Arc::new(
        crate::kiro::dispatch::GroupDispatcher::new(cache),
    ));
    manager.set_load_balancing_mode("weighted".to_string()).unwrap();

    // 余额高的应被优先选中
    assert_eq!(manager.acquire_context(None, None).await.unwrap().id, 2);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test weighted_prefers_higher_remaining -- --nocapture`
Expected: 编译失败（`with_dispatcher` 未定义）

- [ ] **Step 3: 实现接入**

`MultiTokenManager` 增加字段 `dispatcher: parking_lot::RwLock<Option<Arc<GroupDispatcher>>>`（用 `RwLock<Option<_>>` 而非构造参数，避免改动所有既有构造点）与：

```rust
    pub fn with_dispatcher(self, d: std::sync::Arc<crate::kiro::dispatch::GroupDispatcher>) -> Self {
        *self.dispatcher.write() = Some(d);
        self
    }
```

`select_next_credential_excluding` 的签名增加 `sticky_key: Option<&str>`，并新增 `Weighted` 分支。**关键：必须在调用 `pick` 之前释放 `entries` 与 `credential_support`**：

```rust
            LoadBalancingMode::Weighted => {
                let Some(dispatcher) = self.dispatcher.read().clone() else {
                    // 未注入 dispatcher（如单测老用例）：退化为 priority，不 panic
                    let entry = available.iter().min_by_key(|e| e.credentials.priority)?;
                    return Some((entry.id, entry.credentials.clone()));
                };

                // 构造候选快照并把凭据 clone 出来，随后释放两把锁再调 pick
                let cands: Vec<crate::kiro::dispatch::Candidate> = available
                    .iter()
                    .map(|e| crate::kiro::dispatch::Candidate {
                        id: e.id,
                        priority: e.credentials.priority.unwrap_or(i32::MAX),
                    })
                    .collect();
                let creds: std::collections::HashMap<u64, KiroCredentials> = available
                    .iter()
                    .map(|e| (e.id, e.credentials.clone()))
                    .collect();

                // 硬过滤已剔除的账号按原因分类，供粘滞判定使用。
                // RPM 与 429 冷却都是「几十秒到半小时后自愈」的临时状态，
                // 不应触发会话永久迁移；只有 disabled（含 quota 耗尽）才是长期不可用。
                let excluded_kinds: std::collections::HashMap<u64, crate::kiro::dispatch::ExclusionKind> =
                    entries
                        .iter()
                        .filter(|e| !cands.iter().any(|c| c.id == e.id))
                        .map(|e| {
                            let kind = if e.disabled {
                                crate::kiro::dispatch::ExclusionKind::Durable
                            } else {
                                crate::kiro::dispatch::ExclusionKind::Transient
                            };
                            (e.id, kind)
                        })
                        .collect();

                drop(credential_support);
                drop(entries);

                let r = dispatcher.pick(group, &cands, &excluded_kinds, sticky_key, Instant::now());
                tracing::debug!(
                    cred_id = r.cred_id,
                    reason = ?r.reason,
                    candidates = cands.len(),
                    "weighted 选号"
                );
                creds.get(&r.cred_id).map(|c| (r.cred_id, c.clone()))
            }
```

`acquire_context_excluding` 里两处 `select_next_credential_excluding(model, group, excluded)` 调用补上 `sticky_key`。

`src/main.rs` 在创建 `balance_cache`（Task 2 Step 6）之后、`token_manager` 之后：

```rust
let dispatcher = std::sync::Arc::new(
    crate::kiro::dispatch::GroupDispatcher::new(balance_cache.clone()),
);
// token_manager 构造处链上 .with_dispatcher(dispatcher.clone())
```

并把 `dispatcher` 放进 `AppState` 供 `UsageRecordHook`（Task 6）取用。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test weighted_ -- --nocapture`
Expected: Task 1 的 2 条 + 本任务 1 条全部 PASS

- [ ] **Step 5: 补重试预算测试**

```rust
#[tokio::test]
async fn weighted_retry_budget_is_capped_at_four() {
    // 规格 §7 第 17 条：MAX_TOTAL_RETRIES = 4。7 个账号的组里，
    // 若前 4 个高有效剩余账号全部失败，请求会失败而非继续尝试剩余健康账号。
    // 本测试锁定该行为，避免日后误以为它会穷举整组。
    // 断言：max_retries == 4（见 provider.rs 的 retry_budget）
    assert_eq!(crate::kiro::provider::KiroProvider::retry_budget(7, None), 4);
}
```

若 `retry_budget` 当前不是 `pub(crate)`，改为 `pub(crate)` 以便测试。

- [ ] **Step 6: 全量回归**

Run: `cargo test 2>&1 | tail -30`
Expected: 全部 PASS，`priority` / `balanced` 既有测试无变化

- [ ] **Step 7: 提交**

```bash
git add src/kiro/token_manager.rs src/main.rs
git commit -m "feat(dispatch): weighted 模式接入 GroupDispatcher

选号前先构造候选快照并释放 entries 与 credential_support，
再调 pick——pick 内只持有 DispatchState 一把锁，可证明无锁环。
RPM 与 429 冷却归 Transient（几十秒到半小时自愈），
只有 disabled 归 Durable 才触发会话迁移。"
```

---

### Task 8: refresher 按模式启停 + 前端选项

**Files:**
- Modify: `src/main.rs`（refresher 移出 admin 分支，改为按模式启停）
- Modify: `src/admin/service.rs`（`set_load_balancing_mode` 切换时启停 refresher）
- Modify: `admin-ui/src/components/topbar-tools.tsx`
- Test: `src/admin/service.rs` 的 `mod tests`

**Interfaces:**
- Consumes: `AdminService::start_balance_refresher`（既有）

- [ ] **Step 1: 写失败的测试**

```rust
    #[test]
    fn weighted_mode_is_accepted_by_admin_api() {
        let service = service_with_balancing_mode("priority");
        let r = service.set_load_balancing_mode(SetLoadBalancingModeRequest {
            mode: "weighted".to_string(),
        });
        assert!(r.is_ok(), "admin API 必须接受 weighted");
        assert_eq!(service.token_manager.get_load_balancing_mode(), "weighted");
    }

    #[test]
    fn invalid_mode_still_rejected() {
        let service = service_with_balancing_mode("priority");
        assert!(service
            .set_load_balancing_mode(SetLoadBalancingModeRequest { mode: "nonsense".into() })
            .is_err());
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test weighted_mode_is_accepted_by_admin_api invalid_mode_still_rejected -- --nocapture`
Expected: 前者 FAIL（若 Task 1 已改白名单则 PASS，那就直接进 Step 3）

- [ ] **Step 3: refresher 按模式启停**

规格 D10：无条件启动会让默认（`priority`）部署凭空多出周期性上游余额请求，违反「默认行为零变化」。

`src/main.rs` 把 refresher 启动从 admin 分支内移出，改为：

```rust
// 余额刷新只在 weighted 模式下需要——priority/balanced 不读余额，
// 无条件启动会让默认部署凭空多出周期性上游请求。
if token_manager.get_load_balancing_mode() == "weighted" {
    admin_service.start_balance_refresher(std::time::Duration::from_secs(300));
}
```

`AdminService` 增加一个 `balance_refresher_running: AtomicBool`，`start_balance_refresher` 开头 `compare_exchange` 防重复启动；`set_load_balancing_mode` 成功后若新模式为 `weighted` 则启动之（切走时不停——已启动的任务留着代价极小，且切回来无需重启）。

- [ ] **Step 4: 前端下拉加选项**

`admin-ui/src/components/topbar-tools.tsx`：在既有的 `priority` / `balanced` 选项后增加

```tsx
<option value="weighted">按余额加权（会话粘滞）</option>
```

（以文件内既有选项的实际写法为准，可能是 `SelectItem` 或数组常量——`grep -n 'balanced' admin-ui/src/components/topbar-tools.tsx` 确认后照式样添加。）

- [ ] **Step 5: 验证**

Run: `cargo test 2>&1 | tail -20`
Expected: 全部 PASS

Run: `cd admin-ui && npm run build 2>&1 | tail -10`
Expected: 构建成功

- [ ] **Step 6: 提交**

```bash
git add src/main.rs src/admin/service.rs admin-ui/src/components/topbar-tools.tsx
git commit -m "feat(dispatch): weighted 模式下才启动余额刷新 + 前端选项

余额刷新只有 weighted 需要。无条件启动会让默认 priority 部署
凭空多出周期性上游余额请求，违反「默认行为零变化」。"
```

---

### Task 9: quota 重置后的重新探测 + 调度可观测字段

规格 §4.2 最后一条与 §8「可观测字段」。这两项都不影响选号正确性，但缺了它们功能在生产里不可运维：被 402 禁用的账号月度重置后永远回不来，且线上出现倾斜时无法归因。

**Files:**
- Modify: `src/admin/service.rs`（`refresh_all_balances` 覆盖 quota 禁用账号）
- Modify: `src/kiro/token_manager.rs`（quota 重置后自动重新启用）
- Modify: `src/kiro/dispatch.rs`（`PickResult` 携带归因字段）
- Test: `src/kiro/token_manager.rs`、`src/kiro/dispatch.rs` 的 `mod tests`

**Interfaces:**
- Produces: `PickResult { cred_id, reason, effective_remaining: f64, balance_age_secs: f64, generation: u64, candidate_count: usize }`

- [ ] **Step 1: 写失败的测试**

`src/kiro/token_manager.rs`：

```rust
#[test]
fn quota_disabled_credential_is_reenabled_after_reset() {
    let manager = test_manager_with_two_credentials();
    manager.report_quota_exhausted(1);
    assert!(manager.snapshot().entries.iter().any(|e| e.id == 1 && e.disabled));

    // 上游返回了新周期的余额（remaining 恢复），应自动解除 QuotaExceeded 禁用
    manager.clear_quota_disable_if_replenished(1, 9000.0);
    let s = manager.snapshot();
    let e = s.entries.iter().find(|e| e.id == 1).unwrap();
    assert!(!e.disabled, "月度重置后 quota 禁用应自动解除");
}

#[test]
fn reenable_does_not_touch_other_disable_reasons() {
    let manager = test_manager_with_two_credentials();
    manager.disable_credential(1, "手动停用".to_string()).unwrap();
    manager.clear_quota_disable_if_replenished(1, 9000.0);
    assert!(
        manager.snapshot().entries.iter().any(|e| e.id == 1 && e.disabled),
        "手动禁用不得被余额恢复解除"
    );
}
```

`src/kiro/dispatch.rs`：

```rust
    #[test]
    fn pick_result_carries_attribution() {
        let d = disp(&[(1, 7000.0), (2, 3000.0)]);
        let c = cands(&[1, 2]);
        let r = d.pick(None, &c, &HashMap::new(), None, Instant::now());
        assert_eq!(r.cred_id, 1);
        assert_eq!(r.effective_remaining, 7000.0);
        assert_eq!(r.candidate_count, 2);
        assert!(r.balance_age_secs >= 0.0 && r.balance_age_secs < 60.0);
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test quota_disabled_credential reenable_does_not pick_result_carries -- --nocapture`
Expected: 编译失败（`clear_quota_disable_if_replenished`、`effective_remaining` 未定义）

- [ ] **Step 3: 实现重新探测**

`src/admin/service.rs` 的 `refresh_all_balances`：当前只遍历未禁用账号，改为**也遍历 `disabled_reason == Some(DisabledReason::QuotaExceeded)` 的账号**（其余禁用原因仍跳过——手动停用、refresh token 失效等不该被余额探测唤醒）。

`src/kiro/token_manager.rs` 新增：

```rust
    /// 余额刷新发现该账号额度已恢复时，解除 QuotaExceeded 禁用。
    ///
    /// 只解除 QuotaExceeded：手动停用、refresh token 失效等原因与额度无关，
    /// 被余额恢复顺手唤醒会绕过人的意图。
    pub fn clear_quota_disable_if_replenished(&self, id: u64, remaining: f64) {
        if remaining <= 0.0 {
            return;
        }
        let mut entries = self.entries.lock();
        if let Some(e) = entries.iter_mut().find(|e| e.id == id)
            && e.disabled
            && e.disabled_reason == Some(DisabledReason::QuotaExceeded)
        {
            tracing::info!("凭据 #{} 额度已恢复（remaining={:.2}），解除 quota 禁用", id, remaining);
            e.disabled = false;
            e.disabled_reason = None;
            e.failure_count = 0;
        }
    }
```

在 `refresh_all_balances` 每次成功拿到余额后调用 `token_manager.clear_quota_disable_if_replenished(id, resp.remaining)`。

- [ ] **Step 4: 实现归因字段**

`src/kiro/dispatch.rs` 的 `PickResult` 扩展：

```rust
pub struct PickResult {
    pub cred_id: u64,
    pub reason: PickReason,
    /// 选中者的有效剩余（= 余额 − 本代次已消耗）
    pub effective_remaining: f64,
    /// 选中者余额快照的年龄（秒）。判断倾斜是否源自余额陈旧。
    pub balance_age_secs: f64,
    /// 余额快照代次
    pub generation: u64,
    /// 本次候选池大小。判断倾斜是否源自候选被过滤掉。
    pub candidate_count: usize,
}
```

`select_by_effective_remaining` 改为返回 `(u64, f64)`（id 与有效剩余），三处构造 `PickResult` 的地方补齐字段。`balance_age_secs` 由 `now_ts - entries[&id].cached_at` 得出，条目缺失时取 `f64::INFINITY`。

Task 7 的 `tracing::debug!` 相应补全：

```rust
                tracing::debug!(
                    cred_id = r.cred_id,
                    reason = ?r.reason,
                    effective_remaining = r.effective_remaining,
                    balance_age_secs = r.balance_age_secs,
                    generation = r.generation,
                    candidates = r.candidate_count,
                    "weighted 选号"
                );
```

- [ ] **Step 5: 运行测试确认通过**

Run: `cargo test dispatch:: quota_disabled reenable_does_not -- --nocapture`
Expected: 全部 PASS

- [ ] **Step 6: 全量回归**

Run: `cargo test 2>&1 | tail -30`
Expected: 全部 PASS

- [ ] **Step 7: 提交**

```bash
git add src/kiro/dispatch.rs src/kiro/token_manager.rs src/admin/service.rs
git commit -m "feat(dispatch): quota 重置后重新探测 + 调度归因字段

后台余额刷新此前只遍历未禁用账号，而 402 会把账号 disabled，
导致月度重置后永远回不到调度池。改为额外覆盖 QuotaExceeded 一种原因，
其余禁用原因仍跳过——手动停用不该被余额探测唤醒。

PickResult 补 effective_remaining / balance_age / generation /
candidate_count：否则线上出现倾斜时无法区分成因。"
```

---

## 上线与验证

切换：`PUT /api/admin/config/load-balancing`（真实路由见 `src/admin/router.rs:110`），`{"mode": "weighted"}`，热生效不重启。

回退：切回 `"priority"`。内存中的粘滞表与消耗表留存，不影响任何行为。

**验证口径（规格 §8）**：

- **不能用 `success_count` 增量对比余额份额**——那是请求次数，与本功能承诺的额度口径不同量纲。
- 判定一：按 `credential_id` 聚合 `usage_records.credits` 增量，组内分布应与切换时刻各账号的 `remaining` 正向对应（余额多的分到更多消耗）。
- 判定二：`kiro_balance_cache.json` 中组内各号 `usagePercentage` 的**极差**随时间收敛而非发散。
- 两条都看增量或极差，不看绝对值——id2 已累计 7323 次请求、26% 额度，历史包袱在数万次请求内都会掩盖新行为。

查询示例（DuckDB 被运行中的服务独占，需先复制）：

```bash
cp data/kiro.duckdb /tmp/k.duckdb && cp data/kiro.duckdb.wal /tmp/k.duckdb.wal
python3 -c "
import duckdb
c = duckdb.connect('/tmp/k.duckdb')
for r in c.execute('''select credential_id, count(*), round(sum(credits),2)
from usage_records where status='success' and ts >= '<切换时刻>'
group by credential_id order by 3 desc''').fetchall(): print(r)
"
```

## 待本人确认的两项（规格 §9）

1. **粘滞迁移阈值**：是否在「粘滞账号的有效剩余低于组内最大值一定比例」时主动迁移。当前实现只在排除时迁移。若要加，落点在 `dispatch.rs` 的 `pick` 步骤 1a。
2. **`MAX_STALE_SECS = 3600`**：外部消耗不进本地计数，陈旧期越长偏差越大。落点 `dispatch.rs` 常量。

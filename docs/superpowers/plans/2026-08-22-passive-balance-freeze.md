# 余额被动化 + 402 冷冻自然恢复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 删除 300s 全天候余额刷新，改为请求驱动的被动刷新；402 配额耗尽从「禁用 + 探测自愈」改为「冷冻时间戳 + 惰性到期自然回池」。

**Architecture:** 完全照抄仓库里已有的 `throttled_until`（429 账号冷却）模式做冷冻：凭据上一个持久化的 `frozen_until` epoch 秒，所有选号过滤点惰性比较，无后台解冻任务；到期时间优先取余额快照的 `next_reset_at`（经 token_manager 已注入的 dispatcher → BalanceCache 取），缺失则兜底 1 小时乐观回池。被动刷新用 `Notify` 反向解耦：dispatch 选号发现快照过期就 poke `BalanceCache` 上的通知器，AdminService 侧的监听任务收到后做 single-flight + 60s 防抖的刷新。

**Tech Stack:** Rust / tokio (Notify, spawn) / serde / parking_lot；面板 React。

Spec：`docs/superpowers/specs/2026-08-22-passive-balance-freeze-design.md`（已用户确认）。

## Global Constraints

- 本 crate **无 lib target**：测试一律 `cargo test` / `cargo test <模块路径>`。注释一律中文。
- 全量测试基线 **960 passed / 0 failed**；唯一双态 flake：`http_client::tests::reqwest_timeout_error_is_tagged_before_the_chain`（沙箱把 192.0.2.1 代理成 502，过与不过都正常）。其他失败 = 真回归。
- clippy 只看所碰文件的**增量**（基线本就不干净）。
- 常量取值照 spec：冷冻兜底 `FREEZE_FALLBACK_SECS = 3600`；reset 安全余量 `FREEZE_RESET_MARGIN_SECS = 300`；被动刷新防抖 `PASSIVE_REFRESH_MIN_INTERVAL_SECS = 60`；TTL 沿用既有 `BALANCE_CACHE_TTL_SECS = 300`。**不新增配置项。**
- 「可自愈进冷冻、不可自愈才禁用」：手动禁用 / refresh token 失效等仍走 `disabled`，只有 402 配额路径改冷冻。

## 已核实的现状（实现者直接采信，不要重查）

- **同构先例 `throttled_until`**（`src/kiro/token_manager.rs:935`，`Option<Instant>`，内存态）：选号过滤在 **:1608 / :1790 / :1950 / :2819** 四处，写法 `!e.disabled && !e.throttled_until.map(|t| t > now).unwrap_or(false)`；`report_success`（:2488）成功即清；快照导出 `throttled_remaining_secs`（:1018、:2886）；面板有 `clearThrottle` API（admin-ui `api/credentials.ts:142`）与对应按钮。冷冻与它的差别只有一个：**必须持久化**（月度重置跨进程生命周期），所以放 `KiroCredentials` 上用 epoch 秒，不是 Instant。
- `report_quota_exhausted`（:2575）现在置 `disabled=true + DisabledReason::QuotaExceeded + failure_count=MAX`，并切换 current_id。
- `token_manager` 持有 `dispatcher: RwLock<Option<Arc<GroupDispatcher>>>`（:1116，main 经 `with_dispatcher` 注入；**单测里可能是 None，冻结取 reset 时间必须容忍 None → 走兜底**）。`GroupDispatcher` 内有 `balance: SharedBalanceCache`（dispatch.rs:82）。`CachedBalance.data` 是 `BalanceResponse`，含 `next_reset_at: Option<f64>`（admin/types.rs:334）。
- `clear_quota_disable_if_replenished`（token_manager.rs:2623）是现行自愈唯一路径；`refresh_all_balances` 对 QuotaExceeded 禁用凭据特判继续探测（service.rs:973）；测试 `refresh_all_balances_runs_regardless_of_mode`（service.rs:5524）钉着这条链路。
- 周期刷新：`start_balance_refresher`（service.rs:1032），main.rs:548 启动，:540-546 有大段注释解释「为什么无条件启动」——本轮整体替换。
- 快照契约测试先例：token_manager.rs:4888（断言序列化字面量 `"QuotaExceeded"`）。
- `persist_credentials`（:2211）是凭据落盘唯一入口；`save_stats_debounced` 只落统计不落凭据。
- 面板凭据卡片：`admin-ui/src/components/credential-card.tsx`；`clearThrottle` 按钮就在其中，冷冻 UI 照它抄。

---

### Task 1: token_manager 冷冻核心

**Files:**
- Modify: `src/kiro/model/credentials.rs`（`KiroCredentials` 加字段）
- Modify: `src/kiro/token_manager.rs`（冻结/解冻/过滤/迁移）
- Modify: `src/kiro/dispatch.rs`（暴露 `balance_next_reset_at`）
- Test: `token_manager.rs` 的 `mod tests`

**Interfaces:**
- Produces: `KiroCredentials.frozen_until: Option<i64>`（epoch 秒；serde `default` + `skip_serializing_if = "Option::is_none"`，旧 credentials.json 兼容）
- Produces: `MultiTokenManager::clear_freeze(&self, id: u64) -> anyhow::Result<()>`（Task 4 的解冻端点用）
- Produces: `GroupDispatcher::balance_next_reset_at(&self, cred_id: u64) -> Option<i64>`
- Produces: 冷冻判定统一辅助 `fn entry_frozen(e: &TokenEntry, now_ts: i64) -> bool`（模块级私有函数，四个过滤点共用）

- [ ] **Step 1: 写失败测试**

`token_manager.rs` `mod tests` 追加（构造方式照同文件既有测试的 `MultiTokenManager::new(config, vec![creds], None, None, false)` 范式；`entries` 访问不到就通过公开行为断言）：

```rust
    /// 402 不再禁用而是冷冻：到期前不可调度，到期后无需任何刷新即自然回池。
    #[tokio::test]
    async fn quota_exhausted_freezes_instead_of_disabling() {
        let config = Config::default();
        let mut c1 = KiroCredentials::default();
        c1.refresh_token = Some("a".repeat(150));
        let mut c2 = KiroCredentials::default();
        c2.refresh_token = Some("b".repeat(150));
        let manager = MultiTokenManager::new(config, vec![c1, c2], None, None, false).unwrap();

        // 402：无 dispatcher（None）→ 走兜底 now+3600
        let has_available = manager.report_quota_exhausted(1);
        assert!(has_available, "另一张凭据仍可用");
        let snap = manager.snapshot();
        let e1 = snap.entries.iter().find(|e| e.id == 1).unwrap();
        assert!(!e1.disabled, "402 不得再置 disabled");
        let fu = e1.frozen_until.expect("必须写入 frozen_until");
        let now = chrono::Utc::now().timestamp();
        assert!((fu - now - 3600).abs() <= 5, "兜底应为 now+3600±5s，实际 {}", fu - now);

        // 冷冻中不可被选中：可用性判定应只剩凭据 2
        // （用 total_count 之类公开口径不行——用 acquire/available 行为断言，
        //   照同文件既有「禁用后不被选中」测试的写法适配。）

        // 到期自然回池：把 frozen_until 改到过去（测试后门：clear_freeze 之外
        // 直接经 update 通道或测试辅助设置），再断言可被选中。
        manager.set_frozen_until_for_test(1, Some(now - 10));
        let snap = manager.snapshot();
        let e1 = snap.entries.iter().find(|e| e.id == 1).unwrap();
        assert!(e1.frozen_until.is_some(), "过期时间戳保留（成功请求才清）");
        // 断言选号过滤视其为可调度（调用与 :1608 相同判定路径的公开方法）

        // 成功请求清冻结
        manager.report_success(1);
        let snap = manager.snapshot();
        assert!(snap.entries.iter().find(|e| e.id == 1).unwrap().frozen_until.is_none());
    }

    /// 再次 402 是覆盖式重冻：新时间戳整体替换，不叠加。
    #[tokio::test]
    async fn refreeze_overwrites_deadline() {
        let config = Config::default();
        let mut c1 = KiroCredentials::default();
        c1.refresh_token = Some("a".repeat(150));
        let manager = MultiTokenManager::new(config, vec![c1], None, None, false).unwrap();
        manager.report_quota_exhausted(1);
        let first = manager.snapshot().entries[0].frozen_until.unwrap();
        manager.set_frozen_until_for_test(1, Some(chrono::Utc::now().timestamp() - 10));
        manager.report_quota_exhausted(1);
        let second = manager.snapshot().entries[0].frozen_until.unwrap();
        assert!(second > first, "重冻必须重新计算，不是沿用旧值");
    }

    /// 存量迁移：加载时 disabled+QuotaExceeded → 冷冻态。
    #[tokio::test]
    async fn legacy_quota_disabled_migrates_to_frozen_on_load() {
        let config = Config::default();
        let mut c1 = KiroCredentials::default();
        c1.refresh_token = Some("a".repeat(150));
        c1.disabled = true;
        c1.disabled_reason = Some("QuotaExceeded".to_string());
        let mut c2 = KiroCredentials::default();
        c2.refresh_token = Some("b".repeat(150));
        c2.disabled = true;
        c2.disabled_reason = Some("Manual".to_string());
        let manager = MultiTokenManager::new(config, vec![c1, c2], None, None, false).unwrap();
        let snap = manager.snapshot();
        let e1 = snap.entries.iter().find(|e| e.id == 1).unwrap();
        assert!(!e1.disabled && e1.frozen_until.is_some(), "QuotaExceeded 禁用应转冷冻");
        let e2 = snap.entries.iter().find(|e| e.id == 2).unwrap();
        assert!(e2.disabled && e2.frozen_until.is_none(), "其他禁用原因不动");
    }
```

注：`KiroCredentials.disabled/disabled_reason` 字段名以 `src/kiro/model/credentials.rs` 实际为准（先读再写测试）；快照结构里 `frozen_until` 字段在 Step 3 一并加。`set_frozen_until_for_test` 用 `#[cfg(test)]` 标注。

- [ ] **Step 2: 跑测试确认编译失败**

Run: `cargo test kiro::token_manager::tests::quota_exhausted_freezes -- --nocapture`
Expected: 编译错误（无 `frozen_until` / `set_frozen_until_for_test`）

- [ ] **Step 3: 实现**

3a. `src/kiro/model/credentials.rs`：`KiroCredentials` 加

```rust
    /// 402 冷冻截止时间（epoch 秒）。冷冻期内不参与调度，到期惰性回池——
    /// 无任何后台解冻任务（参照 sub2api 的惰性冷却模式）。None = 未冷冻。
    /// 与 `disabled` 的分界：可自愈的（配额月度重置）进冷冻，不可自愈的
    /// （手动停用、refresh token 失效）才禁用。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen_until: Option<i64>,
```

3b. `token_manager.rs` 常量区加：

```rust
/// 402 冷冻兜底时长：拿不到 next_reset_at 时乐观冻 1 小时——到期回池，
/// 仍超限会被下一个真实 402 覆盖式重冻，每小时一次试探成本可忽略。
const FREEZE_FALLBACK_SECS: i64 = 3600;
/// next_reset_at 的安全余量：上游重置生效可能滞后于时间戳本身。
const FREEZE_RESET_MARGIN_SECS: i64 = 300;
```

3c. 模块级辅助（与四个过滤点同文件）：

```rust
/// 冷冻判定：与 throttled_until 同构的惰性比较，到期即视为可调度，无需写回。
fn entry_frozen(e: &TokenEntry, now_ts: i64) -> bool {
    e.credentials.frozen_until.is_some_and(|t| now_ts > 0 && now_ts < t)
}
```

3d. **四个选号过滤点**（:1608 / :1790 / :1950 / :2819，行号会漂移，用
`grep -n 'throttled_until.map(|t| t > now)' src/kiro/token_manager.rs` 逐个定位）在既有条件后追加 `&& !entry_frozen(e, now_ts)`（`now_ts = chrono::Utc::now().timestamp()`，在各过滤闭包外算一次）。**改完后再 grep 一遍 `!e.disabled` 确认没有漏网的第五处候选过滤**——有就一并加并在报告里列出。

3e. `report_quota_exhausted` 重写核心段（保留切换 current_id 的既有逻辑，把「凭据可用」判定同步换成 `!disabled && !frozen`）：

```rust
            // 402 = 可自愈状态（月度重置），进冷冻不进禁用。
            // 截止时间优先取余额快照的 next_reset_at（+安全余量）；
            // dispatcher 未注入（单测）或快照缺失 → 兜底 1 小时。
            let now_ts = Utc::now().timestamp();
            let reset_based = self
                .dispatcher
                .read()
                .as_ref()
                .and_then(|d| d.balance_next_reset_at(id))
                .filter(|t| *t > now_ts)
                .map(|t| t + FREEZE_RESET_MARGIN_SECS);
            entry.credentials.frozen_until =
                Some(reset_based.unwrap_or(now_ts + FREEZE_FALLBACK_SECS));
            entry.last_used_at = Some(Utc::now().to_rfc3339());
            entry.total_failure_count += 1;
            // 不再置 disabled / disabled_reason / failure_count=MAX：
            // 面板以「冷冻中」状态呈现，不该伪装成失败爆表。
```

函数尾部把 `save_stats_debounced()` 换成/追加 `let _ = self.persist_credentials();`（frozen_until 在凭据文件而非统计文件；持久化失败打 warn 不炸请求路径——照 `update_credential` 的错误处理写法）。

3f. `report_success`（:2488）在清 `throttled_until` 旁边加：

```rust
                // 成功 = 额度可用，冷冻解除（含清理过期残留的时间戳）
                let was_frozen = entry.credentials.frozen_until.take().is_some();
```

块外 `if was_frozen { let _ = self.persist_credentials(); }`（成功路径高频，只有真清了才落盘）。

3g. `clear_freeze`（供 Task 4 端点）：

```rust
    /// 人工立即解冻（面板动作）。只清冷冻，不碰 disabled。
    pub fn clear_freeze(&self, id: u64) -> anyhow::Result<()> {
        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials.frozen_until = None;
        }
        self.persist_credentials()?;
        Ok(())
    }
```

3h. 存量迁移：在构造/加载路径（`MultiTokenManager::new` 里 entries 初始化完成后）：

```rust
        // 存量迁移：旧版把 402 记作 disabled+QuotaExceeded（自愈靠余额刷新，
        // 该链路已被冷冻机制取代）。一次性转为冷冻态，其他禁用原因不动。
        for e in entries.iter_mut() {
            if e.disabled && e.disabled_reason == Some(DisabledReason::QuotaExceeded) {
                e.disabled = false;
                e.disabled_reason = None;
                e.credentials.frozen_until =
                    Some(Utc::now().timestamp() + FREEZE_FALLBACK_SECS);
            }
        }
```

（`disabled/disabled_reason` 在 entry 还是 credentials 上以实际代码为准。）

3i. 快照结构（:1015 附近）加导出字段，紧挨 `throttled_remaining_secs`：

```rust
    /// 冷冻截止（epoch 秒）；None = 未冷冻。到期后字段保留到下次成功请求。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen_until: Option<i64>,
```

3j. `dispatch.rs` 加：

```rust
    /// 读某凭据余额快照里的 next_reset_at（epoch 秒）。无快照或无该字段返回 None。
    /// 供 402 冷冻计算截止时间；只读，不触发刷新。
    pub fn balance_next_reset_at(&self, cred_id: u64) -> Option<i64> {
        self.balance
            .snapshot()
            .entries
            .get(&cred_id)
            .and_then(|c| c.data.next_reset_at)
            .map(|t| t as i64)
    }
```

3k. `set_frozen_until_for_test`：`#[cfg(test)] pub fn`，直接改 entry 后不落盘。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test kiro::token_manager`
Expected: 新增 3 测全绿；既有测试若因快照结构加字段编译失败，补 `frozen_until: None` 字面量，不改断言语义。

- [ ] **Step 5: Commit**

```bash
git add src/kiro/model/credentials.rs src/kiro/token_manager.rs src/kiro/dispatch.rs
git commit -m "feat(freeze): 402 配额耗尽改为冷冻时间戳 + 惰性到期回池

不再 disabled+探测自愈：冻结截止取余额快照 next_reset_at+300s 余量，
缺失兜底 1h 乐观回池，再 402 覆盖式重冻。四处选号过滤加冷冻判定，
成功请求清冻结，存量 disabled+QuotaExceeded 加载时迁移。
参照 sub2api 惰性冷却模式与本仓库既有 throttled_until 同构先例。"
```

---

### Task 2: service 侧——自愈链路替换

**Files:**
- Modify: `src/admin/service.rs`（`refresh_all_balances` 特判、`clear_quota_disable_if_replenished` 改造、测试改写）
- Modify: `src/kiro/token_manager.rs`（`clear_quota_disable_if_replenished` → 冷冻语义）
- Test: `service.rs` / `token_manager.rs` 的 `mod tests`

**Interfaces:**
- Consumes: Task 1 的 `frozen_until` / `entry_frozen`
- Produces: `MultiTokenManager::thaw_if_replenished(&self, id: u64, remaining: f64) -> bool`（改造自 `clear_quota_disable_if_replenished`，:2623）

- [ ] **Step 1: 改写 token_manager 侧**

`clear_quota_disable_if_replenished`（:2623）改名 `thaw_if_replenished`，语义：该凭据**冷冻中**且 `remaining > 0.0` → 清 `frozen_until` + `persist_credentials`，返回 true；否则 false。旧的「解除 QuotaExceeded 禁用」分支删除（迁移后不存在该状态）。原调用点（service.rs 里 fetch 到余额后）同步改名。文档注释写明：这是**提前解冻的兜底优化**（被动刷新恰好看到额度已恢复时不用等冷冻到期），正路是冷冻到期。

- [ ] **Step 2: refresh_all_balances 去特判**

service.rs:973 的「disabled 但 QuotaExceeded 继续探测」特例删除，改为统一跳过所有 `disabled`；**冷冻凭据不是 disabled，自然会被刷**（这正是「冷冻中但 reset 未知→尽快拿到真实时间」的实现载体：刷到余额后，若该凭据仍冷冻且拿到了未来的 `next_reset_at`，把 `frozen_until` 校正为 `next_reset_at + 300`——在 thaw_if_replenished 返回 false 的分支里顺手做，新增 `MultiTokenManager::refine_freeze_deadline(&self, id, next_reset_at: i64)`）。

- [ ] **Step 3: 改写保护测试（语义继承）**

`refresh_all_balances_runs_regardless_of_mode`（service.rs:5524）删除，替换为：

```rust
    /// 语义继承自 refresh_all_balances_runs_regardless_of_mode：那条测试守的是
    /// 「402 凭据必须能自愈」（当年门控刷新导致凭据永久回不了池的回归）。
    /// 冷冻机制下自愈不再依赖任何刷新——本测试断言冷冻到期后凭据在
    /// **零刷新**条件下自然回池。
    #[tokio::test]
    async fn frozen_credential_returns_to_pool_without_any_refresh() {
        // 构造单凭据 manager → report_quota_exhausted → set_frozen_until_for_test(过去)
        // → 断言 acquire/选号视其可用（复用 Task 1 测试的行为断言写法）
        // → 全程不调用任何 refresh_all_balances。
    }
```

（实现体展开方式：Task 1 的测试此时**已提交在仓库里**，读
`token_manager.rs` 里的 `quota_exhausted_freezes_instead_of_disabling` 照它的
构造与行为断言写法适配即可；**不许**只删不替。）

- [ ] **Step 4: 全量跑 + Commit**

Run: `cargo test kiro::token_manager && cargo test admin::service`

```bash
git add src/kiro/token_manager.rs src/admin/service.rs
git commit -m "feat(freeze): 自愈链路从刷新探测切换到冷冻语义

thaw_if_replenished 降级为提前解冻兜底；refresh_all_balances 去掉
QuotaExceeded 探测特判；盲冻凭据在被动刷新拿到真实 next_reset_at 后
校正截止时间。保护测试语义继承改写（402 自愈不再依赖刷新）。"
```

---

### Task 3: 余额被动刷新

**Files:**
- Modify: `src/admin/balance_cache.rs`（加 Notify 触发器）
- Modify: `src/kiro/dispatch.rs`（pick 检测过期 → poke）
- Modify: `src/admin/service.rs`（删周期循环，加被动监听任务）
- Modify: `src/main.rs`（:540-556 注释与启动调用替换）
- Test: 各文件 `mod tests`

**Interfaces:**
- Consumes: 既有 `BALANCE_CACHE_TTL_SECS = 300`（balance_cache.rs:21）、`refresh_all_balances`
- Produces: `BalanceCache::request_passive_refresh(&self)` / `BalanceCache::passive_refresh_requested(&self) -> &tokio::sync::Notify`
- Produces: `AdminService::start_passive_balance_refresher(self: &Arc<Self>)`（替代 `start_balance_refresher`）

- [ ] **Step 1: BalanceCache 加触发器**

```rust
    /// 被动刷新触发器：选号侧发现快照过期时 poke，AdminService 侧的监听
    /// 任务被唤醒后做防抖刷新。Notify 天然合并重复通知，poke 是零成本操作。
    passive_refresh: tokio::sync::Notify,
```

（构造处初始化；`request_passive_refresh` = `self.passive_refresh.notify_one()`；`passive_refresh_requested` 返回引用供监听侧 `.notified().await`。）

- [ ] **Step 2: dispatch.pick 检测过期**

`pick()` 内已逐候选计算陈旧秒数（dispatch.rs:335 `staleness` 辅助）。在候选集组装完成处加：

```rust
        // 被动刷新：任一候选余额快照过期即 poke（weighted 才会走到本函数，
        // priority 模式天然零触发）。poke 只是 Notify，不阻塞、不重复排队。
        if candidates_stale(&entries, now_ts) {
            self.balance.request_passive_refresh();
        }
```

`candidates_stale` = 任一候选 `now_ts - cached_at > BALANCE_CACHE_TTL_SECS as f64`（缺失条目视为过期）。

- [ ] **Step 3: AdminService 监听任务（替代周期循环）**

删除 `start_balance_refresher`（service.rs:1032-1057），新增：

```rust
    /// 被动余额刷新监听：等待选号侧 poke，single-flight + 逐凭据 60s 防抖。
    /// 与旧的 300s 周期循环的区别：无流量 = 零上游查询。
    pub fn start_passive_balance_refresher(self: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        if self
            .balance_refresher_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let svc = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                svc.balance_cache.passive_refresh_requested().notified().await;
                let started = std::time::Instant::now();
                let (ok, err) = svc.refresh_stale_balances().await;
                if ok + err > 0 {
                    tracing::info!(
                        "被动余额刷新：成功 {}，失败 {}，耗时 {:.1}s",
                        ok, err, started.elapsed().as_secs_f32()
                    );
                }
                // 防抖下限：一轮结束后至少歇 PASSIVE_REFRESH_MIN_INTERVAL_SECS
                // 再接受下一轮，poke 在 Notify 里自动合并。
                tokio::time::sleep(std::time::Duration::from_secs(
                    PASSIVE_REFRESH_MIN_INTERVAL_SECS,
                )).await;
            }
        });
    }
```

`refresh_stale_balances`：复制 `refresh_all_balances` 的循环骨架，但只刷「未禁用且（`now - cached_at > PASSIVE_REFRESH_MIN_INTERVAL_SECS` 的过期凭据 或 冷冻中且余额快照无 `next_reset_at` 的凭据）」；刷到余额后走 Task 2 的 `thaw_if_replenished` / `refine_freeze_deadline`。`refresh_all_balances` 本体保留（面板手动刷新用）。`PASSIVE_REFRESH_MIN_INTERVAL_SECS: u64 = 60` 常量放 service.rs 顶部常量区。

- [ ] **Step 4: main.rs 替换**

:540-548 的注释块与调用替换为：

```rust
            // 被动余额刷新（2026-08-22 起）：无流量零查询。选号侧发现快照过期
            // 才 poke 刷新；402 自愈已改为冷冻到期惰性回池（见 passive-balance-
            // freeze 设计），不再依赖周期刷新链路。面板余额列与趋势图在无流量
            // 时段会停更——这是已接受的代价。
            admin_state.service.start_passive_balance_refresher();
```

- [ ] **Step 5: 测试**

- balance_cache：poke 后 `notified()` 立即返回；无 poke 时 `now_or_never` 为 None。
- dispatch：构造过期快照 → `pick` 后 Notify 已被触发；快照新鲜 → 未触发。
- service：`refresh_stale_balances` 只刷过期凭据（新鲜的跳过）——照 `refresh_all_balances_runs_regardless_of_mode` 旧测试的构造方式（2 凭据 manager + 无上游的失败计数断言）适配。

- [ ] **Step 6: 全量跑 + Commit**

Run: `cargo test`
Expected: 全绿（除已知 flake）。

```bash
git add src/admin/balance_cache.rs src/kiro/dispatch.rs src/admin/service.rs src/main.rs
git commit -m "feat(balance): 删除 300s 周期刷新，改为选号触发的被动刷新

dispatch 发现快照过期 poke Notify，AdminService 监听侧 single-flight +
逐凭据 60s 防抖。无流量时段零上游查询；priority 模式天然零触发。
生产基线：balance_refresh 16603 次/7.6 天（推理请求的 2 倍）。"
```

---

### Task 4: 解冻端点 + 面板冷冻态

**Files:**
- Modify: `src/admin/handlers.rs` + `src/admin/router.rs`（`POST /credentials/{id}/unfreeze`）
- Modify: `src/admin/types.rs` 或快照透传处（确认 `frozen_until` 一路到 admin API 响应，跟着 `throttled_remaining_secs` 的既有路径走）
- Modify: `admin-ui/src/api/credentials.ts` / `admin-ui/src/types/api.ts` / `admin-ui/src/components/credential-card.tsx`
- Test: handlers 契约测试 + `cd admin-ui && /root/.bun/bin/bun run build`

**Interfaces:**
- Consumes: Task 1 的 `clear_freeze(id)`、快照 `frozen_until`
- Produces: `POST /api/admin/credentials/{id}/unfreeze` → `SuccessResponse`

- [ ] **Step 1: 后端端点**

handler 照 `clear_throttle`（handlers.rs 里搜）逐字模仿：调 `token_manager.clear_freeze(id)`，成功返回 `SuccessResponse::new(format!("凭据 #{} 已解冻", id))`。router 挂在 `/credentials/{id}/clear-throttle` 旁边。契约测试：快照序列化含 `frozenUntil`（若快照 serde 是 camelCase）或 `frozen_until`（若 snake）——**以 `throttled_remaining_secs` 在 API 响应里的实际形态为准**，测试断言字面量（先例 :4888）。

- [ ] **Step 2: 前端**

- `types/api.ts`：`CredentialStatusItem` 加 `frozenUntil?: number`（字段名跟 Step 1 实测一致）。
- `api/credentials.ts`：`clearFreeze(id)` 照 `clearThrottle`（:142）逐字模仿。
- `credential-card.tsx`：`frozenUntil` 存在且大于当前时间 → 显示「❄ 冷冻中（至 HH:MM）」徽标（暖色计时风格，与红色「已禁用」区分）+「立即解冻」小按钮（调 `clearFreeze` + 刷新查询，交互照清除冷却按钮）。过期未清的时间戳不显示徽标。

- [ ] **Step 3: 构建验证 + Commit**

Run: `cargo test admin:: && cd admin-ui && /root/.bun/bin/bun run build`

```bash
git add src/admin/handlers.rs src/admin/router.rs src/admin/types.rs admin-ui/src/api/credentials.ts admin-ui/src/types/api.ts admin-ui/src/components/credential-card.tsx
git commit -m "feat(admin): 冷冻状态展示与立即解冻动作"
```

---

### Task 5: 版本收尾与全量验证

**Files:** `Cargo.toml` / `Cargo.lock` / `CHANGELOG.md`

- [ ] **Step 1**: 全量 `cargo test`（预期 ≥965，除 flake 全绿）+ clippy 增量核对（所碰文件不升）+ `bun run build` 复验。
- [ ] **Step 2**: version → `0.9.17`；CHANGELOG 顶部新增 `## [0.9.17] - 2026-08-22`：余额被动化（含生产量化：16603 次/7.6 天 → 预期降一个数量级）+ 402 冷冻自然恢复（含「可自愈进冷冻、不可自愈才禁用」的分界线与 sub2api 参照）+ 已接受代价（无流量时段面板曲线停更）。
- [ ] **Step 3**: Commit `chore: 版本号升至 0.9.17（余额被动化 + 402 冷冻自然恢复）`。
- [ ] **Step 4（部署后验证，需用户在场，不自动执行）**：
  - `operation='balance_refresh'` 日增量对比（基线 ~2185/天）
  - `set_frozen_until_for_test` 不可用于生产——用面板把一张凭据的冷冻场景走一遍：人为触发 402（或等自然发生）→ 面板显示冷冻 → 「立即解冻」可用
  - 无流量时段确认日志零「被动余额刷新」条目

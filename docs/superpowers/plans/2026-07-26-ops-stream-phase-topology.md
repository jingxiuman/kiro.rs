# 流式分段埋点与错误详情拓扑图 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让错误详情能定位「流在哪一段断的」，并给出同维度历史对照，使单条错误可归因。

**Architecture:** 新增 `trace_phases` 表记录流生命周期三段（`first_token` / `streaming` / `finish`），
沿用 `RequestTracer` 已有的「进程内累积、`finalize` 时同事务落库」模式，不新开写入路径。
前端把已有 `trace_attempts`（N 跳重试）与新增 `trace_phases`（1 条流）拼接成两层泳道。

**Tech Stack:** Rust 2024 / axum / rusqlite(SQLite WAL) / React + TypeScript + Vite

**Spec:** `docs/superpowers/specs/2026-07-26-ops-stream-phase-topology-design.md`

## Global Constraints

- 埋点失败不得影响主流程。沿用 `finalize` 开头 `let Some(store) = &self.store else { return }` 的 no-op 约定。
- phases 与 attempts 同事务写入，失败一起丢，不做补偿。
- 历史行**不回填**。`proxy_url IS NULL` 永久保留「未知」语义。
- `client_gone` 判定取保守方向：仅在明确检测到客户端断开时判定，其余归上游断。宁可冤枉代理，不可漏放。
- 新表通过 `SCHEMA` 常量里的 `CREATE TABLE IF NOT EXISTS` 建立。已验证 `TraceStore::open`（`src/admin/trace_db.rs:233`）与 `open_in_memory`（`:245`）均执行 `execute_batch(SCHEMA)`，老库自动补表，**无需改 `migrate()`**。
- Rust 测试：`cargo test <name> -- --nocapture`。**验证在制品是否可编译用 `cargo test --no-run`，不要用 `cargo build`**（`cargo build` 不编译测试代码，会给假信号）。
- **本 crate 是 bin-only，没有 lib target。`cargo test --lib` 会直接报 `no library targets found`（已实测）。需要限定 target 时用 `cargo test --bin kiro-rs <name>`。**
- 前端构建：`cd admin-ui && npm run build`（即 `tsc -b && vite build`）。
- 每个 Task 结束提交一次。

---

### Task 1: proxy_url 三态语义修正

把「真直连」写成字面量 `"direct"`，让 `NULL` 只剩「未知」一种含义。前端三态渲染。

**Files:**
- Modify: `src/kiro/provider.rs:888`（`emit_attempt` 内 `proxy_url` 落库处）
- Modify: `src/admin/trace_db.rs:44`（`TraceAttempt.proxy_url` 文档注释）
- Modify: `admin-ui/src/components/trace-log-page.tsx:170-172`（`AttemptRow` 出口渲染）
- Modify: `admin-ui/src/types/api.ts:482-483`（`TraceAttempt.proxyUrl` 注释）
- Test: `src/admin/trace_db.rs`（`mod tests`，新增用例）

**Interfaces:**
- Consumes: `KiroCredentials::PROXY_DIRECT`（已存在，`src/kiro/model/credentials.rs:365`，值为 `"direct"`）
- Produces: 新写入的 `trace_attempts.proxy_url` 恒为非 NULL；`NULL` 仅存在于历史行

**关键约束（改错会引发线上误判）：** 只在 `emit_attempt` 这一个点做映射。
`provider.rs:501` 的 `let proxy_url = self.proxy_label(...)` 返回的 `Option<String>` 还会流向
`KiroResponse.proxy_url`（`:596`）并最终传给 `OpsRuntime::report_proxy_failure`（`src/admin/ops.rs:532`），
那里 `let Some(url) = proxy_url else { return }` 依赖 `None` 表示直连。
**若在上游改成 `Some("direct")`，代理池会去给一个名叫 "direct" 的不存在代理记失败计数。**

> **TDD 节奏说明：** 缺陷在**写入侧**（`emit_attempt` 丢掉了直连信息），所以红灯测试是 Step 1。
> Step 5 的存储层测试是**回归断言**，不驱动实现——它锁住「存储层不得把 direct 塌陷成 NULL」
> 这个不变量，防止日后有人在 insert/query 里加「空值归一」之类的好心优化。
> 按 Step 1→11 顺序执行即可。

- [ ] **Step 1: 写失败测试（红灯）**

在 `src/kiro/provider.rs` 文件末尾（若无 `mod tests` 则新建）：

```rust
#[cfg(test)]
mod emit_attempt_tests {
    use super::*;
    use crate::admin::trace_db::{TraceAttempt, TraceSink};
    use parking_lot::Mutex;
    use std::time::Instant;

    struct CollectSink(Mutex<Vec<TraceAttempt>>);

    impl TraceSink for CollectSink {
        fn on_attempt(&self, attempt: TraceAttempt) {
            self.0.lock().push(attempt);
        }
    }

    #[test]
    fn emit_attempt_maps_none_proxy_to_direct_literal() {
        let sink = CollectSink(Mutex::new(Vec::new()));
        KiroProvider::emit_attempt(
            Some(&sink),
            0,
            7,
            "ide",
            Some(200),
            crate::admin::trace_db::outcome::SUCCESS,
            None,
            Instant::now(),
            None, // 直连
        );
        let got = sink.0.lock();
        assert_eq!(
            got[0].proxy_url.as_deref(),
            Some("direct"),
            "直连必须落库为 direct 字面量，NULL 只留给历史行"
        );
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test emit_attempt_maps_none_proxy_to_direct_literal -- --nocapture`
Expected: FAIL，断言显示 `left: None, right: Some("direct")`

- [ ] **Step 3: 实现映射**

`src/kiro/provider.rs:888`，把：

```rust
            proxy_url: proxy_url.map(|s| s.to_string()),
```

改为：

```rust
            // 直连落库为字面量 direct；NULL 只保留给「该列存在前的历史行」= 未知。
            // 注意：仅在此处映射。上游 proxy_url 的 Option 语义还被 OpsRuntime
            // 的代理健康统计消费，在那里 None 必须继续表示「无代理可罚」。
            proxy_url: Some(
                proxy_url
                    .unwrap_or(crate::kiro::model::credentials::KiroCredentials::PROXY_DIRECT)
                    .to_string(),
            ),
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test emit_attempt_maps_none_proxy_to_direct_literal -- --nocapture`
Expected: PASS

- [ ] **Step 5: 补存储层回归断言**

在 `src/admin/trace_db.rs` 的 `mod tests` 末尾新增：

```rust
    #[test]
    fn proxy_url_tri_state() {
        let store = mem_store();
        let mut rec = sample(TraceSample {
            trace_id: "t-tri",
            status: "success",
            credential_id: 5,
            model: "m1",
        });
        // 第 0 跳：走代理；第 1 跳：真直连（字面量 direct）
        rec.attempts[0].proxy_url = Some("socks5://p1:1080".to_string());
        rec.attempts[1].proxy_url = Some("direct".to_string());
        store.insert(&rec);

        let out = store.query(&TraceQuery {
            limit: 50,
            ..Default::default()
        });
        assert_eq!(
            out[0].attempts[0].proxy_url.as_deref(),
            Some("socks5://p1:1080")
        );
        assert_eq!(
            out[0].attempts[1].proxy_url.as_deref(),
            Some("direct"),
            "真直连必须写成字面量 direct，不能塌陷成 NULL"
        );
    }
```

- [ ] **Step 6: 运行两个测试**

Run: `cargo test emit_attempt -- --nocapture && cargo test proxy_url_tri_state -- --nocapture`
Expected: 两个都 PASS。`proxy_url_tri_state` 本就该通过——它记录不变量，不驱动改动。

- [ ] **Step 7: 确认 ops 侧未被波及**

Run: `cargo test --no-run 2>&1 | tail -5`
Expected: 编译通过，无 warning 新增。
再人工核对 `src/kiro/provider.rs:596` 的 `KiroResponse.proxy_url` 仍是 `Option<String>` 原值，未被改成 `"direct"`。

- [ ] **Step 8: 改前端三态渲染**

`admin-ui/src/components/trace-log-page.tsx`，把第 170-172 行：

```tsx
        <span className="max-w-[220px] truncate font-mono text-[12px]" title={a.proxyUrl ?? '直连'}>
          {a.proxyUrl ?? '直连'}
        </span>
```

替换为：

```tsx
        <ProxyLabel url={a.proxyUrl} />
```

并在 `AttemptRow` 函数**之前**新增组件：

```tsx
/** 出口三态：direct = 直连；null/undefined = 未知（该列存在前的历史行）；其余 = 代理 URL */
function ProxyLabel({ url }: { url?: string | null }) {
  if (url == null) {
    return (
      <span className="font-mono text-[12px] text-muted-foreground/60" title="该记录早于出口埋点，真实出口不可知">
        未知
      </span>
    )
  }
  const text = url === 'direct' ? '直连' : url
  return (
    <span className="max-w-[220px] truncate font-mono text-[12px]" title={text}>
      {text}
    </span>
  )
}
```

- [ ] **Step 9: 更新类型注释**

`admin-ui/src/types/api.ts:482-483`，把：

```ts
  /** 本跳实际使用的出口代理 URL；null/undefined = 直连 */
  proxyUrl?: string | null
```

改为：

```ts
  /** 本跳出口：'direct' = 直连；null/undefined = 未知（该列存在前的历史行）；其余为代理 URL */
  proxyUrl?: string | null
```

同步改 `src/admin/trace_db.rs:44` 的 Rust 侧注释：

```rust
    /// 本跳出口：`"direct"` = 直连；`None` = 未知（该列存在前的历史行）
```

- [ ] **Step 10: 前端构建**

Run: `cd admin-ui && npm run build`
Expected: 构建成功，无 TS 报错

- [ ] **Step 11: 提交**

```bash
git add src/kiro/provider.rs src/admin/trace_db.rs admin-ui/src/components/trace-log-page.tsx admin-ui/src/types/api.ts
git commit -m "fix(trace): proxy_url 三态语义 —— 直连写字面量，NULL 只表示未知

前端此前硬编码 proxyUrl ?? '直连'，把该列存在前的历史行渲染成直连，
据此做出的归因结论是错的。映射只在 emit_attempt 一处做，
上游 Option 语义仍被代理健康统计依赖，不能动。"
```

---

### Task 2: trace_phases 表与读写往返

**Files:**
- Modify: `src/admin/trace_db.rs`（`TracePhase` 类型、`SCHEMA`、`TraceRecord.phases`、`insert`、`query`、`cleanup`、`delete_for_credential`）
- Test: `src/admin/trace_db.rs`（`mod tests`）

**Interfaces:**
- Produces:
  - `pub struct TracePhase { pub seq: u32, pub phase: String, pub started_ms: u64, pub duration_ms: u64, pub outcome: String, pub bytes: Option<u64>, pub detail: Option<String> }`（serde `rename_all = "camelCase"`）
  - `pub mod phase { pub const FIRST_TOKEN: &str; pub const STREAMING: &str; pub const FINISH: &str; }`
  - `pub const outcome::CLIENT_DISCONNECTED: &str = "client_disconnected"`
  - `TraceRecord.phases: Vec<TracePhase>`

- [ ] **Step 1: 写失败测试**

在 `src/admin/trace_db.rs` 的 `mod tests` 末尾新增：

```rust
    #[test]
    fn phases_roundtrip() {
        let store = mem_store();
        let mut rec = sample(TraceSample {
            trace_id: "t-ph",
            status: "error",
            credential_id: 5,
            model: "m1",
        });
        rec.phases = vec![
            TracePhase {
                seq: 0,
                phase: phase::FIRST_TOKEN.to_string(),
                started_ms: 0,
                duration_ms: 1200,
                outcome: outcome::SUCCESS.to_string(),
                bytes: Some(0),
                detail: None,
            },
            TracePhase {
                seq: 1,
                phase: phase::STREAMING.to_string(),
                started_ms: 1200,
                duration_ms: 18400,
                outcome: outcome::SUCCESS.to_string(),
                bytes: Some(20211),
                detail: None,
            },
            TracePhase {
                seq: 2,
                phase: phase::FINISH.to_string(),
                started_ms: 19600,
                duration_ms: 3,
                outcome: outcome::UPSTREAM_TRUNCATED.to_string(),
                bytes: Some(20211),
                detail: Some("buffered 331 bytes".to_string()),
            },
        ];
        store.insert(&rec);

        let out = store.query(&TraceQuery {
            limit: 50,
            ..Default::default()
        });
        assert_eq!(out[0].phases.len(), 3);
        assert_eq!(out[0].phases[0].phase, phase::FIRST_TOKEN);
        assert_eq!(out[0].phases[2].outcome, outcome::UPSTREAM_TRUNCATED);
        assert_eq!(out[0].phases[2].bytes, Some(20211));
        assert_eq!(
            out[0].phases[2].detail.as_deref(),
            Some("buffered 331 bytes")
        );
        // 顺序由 seq 保证
        assert_eq!(
            out[0].phases.iter().map(|p| p.seq).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn cleanup_removes_phases() {
        let store = mem_store();
        let mut rec = sample(TraceSample {
            trace_id: "t-old",
            status: "error",
            credential_id: 5,
            model: "m1",
        });
        // 造一条 30 天前的记录
        rec.ts = (Utc::now() - chrono::Duration::days(30)).to_rfc3339();
        rec.phases = vec![TracePhase {
            seq: 0,
            phase: phase::STREAMING.to_string(),
            started_ms: 0,
            duration_ms: 10,
            outcome: outcome::SUCCESS.to_string(),
            bytes: Some(1),
            detail: None,
        }];
        store.insert(&rec);
        store.cleanup();

        let conn = store.conn.lock();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM trace_phases", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "过期 trace 的 phases 必须一并清理，不能留孤儿行");
    }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test phases_roundtrip -- --nocapture`
Expected: 编译失败，`cannot find type TracePhase` / `no field phases on TraceRecord`

- [ ] **Step 3: 定义类型与常量**

在 `src/admin/trace_db.rs` 的 `TraceAttempt` 定义之后新增：

```rust
/// 流生命周期的一段。仅流式请求产生；非流式请求无此记录。
///
/// 与 [`TraceAttempt`] 的分工：attempt 覆盖 connect→headers（N 跳，含重试），
/// phase 覆盖 headers 之后的流生命周期（1 条流）。两者基数不同，不合表。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracePhase {
    /// 段序号，从 0 递增，保证渲染顺序
    pub seq: u32,
    /// 段名，见 [`phase`]
    pub phase: String,
    /// 相对请求起点的偏移（毫秒）
    pub started_ms: u64,
    /// 本段耗时（毫秒）
    pub duration_ms: u64,
    /// 本段结局，复用 [`outcome`] 常量
    pub outcome: String,
    /// 该段结束时已下发给客户端的累计字节数
    pub bytes: Option<u64>,
    /// 错误片段 / 判别位摘要
    pub detail: Option<String>,
}

/// 流生命周期段名
pub mod phase {
    /// 建连成功到首个上游 chunk 到达
    pub const FIRST_TOKEN: &str = "first_token";
    /// 首 chunk 之后的持续传输
    pub const STREAMING: &str = "streaming";
    /// 流结束时的收尾判定（含 tool_use 累积器 finish 结果）
    pub const FINISH: &str = "finish";
}
```

在 `mod outcome` 内（`src/admin/trace_db.rs:136`）追加：

```rust
    /// 仅用作 phase.outcome：客户端主动断开（非上游故障，不计入代理健康）
    pub const CLIENT_DISCONNECTED: &str = "client_disconnected";
```

- [ ] **Step 4: 给 TraceRecord 加字段**

在 `TraceRecord` 的 `attempts` 字段旁新增（保持 `attempts` 在前）：

```rust
    /// 流生命周期分段；非流式请求为空
    #[serde(default)]
    pub phases: Vec<TracePhase>,
```

- [ ] **Step 5: 建表**

在 `SCHEMA` 常量末尾（`CREATE INDEX IF NOT EXISTS idx_attempts_trace ...` 之后）追加：

```sql
CREATE TABLE IF NOT EXISTS trace_phases (
    trace_id    TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    phase       TEXT NOT NULL,
    started_ms  INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    outcome     TEXT NOT NULL,
    bytes       INTEGER,
    detail      TEXT,
    PRIMARY KEY (trace_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_phases_trace ON trace_phases(trace_id);
CREATE INDEX IF NOT EXISTS idx_phases_phase_outcome ON trace_phases(phase, outcome);
```

- [ ] **Step 6: 写入**

在 `insert` 的 `for a in &rec.attempts { ... }` 循环之后追加：

```rust
            for p in &rec.phases {
                tx.execute(
                    "INSERT OR REPLACE INTO trace_phases (trace_id, seq, phase, started_ms, \
                     duration_ms, outcome, bytes, detail) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    rusqlite::params![
                        rec.trace_id,
                        p.seq as i64,
                        p.phase,
                        p.started_ms as i64,
                        p.duration_ms as i64,
                        p.outcome,
                        p.bytes.map(|v| v as i64),
                        p.detail,
                    ],
                )?;
            }
```

- [ ] **Step 7: 读取**

在 `query` 里构造 `TraceRecord` 的地方，`attempts: Vec::new(),` 之后加 `phases: Vec::new(),`。

然后在「批量取每条 trace 的 attempts」那段循环之后追加：

```rust
        // 批量取每条 trace 的 phases
        let mut phase_stmt = conn.prepare(
            "SELECT seq, phase, started_ms, duration_ms, outcome, bytes, detail \
             FROM trace_phases WHERE trace_id = ? ORDER BY seq ASC",
        )?;
        for rec in &mut records {
            let phases = phase_stmt.query_map([&rec.trace_id], |row| {
                Ok(TracePhase {
                    seq: row.get::<_, i64>(0)? as u32,
                    phase: row.get(1)?,
                    started_ms: row.get::<_, i64>(2)? as u64,
                    duration_ms: row.get::<_, i64>(3)? as u64,
                    outcome: row.get(4)?,
                    bytes: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    detail: row.get(6)?,
                })
            })?;
            rec.phases = phases.collect::<rusqlite::Result<_>>()?;
        }
```

- [ ] **Step 8: 清理联动**

`cleanup` 里，在 `DELETE FROM trace_attempts ...` **之前**加一条同形状的语句：

```rust
            tx.execute(
                "DELETE FROM trace_phases WHERE trace_id IN \
                 (SELECT trace_id FROM traces WHERE ts_epoch < ?1)",
                [cutoff],
            )?;
```

`delete_for_credential` 里同样补一条，条件与该函数已有的 `trace_attempts` 删除语句保持一致
（照抄那条语句，把表名换成 `trace_phases`）。

- [ ] **Step 9: 修补测试辅助**

`fn sample(...)` 里给 `TraceRecord` 补上 `phases: Vec::new(),`，否则既有测试编译不过。

- [ ] **Step 10: 运行全部测试**

Run: `cargo test --bin kiro-rs trace_db -- --nocapture`
Expected: `phases_roundtrip`、`cleanup_removes_phases` 与所有既有 trace_db 测试全部 PASS

- [ ] **Step 11: 提交**

```bash
git add src/admin/trace_db.rs
git commit -m "feat(trace): 新增 trace_phases 表记录流生命周期三段

attempt 只覆盖到响应头，流中截断记为 success/200，是本次事故看不见的直接原因。
phases 覆盖 headers 之后：first_token / streaming / finish。
与 attempts 同事务写入，随 cleanup 与凭据删除一并清理。"
```

---

### Task 3: RequestTracer 累积 phases

**Files:**
- Modify: `src/anthropic/handlers.rs:124-230`（`RequestTracer`）
- Test: `src/anthropic/handlers.rs`（新增 `mod tracer_tests`）

**Interfaces:**
- Consumes: `TracePhase`、`phase::*`、`outcome::*`（Task 2 产出）
- Produces:
  - `RequestTracer::open_phase(&self, name: &'static str)` — 记录该段起点
  - `RequestTracer::close_phase(&self, name: &'static str, outcome: &str, bytes: Option<u64>, detail: Option<String>)` — 关闭当前段并入队
  - `finalize` 把累积的 phases 写入 `TraceRecord.phases`

- [ ] **Step 1: 写失败测试**

在 `src/anthropic/handlers.rs` 末尾新增：

```rust
#[cfg(test)]
mod tracer_tests {
    use super::*;
    use crate::admin::trace_db::{outcome, phase};

    /// 构造一个 store 为 None 的 tracer：验证 phase API 在未启用 trace 时不 panic
    fn detached_tracer() -> RequestTracer {
        RequestTracer {
            store: None,
            trace_id: "t".to_string(),
            ts: "now".to_string(),
            key_id: 0,
            key_source: TraceKeySource::MasterApiKey,
            model: "m".to_string(),
            is_stream: true,
            started_at: Instant::now(),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
        }
    }

    #[test]
    fn phases_accumulate_in_order_with_seq() {
        let t = detached_tracer();
        t.open_phase(phase::FIRST_TOKEN);
        t.close_phase(phase::FIRST_TOKEN, outcome::SUCCESS, Some(0), None);
        t.open_phase(phase::STREAMING);
        t.close_phase(phase::STREAMING, outcome::SUCCESS, Some(20211), None);
        t.open_phase(phase::FINISH);
        t.close_phase(
            phase::FINISH,
            outcome::UPSTREAM_TRUNCATED,
            Some(20211),
            Some("buffered 331 bytes".to_string()),
        );

        let got = t.phases.lock();
        assert_eq!(got.len(), 3);
        assert_eq!(
            got.iter().map(|p| p.seq).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "seq 必须连续递增"
        );
        assert_eq!(got[2].outcome, outcome::UPSTREAM_TRUNCATED);
        assert_eq!(got[2].bytes, Some(20211));
    }

    #[test]
    fn close_without_open_is_ignored_not_panic() {
        let t = detached_tracer();
        // 异常路径：埋点漏了 open 直接 close，不得 panic、不得写入半截段
        t.close_phase(phase::STREAMING, outcome::SUCCESS, None, None);
        assert!(t.phases.lock().is_empty());
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test phases_accumulate_in_order_with_seq -- --nocapture`
Expected: 编译失败，`struct RequestTracer has no field named phases`

- [ ] **Step 3: 加字段**

`src/anthropic/handlers.rs:124` 的 `RequestTracer` 内，`attempts` 之后追加：

```rust
    /// 已关闭的流生命周期段
    phases: parking_lot::Mutex<Vec<TracePhase>>,
    /// 当前打开的段：(段名, 起点)
    open_phase: parking_lot::Mutex<Option<(&'static str, Instant)>>,
```

`RequestTracer::new` 的构造体里对应追加：

```rust
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
```

- [ ] **Step 4: 实现 open_phase / close_phase**

在 `mark_first_token` 之后新增：

```rust
    /// 打开一个流生命周期段。重复 open 会覆盖前一个未关闭的段（视为埋点漏关，丢弃之）。
    pub fn open_phase(&self, name: &'static str) {
        *self.open_phase.lock() = Some((name, Instant::now()));
    }

    /// 关闭当前段并入队。名字不匹配或未 open 时静默忽略——埋点错误不得影响主流程。
    pub fn close_phase(
        &self,
        name: &'static str,
        outcome: &str,
        bytes: Option<u64>,
        detail: Option<String>,
    ) {
        let Some((open_name, started)) = self.open_phase.lock().take() else {
            return;
        };
        if open_name != name {
            return;
        }
        let mut phases = self.phases.lock();
        let seq = phases.len() as u32;
        phases.push(TracePhase {
            seq,
            phase: name.to_string(),
            started_ms: started.duration_since(self.started_at).as_millis() as u64,
            duration_ms: started.elapsed().as_millis() as u64,
            outcome: outcome.to_string(),
            bytes,
            detail,
        });
    }
```

在文件顶部 `use` 区补 `TracePhase`、`phase` 的导入（与既有 `TraceAttempt` 的导入同一行/同一块）。

- [ ] **Step 5: finalize 带上 phases**

`finalize` 内，`let attempts = std::mem::take(&mut *self.attempts.lock());` 之后加：

```rust
        let phases = std::mem::take(&mut *self.phases.lock());
```

`TraceRecord { ... }` 构造里 `attempts,` 之后加 `phases,`。

- [ ] **Step 6: 运行测试确认通过**

Run: `cargo test tracer_tests -- --nocapture`
Expected: 两个测试 PASS

- [ ] **Step 7: 提交**

```bash
git add src/anthropic/handlers.rs
git commit -m "feat(trace): RequestTracer 累积流生命周期段

沿用 attempts 的进程内累积 + finalize 同事务落库模式。
close 无 open 时静默忽略：埋点错误不得影响主流程。"
```

---

### Task 4: 客户端断开检测（先验证，再决定实现）

**这个 Task 有一个未验证的前提，必须先做探针再写实现。** 计划不假装它已解决。

**Files:**
- Test: `src/anthropic/handlers.rs`（`mod tracer_tests` 内追加）
- Modify: `src/anthropic/handlers.rs`（视探针结果决定）

**Interfaces:**
- Produces: `StreamPhaseGuard`（若探针通过）或 启发式判别位（若探针失败）

**背景假设（待验证）：** 客户端断开时，axum 会 drop 掉 response body，
`stream::unfold`（`handlers.rs:771`）的 future 不再被 poll，
于是 `None` 分支永不执行 → `finalize` 永不调用 → **该请求在 traces 里完全不存在**。

若属实，这不只影响 `client_gone` 判别位，而是一个既有的观测缺口：客户端断开的请求当前是隐形的。

- [ ] **Step 1: 探针 —— 验证 drop 行为**

写一个最小测试，确认「stream 被 drop 时 unfold 的状态会被 drop、且可在 Drop 里执行逻辑」：

```rust
    #[test]
    fn dropped_unfold_state_runs_drop_impl() {
        use futures::StreamExt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Marker(Arc<AtomicBool>);
        impl Drop for Marker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let flag = Arc::new(AtomicBool::new(false));
        let marker = Marker(flag.clone());

        let s = futures::stream::unfold(marker, |m| async move {
            Some((1u8, m))
        });
        let s = Box::pin(s);
        drop(s); // 模拟客户端断开：整个 stream 被丢弃

        assert!(
            flag.load(Ordering::SeqCst),
            "unfold 状态被 drop 时 Drop impl 应执行；若此断言失败，客户端断开检测需改用其它手段"
        );
    }
```

- [ ] **Step 2: 运行探针**

Run: `cargo test dropped_unfold_state_runs_drop_impl -- --nocapture`

**分支决策（把结果写进本文件再继续）：**
- PASS → 走 Step 3（Drop guard 方案）
- FAIL → **停止本 Task，回报结果**。退化方案是只用 `bytes` + `idle_ms` 做启发式，
  但那会削弱「不冤枉代理」的保证，属于设计变更，需要重新确认，不得自行降级。

- [ ] **Step 3: 写失败测试（仅在 Step 2 PASS 时执行）**

```rust
    #[test]
    fn client_disconnect_marks_phase_and_does_not_charge_proxy() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::STREAMING);
        {
            let _guard = StreamPhaseGuard::new(t.clone(), 4096);
            // guard 在此作用域结束时 drop —— 模拟客户端断开
        }
        let got = t.phases.lock();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].outcome,
            outcome::CLIENT_DISCONNECTED,
            "客户端断开必须与上游断流区分，否则会冤枉代理"
        );
        assert_eq!(got[0].bytes, Some(4096));
    }

    #[test]
    fn normal_completion_does_not_mark_client_disconnect() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::STREAMING);
        {
            let guard = StreamPhaseGuard::new(t.clone(), 4096);
            guard.into_completed(4096); // 正常收尾：按值消费，记 streaming 成功
        }
        let got = t.phases.lock();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].outcome,
            outcome::SUCCESS,
            "正常收尾不得被误记成客户端断开"
        );
    }

    #[test]
    fn upstream_error_does_not_mark_client_disconnect() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::STREAMING);
        {
            let guard = StreamPhaseGuard::new(t.clone(), 512);
            guard.into_upstream_error(512, &"connection reset");
        }
        let got = t.phases.lock();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].outcome,
            outcome::STREAM_INTERRUPTED,
            "上游断流必须记成 stream_interrupted，不得吞成客户端断开"
        );
        assert!(
            got[0].detail.as_deref().unwrap().contains("client_gone=false"),
            "判别位必须明确写 client_gone=false"
        );
    }
```

- [ ] **Step 4: 运行确认失败**

Run: `cargo test client_disconnect_marks_phase -- --nocapture`
Expected: 编译失败，`cannot find type StreamPhaseGuard`

- [ ] **Step 5: 实现 guard**

在 `src/anthropic/handlers.rs` 的 `RequestTracer` impl 之后新增：

```rust
/// 挂在流状态里的哨兵：流被 drop 而**未经任何消费式终结方法**时，说明客户端提前断开。
///
/// 需要它的原因：客户端断开时 axum 直接 drop response body，unfold 的
/// `None` 分支永不执行，`finalize` 也就永不调用——该请求会在 traces 里完全消失。
/// Drop 是唯一能观测到这件事的位置。
///
/// **方向性风险**（这是本设计最脆弱的一点，改动前先读）：`armed` 默认为 true，
/// 意味着「未解释的 drop」默认被判成客户端断开。而约束要求的是相反方向——
/// 仅在明确检测到客户端断开时才判定，其余归上游断。两者的缓冲带就是
/// `into_completed` / `into_upstream_error` 这两个**按值消费**的终结方法：
/// 凡是显式处理了结局的分支都必须交出 guard 的所有权，编译器会盯着这件事。
/// 新增任何流退出分支时，必须走其中之一，否则真实的上游故障会被静默记成
/// 客户端断开——代理不被追责、故障永久隐身，正是「不可漏放」要防的。
pub(crate) struct StreamPhaseGuard {
    tracer: std::sync::Arc<RequestTracer>,
    sent_bytes: u64,
    last_chunk_at: Instant,
    armed: bool,
}

impl StreamPhaseGuard {
    pub fn new(tracer: std::sync::Arc<RequestTracer>, sent_bytes: u64) -> Self {
        Self {
            tracer,
            sent_bytes,
            last_chunk_at: Instant::now(),
            armed: true,
        }
    }

    /// 更新已发送字节数与最后一个 chunk 的时刻（每个 chunk 后调用）
    pub fn set_bytes(&mut self, sent_bytes: u64) {
        self.sent_bytes = sent_bytes;
        self.last_chunk_at = Instant::now();
    }

    /// 距上一个 chunk 的间隔——区分「突然断」与「先卡死再断」
    pub fn idle_ms(&self) -> u64 {
        self.last_chunk_at.elapsed().as_millis() as u64
    }

    /// 流正常结束：**按值消费** guard，记 streaming 段成功。
    /// 消费式而非 `&mut self` 的 disarm，是为了让「已显式处理结局」的分支
    /// 在类型层面吃掉 guard——忘调的面积从「每条分支」缩小到「真正未处理的分支」。
    pub fn into_completed(mut self, sent_bytes: u64) {
        self.armed = false; // 先解除，随后的 Drop 成为 no-op
        self.tracer.close_phase(
            crate::admin::trace_db::phase::STREAMING,
            crate::admin::trace_db::outcome::SUCCESS,
            Some(sent_bytes),
            None,
        );
    }

    /// 上游断流：**按值消费** guard，记 stream_interrupted 并写齐三个判别位。
    pub fn into_upstream_error(mut self, sent_bytes: u64, err: &dyn std::fmt::Display) {
        self.armed = false;
        let idle_ms = self.idle_ms();
        self.tracer.close_phase(
            crate::admin::trace_db::phase::STREAMING,
            crate::admin::trace_db::outcome::STREAM_INTERRUPTED,
            Some(sent_bytes),
            Some(format!(
                "client_gone=false bytes={} idle_ms={} err={}",
                sent_bytes, idle_ms, err
            )),
        );
    }
}

impl Drop for StreamPhaseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.tracer.close_phase(
            crate::admin::trace_db::phase::STREAMING,
            crate::admin::trace_db::outcome::CLIENT_DISCONNECTED,
            Some(self.sent_bytes),
            Some(format!(
                "client_gone=true bytes={} idle_ms={}",
                self.sent_bytes,
                self.idle_ms()
            )),
        );
    }
}
```

- [ ] **Step 6: 运行测试确认通过**

Run: `cargo test --bin kiro-rs stream_phase_guard -- --nocapture`
（三个用例：客户端断开触发 / 正常收尾不触发 / 上游断流不触发）
Expected: 三个 PASS

- [ ] **Step 7: 提交**

```bash
git add src/anthropic/handlers.rs
git commit -m "feat(trace): StreamPhaseGuard 检测客户端提前断开

客户端断开时 axum 直接 drop response body，unfold 的 None 分支不执行，
finalize 不调用，请求在 traces 里完全消失。Drop 是唯一观测点。
终结方法按值消费 guard：显式处理结局的分支必须交出所有权，
编译器帮忙盯住「忘记标记」这个失效模式。"
```

---

### Task 5: 实时流与缓冲流路径接线

**Files:**
- Modify: `src/anthropic/handlers.rs:769-860`（实时流 `stream::unfold`）
- Modify: `src/anthropic/handlers.rs:1586-1680`（`create_buffered_sse_stream`）
- Test: `src/anthropic/stream.rs`（`mod tests`，核心回归）

**Interfaces:**
- Consumes: `RequestTracer::open_phase` / `close_phase`（Task 3）、`StreamPhaseGuard`（Task 4）

- [ ] **Step 1: 写核心回归测试**

在 `src/anthropic/stream.rs` 的 `mod tests` 末尾新增。

真实签名（已核对）：
- `ToolJsonAccumulator::new() -> Self`（`stream.rs:973`）
- `push(&mut self, tool_use: &ToolUseEvent, tool_name_map: &HashMap<String,String>) -> Result<Option<CompletedToolUse>, ToolJsonAccumulatorError>`（`:985`）
- `finish(&mut self) -> Result<(), ToolJsonAccumulatorError>`（`:1030`）

**两类错误的产生位置不同**：`IncompleteJson` 由 `finish()` 产生（缓冲区还有残留），
`InvalidJson` 由 `push()` 在 `stop=true` 那一片解析失败时当场产生。测试要分别打到这两处。

```rust
    fn tool_use_event(id: &str, name: &str, input: &str, stop: bool)
        -> crate::kiro::model::events::ToolUseEvent
    {
        crate::kiro::model::events::ToolUseEvent {
            tool_use_id: id.to_string(),
            name: name.to_string(),
            input: input.to_string(),
            stop,
        }
    }

    #[test]
    fn truncated_tool_json_maps_to_upstream_truncated_outcome() {
        let mut acc = ToolJsonAccumulator::new();
        let map = HashMap::new();
        // 分片写到一半，从未收到 stop=true
        acc.push(
            &tool_use_event("tu-1", "str_replace", r#"{"file_path":"/a/b.rs","old_"#, false),
            &map,
        )
        .expect("未 stop 的分片只累积，不应报错");

        let err = acc.finish().expect_err("半截 JSON 必须由 finish 报错");
        assert!(err.is_incomplete(), "应归 IncompleteJson（传输截断）而非 InvalidJson");
        assert_eq!(err.error_type(), "upstream_tool_json_error");
        assert_eq!(
            phase_outcome_for(&err),
            crate::admin::trace_db::outcome::UPSTREAM_TRUNCATED
        );
    }

    #[test]
    fn invalid_tool_json_maps_to_upstream_invalid_outcome() {
        let mut acc = ToolJsonAccumulator::new();
        let map = HashMap::new();
        // 完整收到 stop=true，但 JSON 非法 —— push 当场报错
        let err = acc
            .push(&tool_use_event("tu-2", "str_replace", r#"{"file_path": }"#, true), &map)
            .expect_err("非法 JSON 必须由 push 报错");
        assert!(!err.is_incomplete(), "完整但非法应归 InvalidJson，不罚代理");
        assert_eq!(
            phase_outcome_for(&err),
            crate::admin::trace_db::outcome::UPSTREAM_INVALID
        );
    }
```

**若 `ToolUseEvent` 的字段名与上面不符**，用
`grep -n "struct ToolUseEvent" -A 10 src/kiro/model/events.rs` 核对后按真实字段写，
断言语义保持不变。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test truncated_tool_json_maps_to -- --nocapture`
Expected: 编译失败，`cannot find function phase_outcome_for`

- [ ] **Step 3: 实现映射函数**

在 `src/anthropic/stream.rs` 的 `impl ToolJsonAccumulatorError` 之后新增：

```rust
/// 把累积器错误映射到 phase outcome。
/// 截断 = 传输链路问题（计入代理健康）；非法 = 上游内容问题（不计入）。
/// 与 `is_incomplete()` 的注释保持同一口径。
pub fn phase_outcome_for(err: &ToolJsonAccumulatorError) -> &'static str {
    if err.is_incomplete() {
        crate::admin::trace_db::outcome::UPSTREAM_TRUNCATED
    } else {
        crate::admin::trace_db::outcome::UPSTREAM_INVALID
    }
}
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test tool_json_maps_to -- --nocapture`
Expected: 两个 PASS

- [ ] **Step 5: 实时流接线**

`src/anthropic/handlers.rs:771` 的 `stream::unfold`：

1. 把初始状态元组里加入 `StreamPhaseGuard`，构造前先 `tracer.open_phase(phase::FIRST_TOKEN)`。
   `tracer` 需为 `Arc<RequestTracer>`——若当前不是，先改成 `Arc` 并同步所有使用点。
2. `Some(Ok(chunk))` 分支内，现有 `tracer.mark_first_token();` 之后追加：

```rust
                            if first_chunk {
                                tracer.close_phase(
                                    phase::FIRST_TOKEN,
                                    outcome::SUCCESS,
                                    Some(0),
                                    None,
                                );
                                tracer.open_phase(phase::STREAMING);
                                first_chunk = false;
                            }
                            guard.set_bytes(sent_bytes);
```

（`first_chunk: bool` 加入状态元组，初值 `true`。）

3. `Some(Err(e))` 分支内，现有 `tracing::error!("读取响应流失败: {}", e);` 之后追加：

```rust
                            // 按值消费 guard：本分支已显式处理结局，Drop 不得再判客户端断开
                            guard.into_upstream_error(sent_bytes, &e);
```

4. `None` 分支内，现有 `let final_events = ctx.generate_final_events();` 之后追加：

```rust
                            // 按值消费 guard：正常收尾，Drop 不得再判客户端断开
                            guard.into_completed(sent_bytes);
                            tracer.open_phase(phase::FINISH);
                            let finish_outcome = match ctx.tool_json_error() {
                                Some(err) => crate::anthropic::stream::phase_outcome_for(err),
                                None => outcome::SUCCESS,
                            };
                            tracer.close_phase(
                                phase::FINISH,
                                finish_outcome,
                                Some(sent_bytes),
                                ctx.tool_json_error_message(),
                            );
```

若 `ctx` 只暴露 `tool_json_error_message()` 而无 `tool_json_error()`，
在 `stream.rs` 里补一个 `pub fn tool_json_error(&self) -> Option<&ToolJsonAccumulatorError> { self.tool_json_error.as_ref() }`。

- [ ] **Step 6: 抽出共享埋点逻辑**

Step 5 的三处埋点会在缓冲流路径上原样重复。**不要照抄**——把三个决策点抽成
不依赖状态元组形状的自由函数，两条路径各自调用。

在 `src/anthropic/handlers.rs` 的 `StreamPhaseGuard` 之后新增：

```rust
/// 首个 chunk 到达：关闭 first_token 段，打开 streaming 段。
/// 幂等由调用方的 first_chunk 标志保证。
pub(crate) fn phase_on_first_chunk(tracer: &RequestTracer) {
    tracer.close_phase(phase::FIRST_TOKEN, outcome::SUCCESS, Some(0), None);
    tracer.open_phase(phase::STREAMING);
}

/// 流正常结束的 finish 段：按 tool_use 累积器结果开合。
/// streaming 段本身由 `StreamPhaseGuard::into_completed` 关闭，此处只管 finish。
pub(crate) fn phase_on_finish(
    tracer: &RequestTracer,
    sent_bytes: u64,
    tool_json_error: Option<&ToolJsonAccumulatorError>,
    tool_json_message: Option<String>,
) {
    tracer.open_phase(phase::FINISH);
    let finish_outcome = match tool_json_error {
        Some(err) => crate::anthropic::stream::phase_outcome_for(err),
        None => outcome::SUCCESS,
    };
    tracer.close_phase(
        phase::FINISH,
        finish_outcome,
        Some(sent_bytes),
        tool_json_message,
    );
}
```

**streaming 段的两个结局不在这里重复**——它们已经是 `StreamPhaseGuard` 的消费式
终结方法（`into_upstream_error` / `into_completed`），两条路径直接调用同一份实现。
guard 承载 streaming 段，这两个自由函数承载 first_token 与 finish 段，职责不重叠。

两个自由函数只接 `&RequestTracer` 和标量，**与两条路径的状态元组形状无关**，
不需要泛型或 trait object。把 Step 5 里的内联代码替换成对它们的调用。

- [ ] **Step 7: 缓冲流接线**

`create_buffered_sse_stream`（`handlers.rs:1586`）：`mark_first_token` 在 `:1631`、
`Some(Err(e))` 在 `:1653`。在这两处及流结束处分别调用 Step 6 抽出的
`phase_on_first_chunk` / `phase_on_finish`，并同样把 `StreamPhaseGuard`
放进该路径的流状态里，用 `into_upstream_error` / `into_completed` 终结。

- [ ] **Step 8: 验证 axum drop 前提（端到端，真实 socket 关闭）**

Task 4 的探针只证明了 Rust 的 drop 语义，**没有**证明 axum 在客户端断开时
真的会 drop response body、且时机上来得及让 guard 干活。整个 `client_disconnected`
机制是否真会触发，到此仍未被证实。这一步补上。

写一个集成测试（`#[tokio::test]`）：

1. 用 axum 起一个最小服务，路由返回一个**慢速 SSE 流**（每 50ms 发一个 chunk，
   共 20 个），流状态里挂上 `StreamPhaseGuard`，tracer 用内存 `TraceStore`
   （`TraceStore::open_in_memory()`）
2. 用 `tokio::net::TcpStream` 直接连上去发原始 HTTP 请求，读到前几个 chunk 后
   **直接 drop 掉 socket**
3. 等待足够长（例如 500ms）让服务端感知断开
4. 断言：该 trace 的 phases 里出现 `outcome == client_disconnected` 的 streaming 段，
   且 `bytes > 0`（证明断开发生在已发出内容之后）

Run: `cargo test --bin kiro-rs client_disconnect_end_to_end -- --nocapture`

**若该测试无法让服务端观测到断开**（例如 Drop 根本不触发，或触发时机晚于进程判定），
**不要删掉测试或放宽断言**。停下来报告实测现象——这说明整个 `client_disconnected`
判别不成立，是设计问题而非测试问题，需要人决策。

- [ ] **Step 9: 全量编译与测试**

Run: `cargo test --no-run 2>&1 | tail -5 && cargo test -- --nocapture 2>&1 | tail -20`
Expected: 编译通过，全部测试 PASS

- [ ] **Step 10: 提交**

```bash
git add src/anthropic/handlers.rs src/anthropic/stream.rs
git commit -m "feat(trace): 实时流与缓冲流接入分段埋点

tool_use 累积器的 IncompleteJson/InvalidJson 映射为 finish 段的
upstream_truncated/upstream_invalid，本次事故从此可定位到确切段落。
非流式路径不记 phases —— 无流生命周期，不造空壳段。"
```

---

### Task 6: 对照基线聚合

**Files:**
- Modify: `src/admin/ops.rs`（新增 `PhaseBaselineRow` 与 `phase_baseline`）
- Modify: `src/admin/router.rs`（新增路由）
- Test: `src/admin/ops.rs`（`mod tests`）

**Interfaces:**
- Produces:
  - `pub struct PhaseBaselineRow { pub phase: String, pub proxy_url: String, pub total: u64, pub failed: u64 }`
  - `OpsStore::phase_baseline(&self, hours: i64) -> Vec<PhaseBaselineRow>`
  - `GET /api/admin/ops/phase-baseline?hours=24`

- [ ] **Step 1: 写失败测试**

`src/admin/ops.rs` 的 `mod tests` 已有三个 helper（已核对）：
`mem_store_with_traces()`（`:636`）、`insert_trace(store, trace_id, epoch_offset_secs, final_status, error_type, credential_id, duration_ms)`（`:640`）、
`insert_attempt(store, trace_id, outcome, credential_id, proxy_url)`（`:669`）。

**需要新增第四个 helper**（放在 `insert_attempt` 之后）：

```rust
    fn insert_phase(
        store: &OpsStore,
        trace_id: &str,
        seq: u32,
        phase: &str,
        outcome: &str,
    ) {
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO trace_phases (trace_id, seq, phase, started_ms, duration_ms, outcome) \
                 VALUES (?1, ?2, ?3, 0, 100, ?4)",
                rusqlite::params![trace_id, seq as i64, phase, outcome],
            )
            .unwrap();
    }
```

然后在 `mod tests` 末尾新增：

```rust
    #[test]
    fn phase_baseline_groups_by_phase_and_proxy() {
        let store = mem_store_with_traces();
        // 出口 A：3 条 streaming，其中 1 条中断
        for (i, oc) in ["success", "success", "stream_interrupted"].iter().enumerate() {
            let id = format!("a{}", i);
            insert_trace(&store, &id, 60, "success", None, 1, 100);
            insert_attempt(&store, &id, "success", 1, Some("socks5://a:1080"));
            insert_phase(&store, &id, 0, "streaming", oc);
        }
        // 出口 B：1 条 streaming，全成功
        insert_trace(&store, "b0", 60, "success", None, 2, 100);
        insert_attempt(&store, "b0", "success", 2, Some("socks5://b:1080"));
        insert_phase(&store, "b0", 0, "streaming", "success");

        let rows = store.phase_baseline(24);
        let a = rows
            .iter()
            .find(|r| r.phase == "streaming" && r.proxy_url == "socks5://a:1080")
            .expect("应有出口 A 的 streaming 行");
        assert_eq!(a.total, 3);
        assert_eq!(a.failed, 1);

        let b = rows
            .iter()
            .find(|r| r.phase == "streaming" && r.proxy_url == "socks5://b:1080")
            .expect("应有出口 B 的 streaming 行");
        assert_eq!(b.failed, 0);
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test phase_baseline_groups_by -- --nocapture`
Expected: 编译失败，`no method named phase_baseline`

- [ ] **Step 3: 实现聚合**

在 `src/admin/ops.rs` 的 `by_proxy` 之后新增：

```rust
/// 某段 × 某出口的窗口失败率，用于错误详情里的对照基线
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseBaselineRow {
    pub phase: String,
    /// 出口：'direct' = 直连；'' = 未知（该列存在前的历史行）
    pub proxy_url: String,
    pub total: u64,
    pub failed: u64,
}

impl OpsStore {
    /// 按 (段, 出口) 统计窗口内的失败率。
    /// 出口取该 trace 最后一跳的 proxy_url —— 流生命周期发生在最终成功建连的那一跳上。
    pub fn phase_baseline(&self, hours: i64) -> Vec<PhaseBaselineRow> {
        let cutoff = (Utc::now() - chrono::Duration::hours(hours)).timestamp();
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT p.phase, \
                    COALESCE((SELECT a.proxy_url FROM trace_attempts a \
                              WHERE a.trace_id = p.trace_id \
                              ORDER BY a.attempt DESC LIMIT 1), '') AS proxy, \
                    COUNT(*) AS total, \
                    SUM(CASE WHEN p.outcome != 'success' THEN 1 ELSE 0 END) AS failed \
             FROM trace_phases p \
             JOIN traces t ON t.trace_id = p.trace_id \
             WHERE t.ts_epoch >= ?1 \
             GROUP BY p.phase, proxy",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("phase_baseline 查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map([cutoff], |row| {
            Ok(PhaseBaselineRow {
                phase: row.get(0)?,
                proxy_url: row.get(1)?,
                total: row.get::<_, i64>(2)? as u64,
                failed: row.get::<_, i64>(3)? as u64,
            })
        });
        match rows {
            Ok(r) => r.filter_map(|x| x.ok()).collect(),
            Err(e) => {
                tracing::warn!("phase_baseline 读取失败: {}", e);
                Vec::new()
            }
        }
    }
}
```

**注意：** `client_disconnected` 也会被算进 `failed`。这是错的——客户端断开不是故障。
在 SQL 的 `CASE WHEN` 里排除它：

```sql
SUM(CASE WHEN p.outcome NOT IN ('success','client_disconnected') THEN 1 ELSE 0 END)
```

- [ ] **Step 4: 补测试覆盖 client_disconnected 不计入**

```rust
    #[test]
    fn client_disconnect_not_counted_as_failure_in_baseline() {
        let store = mem_store_with_traces();
        insert_trace(&store, "c0", 60, "success", None, 1, 100);
        insert_attempt(&store, "c0", "success", 1, Some("socks5://a:1080"));
        insert_phase(&store, "c0", 0, "streaming", "client_disconnected");

        let rows = store.phase_baseline(24);
        let row = rows.iter().find(|r| r.phase == "streaming").unwrap();
        assert_eq!(row.failed, 0, "客户端断开不是故障，不得计入失败率");
        assert_eq!(row.total, 1, "但仍计入总数");
    }
```

- [ ] **Step 5: 运行确认通过**

Run: `cargo test phase_baseline -- --nocapture && cargo test client_disconnect_not_counted -- --nocapture`
Expected: 全 PASS

- [ ] **Step 6: 加路由**

`src/admin/router.rs`，照 `:175` 的 `.route("/traces/failure-stats", get(trace_failure_stats))` 形状加：

```rust
        .route("/ops/phase-baseline", get(ops_phase_baseline))
```

handler 照同文件既有 ops handler 的形状写，读 `hours` query 参数（默认 24），
返回 `Json(Vec<PhaseBaselineRow>)`。

- [ ] **Step 7: 编译并提交**

Run: `cargo test --no-run 2>&1 | tail -3`

```bash
git add src/admin/ops.rs src/admin/router.rs
git commit -m "feat(ops): 段 × 出口 的窗口失败率聚合

错误详情的对照基线数据源。client_disconnected 计入总数但不计入失败——
客户端断开不是故障，混进去会污染代理归因。"
```

---

### Task 7: 错误详情泳道 UI

**Files:**
- Modify: `admin-ui/src/types/api.ts`（`TracePhase` 类型、`TraceRecord.phases`）
- Modify: `admin-ui/src/api/ops.ts`（`fetchPhaseBaseline`）
- Modify: `admin-ui/src/hooks/use-ops.ts`（`usePhaseBaseline`）
- Create: `admin-ui/src/components/trace-phase-lane.tsx`（泳道组件）
- Modify: `admin-ui/src/components/trace-log-page.tsx:330-338`（详情区插入泳道）

**Interfaces:**
- Consumes: `GET /api/admin/ops/phase-baseline`（Task 6）、`TraceRecord.phases`（Task 2）

**为何单独建文件：** `trace-log-page.tsx` 已 636 行。泳道含自己的布局、配色与基线拼接逻辑，
塞进去会让该文件继续膨胀。按职责拆，符合「files that change together live together」。

- [ ] **Step 0: 抽出 formatDuration 到共享模块**

已核对：`formatDuration` 目前是**局部函数**，在 `trace-log-page.tsx:109` 和 `ops-page.tsx` 里各有一份副本，
`@/lib/format` 并不存在（`admin-ui/src/lib/` 只有 `storage.ts` 和 `utils.ts`）。
泳道组件是第三个消费方，且不能从 `trace-log-page.tsx` 导入（会形成循环依赖）。

Create `admin-ui/src/lib/format.ts`：

```ts
/** 毫秒时长的紧凑展示：1s 以内显示 ms，否则显示两位小数的秒 */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}
```

删除 `trace-log-page.tsx:109-112` 与 `ops-page.tsx` 里的同名局部函数，
两个文件改为 `import { formatDuration } from '@/lib/format'`。

Run: `cd admin-ui && npm run build`
Expected: 构建成功。**此步骤不改变任何行为，先单独验证通过再往下走。**

- [ ] **Step 1: 加类型**

`admin-ui/src/types/api.ts`，在 `TraceAttempt` 之后新增：

```ts
/** 流生命周期的一段；仅流式请求有 */
export interface TracePhase {
  seq: number
  /** first_token | streaming | finish */
  phase: string
  startedMs: number
  durationMs: number
  /** 复用 outcome 常量；client_disconnected = 客户端断开，非故障 */
  outcome: string
  /** 该段结束时已下发字节数 */
  bytes?: number | null
  detail?: string | null
}

/** 段 × 出口 的窗口失败率 */
export interface PhaseBaselineRow {
  phase: string
  /** 'direct' = 直连；'' = 未知 */
  proxyUrl: string
  total: number
  failed: number
}
```

并在 `TraceRecord`（`:521` 附近 `attempts: TraceAttempt[]` 处）追加：

```ts
  /** 流生命周期分段；非流式为空数组 */
  phases?: TracePhase[]
```

- [ ] **Step 2: 加 API 与 hook**

`admin-ui/src/api/ops.ts`，照 `:44` 的 `api.get<OpsEvent[]>('/ops/events', ...)` 形状加：

```ts
export async function fetchPhaseBaseline(hours = 24) {
  const { data } = await api.get<PhaseBaselineRow[]>('/ops/phase-baseline', {
    params: { hours },
  })
  return data
}
```

`admin-ui/src/hooks/use-ops.ts` 照同文件 `useOpsEvents` 的形状加 `usePhaseBaseline`。

- [ ] **Step 3: 建泳道组件**

Create `admin-ui/src/components/trace-phase-lane.tsx`：

```tsx
import type { PhaseBaselineRow, TracePhase } from '@/types/api'
import { Badge } from '@/components/ui/badge'
import { formatDuration } from '@/lib/format'

const PHASE_LABEL: Record<string, string> = {
  first_token: '首 token',
  streaming: '流传输',
  finish: '收尾',
}

/** 该段是否算失败。client_disconnected 是客户端行为，不算故障。 */
function isFailed(outcome: string) {
  return outcome !== 'success' && outcome !== 'client_disconnected'
}

function baselineFor(
  rows: PhaseBaselineRow[] | undefined,
  phase: string,
  proxyUrl: string | null | undefined,
) {
  if (!rows) return null
  const key = proxyUrl ?? ''
  const row = rows.find((r) => r.phase === phase && r.proxyUrl === key)
  if (!row || row.total === 0) return null
  return {
    pct: ((row.failed / row.total) * 100).toFixed(1),
    failed: row.failed,
    total: row.total,
  }
}

export function TracePhaseLane({
  phases,
  proxyUrl,
  baseline,
}: {
  phases: TracePhase[]
  /** 最终那一跳的出口，用于挑对照基线 */
  proxyUrl: string | null | undefined
  baseline: PhaseBaselineRow[] | undefined
}) {
  if (phases.length === 0) {
    return (
      <div className="text-[12px] text-muted-foreground">
        非流式请求，无流生命周期分段
      </div>
    )
  }
  return (
    <div className="flex flex-wrap gap-2">
      {phases.map((p) => {
        const failed = isFailed(p.outcome)
        const base = baselineFor(baseline, p.phase, proxyUrl)
        return (
          <div
            key={p.seq}
            className={`min-w-[160px] flex-1 rounded-lg border p-2 ${
              failed ? 'border-destructive/50 bg-destructive/5' : 'border-border/50 bg-secondary/30'
            }`}
          >
            <div className="flex items-center gap-2 text-[13px]">
              <span className="font-medium">{PHASE_LABEL[p.phase] ?? p.phase}</span>
              <Badge variant={failed ? 'destructive' : 'secondary'}>{p.outcome}</Badge>
              <span className="ml-auto font-mono text-muted-foreground">
                {formatDuration(p.durationMs)}
              </span>
            </div>
            {p.bytes != null && (
              <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                累计 {p.bytes} B
              </div>
            )}
            {base && (
              <div className="mt-1 text-[11px] text-muted-foreground/80">
                近24h 同出口该段失败率 {base.pct}% ({base.failed}/{base.total})
              </div>
            )}
            {p.detail && (
              <pre className="mt-1 max-h-24 overflow-auto whitespace-pre-wrap break-all rounded bg-background/60 p-1 font-mono text-[11px] text-muted-foreground">
                {p.detail}
              </pre>
            )}
          </div>
        )
      })}
    </div>
  )
}
```

导入路径已核对：`Badge` 来自 `@/components/ui/badge`（与 `trace-log-page.tsx:16` 一致），
`formatDuration` 来自 Step 0 新建的 `@/lib/format`。

- [ ] **Step 4: 接入详情区**

`admin-ui/src/components/trace-log-page.tsx:330` 的「尝试链路」区块**之后**插入：

```tsx
      <div className="mt-3 text-[13px] font-medium text-muted-foreground">流生命周期</div>
      <div className="mt-2">
        <TracePhaseLane
          phases={rec.phases ?? []}
          proxyUrl={rec.attempts[rec.attempts.length - 1]?.proxyUrl}
          baseline={baseline}
        />
      </div>
```

`baseline` 由该页组件顶层的 `usePhaseBaseline(24)` 提供，逐层传下。
并在文件顶部加 `import { TracePhaseLane } from '@/components/trace-phase-lane'`。

- [ ] **Step 5: 前端构建**

Run: `cd admin-ui && npm run build`
Expected: 构建成功，无 TS 报错

- [ ] **Step 6: 端到端验证**

```bash
cargo build --release
# 或按 docs/local-deploy.md 的方式重建镜像并起容器
```

在 Admin UI 的链路日志里筛 `error_type = 上游截断`，确认：
- 详情区出现「流生命周期」泳道
- `finish` 段标红且 outcome 为 `upstream_truncated`
- 出口列对历史行显示「未知」而非「直连」

**若线上暂无 `upstream_truncated` 样本**，以 `stream_interrupted` 的记录代替验证泳道渲染，
并在提交信息里注明未取得截断样本的端到端证据。**不得声称已验证未验证的部分。**

- [ ] **Step 7: 提交**

```bash
git add admin-ui/src/types/api.ts admin-ui/src/api/ops.ts admin-ui/src/hooks/use-ops.ts \
        admin-ui/src/components/trace-phase-lane.tsx admin-ui/src/components/trace-log-page.tsx
git commit -m "feat(ui): 错误详情新增流生命周期泳道与对照基线

两层拼接：attempts 供重试链路，phases 供流生命周期。
每段挂近24h 同出口同段失败率，回答「本段失败是特例还是该出口常态」。"
```

---

## 完成后仍未解决的问题

**「代理是否为流中断根因」本计划不回答。**

当前 455 跳中 453 跳走同一出口 `socks5h://192.168.110.56:10301`，代理与上游的贡献完全共线。
Task 6 的基线能算出「该出口该段失败率」，但**没有第二个出口可比**，数字仍不构成归因。

定论需人为造对照：将某凭据 `proxyUrl` 设为 `"direct"` 或换出口，同一超长 prompt 各跑 N 次，
比对 `IncompleteJson` 发生率。该动作影响线上，需单独决策。

本计划交付的是「下次能看见断在哪一段、以及该段是否异常」。

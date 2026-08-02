# DuckDB 统一存储迁移实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** usage_log（JSONL + 内存聚合器）与 trace/ops（SQLite）全部迁移到单个 DuckDB 文件 `kiro.duckdb`，统计查询直打 SQL，删除 rusqlite 依赖。

**Architecture:** 新增 `src/admin/duck.rs` 负责打开唯一 DuckDB 实例并初始化 schema；`UsageStore` 替代 `UsageRecorder`+`UsageAggregator`（写入 = INSERT，查询 = GROUP BY date_trunc）；`TraceStore`/`OpsStore` 从 rusqlite 机械迁移到 duckdb crate（API 同构）。历史 JSONL 用 `read_json` 一次性导入后归档；traces.db 历史不导入（7 天保留期，价值低），旧文件原地保留。

**Tech Stack:** duckdb crate（bundled feature，实测含 json 扩展）、现有 axum/tokio/parking_lot 栈不变。

## Global Constraints

- 版本号升 `0.9.0`（Cargo.toml，存储层重构）。有并行工作流在动版本号，执行时以当时 main 上的值为准往上升 minor。
- 5 个统计端点（/stats/overview、timeseries、by-model、by-credential、credits-by-credential)的 JSON 响应结构必须逐字段不变（前端 admin-ui 不改）。
- 时间桶语义必须保持「本地时区小时桶/天桶」：连接打开后 `SET TimeZone` 为进程本地时区（用 `iana_time_zone` crate 或读 `/etc/localtime`——见 Task 1 实测代码），SQL 用 `date_trunc('hour'|'day', ts)`（ts 为 TIMESTAMPTZ）。
- 实测依据（2026-08-01 glibc 主机验证）：同进程重复 `Connection::open` 同一文件 OK；`try_clone` OK；`read_json(path, format='newline_delimited')` OK；`SET TimeZone='Asia/Shanghai'` 后 `date_trunc('day', TIMESTAMPTZ '2026-07-31 17:00:00+00')` = `2026-08-01 00:00:00+08`。
- Alpine/musl 编译验证进行中（容器 duckdb-musl-test）——Task 7 前必须先确认它通过；不通过则整个方案要回到用户处重新决策（Dockerfile 改 debian base 或放弃）。
- clippy 门槛沿用项目现状：`cargo clippy -- -D warnings`（只卡生产代码）。
- 提交信息遵循 conventional commits（feat:/refactor:/test:），每个 Task 至少一个提交。
- DuckDB 无 `AUTOINCREMENT`：ops_events 的自增 id 用 `CREATE SEQUENCE` + `DEFAULT nextval(...)`。
- DuckDB 无 `PRAGMA journal_mode/synchronous/busy_timeout`——直接删，不要找等价物（单进程内 MVCC，append 不冲突）。
- `PRAGMA table_info(t)` 在 DuckDB 可用，列迁移逻辑可保留。

---

### Task 1: duckdb 依赖 + 共享实例模块 `src/admin/duck.rs`

**Files:**
- Modify: `Cargo.toml`（+duckdb，版本号 0.9.0）
- Create: `src/admin/duck.rs`
- Modify: `src/admin/mod.rs`（注册模块 + pub use）

**Interfaces:**
- Produces: `pub fn open_shared(path: &Path) -> duckdb::Result<Connection>`（打开或复用 kiro.duckdb，SET TimeZone，跑各表 schema）；`pub fn local_tz_name() -> String`
- 后续 Task 的 UsageStore/TraceStore/OpsStore 都从这里拿 Connection（各自 `try_clone` 或再次 `open_shared`，两者实测等价）。

- [ ] **Step 1: Cargo.toml 加依赖、升版本**

```toml
version = "0.9.0"
# [dependencies] 增加：
duckdb = { version = "1", features = ["bundled"] }
iana-time-zone = "0.1"   # 取 IANA 时区名喂给 SET TimeZone
```

（rusqlite 此时先不删，Task 6 统一删。）

- [ ] **Step 2: 写失败测试（schema 幂等 + 时区生效）**

`src/admin/duck.rs` 尾部：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_shared_idempotent_and_tz_set() {
        let dir = std::env::temp_dir().join(format!("ducktest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kiro.duckdb");
        let c1 = open_shared(&path).unwrap();
        let c2 = open_shared(&path).unwrap(); // 幂等：二次 open + 二次建 schema 不报错
        let tz: String = c1
            .query_row("select current_setting('TimeZone')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tz, local_tz_name());
        let n: i64 = c2
            .query_row("select count(*) from usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test admin::duck -- --nocapture`
Expected: 编译失败（open_shared 未定义）

- [ ] **Step 4: 实现 duck.rs**

```rust
//! 唯一 DuckDB 实例（kiro.duckdb）的打开与 schema 初始化。
//!
//! usage / trace / ops 三个存储共用此文件。DuckDB 在进程内按路径缓存
//! database 实例，重复 open 得到的是同一实例上的新连接（已实测），
//! 因此各存储可独立调用 open_shared，无锁冲突。

use std::path::Path;

use duckdb::Connection;

/// 进程本地 IANA 时区名；取不到时退到 UTC（桶边界退化为 UTC 语义，不崩）
pub fn local_tz_name() -> String {
    iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_string())
}

/// 打开（或复用进程内已打开的）kiro.duckdb，设置会话时区并确保全部表存在。
pub fn open_shared(path: &Path) -> duckdb::Result<Connection> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        let _ = std::fs::create_dir_all(parent);
    }
    let conn = Connection::open(path)?;
    // 会话级设置：桶边界按本地时区切（date_trunc 依赖它）
    conn.execute_batch(&format!("SET TimeZone='{}';", local_tz_name()))?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

/// 全部表定义。usage_records 在此；traces/ops 各表由 Task 4/5 迁入本常量。
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS usage_records (
    ts                    TIMESTAMPTZ NOT NULL,
    key_id                BIGINT NOT NULL,
    credential_id         BIGINT NOT NULL,
    model                 VARCHAR NOT NULL,
    input_tokens          BIGINT NOT NULL,
    output_tokens         BIGINT NOT NULL,
    cache_creation_tokens BIGINT NOT NULL,
    cache_read_tokens     BIGINT NOT NULL,
    credits               DOUBLE NOT NULL,
    duration_ms           BIGINT NOT NULL,
    status                VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS imported_files (
    file_name   VARCHAR PRIMARY KEY,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows        BIGINT NOT NULL
);
";
```

`src/admin/mod.rs` 增加：

```rust
pub mod duck;
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cargo test admin::duck`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/admin/duck.rs src/admin/mod.rs
git commit -m "feat(storage): 引入 duckdb 共享实例模块 kiro.duckdb"
```

---

### Task 2: UsageStore——写入 + 5 个查询 API 全部 SQL 化

**Files:**
- Modify: `src/admin/usage_stats.rs`（重写：删 UsageRecorder 的 JSONL writer、删 UsageAggregator 全部桶逻辑，保留全部输出结构体）
- Modify: `src/admin/mod.rs`（导出改为 `pub use usage_stats::UsageStore;`）

**Interfaces:**
- Consumes: `duck::open_shared`
- Produces（签名对齐现有调用点，`SharedUsageStore = Arc<UsageStore>`）：
  - `UsageStore::open(db_path: &Path, retention_days: i64) -> duckdb::Result<Self>`
  - `pub fn record(&self, rec: &UsageRecord)`（内部 INSERT，失败仅 warn——语义与旧 record 一致）
  - `pub fn overview(&self) -> OverviewStats`
  - `pub fn query_timeseries(&self, window: StatsQueryWindow, key_id: Option<u64>, cred_filter: Option<&HashSet<u64>>) -> Vec<TimeSeriesPoint>`
  - `pub fn query_by_model(&self, window, key_id) -> Vec<ModelDistribution>`
  - `pub fn query_by_credential(&self, window, key_id, cred_filter) -> Vec<CredentialDistribution>`
  - `pub fn query_credits_by_credential(&self, window, key_id, cred_filter, limit: usize) -> CreditsByCredential`
  - `pub fn retention_days/set_retention_days/cleanup_old_logs`（cleanup 变为 `DELETE FROM usage_records WHERE ts < now() - INTERVAL (N) DAY`）
- 保留不动：`UsageRecord`、`BucketStats`（仅测试用可删）、`TimeSeriesPoint`、`ModelDistribution`、`CredentialDistribution`、`CreditsByCredential`、`CreditSeriesMeta`、`CreditPoint`、`OverviewStats`、`Range`、`StatsGranularity`、`StatsQueryWindow`。

**关键 SQL（timeseries，其余同型）：**

```sql
SELECT epoch(date_trunc($gran, ts))::BIGINT AS bucket_ts,
       sum(input_tokens), sum(output_tokens),
       sum(cache_creation_tokens), sum(cache_read_tokens),
       count(*) AS calls,
       count(*) FILTER (status <> 'success') AS errors,
       sum(credits)
FROM usage_records
WHERE epoch(ts) >= ?1 AND epoch(ts) < ?2
  AND (?3 IS NULL OR key_id = ?3)
  -- cred_filter 有值时：AND credential_id IN (...)（拼接 id 列表，全是 u64 无注入面）
GROUP BY bucket_ts ORDER BY bucket_ts
```

`$gran` 由 `StatsGranularity` 映射为字符串 `'hour'`/`'day'` 直接拼入（枚举值，非用户输入）。
`ts` 输出用 `ts_to_rfc3339(bucket_ts)`（现有函数保留）。

**语义护栏（写测试时必须覆盖，旧实现的既有行为）：**
1. 只产出有记录的桶（GROUP BY 天然满足）；
2. `credits_by_credential` 并列积分按 credential_id 升序破平（`ORDER BY total DESC, credential_id`），Top N 截断后 `total_credentials` 是截断前数量；
3. `query_by_credential`/`credits` 里 `credential_id = 0`（未达上游）不计入 by-credential 维度（旧代码 add_record_to_bucket 对 0 提前 return）——SQL 加 `AND credential_id <> 0`；
4. overview 的 today = 本地 0 点起（`ts >= date_trunc('day', now())`），week = 滚动 7*24h（`ts >= now() - INTERVAL 7 DAY`）；today 有 errors 字段、week 没有。

- [ ] **Step 1: 写失败测试**（替换现有 usage_stats tests 中桶逻辑测试；文件尾 `#[cfg(test)]`）

```rust
fn mk_store() -> UsageStore {
    let dir = std::env::temp_dir().join(format!("uduck-{}-{}", std::process::id(), fastrand::u64(..)));
    std::fs::create_dir_all(&dir).unwrap();
    UsageStore::open(&dir.join("kiro.duckdb"), 31).unwrap()
}

fn rec(ts: &str, key: u64, cred: u64, model: &str, inp: u64, out: u64, credits: f64, status: &str) -> UsageRecord {
    UsageRecord {
        ts: ts.into(), key_id: key, credential_id: cred, model: model.into(),
        input_tokens: inp, output_tokens: out,
        cache_creation_tokens: 0, cache_read_tokens: 0,
        credits, duration_ms: 100, status: status.into(),
    }
}

#[test]
fn timeseries_groups_by_local_hour_and_filters() {
    let s = mk_store();
    let now = chrono::Utc::now();
    let t0 = now - chrono::Duration::hours(2);
    s.record(&rec(&t0.to_rfc3339(), 1, 10, "m1", 100, 10, 0.5, "success"));
    s.record(&rec(&t0.to_rfc3339(), 1, 10, "m1", 200, 20, 0.5, "error"));
    s.record(&rec(&now.to_rfc3339(), 2, 11, "m2", 50, 5, 0.1, "success"));
    let w = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
    let all = s.query_timeseries(w, None, None);
    assert_eq!(all.iter().map(|p| p.calls).sum::<u64>(), 3);
    assert_eq!(all.iter().map(|p| p.errors).sum::<u64>(), 1);
    // key 过滤
    let k1 = s.query_timeseries(w, Some(1), None);
    assert_eq!(k1.iter().map(|p| p.input_tokens).sum::<u64>(), 300);
    // cred 白名单过滤
    let allow: std::collections::HashSet<u64> = [11].into_iter().collect();
    let f = s.query_timeseries(w, None, Some(&allow));
    assert_eq!(f.iter().map(|p| p.calls).sum::<u64>(), 1);
}

#[test]
fn by_credential_excludes_zero_and_counts_errors() {
    let s = mk_store();
    let now = chrono::Utc::now().to_rfc3339();
    s.record(&rec(&now, 1, 0, "m", 9, 0, 0.0, "error"));   // 未达上游
    s.record(&rec(&now, 1, 7, "m", 10, 1, 0.2, "success"));
    s.record(&rec(&now, 1, 7, "m", 10, 1, 0.2, "error"));
    let w = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
    let rows = s.query_by_credential(w, None, None);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].credential_id, 7);
    assert_eq!(rows[0].calls, 2);
    assert_eq!(rows[0].errors, 1);
}

#[test]
fn credits_top_n_tiebreak_and_total_count() {
    let s = mk_store();
    let now = chrono::Utc::now().to_rfc3339();
    for cred in [3u64, 1, 2] {
        s.record(&rec(&now, 1, cred, "m", 1, 1, 1.0, "success")); // 三家并列 1.0
    }
    let w = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
    let out = s.query_credits_by_credential(w, None, None, 2);
    assert_eq!(out.total_credentials, 3);          // 截断前
    assert_eq!(out.series.len(), 2);               // Top 2
    assert_eq!(out.series[0].credential_id, 1);    // 并列按 id 升序
    assert_eq!(out.series[1].credential_id, 2);
}

#[test]
fn overview_today_vs_week() {
    let s = mk_store();
    let now = chrono::Utc::now();
    s.record(&rec(&now.to_rfc3339(), 1, 5, "m", 10, 2, 0.3, "error"));
    let three_days = now - chrono::Duration::days(3);
    s.record(&rec(&three_days.to_rfc3339(), 1, 5, "m", 20, 4, 0.7, "success"));
    let o = s.overview();
    assert_eq!(o.today_calls, 1);
    assert_eq!(o.today_errors, 1);
    assert_eq!(o.week_calls, 2);
    assert!((o.week_credits - 1.0).abs() < 1e-9);
}

#[test]
fn cleanup_deletes_beyond_retention() {
    let s = mk_store();
    s.set_retention_days(7);
    let old = (chrono::Utc::now() - chrono::Duration::days(30)).to_rfc3339();
    s.record(&rec(&old, 1, 5, "m", 1, 1, 0.0, "success"));
    s.record(&rec(&chrono::Utc::now().to_rfc3339(), 1, 5, "m", 1, 1, 0.0, "success"));
    s.cleanup_old_logs();
    let w = StatsQueryWindow::preset(Range::Last30d, StatsGranularity::Day);
    assert_eq!(s.query_timeseries(w, None, None).iter().map(|p| p.calls).sum::<u64>(), 1);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test admin::usage_stats`
Expected: 编译失败（UsageStore 未定义）

- [ ] **Step 3: 实现 UsageStore**

核心骨架（查询函数都是「组 SQL → query_map → 装现有输出结构体」的同型代码）：

```rust
pub struct UsageStore {
    conn: Mutex<duckdb::Connection>,
    retention_days: std::sync::atomic::AtomicI64,
}
pub type SharedUsageStore = Arc<UsageStore>;

impl UsageStore {
    pub fn open(db_path: &Path, retention_days: i64) -> duckdb::Result<Self> {
        Ok(Self {
            conn: Mutex::new(crate::admin::duck::open_shared(db_path)?),
            retention_days: std::sync::atomic::AtomicI64::new(retention_days.max(1)),
        })
    }

    pub fn record(&self, rec: &UsageRecord) {
        let conn = self.conn.lock();
        let r = conn.execute(
            "INSERT INTO usage_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                rec.ts, rec.key_id, rec.credential_id, rec.model,
                rec.input_tokens, rec.output_tokens,
                rec.cache_creation_tokens, rec.cache_read_tokens,
                rec.credits, rec.duration_ms, rec.status,
            ],
        );
        if let Err(e) = r {
            tracing::warn!("usage_records 写入失败: {}", e);
        }
    }
    // ...查询实现见上方 SQL；cred_filter 拼 IN 列表；
    // 空结果直接返回空 Vec，语义与旧桶实现一致。
}
```

注意：`rec.ts` 是 RFC3339 字符串，直接以 VARCHAR 绑给 TIMESTAMPTZ 列由 DuckDB 隐式 cast（测试里验证；若不支持则 `INSERT ... VALUES (CAST(? AS TIMESTAMPTZ), ...)`）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test admin::usage_stats`
Expected: PASS（全部 5 个新测试 + 保留的 parse 测试）

- [ ] **Step 5: Commit**

```bash
git add src/admin/usage_stats.rs src/admin/mod.rs
git commit -m "refactor(usage): UsageRecorder/UsageAggregator 合并为 DuckDB UsageStore"
```

---

### Task 3: 历史 JSONL 一次性导入 + 归档

**Files:**
- Modify: `src/admin/usage_stats.rs`（UsageStore 增加 import 方法）

**Interfaces:**
- Produces: `pub fn import_legacy_jsonl(&self, dir: &Path) -> u64`（返回导入行数；幂等——已导入文件记录在 imported_files 表并被跳过；导入成功的文件改名加 `.imported` 后缀）
- Consumes: Task 1 的 `imported_files` 表。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn import_legacy_jsonl_idempotent_and_archives() {
    let dir = std::env::temp_dir().join(format!("ujsonl-{}-{}", std::process::id(), fastrand::u64(..)));
    std::fs::create_dir_all(&dir).unwrap();
    let f = dir.join("usage_log.2026-07-30.jsonl");
    std::fs::write(&f, concat!(
        r#"{"ts":"2026-07-30T01:00:00+00:00","keyId":1,"credentialId":2,"model":"m","inputTokens":10,"outputTokens":1,"cacheCreationTokens":0,"cacheReadTokens":0,"credits":0.1,"durationMs":5,"status":"success"}"#, "\n",
        r#"{"ts":"2026-07-30T02:00:00+00:00","keyId":1,"credentialId":2,"model":"m","inputTokens":20,"outputTokens":2,"cacheCreationTokens":0,"cacheReadTokens":0,"credits":0.2,"durationMs":5,"status":"error"}"#, "\n",
    )).unwrap();
    let s = UsageStore::open(&dir.join("kiro.duckdb"), 31).unwrap();
    assert_eq!(s.import_legacy_jsonl(&dir), 2);
    assert!(!f.exists());                                    // 原名已归档
    assert!(dir.join("usage_log.2026-07-30.jsonl.imported").exists());
    assert_eq!(s.import_legacy_jsonl(&dir), 0);              // 幂等
    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test import_legacy_jsonl`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// 启动时调用：把目录下未导入的 usage_log.*.jsonl 灌进 usage_records。
/// 每个文件一个事务：INSERT SELECT read_json + 登记 imported_files + 改名归档。
/// 单文件失败仅 warn 并跳过，不影响其余文件与启动流程。
pub fn import_legacy_jsonl(&self, dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    let conn = self.conn.lock();
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else { continue };
        if parse_usage_log_filename(&name).is_none() { continue; }
        let done: i64 = conn
            .query_row("SELECT count(*) FROM imported_files WHERE file_name = ?", [&name], |r| r.get(0))
            .unwrap_or(0);
        if done > 0 { continue; }
        let path_str = entry.path().to_string_lossy().to_string();
        let inserted = conn.execute(
            "INSERT INTO usage_records \
             SELECT ts::TIMESTAMPTZ, keyId, credentialId, model, inputTokens, outputTokens, \
                    coalesce(cacheCreationTokens, 0), coalesce(cacheReadTokens, 0), \
                    coalesce(credits, 0), coalesce(durationMs, 0), status \
             FROM read_json(?, format='newline_delimited')",
            [&path_str],
        );
        match inserted {
            Ok(n) => {
                let _ = conn.execute(
                    "INSERT INTO imported_files (file_name, rows) VALUES (?, ?)",
                    duckdb::params![name, n as i64],
                );
                let _ = std::fs::rename(entry.path(), entry.path().with_extension("jsonl.imported"));
                total += n as u64;
            }
            Err(e) => tracing::warn!("导入 {} 失败: {}", name, e),
        }
    }
    total
}
```

（`parse_usage_log_filename` 是现存函数，保留。read_json 对缺列返回 NULL，`coalesce` 兜住老格式缺省字段——与 serde `#[serde(default)]` 语义对齐。）

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test import_legacy_jsonl`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/admin/usage_stats.rs
git commit -m "feat(usage): 启动时一次性导入历史 usage_log JSONL 并归档"
```

---

### Task 4: 接线——handlers/main/middleware 换 UsageStore，删旧双组件

**Files:**
- Modify: `src/anthropic/handlers.rs:46-110`（`recorder`+`aggregator` 两字段合为 `usage: Option<SharedUsageStore>`；`r.record(&rec); a.ingest(&rec);` 合为 `u.record(&rec);`）
- Modify: `src/admin/middleware.rs:19-53`（`SharedAggregator` → `SharedUsageStore`）
- Modify: `src/admin/handlers.rs:1138,1170,1183,1213,1280`（`state.usage_aggregator.X(...)` → `state.usage_store.X(...)`，签名不变）
- Modify: `src/admin/service.rs:197,536,582-585,2392-2472`（`usage_recorder` 字段类型换成 `SharedUsageStore`，retention get/set 调用点不变）
- Modify: `src/main.rs:245-250,271-285,420-421,445,460`（构造改一处：`UsageStore::open(cache_dir.join("kiro.duckdb"), retention)` + `import_legacy_jsonl(&cache_dir)`；清理循环里 `recorder.cleanup_old_logs()` 调用不变）

**Interfaces:**
- Consumes: Task 2/3 的 UsageStore 全部方法。
- Produces: 编译通过的完整程序；`AdminState.usage_store`、`UsageContext.usage` 命名供后续使用。

- [ ] **Step 1: 机械替换以上调用点**（无新逻辑；`UsageStore::open` 失败时 warn + `std::process::exit(1)`?——不：与旧行为对齐，用 open 失败退化为 panic 不可接受，改为 `expect("打开 kiro.duckdb 失败")`?  实际决定：open 失败直接 `panic!`（带路径与原因）——存储层是核心依赖，起不来就该 fail-fast，与 traces.db 的「失败降级」不同，usage 现在承载统计端点。）

- [ ] **Step 2: 全量测试 + clippy**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: 全部 PASS；如有测试引用已删符号（UsageRecorder/UsageAggregator），修测试。

- [ ] **Step 3: Commit**

```bash
git add -A src/
git commit -m "refactor(usage): 全链路接线 UsageStore，移除 JSONL writer 与内存聚合器"
```

---

### Task 5: TraceStore + OpsStore 迁到 DuckDB

**Files:**
- Modify: `src/admin/trace_db.rs`（rusqlite → duckdb；schema 移入 duck.rs 的 SCHEMA 常量；`open(path,...)` 的 path 参数改为 kiro.duckdb 路径）
- Modify: `src/admin/ops.rs`（同上；ops_events 的 AUTOINCREMENT 改 SEQUENCE）
- Modify: `src/admin/duck.rs`（SCHEMA 常量追加 traces/trace_attempts/trace_phases/ops_events 各表 + 索引 + 序列）
- Modify: `src/main.rs:175-199,433`（`traces_db_path` 改为 `cache_dir.join("kiro.duckdb")`，变量改名 `duckdb_path`）

**Interfaces:**
- Consumes: `duck::open_shared`
- Produces: TraceStore/OpsStore 全部现有公开方法签名不变（返回类型里的 `rusqlite::Result` 改 `duckdb::Result`）。

**机械迁移清单（逐条核对）：**
1. `use rusqlite::...` → `use duckdb::...`（Connection/params/types::Type 同名存在）；
2. 删 `pragma_update(journal_mode/synchronous)`、`busy_timeout`（DuckDB 无这些 PRAGMA）；
3. duck.rs SCHEMA 追加（AUTOINCREMENT 改法）：

```sql
CREATE SEQUENCE IF NOT EXISTS ops_events_id_seq;
CREATE TABLE IF NOT EXISTS ops_events (
    id       BIGINT PRIMARY KEY DEFAULT nextval('ops_events_id_seq'),
    ts       VARCHAR NOT NULL,
    ts_epoch BIGINT NOT NULL,
    category VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    subject  VARCHAR NOT NULL,
    message  VARCHAR NOT NULL
);
```

   traces/trace_attempts/trace_phases 三表原样搬（TEXT→VARCHAR、INTEGER→BIGINT、REAL→DOUBLE；主键/索引语法兼容）；
4. `migrate()` 的 `PRAGMA table_info` 在 DuckDB 可用，保留；`ALTER TABLE ADD COLUMN` 兼容，保留；
5. `query_paged`/`failure_stats`/`cleanup`/`delete_for_credential` 的 SQL 审一遍：`?` 占位符、`LIMIT ?`、`GROUP BY` 均兼容；SQLite 特有函数（如 `datetime()`）如出现改用 epoch 数值比较（现有代码用 ts_epoch 数值列，预计无改动）；
6. `open_in_memory` 兜底路径保留（duckdb 同名 API）；
7. ops.rs 与 trace_db.rs 各自调用 `duck::open_shared(同一路径)`——实测这是同实例新连接，替代原「同文件两个 SQLite 连接 + busy_timeout」。

- [ ] **Step 1: 迁移 trace_db.rs 并跑其现有测试**

Run: `cargo test admin::trace_db`
Expected: PASS（现有测试是行为契约，不重写，只把构造换 duckdb；有断言依赖 SQLite 特有行为的逐个修）

- [ ] **Step 2: 迁移 ops.rs 并跑其现有测试**

Run: `cargo test admin::ops`
Expected: PASS

- [ ] **Step 3: main.rs 接线（duckdb_path）+ 全量测试**

Run: `cargo test`
Expected: 全部 PASS

- [ ] **Step 4: Commit**

```bash
git add src/admin/trace_db.rs src/admin/ops.rs src/admin/duck.rs src/main.rs
git commit -m "refactor(trace,ops): SQLite 迁移到 kiro.duckdb 单文件多表"
```

---

### Task 6: 删 rusqlite + 全量验证

**Files:**
- Modify: `Cargo.toml`（删 rusqlite 行）

- [ ] **Step 1: 删依赖，确认无残留引用**

Run: `grep -rn "rusqlite" src/ Cargo.toml`
Expected: 无输出

- [ ] **Step 2: 全量验证**

Run: `cargo test && cargo clippy -- -D warnings && cargo build --release`
Expected: 全部通过。记录 release 二进制大小（对比迁移前）。

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: 移除 rusqlite，存储统一为 duckdb"
```

---

### Task 7: 镜像构建（musl 前置验证必须已通过）

**Files:**
- Modify: `Dockerfile`（builder 阶段 `apk add` 追加 `g++ cmake make`；其余不动）

- [ ] **Step 0（门禁）: 确认 duckdb-musl-test 容器验证结果**

Run: `podman logs duckdb-musl-test | tail -20`
Expected: 编译成功且测试程序输出 `ok`。**失败则停止本 Task，带证据回用户处决策。**

- [ ] **Step 1: 改 Dockerfile 并构建**

```dockerfile
RUN apk add --no-cache musl-dev g++ make cmake perl
```

Run: `podman build -t localhost/kiro-rs:0.9.0 .`（在 worktree 目录）
Expected: 构建成功。记录镜像大小（旧 29.1MB，预期 ~70-90MB）与构建耗时。

- [ ] **Step 2: 容器冒烟**

Run: 起临时容器挂空目录，curl 管理端 /stats/overview 与 /v1/messages 401 路径，确认 kiro.duckdb 被创建。
Expected: 200/401 各就位，数据目录出现 kiro.duckdb。

- [ ] **Step 3: Commit**

```bash
git add Dockerfile
git commit -m "build: Alpine builder 增加 duckdb bundled 所需 g++/cmake"
```

---

### Task 8: 生产数据迁移演练（用真实 data/ 副本）

**Files:** 无代码改动；产出验证证据。

- [ ] **Step 1: 复制生产 data/ 到临时目录，起新镜像容器指向它**

- [ ] **Step 2: 核对导入数字**

Run: 容器日志中导入行数 vs `cat usage_log.*.jsonl | wc -l`（迁移前实测 16702，以执行时实际为准）
Expected: 行数一致（说不出差异去向就是缺陷）；`/stats/overview`、`/stats/timeseries?range=7d` 与旧版本对应端点输出数值一致（迁移前先在旧容器上抓一份基线 JSON 存档）。

- [ ] **Step 3: 确认旧文件状态**

Expected: JSONL 全部变 `.imported` 后缀；traces.db 原样未动（历史 trace 不迁移，7 天后自然过期，可手工删）。

- [ ] **Step 4: 汇总证据，交用户决定是否部署**（部署本身按 memory 里的容器重建核对集另行执行）

---

## Self-Review 结论

- Spec 覆盖：四个决策（删聚合器/停 JSONL/trace 一并切/单文件多表）分别落在 Task 2/3/5/1。✔
- 无占位符：所有 SQL、测试、结构体名均为具体内容。Task 5 采用「机械迁移清单 + 现有测试当契约」而非逐行重写代码——现有 1442 行 trace_db 的行为由其现有测试锁定，属于有依据的省略而非 TBD。✔
- 类型一致性：`SharedUsageStore = Arc<UsageStore>` 在 Task 2 定义、Task 4 消费；`duck::open_shared` 在 Task 1 定义、Task 2/5 消费。✔
- 风险登记：musl 验证是 Task 7 硬门禁；`ts` 字符串绑 TIMESTAMPTZ 的隐式 cast 在 Task 2 Step 3 标了备选方案。

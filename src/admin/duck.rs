//! 唯一 DuckDB 实例（kiro.duckdb）的打开与 schema 初始化。
//!
//! usage / trace / ops 三个存储共用此文件，且**必须共享同一个数据库实例**：
//! 同进程内对同一文件重复 `Connection::open` 会得到两个独立实例（DuckDB 的
//! 文件锁是 fcntl 型，同进程不互斥），彼此看不到对方 open 之后提交的数据，
//! 并发写同一 WAL 还有损坏风险（实测：后开连接对先开连接的后续 INSERT 恒读 0）。
//! 因此这里按规范化路径缓存首个连接，后续 open_shared 返回 `try_clone`
//! ——同实例上的新连接，写入互相可见（已实测）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use duckdb::Connection;

/// 进程级实例注册表：规范化路径 → 首个连接（后续调用 try_clone 出新连接）
static REGISTRY: Mutex<Option<HashMap<PathBuf, Connection>>> = Mutex::new(None);

/// 打开（或复用进程内已打开的）kiro.duckdb 并确保全部表存在。
///
/// 刻意只用 DuckDB 核心功能，不触发任何扩展（icu/json 等）：musl 静态二进制
/// 不支持动态加载，离线容器也无法运行时下载扩展。时区相关计算全部在 Rust 侧
/// 完成（见 usage_store 的 hour_ts/day_ts 预计算列）。
pub fn open_shared(path: &Path) -> duckdb::Result<Connection> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        let _ = std::fs::create_dir_all(parent);
    }
    // 规范化到父目录真实路径 + 文件名，防止 ./x 与 x 被当成两个实例
    let key = match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => parent
            .canonicalize()
            .map(|p| p.join(name))
            .unwrap_or_else(|_| path.to_path_buf()),
        _ => std::env::current_dir()
            .map(|d| d.join(path))
            .unwrap_or_else(|_| path.to_path_buf()),
    };
    let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let registry = guard.get_or_insert_with(HashMap::new);
    if let Some(existing) = registry.get(&key) {
        return existing.try_clone();
    }
    let conn = Connection::open(&key)?;
    conn.execute_batch(SCHEMA)?;
    let out = conn.try_clone()?;
    registry.insert(key, conn);
    Ok(out)
}

/// 全部表定义（幂等）。usage_records / imported_files 归 UsageStore；
/// traces / trace_attempts / trace_phases 归 TraceStore；ops_events 归 OpsStore。
pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS usage_records (
    ts                    VARCHAR NOT NULL,
    ts_epoch              BIGINT NOT NULL,
    hour_ts               BIGINT NOT NULL,
    day_ts                BIGINT NOT NULL,
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
    imported_at VARCHAR NOT NULL,
    rows        BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_hour ON usage_records(hour_ts);
CREATE INDEX IF NOT EXISTS idx_usage_day ON usage_records(day_ts);

CREATE TABLE IF NOT EXISTS balance_snapshots (
    ts_epoch           BIGINT NOT NULL,
    hour_ts            BIGINT NOT NULL,
    credential_id      BIGINT NOT NULL,
    subscription_title VARCHAR NOT NULL,
    current_usage      DOUBLE NOT NULL,
    usage_limit        DOUBLE NOT NULL,
    remaining          DOUBLE NOT NULL,
    usage_percentage   DOUBLE NOT NULL,
    next_reset_at      BIGINT
);
CREATE INDEX IF NOT EXISTS idx_balance_ts ON balance_snapshots(ts_epoch);
CREATE INDEX IF NOT EXISTS idx_balance_cred ON balance_snapshots(credential_id);

CREATE TABLE IF NOT EXISTS traces (
    trace_id          VARCHAR PRIMARY KEY,
    ts                VARCHAR NOT NULL,
    ts_epoch          BIGINT NOT NULL,
    key_id            BIGINT NOT NULL,
    key_source        VARCHAR,
    operation         VARCHAR NOT NULL DEFAULT 'inference',
    model             VARCHAR NOT NULL,
    is_stream         BIGINT NOT NULL,
    final_status      VARCHAR NOT NULL,
    final_credential_id BIGINT NOT NULL,
    error_type        VARCHAR,
    error_message     VARCHAR,
    total_attempts    BIGINT NOT NULL,
    duration_ms       BIGINT NOT NULL,
    interrupted_after_bytes BIGINT,
    input_tokens      BIGINT NOT NULL DEFAULT 0,
    output_tokens     BIGINT NOT NULL DEFAULT 0,
    cache_creation_tokens BIGINT NOT NULL DEFAULT 0,
    cache_read_tokens BIGINT NOT NULL DEFAULT 0,
    credits           DOUBLE NOT NULL DEFAULT 0,
    first_token_ms    BIGINT,
    session_id        VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_traces_ts ON traces(ts_epoch DESC);
CREATE INDEX IF NOT EXISTS idx_traces_status ON traces(final_status);
CREATE INDEX IF NOT EXISTS idx_traces_cred ON traces(final_credential_id);

CREATE TABLE IF NOT EXISTS trace_attempts (
    trace_id      VARCHAR NOT NULL,
    attempt       BIGINT NOT NULL,
    credential_id BIGINT NOT NULL,
    endpoint      VARCHAR NOT NULL,
    http_status   BIGINT,
    outcome       VARCHAR NOT NULL,
    error_snippet VARCHAR,
    duration_ms   BIGINT NOT NULL,
    started_ms    BIGINT,
    proxy_url     VARCHAR,
    PRIMARY KEY (trace_id, attempt)
);
CREATE INDEX IF NOT EXISTS idx_attempts_trace ON trace_attempts(trace_id);

CREATE TABLE IF NOT EXISTS trace_phases (
    trace_id    VARCHAR NOT NULL,
    seq         BIGINT NOT NULL,
    phase       VARCHAR NOT NULL,
    started_ms  BIGINT NOT NULL,
    duration_ms BIGINT NOT NULL,
    outcome     VARCHAR NOT NULL,
    bytes       BIGINT,
    detail      VARCHAR,
    PRIMARY KEY (trace_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_phases_trace ON trace_phases(trace_id);
CREATE INDEX IF NOT EXISTS idx_phases_phase_outcome ON trace_phases(phase, outcome);

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
CREATE INDEX IF NOT EXISTS idx_ops_events_ts ON ops_events(ts_epoch DESC);
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_shared_idempotent() {
        let dir = std::env::temp_dir().join(format!(
            "ducktest-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kiro.duckdb");
        let c1 = open_shared(&path).unwrap();
        // 幂等：二次 open + 二次建 schema 不报错
        let c2 = open_shared(&path).unwrap();
        let n: i64 = c1
            .query_row("select count(*) from usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        let n2: i64 = c2
            .query_row("select count(*) from usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n2, 0);
        drop((c1, c2));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 回归：两次 open_shared 必须是同一实例——后开连接要能看到先开连接
    /// **在它打开之后**提交的数据。独立实例（旧缺陷）此断言恒败（读 0）。
    #[test]
    fn open_shared_cross_connection_visibility() {
        let dir = std::env::temp_dir().join(format!(
            "duckvis-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kiro.duckdb");
        let c1 = open_shared(&path).unwrap();
        let c2 = open_shared(&path).unwrap(); // 先开 c2，再经 c1 写入
        c1.execute(
            "INSERT INTO ops_events (ts, ts_epoch, category, severity, subject, message) \
             VALUES ('t', 1, 'c', 's', 'sub', 'msg')",
            [],
        )
        .unwrap();
        let n: i64 = c2
            .query_row("select count(*) from ops_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "跨连接必须可见，否则是独立实例");
        drop((c1, c2));
        std::fs::remove_dir_all(&dir).ok();
    }
}

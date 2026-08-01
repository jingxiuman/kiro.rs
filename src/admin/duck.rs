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

/// 全部表定义（幂等）。usage_records / imported_files 归 UsageStore；
/// traces / trace_attempts / trace_phases 归 TraceStore；ops_events 归 OpsStore。
pub(crate) const SCHEMA: &str = "
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

CREATE TABLE IF NOT EXISTS traces (
    trace_id          VARCHAR PRIMARY KEY,
    ts                VARCHAR NOT NULL,
    ts_epoch          BIGINT NOT NULL,
    key_id            BIGINT NOT NULL,
    key_source        VARCHAR,
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
    fn open_shared_idempotent_and_tz_set() {
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
        let tz: String = c1
            .query_row("select current_setting('TimeZone')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tz, local_tz_name());
        let n: i64 = c2
            .query_row("select count(*) from usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        drop((c1, c2));
        std::fs::remove_dir_all(&dir).ok();
    }
}

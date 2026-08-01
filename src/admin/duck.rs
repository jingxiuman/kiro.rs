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
/// traces 系列与 ops_events 在 trace/ops 迁移时并入。
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

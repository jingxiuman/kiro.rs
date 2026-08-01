//! 请求用量存储（DuckDB 版）
//!
//! 替代旧的「JSONL 落盘 + 内存小时/天桶聚合」双组件：写入 = 一行 INSERT 进
//! `usage_records` 表；统计端点的 5 类查询直接 GROUP BY `date_trunc`。
//! 桶边界沿用旧语义：按进程本地时区切小时/天（连接打开时 SET TimeZone，
//! 见 [`crate::admin::duck::open_shared`]）。
//!
//! 与旧聚合器的两处已知行为差异（均为有意为之）：
//! - cred 白名单过滤下，旧代码会为「窗口内有其他账号流量」的桶输出全零点，
//!   新实现不输出该桶（与 key 过滤的既有语义对齐）；
//! - 空白名单（分组下无凭据）直接返回空结果，与 handlers 处注释声明的契约一致。

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Utc};
use parking_lot::Mutex;

use super::usage_stats::{
    CreditPoint, CreditSeriesMeta, CreditsByCredential, CredentialDistribution, ModelDistribution,
    OverviewStats, StatsGranularity, StatsQueryWindow, TimeSeriesPoint, UsageRecord,
};

/// DuckDB 承载的用量存储。写入与查询共用一条连接（Mutex 串行化——
/// 写入是请求尾部的一次 INSERT，查询是管理面板的偶发操作，无争用压力）。
pub struct UsageStore {
    conn: Mutex<duckdb::Connection>,
    /// 保留天数（运行时可改），cleanup_old_logs 时读取。
    retention_days: std::sync::atomic::AtomicI64,
}

pub type SharedUsageStore = Arc<UsageStore>;

/// 粒度对应的「桶起始 Unix 秒」SQL 表达式（date_trunc 按会话时区切）
fn bucket_expr(granularity: StatsGranularity) -> &'static str {
    match granularity {
        StatsGranularity::Hour => "epoch(date_trunc('hour', ts))::BIGINT",
        StatsGranularity::Day => "epoch(date_trunc('day', ts))::BIGINT",
    }
}

/// cred 白名单转 SQL IN 列表（全 u64，无注入面；排序保证 SQL 文本稳定）
fn cred_in_list(allow: &std::collections::HashSet<u64>) -> String {
    let mut ids: Vec<u64> = allow.iter().copied().collect();
    ids.sort_unstable();
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn ts_to_rfc3339(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

impl UsageStore {
    /// 打开（或复用）kiro.duckdb。失败即返回 Err——用量存储同时承载统计端点，
    /// 起不来应当 fail-fast，而非静默降级。
    pub fn open(db_path: &Path, retention_days: i64) -> duckdb::Result<Self> {
        Ok(Self {
            conn: Mutex::new(crate::admin::duck::open_shared(db_path)?),
            retention_days: std::sync::atomic::AtomicI64::new(retention_days.max(1)),
        })
    }

    /// 同步写入一条记录。失败仅 warn，不阻塞请求（与旧 JSONL writer 语义一致）。
    pub fn record(&self, rec: &UsageRecord) {
        let conn = self.conn.lock();
        let r = conn.execute(
            "INSERT INTO usage_records VALUES (CAST(? AS TIMESTAMPTZ), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            duckdb::params![
                rec.ts,
                rec.key_id as i64,
                rec.credential_id as i64,
                rec.model,
                rec.input_tokens as i64,
                rec.output_tokens as i64,
                rec.cache_creation_tokens as i64,
                rec.cache_read_tokens as i64,
                rec.credits,
                rec.duration_ms as i64,
                rec.status,
            ],
        );
        if let Err(e) = r {
            tracing::warn!("usage_records 写入失败: {}", e);
        }
    }

    /// 获取保留天数
    pub fn retention_days(&self) -> i64 {
        self.retention_days
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 设置保留天数（>=1）
    pub fn set_retention_days(&self, days: i64) {
        self.retention_days
            .store(days.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// 清理超过保留期的旧记录。沿用旧文件删除语义：以「本地今天 0 点 - N 天」
    /// 为界（按日期整删，而非滚动 N*24h）。
    pub fn cleanup_old_logs(&self) {
        let today = Local::now().date_naive();
        let cutoff_date = today - Duration::days(self.retention_days());
        let Some(cutoff_ts) = Local
            .with_ymd_and_hms(
                cutoff_date.year(),
                cutoff_date.month(),
                cutoff_date.day(),
                0,
                0,
                0,
            )
            .single()
            .map(|d| d.timestamp())
        else {
            return;
        };
        let conn = self.conn.lock();
        match conn.execute(
            "DELETE FROM usage_records WHERE epoch(ts) < ?",
            [cutoff_ts],
        ) {
            Ok(n) if n > 0 => tracing::info!("已清理过期 usage_records: {} 行", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("清理 usage_records 失败: {}", e),
        }
    }

    /// 时序数据查询。桶过滤沿用旧语义：桶起始时间落在 [start, end) 才计入
    /// （而非按行过滤），保证与旧聚合器输出逐点一致。
    pub fn query_timeseries(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
        cred_filter: Option<&std::collections::HashSet<u64>>,
    ) -> Vec<TimeSeriesPoint> {
        if let Some(allow) = cred_filter
            && allow.is_empty()
        {
            return Vec::new();
        }
        let b = bucket_expr(window.granularity);
        let mut sql = format!(
            "SELECT {b} AS bucket_ts, \
                    sum(input_tokens)::BIGINT, sum(output_tokens)::BIGINT, \
                    sum(cache_creation_tokens)::BIGINT, sum(cache_read_tokens)::BIGINT, \
                    count(*)::BIGINT, \
                    (count(*) FILTER (WHERE status <> 'success'))::BIGINT, \
                    sum(credits) \
             FROM usage_records \
             WHERE {b} >= ? AND {b} < ?"
        );
        let mut params: Vec<i64> = vec![window.start_ts, window.end_ts];
        if let Some(id) = key_id {
            sql.push_str(" AND key_id = ?");
            params.push(id as i64);
        }
        if let Some(allow) = cred_filter {
            sql.push_str(&format!(" AND credential_id IN ({})", cred_in_list(allow)));
        }
        sql.push_str(" GROUP BY bucket_ts ORDER BY bucket_ts");

        let conn = self.conn.lock();
        let mut out = Vec::new();
        let run = || -> duckdb::Result<Vec<TimeSeriesPoint>> {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |r| {
                Ok(TimeSeriesPoint {
                    ts: ts_to_rfc3339(r.get::<_, i64>(0)?),
                    input_tokens: r.get::<_, i64>(1)? as u64,
                    output_tokens: r.get::<_, i64>(2)? as u64,
                    cache_creation_tokens: r.get::<_, i64>(3)? as u64,
                    cache_read_tokens: r.get::<_, i64>(4)? as u64,
                    calls: r.get::<_, i64>(5)? as u64,
                    errors: r.get::<_, i64>(6)? as u64,
                    credits: r.get::<_, f64>(7)?,
                })
            })?;
            rows.collect()
        };
        match run() {
            Ok(points) => out = points,
            Err(e) => tracing::warn!("timeseries 查询失败: {}", e),
        }
        out
    }

    /// 模型分布（包含未达上游的记录——与旧 by_model 桶语义一致）
    pub fn query_by_model(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
    ) -> Vec<ModelDistribution> {
        let b = bucket_expr(window.granularity);
        let mut sql = format!(
            "SELECT model, count(*)::BIGINT, \
                    sum(input_tokens)::BIGINT, sum(output_tokens)::BIGINT, \
                    sum(cache_creation_tokens)::BIGINT, sum(cache_read_tokens)::BIGINT \
             FROM usage_records \
             WHERE {b} >= ? AND {b} < ?"
        );
        let mut params: Vec<i64> = vec![window.start_ts, window.end_ts];
        if let Some(id) = key_id {
            sql.push_str(" AND key_id = ?");
            params.push(id as i64);
        }
        sql.push_str(" GROUP BY model ORDER BY count(*) DESC, model");

        let conn = self.conn.lock();
        let run = || -> duckdb::Result<Vec<ModelDistribution>> {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |r| {
                Ok(ModelDistribution {
                    model: r.get(0)?,
                    calls: r.get::<_, i64>(1)? as u64,
                    input_tokens: r.get::<_, i64>(2)? as u64,
                    output_tokens: r.get::<_, i64>(3)? as u64,
                    cache_creation_tokens: r.get::<_, i64>(4)? as u64,
                    cache_read_tokens: r.get::<_, i64>(5)? as u64,
                })
            })?;
            rows.collect()
        };
        run().unwrap_or_else(|e| {
            tracing::warn!("by_model 查询失败: {}", e);
            Vec::new()
        })
    }

    /// 上游凭据分布。credential_id = 0（未达上游）不计入——旧桶实现同语义。
    pub fn query_by_credential(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
        cred_filter: Option<&std::collections::HashSet<u64>>,
    ) -> Vec<CredentialDistribution> {
        if let Some(allow) = cred_filter
            && allow.is_empty()
        {
            return Vec::new();
        }
        let b = bucket_expr(window.granularity);
        let mut sql = format!(
            "SELECT credential_id, count(*)::BIGINT, \
                    sum(input_tokens)::BIGINT, sum(output_tokens)::BIGINT, \
                    sum(cache_creation_tokens)::BIGINT, sum(cache_read_tokens)::BIGINT, \
                    (count(*) FILTER (WHERE status <> 'success'))::BIGINT \
             FROM usage_records \
             WHERE {b} >= ? AND {b} < ? AND credential_id <> 0"
        );
        let mut params: Vec<i64> = vec![window.start_ts, window.end_ts];
        if let Some(id) = key_id {
            sql.push_str(" AND key_id = ?");
            params.push(id as i64);
        }
        if let Some(allow) = cred_filter {
            sql.push_str(&format!(" AND credential_id IN ({})", cred_in_list(allow)));
        }
        sql.push_str(" GROUP BY credential_id ORDER BY count(*) DESC, credential_id");

        let conn = self.conn.lock();
        let run = || -> duckdb::Result<Vec<CredentialDistribution>> {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |r| {
                Ok(CredentialDistribution {
                    credential_id: r.get::<_, i64>(0)? as u64,
                    calls: r.get::<_, i64>(1)? as u64,
                    input_tokens: r.get::<_, i64>(2)? as u64,
                    output_tokens: r.get::<_, i64>(3)? as u64,
                    cache_creation_tokens: r.get::<_, i64>(4)? as u64,
                    cache_read_tokens: r.get::<_, i64>(5)? as u64,
                    errors: r.get::<_, i64>(6)? as u64,
                })
            })?;
            rows.collect()
        };
        run().unwrap_or_else(|e| {
            tracing::warn!("by_credential 查询失败: {}", e);
            Vec::new()
        })
    }

    /// 各账号积分消耗时序（Top `limit` 个账号，按窗口内总积分降序、并列按 id 升序）。
    ///
    /// points 的桶集合 = 窗口内**有任何记录**的桶（不受 key/cred 过滤影响）——
    /// 与旧实现「物化桶都出点」的行为保持一致，前端据此判断桶的疏密。
    pub fn query_credits_by_credential(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
        cred_filter: Option<&std::collections::HashSet<u64>>,
        limit: usize,
    ) -> CreditsByCredential {
        if let Some(allow) = cred_filter
            && allow.is_empty()
        {
            return CreditsByCredential {
                series: Vec::new(),
                points: Vec::new(),
                total_credentials: 0,
            };
        }
        let b = bucket_expr(window.granularity);
        let key_clause = key_id.map(|_| " AND key_id = ?").unwrap_or("");
        let cred_clause = cred_filter
            .map(|allow| format!(" AND credential_id IN ({})", cred_in_list(allow)))
            .unwrap_or_default();

        let conn = self.conn.lock();

        // 第一步：窗口内每账号积分合计，定 Top N 与 total_credentials
        let totals_sql = format!(
            "SELECT credential_id, sum(credits) AS total \
             FROM usage_records \
             WHERE {b} >= ? AND {b} < ? AND credential_id <> 0{key_clause}{cred_clause} \
             GROUP BY credential_id HAVING sum(credits) > 0 \
             ORDER BY total DESC, credential_id"
        );
        let mut params: Vec<i64> = vec![window.start_ts, window.end_ts];
        if let Some(id) = key_id {
            params.push(id as i64);
        }
        let ranked: Vec<(u64, f64)> = (|| -> duckdb::Result<Vec<(u64, f64)>> {
            let mut stmt = conn.prepare(&totals_sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get::<_, f64>(1)?))
            })?;
            rows.collect()
        })()
        .unwrap_or_else(|e| {
            tracing::warn!("credits totals 查询失败: {}", e);
            Vec::new()
        });
        let total_credentials = ranked.len();
        let selected: Vec<(u64, f64)> = ranked.into_iter().take(limit).collect();
        let selected_ids: std::collections::HashSet<u64> =
            selected.iter().map(|(id, _)| *id).collect();

        // 第二步：窗口内全部物化桶（无过滤）
        let buckets_sql = format!(
            "SELECT DISTINCT {b} AS bucket_ts FROM usage_records \
             WHERE {b} >= ? AND {b} < ? ORDER BY bucket_ts"
        );
        let bucket_list: Vec<i64> = (|| -> duckdb::Result<Vec<i64>> {
            let mut stmt = conn.prepare(&buckets_sql)?;
            let rows = stmt.query_map([window.start_ts, window.end_ts], |r| r.get(0))?;
            rows.collect()
        })()
        .unwrap_or_else(|e| {
            tracing::warn!("credits buckets 查询失败: {}", e);
            Vec::new()
        });

        // 第三步：入选账号的每桶积分（>0 才出现，稀疏语义与旧实现一致）
        let mut per_bucket: HashMap<i64, HashMap<String, f64>> = HashMap::new();
        if !selected_ids.is_empty() {
            let sel_list = {
                let mut ids: Vec<u64> = selected_ids.iter().copied().collect();
                ids.sort_unstable();
                ids.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let points_sql = format!(
                "SELECT {b} AS bucket_ts, credential_id, sum(credits) \
                 FROM usage_records \
                 WHERE {b} >= ? AND {b} < ? AND credential_id IN ({sel_list}){key_clause} \
                 GROUP BY bucket_ts, credential_id HAVING sum(credits) > 0"
            );
            let mut p2: Vec<i64> = vec![window.start_ts, window.end_ts];
            if let Some(id) = key_id {
                p2.push(id as i64);
            }
            let rows: Vec<(i64, u64, f64)> = (|| -> duckdb::Result<Vec<(i64, u64, f64)>> {
                let mut stmt = conn.prepare(&points_sql)?;
                let rows = stmt.query_map(duckdb::params_from_iter(p2.iter()), |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)? as u64,
                        r.get::<_, f64>(2)?,
                    ))
                })?;
                rows.collect()
            })()
            .unwrap_or_else(|e| {
                tracing::warn!("credits points 查询失败: {}", e);
                Vec::new()
            });
            for (bucket_ts, cred, credits) in rows {
                per_bucket
                    .entry(bucket_ts)
                    .or_default()
                    .insert(cred.to_string(), credits);
            }
        }

        CreditsByCredential {
            series: selected
                .into_iter()
                .map(|(credential_id, total_credits)| CreditSeriesMeta {
                    credential_id,
                    total_credits,
                })
                .collect(),
            points: bucket_list
                .into_iter()
                .map(|ts| CreditPoint {
                    ts: ts_to_rfc3339(ts),
                    credits: per_bucket.remove(&ts).unwrap_or_default(),
                })
                .collect(),
            total_credentials,
        }
    }

    /// 启动时调用：把目录下未导入的 usage_log.*.jsonl 灌进 usage_records。
    /// 幂等——已导入文件登记在 imported_files 表并被跳过；导入成功的文件
    /// 改名加 `.imported` 后缀归档。单文件失败仅 warn 并跳过，不影响启动。
    pub fn import_legacy_jsonl(&self, dir: &Path) -> u64 {
        let mut total = 0u64;
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        let conn = self.conn.lock();
        let mut names: Vec<(String, std::path::PathBuf)> = entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                super::usage_stats::parse_usage_log_filename(&name)?;
                Some((name, e.path()))
            })
            .collect();
        names.sort(); // 按日期顺序导入，失败时日志可读
        for (name, path) in names {
            let done: i64 = conn
                .query_row(
                    "SELECT count(*) FROM imported_files WHERE file_name = ?",
                    [&name],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            if done > 0 {
                continue;
            }
            let path_str = path.to_string_lossy().to_string();
            // columns 显式声明：老格式缺键（如 cache*/credits/durationMs）时该列为
            // NULL 而非直接不存在，coalesce 兜底——对齐旧 serde #[serde(default)] 语义
            let inserted = conn.execute(
                "INSERT INTO usage_records \
                 SELECT ts, keyId, credentialId, model, inputTokens, outputTokens, \
                        coalesce(cacheCreationTokens, 0), coalesce(cacheReadTokens, 0), \
                        coalesce(credits, 0), coalesce(durationMs, 0), status \
                 FROM read_json(?, format='newline_delimited', columns={ \
                     ts: 'TIMESTAMPTZ', keyId: 'BIGINT', credentialId: 'BIGINT', \
                     model: 'VARCHAR', inputTokens: 'BIGINT', outputTokens: 'BIGINT', \
                     cacheCreationTokens: 'BIGINT', cacheReadTokens: 'BIGINT', \
                     credits: 'DOUBLE', durationMs: 'BIGINT', status: 'VARCHAR'})",
                [&path_str],
            );
            match inserted {
                Ok(n) => {
                    let _ = conn.execute(
                        "INSERT INTO imported_files (file_name, rows) VALUES (?, ?)",
                        duckdb::params![name, n as i64],
                    );
                    let archived = path.with_extension("jsonl.imported");
                    if let Err(e) = std::fs::rename(&path, &archived) {
                        tracing::warn!("归档 {} 失败（已导入，不影响数据）: {}", name, e);
                    }
                    tracing::info!("已导入 {}: {} 行", name, n);
                    total += n as u64;
                }
                Err(e) => tracing::warn!("导入 {} 失败: {}", name, e),
            }
        }
        total
    }

    /// 概览（今日 + 最近 7 天）。today = 本地 0 点起；week = 滚动 7*24h
    /// （按小时桶起始过滤，沿用旧语义）。
    pub fn overview(&self) -> OverviewStats {
        let conn = self.conn.lock();
        let week_cutoff = Utc::now().timestamp() - 7 * 24 * 3600;
        let sql = "SELECT \
             COALESCE(sum(input_tokens)  FILTER (WHERE ts >= date_trunc('day', now())), 0)::BIGINT, \
             COALESCE(sum(output_tokens) FILTER (WHERE ts >= date_trunc('day', now())), 0)::BIGINT, \
             (count(*)                   FILTER (WHERE ts >= date_trunc('day', now())))::BIGINT, \
             (count(*) FILTER (WHERE ts >= date_trunc('day', now()) AND status <> 'success'))::BIGINT, \
             COALESCE(sum(credits)       FILTER (WHERE ts >= date_trunc('day', now())), 0), \
             COALESCE(sum(input_tokens), 0)::BIGINT, \
             COALESCE(sum(output_tokens), 0)::BIGINT, \
             count(*)::BIGINT, \
             COALESCE(sum(credits), 0) \
             FROM usage_records \
             WHERE epoch(date_trunc('hour', ts)) >= ?";
        conn.query_row(sql, [week_cutoff], |r| {
            Ok(OverviewStats {
                today_input_tokens: r.get::<_, i64>(0)? as u64,
                today_output_tokens: r.get::<_, i64>(1)? as u64,
                today_calls: r.get::<_, i64>(2)? as u64,
                today_errors: r.get::<_, i64>(3)? as u64,
                today_credits: r.get::<_, f64>(4)?,
                week_input_tokens: r.get::<_, i64>(5)? as u64,
                week_output_tokens: r.get::<_, i64>(6)? as u64,
                week_calls: r.get::<_, i64>(7)? as u64,
                week_credits: r.get::<_, f64>(8)?,
            })
        })
        .unwrap_or_else(|e| {
            tracing::warn!("overview 查询失败: {}", e);
            OverviewStats {
                today_calls: 0,
                today_input_tokens: 0,
                today_output_tokens: 0,
                today_errors: 0,
                today_credits: 0.0,
                week_calls: 0,
                week_input_tokens: 0,
                week_output_tokens: 0,
                week_credits: 0.0,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::usage_stats::Range;
    use chrono::Duration;

    fn mk_store() -> UsageStore {
        let dir = std::env::temp_dir().join(format!(
            "uduck-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        UsageStore::open(&dir.join("kiro.duckdb"), 31).unwrap()
    }

    fn rec(
        ts: &str,
        key: u64,
        cred: u64,
        model: &str,
        inp: u64,
        out: u64,
        credits: f64,
        status: &str,
    ) -> UsageRecord {
        UsageRecord {
            ts: ts.into(),
            key_id: key,
            credential_id: cred,
            model: model.into(),
            input_tokens: inp,
            output_tokens: out,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits,
            duration_ms: 100,
            status: status.into(),
        }
    }

    /// 构造一条指定账号/时间/积分的记录，其余字段取无关默认值
    fn credit_rec(ts: DateTime<Utc>, credential_id: u64, credits: f64) -> UsageRecord {
        rec(&ts.to_rfc3339(), 1, credential_id, "m", 0, 0, credits, "success")
    }

    #[test]
    fn store_basic_record_and_overview() {
        let s = mk_store();
        let now = Utc::now();
        let mut r = rec(&now.to_rfc3339(), 1, 5, "claude-opus-4-7", 1000, 200, 0.05, "success");
        r.cache_creation_tokens = 300;
        r.cache_read_tokens = 4000;
        s.record(&r);
        s.record(&r);

        let ov = s.overview();
        assert_eq!(ov.today_calls, 2);
        assert_eq!(ov.today_input_tokens, 2000);

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let series = s.query_timeseries(window, None, None);
        assert!(!series.is_empty());

        let by_model = s.query_by_model(window, None);
        assert_eq!(by_model.len(), 1);
        assert_eq!(by_model[0].model, "claude-opus-4-7");
        assert_eq!(by_model[0].calls, 2);
        assert_eq!(by_model[0].cache_creation_tokens, 600);
        assert_eq!(by_model[0].cache_read_tokens, 8000);

        let by_cred = s.query_by_credential(window, None, None);
        assert_eq!(by_cred.len(), 1);
        assert_eq!(by_cred[0].credential_id, 5);
        assert_eq!(by_cred[0].cache_creation_tokens, 600);
        assert_eq!(by_cred[0].cache_read_tokens, 8000);
    }

    /// 积分时序必须保留时间维度：同一账号跨两个小时桶的消耗不能被压成一个总数
    #[test]
    fn credits_by_credential_keeps_time_dimension() {
        let s = mk_store();
        let now = Utc::now();
        let two_hours_ago = now - Duration::hours(2);
        s.record(&credit_rec(two_hours_ago, 5, 1.0));
        s.record(&credit_rec(now, 5, 0.25));
        s.record(&credit_rec(now, 7, 4.0));

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = s.query_credits_by_credential(window, None, None, 10);

        assert_eq!(out.total_credentials, 2);
        assert_eq!(
            out.series.iter().map(|s| s.credential_id).collect::<Vec<_>>(),
            vec![7, 5]
        );
        assert!((out.series[0].total_credits - 4.0).abs() < 1e-9);
        assert!((out.series[1].total_credits - 1.25).abs() < 1e-9);

        let buckets_with_5: Vec<f64> = out
            .points
            .iter()
            .filter_map(|p| p.credits.get("5").copied())
            .collect();
        assert_eq!(buckets_with_5.len(), 2, "跨两小时的消耗应占两个桶");
        assert!(buckets_with_5.iter().any(|v| (v - 1.0).abs() < 1e-9));
        assert!(buckets_with_5.iter().any(|v| (v - 0.25).abs() < 1e-9));

        let ts: Vec<&str> = out.points.iter().map(|p| p.ts.as_str()).collect();
        let mut sorted = ts.clone();
        sorted.sort_unstable();
        assert_eq!(ts, sorted, "points 必须按时间升序");
        assert_eq!(out.points.len(), 2, "只产出有记录的桶");
    }

    /// Top N 截断时 total_credentials 必须报全量
    #[test]
    fn credits_by_credential_reports_truncation() {
        let s = mk_store();
        let now = Utc::now();
        for id in 1..=5u64 {
            s.record(&credit_rec(now, id, id as f64));
        }
        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = s.query_credits_by_credential(window, None, None, 2);

        assert_eq!(out.series.len(), 2, "limit 应生效");
        assert_eq!(out.total_credentials, 5, "总数须报全量而非截断后的数量");
        assert_eq!(
            out.series.iter().map(|s| s.credential_id).collect::<Vec<_>>(),
            vec![5, 4]
        );
        assert!(
            out.points
                .iter()
                .all(|p| p.credits.keys().all(|k| k == "5" || k == "4")),
            "points 只应包含入选账号"
        );
    }

    /// 并列积分按 credential_id 升序破平，保证图例顺序稳定
    #[test]
    fn credits_top_n_tiebreak() {
        let s = mk_store();
        let now = Utc::now();
        for cred in [3u64, 1, 2] {
            s.record(&credit_rec(now, cred, 1.0));
        }
        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = s.query_credits_by_credential(window, None, None, 2);
        assert_eq!(out.total_credentials, 3);
        assert_eq!(out.series.len(), 2);
        assert_eq!(out.series[0].credential_id, 1);
        assert_eq!(out.series[1].credential_id, 2);
    }

    /// 零积分账号不该占一条线
    #[test]
    fn credits_by_credential_skips_zero_credit_accounts() {
        let s = mk_store();
        let now = Utc::now();
        s.record(&credit_rec(now, 5, 0.0));
        s.record(&credit_rec(now, 7, 2.0));

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = s.query_credits_by_credential(window, None, None, 10);
        assert_eq!(out.total_credentials, 1);
        assert_eq!(
            out.series.iter().map(|s| s.credential_id).collect::<Vec<_>>(),
            vec![7]
        );
    }

    /// group 白名单必须同时约束 Top N 排名与 points
    #[test]
    fn credits_by_credential_respects_cred_filter() {
        let s = mk_store();
        let now = Utc::now();
        s.record(&credit_rec(now, 5, 1.0));
        s.record(&credit_rec(now, 7, 9.0));

        let allow: std::collections::HashSet<u64> = [5u64].into_iter().collect();
        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = s.query_credits_by_credential(window, None, Some(&allow), 10);

        assert_eq!(out.total_credentials, 1);
        assert_eq!(out.series[0].credential_id, 5);
        assert!(
            out.points.iter().all(|p| !p.credits.contains_key("7")),
            "组外账号不得出现在 points"
        );
    }

    #[test]
    fn store_filters_by_client_key() {
        let s = mk_store();
        let now = Utc::now().to_rfc3339();
        s.record(&rec(&now, 1, 5, "m-a", 100, 20, 0.01, "success"));
        s.record(&rec(&now, 2, 6, "m-b", 300, 40, 0.02, "error"));

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let series = s.query_timeseries(window, Some(1), None);
        assert_eq!(series.iter().map(|p| p.calls).sum::<u64>(), 1);
        assert_eq!(series.iter().map(|p| p.input_tokens).sum::<u64>(), 100);

        let by_model = s.query_by_model(window, Some(1));
        assert_eq!(by_model.len(), 1);
        assert_eq!(by_model[0].model, "m-a");

        let by_cred = s.query_by_credential(window, Some(1), None);
        assert_eq!(by_cred.len(), 1);
        assert_eq!(by_cred[0].credential_id, 5);
    }

    #[test]
    fn store_filters_by_custom_window_and_granularity() {
        let s = mk_store();
        let today = Local::now().date_naive();
        let yesterday = today - Duration::days(1);
        let yesterday_noon = Local
            .with_ymd_and_hms(
                yesterday.year(),
                yesterday.month(),
                yesterday.day(),
                12,
                0,
                0,
            )
            .single()
            .unwrap()
            .with_timezone(&Utc)
            .to_rfc3339();
        let today_noon = Local
            .with_ymd_and_hms(today.year(), today.month(), today.day(), 12, 0, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc)
            .to_rfc3339();
        s.record(&rec(&yesterday_noon, 0, 5, "m-yesterday", 100, 20, 0.01, "success"));
        s.record(&rec(&today_noon, 0, 5, "m-today", 300, 40, 0.02, "success"));

        let start_ts = Local
            .with_ymd_and_hms(today.year(), today.month(), today.day(), 0, 0, 0)
            .single()
            .unwrap()
            .timestamp();
        let end_ts = Local
            .with_ymd_and_hms(today.year(), today.month(), today.day(), 23, 59, 59)
            .single()
            .unwrap()
            .timestamp();
        let hour_window = StatsQueryWindow {
            start_ts,
            end_ts,
            granularity: StatsGranularity::Hour,
        };
        let day_window = StatsQueryWindow {
            start_ts,
            end_ts,
            granularity: StatsGranularity::Day,
        };

        let hourly = s.query_timeseries(hour_window, None, None);
        assert_eq!(hourly.iter().map(|p| p.calls).sum::<u64>(), 1);
        assert_eq!(hourly.iter().map(|p| p.input_tokens).sum::<u64>(), 300);

        let daily = s.query_timeseries(day_window, None, None);
        assert_eq!(daily.iter().map(|p| p.calls).sum::<u64>(), 1);
        assert_eq!(daily.iter().map(|p| p.output_tokens).sum::<u64>(), 40);
    }

    #[test]
    fn error_record_increments_errors() {
        let s = mk_store();
        s.record(&rec(&Utc::now().to_rfc3339(), 0, 0, "m", 0, 0, 0.0, "error"));
        let ov = s.overview();
        assert_eq!(ov.today_errors, 1);
    }

    /// credential_id = 0（未达上游）不计入 by-credential 维度
    #[test]
    fn by_credential_excludes_zero() {
        let s = mk_store();
        let now = Utc::now().to_rfc3339();
        s.record(&rec(&now, 1, 0, "m", 9, 0, 0.0, "error"));
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
    fn import_legacy_jsonl_idempotent_and_archives() {
        let dir = std::env::temp_dir().join(format!(
            "ujsonl-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("usage_log.2026-07-30.jsonl");
        std::fs::write(&f, concat!(
            r#"{"ts":"2026-07-30T01:00:00+00:00","keyId":1,"credentialId":2,"model":"m","inputTokens":10,"outputTokens":1,"cacheCreationTokens":0,"cacheReadTokens":0,"credits":0.1,"durationMs":5,"status":"success"}"#, "\n",
            r#"{"ts":"2026-07-30T02:00:00+00:00","keyId":1,"credentialId":2,"model":"m","inputTokens":20,"outputTokens":2,"cacheCreationTokens":0,"cacheReadTokens":0,"credits":0.2,"durationMs":5,"status":"error"}"#, "\n",
        )).unwrap();
        // 老格式缺省字段（无 credits/durationMs/cache*）也要能导入
        let f_old = dir.join("usage_log.2026-07-29.jsonl");
        std::fs::write(&f_old, concat!(
            r#"{"ts":"2026-07-29T01:00:00+00:00","keyId":0,"credentialId":1,"model":"m","inputTokens":5,"outputTokens":1,"status":"success"}"#, "\n",
        )).unwrap();
        let s = UsageStore::open(&dir.join("kiro.duckdb"), 31).unwrap();
        assert_eq!(s.import_legacy_jsonl(&dir), 3);
        assert!(!f.exists(), "原名文件应已归档");
        assert!(dir.join("usage_log.2026-07-30.jsonl.imported").exists());
        assert_eq!(s.import_legacy_jsonl(&dir), 0, "重复导入必须为 0（幂等）");
        // 数据核对：3 行都进了表
        let w = StatsQueryWindow {
            start_ts: 0,
            end_ts: i64::MAX,
            granularity: StatsGranularity::Day,
        };
        assert_eq!(
            s.query_timeseries(w, None, None)
                .iter()
                .map(|p| p.calls)
                .sum::<u64>(),
            3
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cleanup_deletes_beyond_retention() {
        let s = mk_store();
        s.set_retention_days(7);
        let old = (Utc::now() - Duration::days(30)).to_rfc3339();
        s.record(&rec(&old, 1, 5, "m", 1, 1, 0.0, "success"));
        s.record(&rec(&Utc::now().to_rfc3339(), 1, 5, "m", 1, 1, 0.0, "success"));
        s.cleanup_old_logs();
        let w = StatsQueryWindow::preset(Range::Last30d, StatsGranularity::Day);
        assert_eq!(
            s.query_timeseries(w, None, None)
                .iter()
                .map(|p| p.calls)
                .sum::<u64>(),
            1
        );
    }
}

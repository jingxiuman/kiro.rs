//! 请求链路追踪（Trace）持久化
//!
//! 记录每次 `/v1/messages` 请求的完整重试链路，用于排查"中断"类问题：
//! - 一个外部请求 = 1 条 [`TraceRecord`] 汇总 + N 条 [`TraceAttempt`] 子记录
//! - 每跳记录命中凭据、HTTP 状态码、失败分类、上游错误体片段、耗时
//!
//! 存储：DuckDB（`kiro.duckdb`，与 usage / ops 共库，schema 见 [`super::duck`]）。
//! 前端查询直接走 SQL（索引 + WHERE + LIMIT），不维护内存缓冲。
//! 后台任务定期清理超过保留天数的记录（保留天数与启用开关运行时可改）。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::Utc;
use duckdb::{Connection, types::Type};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// trace 记录默认保留天数
const DEFAULT_RETENTION_DAYS: u64 = 7;
/// 上游错误体片段最大长度（字节）
const ERROR_SNIPPET_MAX: usize = 2048;
/// 查询默认返回条数
pub const DEFAULT_QUERY_LIMIT: usize = 200;

/// 单次上游尝试的结果
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceAttempt {
    /// 第几次尝试（0-based）
    pub attempt: u32,
    /// 命中的上游凭据 id；0 表示未取到凭据
    pub credential_id: u64,
    /// 端点名（ide / cli）
    pub endpoint: String,
    /// 上游 HTTP 状态码；None 表示网络层失败（请求未发出/无响应）
    pub http_status: Option<u16>,
    /// 失败分类，见 [`Outcome`]
    pub outcome: String,
    /// 上游错误体片段（截断到 [`ERROR_SNIPPET_MAX`]）
    pub error_snippet: Option<String>,
    /// 本跳耗时（毫秒）
    pub duration_ms: u64,
    /// 本跳相对请求起点的偏移（毫秒）。`None` = 未知（该列存在前的历史行）。
    ///
    /// 由 `RequestTracer::on_attempt` 换算填入——provider 不持有请求起点，算不出。
    /// 有了它，相邻两跳之间的空隙才能被识别为重试 backoff（否则色块条只能顺序堆）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_ms: Option<u64>,
    /// 本跳出口：`"direct"` = 直连；`None` = 未知（该列存在前的历史行）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
}

/// 响应处理的一段。流式与非流式都产生，段名不同（见 [`phase`]）。
///
/// 与 [`TraceAttempt`] 的分工：attempt 覆盖 connect→headers（N 跳，含重试），
/// phase 覆盖 headers 之后的响应处理（1 条响应）。两者基数不同，不合表。
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

/// 响应处理段名
///
/// [`FIRST_TOKEN`] 两条路径共用（语义同为「等上游吐第一口」），故 `phase_baseline`
/// 与前端标签自动通用；其后的段名按路径分开，因为工作内容不同：流式是边收边发，
/// 非流式是收全 → 解码 → 组装。
pub mod phase {
    /// 建连成功到首个上游 chunk 到达（流式 / 非流式共用）
    pub const FIRST_TOKEN: &str = "first_token";
    /// 首 chunk 之后的持续传输（流式）
    pub const STREAMING: &str = "streaming";
    /// 流结束时的收尾判定（含 tool_use 累积器 finish 结果）（流式）
    pub const FINISH: &str = "finish";
    /// 首 chunk 之后收完剩余响应体（非流式）
    pub const BODY_READ: &str = "body_read";
    /// EventStream 解码 + 事件累积（非流式）
    pub const DECODE: &str = "decode";
    /// 响应内容组装 + 输出 token 估算（非流式）
    pub const ASSEMBLE: &str = "assemble";
}

/// 调用方使用的入口 Key 类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TraceKeySource {
    /// 管理员API密钥。
    MasterApiKey,
    /// Admin UI 中创建并分发的客户端 Key。
    ClientKey,
}

impl TraceKeySource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MasterApiKey => "masterApiKey",
            Self::ClientKey => "clientKey",
        }
    }

    fn from_db(value: &str, column: usize) -> duckdb::Result<Self> {
        match value {
            "masterApiKey" => Ok(Self::MasterApiKey),
            "clientKey" => Ok(Self::ClientKey),
            other => Err(duckdb::Error::FromSqlConversionFailure(
                column,
                Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("未知 trace key_source: {other}"),
                )),
            )),
        }
    }
}

/// 一个外部请求的完整链路
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceRecord {
    /// 链路 id（uuid v4），前端 key
    pub trace_id: String,
    /// 请求开始时间（RFC3339）
    pub ts: String,
    /// 客户端 Key id；0 表示 master apiKey
    pub key_id: u64,
    /// 入口 Key 类型，区分管理员API密钥与创建的客户端 Key。
    pub key_source: TraceKeySource,
    /// 模型名
    pub model: String,
    /// 是否流式
    pub is_stream: bool,
    /// 最终状态：success / error / interrupted
    pub final_status: String,
    /// 最终命中（成功）或最后尝试的凭据 id
    pub final_credential_id: u64,
    /// 失败分类（顶层，便于筛选）
    pub error_type: Option<String>,
    /// 给用户的简明错误信息
    pub error_message: Option<String>,
    /// 总尝试次数
    pub total_attempts: u32,
    /// 端到端耗时（毫秒）
    pub duration_ms: u64,
    /// 中断时的字节数（区分完整失败 vs 半截中断）。
    ///
    /// 两条路径语义不同：流式 = 已下发给客户端的字节；非流式 = 从上游收到多少就断了
    /// （非流式在响应组装完成前不向客户端写任何字节）。前端按 `is_stream` 分文案。
    pub interrupted_after_bytes: Option<u64>,
    /// 输入 token（Anthropic 口径）
    #[serde(default)]
    pub input_tokens: u64,
    /// 输出 token
    #[serde(default)]
    pub output_tokens: u64,
    /// 缓存创建 token
    #[serde(default)]
    pub cache_creation_tokens: u64,
    /// 缓存读取 token
    #[serde(default)]
    pub cache_read_tokens: u64,
    /// 费用（上游 meteringEvent 累计的 credits）
    #[serde(default)]
    pub credits: f64,
    /// 首 Token 延迟（毫秒）。两条路径都有值；None = 未收到任何 chunk（早期失败）
    /// 或该列存在前的历史行。
    #[serde(default)]
    pub first_token_ms: Option<u64>,
    /// Claude Code 会话 id（取自 metadata.user_id 的 `_session_<uuid>`）。
    /// 同一把客户端 Key 上区分不同会话/子代理；也用于观测 compact 是否更换 session。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// 每跳明细
    pub attempts: Vec<TraceAttempt>,
    /// 响应处理分段；早期失败（未到达上游）为空
    #[serde(default)]
    pub phases: Vec<TracePhase>,
}

/// 失败分类（attempt.outcome / record.error_type 取值）
pub mod outcome {
    pub const SUCCESS: &str = "success";
    pub const QUOTA_EXHAUSTED: &str = "quota_exhausted";
    pub const ACCOUNT_THROTTLED: &str = "account_throttled";
    pub const AUTH_FAILED: &str = "auth_failed";
    pub const TRANSIENT: &str = "transient";
    pub const NETWORK_ERROR: &str = "network_error";
    pub const BAD_REQUEST: &str = "bad_request";
    pub const UNKNOWN: &str = "unknown";
    /// 仅用作 record.error_type：流式响应已开始但上游中途断开
    pub const STREAM_INTERRUPTED: &str = "stream_interrupted";
    /// 仅用作 record.error_type：流正常收尾但 tool_use 参数 JSON 被上游截断
    /// （IncompleteJson）。属传输链路问题，计入代理健康，与客户端 bad_request 区分。
    pub const UPSTREAM_TRUNCATED: &str = "upstream_truncated";
    /// 仅用作 record.error_type：上游返回了完整但非法的 tool_use JSON（InvalidJson）。
    /// 属上游内容问题（非截断、非客户端错误），不计入代理健康。
    pub const UPSTREAM_INVALID: &str = "upstream_invalid";
    /// 仅用作 phase.outcome：客户端主动断开（非上游故障，不计入代理健康）
    pub const CLIENT_DISCONNECTED: &str = "client_disconnected";
}

/// 把上游错误体截断到安全长度（按字符边界，避免切碎 UTF-8）
pub fn truncate_snippet(body: &str) -> Option<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() <= ERROR_SNIPPET_MAX {
        return Some(trimmed.to_string());
    }
    let mut end = ERROR_SNIPPET_MAX;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    Some(format!("{}…(truncated)", &trimmed[..end]))
}

/// 链路上报接收端：provider 在重试循环里每跳调用 [`Self::on_attempt`]
pub trait TraceSink: Send + Sync {
    fn on_attempt(&self, attempt: TraceAttempt);
}

/// 查询过滤条件
#[derive(Debug, Default, Clone)]
pub struct TraceQuery {
    /// final_status 精确匹配（success/error/interrupted）
    pub status: Option<String>,
    /// error_type 精确匹配
    pub error_type: Option<String>,
    /// 最终凭据 id
    pub credential_id: Option<u64>,
    /// 客户端 Key id（0 = master apiKey）
    pub key_id: Option<u64>,
    /// 该凭据在某一跳失败过（attempt 级，跨 trace 最终状态）。
    /// 用于"凭据失败详情"：即便整条 trace 最终成功，只要该凭据某跳失败也会命中。
    pub failed_attempt_credential_id: Option<u64>,
    /// 模型名
    pub model: Option<String>,
    /// 仅返回非 success
    pub only_failed: bool,
    /// 按账号分组筛选：只返回最终凭据属于这些 id 的 trace。
    /// 由 handler 层在查询前根据 group 参数转换为凭据 id 白名单填入。
    pub credential_ids: Option<Vec<u64>>,
    /// 返回条数上限
    pub limit: usize,
    /// 偏移量（分页用）
    pub offset: usize,
}

/// DuckDB 持久化存储
pub struct TraceStore {
    conn: Mutex<Connection>,
    /// 是否启用 trace 写入（运行时可改）。false 时 insert 直接短路。
    enabled: AtomicBool,
    /// 记录保留天数（运行时可改），cleanup 时读取。
    retention_days: AtomicU64,
}

impl TraceStore {
    /// 打开（或创建）数据库并建表。空路径归一为当前目录下的 kiro.duckdb。
    /// path 指向共享的 kiro.duckdb；建表由 [`super::duck::open_shared`] 完成，
    /// 同文件的 OpsStore / UsageStore 各持同一 DuckDB 实例上的独立连接（进程内
    /// MVCC，append 不冲突），不再需要 SQLite 的 WAL / busy_timeout。
    pub fn open(path: PathBuf, enabled: bool, retention_days: u32) -> duckdb::Result<Self> {
        let path = if path.as_os_str().is_empty() {
            PathBuf::from("kiro.duckdb")
        } else {
            path
        };
        let conn = crate::admin::duck::open_shared(&path)?;
        Self::migrate(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            enabled: AtomicBool::new(enabled),
            retention_days: AtomicU64::new(retention_days.max(1) as u64),
        })
    }

    /// 内存数据库（kiro.duckdb 打开失败时的兜底；进程退出即丢，但保证 Admin 查询不崩）。
    /// 内存库不经过 [`super::duck::open_shared`]，需自行执行建表。
    pub fn open_in_memory() -> duckdb::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(crate::admin::duck::SCHEMA)?;
        Self::migrate(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            enabled: AtomicBool::new(true),
            retention_days: AtomicU64::new(DEFAULT_RETENTION_DAYS),
        })
    }

    /// 读取指定表的现有列名集合
    fn table_columns(
        conn: &Connection,
        table: &str,
    ) -> duckdb::Result<std::collections::HashSet<String>> {
        let mut existing: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for name in rows {
            existing.insert(name?);
        }
        Ok(existing)
    }

    /// 旧库迁移：为 traces / trace_attempts 表补齐新增列（幂等，缺哪列加哪列）。
    /// 老版本的库只有基础列，新增的 token/credits/first_token_ms/key_source
    /// 以及 attempt 级 proxy_url 需在此 ALTER。
    fn migrate(conn: &Connection) -> duckdb::Result<()> {
        let existing = Self::table_columns(conn, "traces")?;
        // (列名, 定义) —— 与 duck.rs SCHEMA 中新增列保持一致。
        // 注意全部不带 NOT NULL：DuckDB 的 ALTER ADD COLUMN 不支持附加约束
        // （"Adding columns with constraints not yet supported"）；带 DEFAULT 的列
        // 老行会被填成默认值，key_source 以 NULL 添加后在下方回填。新插入永远写入合法值。
        let columns: [(&str, &str); 8] = [
            ("input_tokens", "BIGINT DEFAULT 0"),
            ("output_tokens", "BIGINT DEFAULT 0"),
            ("cache_creation_tokens", "BIGINT DEFAULT 0"),
            ("cache_read_tokens", "BIGINT DEFAULT 0"),
            ("credits", "DOUBLE DEFAULT 0"),
            ("first_token_ms", "BIGINT"),
            ("key_source", "VARCHAR"),
            ("session_id", "VARCHAR"),
        ];
        let key_source_added = !existing.contains("key_source");
        for (name, def) in columns {
            if !existing.contains(name) {
                conn.execute_batch(&format!(
                    "ALTER TABLE traces ADD COLUMN {} {};",
                    name, def
                ))?;
            }
        }
        // 老库 key_source 列首次添加后，按 key_id 语义回填：master apiKey (key_id=0) 之外都视为客户端 Key。
        if key_source_added {
            conn.execute_batch(
                "UPDATE traces SET key_source = CASE WHEN key_id = 0 \
                 THEN 'masterApiKey' ELSE 'clientKey' END WHERE key_source IS NULL;",
            )?;
        }
        // trace_attempts 补列：历史行保持 NULL = 未知
        // （proxy_url：未知出口/直连；started_ms：起点不可知，前端退化为顺序堆叠）
        let attempt_cols = Self::table_columns(conn, "trace_attempts")?;
        for (name, def) in [("proxy_url", "VARCHAR"), ("started_ms", "BIGINT")] {
            if !attempt_cols.contains(name) {
                conn.execute_batch(&format!(
                    "ALTER TABLE trace_attempts ADD COLUMN {} {};",
                    name, def
                ))?;
            }
        }
        Ok(())
    }

    /// 是否启用 trace 写入
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// 设置启用开关
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// 获取保留天数
    pub fn retention_days(&self) -> u64 {
        self.retention_days.load(Ordering::Relaxed)
    }

    /// 设置保留天数（>=1）
    pub fn set_retention_days(&self, days: u32) {
        self.retention_days
            .store(days.max(1) as u64, Ordering::Relaxed);
    }

    /// 写入一条完整链路（traces + attempts 在一个事务里）。失败仅 warn，不阻塞请求。
    /// trace 关闭时直接短路。
    pub fn insert(&self, rec: &TraceRecord) {
        if !self.is_enabled() {
            return;
        }
        let mut conn = self.conn.lock();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("trace 事务开启失败: {}", e);
                return;
            }
        };
        let ts_epoch = chrono::DateTime::parse_from_rfc3339(&rec.ts)
            .map(|d| d.timestamp())
            .unwrap_or_else(|_| Utc::now().timestamp());
        let res = (|| -> duckdb::Result<()> {
            tx.execute(
                "INSERT OR REPLACE INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, \
                 is_stream, final_status, final_credential_id, error_type, error_message, \
                 total_attempts, duration_ms, interrupted_after_bytes, \
                 input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, \
                 credits, first_token_ms, session_id) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
                duckdb::params![
                    rec.trace_id,
                    rec.ts,
                    ts_epoch,
                    rec.key_id as i64,
                    rec.key_source.as_str(),
                    rec.model,
                    rec.is_stream as i64,
                    rec.final_status,
                    rec.final_credential_id as i64,
                    rec.error_type,
                    rec.error_message,
                    rec.total_attempts as i64,
                    rec.duration_ms as i64,
                    rec.interrupted_after_bytes.map(|v| v as i64),
                    rec.input_tokens as i64,
                    rec.output_tokens as i64,
                    rec.cache_creation_tokens as i64,
                    rec.cache_read_tokens as i64,
                    rec.credits,
                    rec.first_token_ms.map(|v| v as i64),
                    rec.session_id,
                ],
            )?;
            for a in &rec.attempts {
                tx.execute(
                    "INSERT OR REPLACE INTO trace_attempts (trace_id, attempt, credential_id, \
                     endpoint, http_status, outcome, error_snippet, duration_ms, started_ms, proxy_url) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    duckdb::params![
                        rec.trace_id,
                        a.attempt as i64,
                        a.credential_id as i64,
                        a.endpoint,
                        a.http_status.map(|v| v as i64),
                        a.outcome,
                        a.error_snippet,
                        a.duration_ms as i64,
                        a.started_ms.map(|v| v as i64),
                        a.proxy_url,
                    ],
                )?;
            }
            for p in &rec.phases {
                tx.execute(
                    "INSERT OR REPLACE INTO trace_phases (trace_id, seq, phase, started_ms, \
                     duration_ms, outcome, bytes, detail) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    duckdb::params![
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
            Ok(())
        })();
        match res {
            Ok(()) => {
                if let Err(e) = tx.commit() {
                    tracing::warn!("trace 提交失败: {}", e);
                }
            }
            Err(e) => {
                tracing::warn!("trace 写入失败: {}", e);
            }
        }
    }

    /// 分页查询：返回 (当前页记录, 符合条件的总数)。仅 warn 失败，返回 (空, 0)。
    pub fn query_paged(&self, q: &TraceQuery) -> (Vec<TraceRecord>, usize) {
        let conn = self.conn.lock();
        match Self::query_inner(&conn, q) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("trace 查询失败: {}", e);
                (Vec::new(), 0)
            }
        }
    }

    /// 测试辅助：仅取记录、忽略总数
    #[cfg(test)]
    fn query(&self, q: &TraceQuery) -> Vec<TraceRecord> {
        self.query_paged(q).0
    }

    /// 把 [`TraceQuery`] 的过滤条件拼成 WHERE 子句 + 参数（值全部参数化绑定）
    fn build_where(q: &TraceQuery) -> (String, Vec<Box<dyn duckdb::ToSql>>) {
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
        if let Some(s) = &q.status {
            clauses.push("final_status = ?".to_string());
            params.push(Box::new(s.clone()));
        }
        if let Some(t) = &q.error_type {
            clauses.push("error_type = ?".to_string());
            params.push(Box::new(t.clone()));
        }
        if let Some(c) = q.credential_id {
            clauses.push("final_credential_id = ?".to_string());
            params.push(Box::new(c as i64));
        }
        if let Some(k) = q.key_id {
            clauses.push("key_id = ?".to_string());
            params.push(Box::new(k as i64));
        }
        if let Some(c) = q.failed_attempt_credential_id {
            // 该凭据在某一跳失败过（不论 trace 最终成功与否）
            clauses.push(
                "EXISTS (SELECT 1 FROM trace_attempts a \
                 WHERE a.trace_id = traces.trace_id \
                 AND a.credential_id = ? AND a.outcome != 'success')"
                    .to_string(),
            );
            params.push(Box::new(c as i64));
        }
        if let Some(m) = &q.model {
            clauses.push("model = ?".to_string());
            params.push(Box::new(m.clone()));
        }
        if let Some(ids) = &q.credential_ids {
            if ids.is_empty() {
                // 空白名单 = 该分组下无凭据 → 强制零匹配
                clauses.push("1=0".to_string());
            } else {
                let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
                clauses.push(format!(
                    "final_credential_id IN ({})",
                    placeholders.join(",")
                ));
                for id in ids {
                    params.push(Box::new(*id as i64));
                }
            }
        }
        if q.only_failed {
            clauses.push("final_status != 'success'".to_string());
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        (where_sql, params)
    }

    fn query_inner(
        conn: &Connection,
        q: &TraceQuery,
    ) -> duckdb::Result<(Vec<TraceRecord>, usize)> {
        let (where_sql, params) = Self::build_where(q);
        let param_refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|b| b.as_ref()).collect();

        // 总数（用于前端分页）
        let count_sql = format!("SELECT COUNT(*) FROM traces {}", where_sql);
        let total: i64 = conn.query_row(&count_sql, param_refs.as_slice(), |row| row.get(0))?;

        let limit = if q.limit == 0 {
            DEFAULT_QUERY_LIMIT
        } else {
            q.limit
        };
        let sql = format!(
            "SELECT trace_id, ts, key_id, key_source, model, is_stream, final_status, final_credential_id, \
             error_type, error_message, total_attempts, duration_ms, interrupted_after_bytes, \
             input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, credits, first_token_ms, \
             session_id \
             FROM traces {} ORDER BY ts_epoch DESC LIMIT {} OFFSET {}",
            where_sql, limit, q.offset
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(TraceRecord {
                trace_id: row.get(0)?,
                ts: row.get(1)?,
                key_id: row.get::<_, i64>(2)? as u64,
                key_source: TraceKeySource::from_db(row.get::<_, String>(3)?.as_str(), 3)?,
                model: row.get(4)?,
                is_stream: row.get::<_, i64>(5)? != 0,
                final_status: row.get(6)?,
                final_credential_id: row.get::<_, i64>(7)? as u64,
                error_type: row.get(8)?,
                error_message: row.get(9)?,
                total_attempts: row.get::<_, i64>(10)? as u32,
                duration_ms: row.get::<_, i64>(11)? as u64,
                interrupted_after_bytes: row.get::<_, Option<i64>>(12)?.map(|v| v as u64),
                input_tokens: row.get::<_, i64>(13)? as u64,
                output_tokens: row.get::<_, i64>(14)? as u64,
                cache_creation_tokens: row.get::<_, i64>(15)? as u64,
                cache_read_tokens: row.get::<_, i64>(16)? as u64,
                credits: row.get::<_, f64>(17)?,
                first_token_ms: row.get::<_, Option<i64>>(18)?.map(|v| v as u64),
                session_id: row.get::<_, Option<String>>(19)?,
                attempts: Vec::new(),
                phases: Vec::new(),
            })
        })?;
        let mut records: Vec<TraceRecord> = rows.collect::<duckdb::Result<_>>()?;

        // 批量取每条 trace 的 attempts
        let mut attempt_stmt = conn.prepare(
            "SELECT attempt, credential_id, endpoint, http_status, outcome, error_snippet, \
             duration_ms, started_ms, proxy_url FROM trace_attempts WHERE trace_id = ? \
             ORDER BY attempt ASC",
        )?;
        for rec in &mut records {
            let attempts = attempt_stmt.query_map([&rec.trace_id], |row| {
                Ok(TraceAttempt {
                    attempt: row.get::<_, i64>(0)? as u32,
                    credential_id: row.get::<_, i64>(1)? as u64,
                    endpoint: row.get(2)?,
                    http_status: row.get::<_, Option<i64>>(3)?.map(|v| v as u16),
                    outcome: row.get(4)?,
                    error_snippet: row.get(5)?,
                    duration_ms: row.get::<_, i64>(6)? as u64,
                    started_ms: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                    proxy_url: row.get(8)?,
                })
            })?;
            rec.attempts = attempts.collect::<duckdb::Result<_>>()?;
        }

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
            rec.phases = phases.collect::<duckdb::Result<_>>()?;
        }
        Ok((records, total as usize))
    }

    /// 删除超过保留期的记录（traces + 关联 attempts）。仅 warn 失败。
    pub fn cleanup(&self) {
        let cutoff =
            (Utc::now() - chrono::Duration::days(self.retention_days() as i64)).timestamp();
        let mut conn = self.conn.lock();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("trace 清理事务失败: {}", e);
                return;
            }
        };
        let res = (|| -> duckdb::Result<usize> {
            tx.execute(
                "DELETE FROM trace_phases WHERE trace_id IN \
                 (SELECT trace_id FROM traces WHERE ts_epoch < ?1)",
                [cutoff],
            )?;
            tx.execute(
                "DELETE FROM trace_attempts WHERE trace_id IN \
                 (SELECT trace_id FROM traces WHERE ts_epoch < ?1)",
                [cutoff],
            )?;
            let n = tx.execute("DELETE FROM traces WHERE ts_epoch < ?1", [cutoff])?;
            Ok(n)
        })();
        match res {
            Ok(n) => {
                if let Err(e) = tx.commit() {
                    tracing::warn!("trace 清理提交失败: {}", e);
                } else if n > 0 {
                    tracing::info!("已清理 {} 条过期 trace 记录", n);
                }
            }
            Err(e) => tracing::warn!("trace 清理失败: {}", e),
        }
    }

    /// 删除指定凭据关联的 trace 记录，避免删除账号后新账号复用同一 credential_id
    /// 时继承旧账号的失败统计。
    pub fn delete_for_credential(&self, credential_id: u64) {
        if credential_id == 0 {
            return;
        }
        let mut conn = self.conn.lock();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("trace 凭据清理事务失败: {}", e);
                return;
            }
        };
        let res = (|| -> duckdb::Result<usize> {
            // trace_phases 无 credential_id 列，只能走 trace_id 子查询这半支
            // （trace_attempts 那条语句里 "credential_id = ?1 OR" 这半支对 trace_phases 不适用）
            tx.execute(
                "DELETE FROM trace_phases WHERE trace_id IN \
                 (SELECT trace_id FROM traces WHERE final_credential_id = ?1)",
                [credential_id as i64],
            )?;
            tx.execute(
                "DELETE FROM trace_attempts WHERE credential_id = ?1 \
                 OR trace_id IN (SELECT trace_id FROM traces WHERE final_credential_id = ?1)",
                [credential_id as i64],
            )?;
            let n = tx.execute(
                "DELETE FROM traces WHERE final_credential_id = ?1",
                [credential_id as i64],
            )?;
            Ok(n)
        })();
        match res {
            Ok(n) => {
                if let Err(e) = tx.commit() {
                    tracing::warn!("trace 凭据清理提交失败: {}", e);
                } else if n > 0 {
                    tracing::info!("已清理凭据 #{} 的 {} 条 trace 记录", credential_id, n);
                }
            }
            Err(e) => tracing::warn!("trace 凭据清理失败: {}", e),
        }
    }

    /// 按凭据聚合失败跳数，归并为三类：鉴权 / 账号风控 / 其他。
    /// 统计 trace_attempts 里 outcome != 'success' 的跳，按 credential_id + outcome 分组。
    /// 返回 credential_id → (auth, throttle, other)。仅 warn 失败，返回空。
    pub fn failure_stats(&self) -> std::collections::HashMap<u64, FailureStats> {
        let conn = self.conn.lock();
        let mut out: std::collections::HashMap<u64, FailureStats> =
            std::collections::HashMap::new();
        let mut stmt = match conn.prepare(
            "SELECT credential_id, outcome, COUNT(*) FROM trace_attempts \
             WHERE outcome != 'success' AND credential_id != 0 \
             GROUP BY credential_id, outcome",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("trace failure_stats prepare 失败: {}", e);
                return out;
            }
        };
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
            ))
        });
        let rows = match rows {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("trace failure_stats 查询失败: {}", e);
                return out;
            }
        };
        for r in rows.flatten() {
            let (cred, outcome_str, cnt) = r;
            let s = out.entry(cred).or_default();
            match outcome_str.as_str() {
                "auth_failed" => s.auth += cnt,
                "account_throttled" => s.throttle += cnt,
                _ => s.other += cnt,
            }
        }
        // 流中断 / 上游截断只写 trace 顶层（attempts 里最后一跳仍是 success），
        // 需要从 traces 表按最终凭据单独归集，否则凭据卡片看不到这类上游问题。
        let mut trace_stmt = match conn.prepare(
            "SELECT final_credential_id, COUNT(*) FROM traces \
             WHERE error_type IN ('stream_interrupted', 'upstream_truncated') \
             AND final_credential_id != 0 GROUP BY final_credential_id",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("trace failure_stats 中断统计 prepare 失败: {}", e);
                return out;
            }
        };
        let trace_rows = trace_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
        });
        match trace_rows {
            Ok(rows) => {
                for (cred, cnt) in rows.flatten() {
                    out.entry(cred).or_default().interrupted += cnt;
                }
            }
            Err(e) => tracing::warn!("trace failure_stats 中断统计查询失败: {}", e),
        }
        out
    }
}

/// 按凭据的失败分类计数（鉴权 / 账号风控 / 流中断·上游截断 / 其他）
#[derive(Debug, Default, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureStats {
    pub auth: u64,
    pub throttle: u64,
    pub other: u64,
    /// stream_interrupted + upstream_truncated（上游中途断流类）
    #[serde(default)]
    pub interrupted: u64,
}

/// 共享存储句柄
pub type SharedTraceStore = Arc<TraceStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::duck::SCHEMA;

    struct TraceSample<'a> {
        trace_id: &'a str,
        status: &'a str,
        credential_id: u64,
        model: &'a str,
    }

    fn sample(input: TraceSample<'_>) -> TraceRecord {
        TraceRecord {
            trace_id: input.trace_id.to_string(),
            ts: Utc::now().to_rfc3339(),
            key_id: 1,
            key_source: TraceKeySource::ClientKey,
            model: input.model.to_string(),
            is_stream: true,
            final_status: input.status.to_string(),
            final_credential_id: input.credential_id,
            error_type: if input.status == "success" {
                None
            } else {
                Some(outcome::ACCOUNT_THROTTLED.to_string())
            },
            error_message: if input.status == "success" {
                None
            } else {
                Some("blocked".to_string())
            },
            total_attempts: 2,
            duration_ms: 1200,
            interrupted_after_bytes: None,
            input_tokens: 1093,
            output_tokens: 779,
            cache_creation_tokens: 0,
            cache_read_tokens: 101760,
            credits: 0.0,
            first_token_ms: None,
            session_id: Some("sess-test".to_string()),
            attempts: vec![
                TraceAttempt {
                    attempt: 0,
                    credential_id: 9,
                    endpoint: "ide".to_string(),
                    http_status: Some(429),
                    outcome: outcome::ACCOUNT_THROTTLED.to_string(),
                    error_snippet: Some("suspicious activity".to_string()),
                    duration_ms: 400,
                    started_ms: Some(0),
                    proxy_url: Some("socks5://p1:1080".to_string()),
                },
                TraceAttempt {
                    attempt: 1,
                    credential_id: input.credential_id,
                    endpoint: "ide".to_string(),
                    http_status: if input.status == "success" {
                        Some(200)
                    } else {
                        None
                    },
                    outcome: input.status.to_string(),
                    error_snippet: None,
                    duration_ms: 800,
                    // 与第 0 跳留出 400→900 的空隙，代表一次重试 backoff
                    started_ms: Some(900),
                    proxy_url: None,
                },
            ],
            phases: Vec::new(),
        }
    }

    fn mem_store() -> TraceStore {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        TraceStore {
            conn: Mutex::new(conn),
            enabled: AtomicBool::new(true),
            retention_days: AtomicU64::new(DEFAULT_RETENTION_DAYS),
        }
    }

    #[test]
    fn insert_and_query_roundtrip() {
        let store = mem_store();
        store.insert(&sample(TraceSample {
            trace_id: "t1",
            status: "success",
            credential_id: 5,
            model: "claude-opus-4-7",
        }));
        let out = store.query(&TraceQuery {
            limit: 50,
            ..Default::default()
        });
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trace_id, "t1");
        assert_eq!(out[0].attempts.len(), 2);
        assert_eq!(out[0].attempts[0].outcome, outcome::ACCOUNT_THROTTLED);
        assert_eq!(out[0].key_source, TraceKeySource::ClientKey);
        // token 分项往返
        assert_eq!(out[0].input_tokens, 1093);
        assert_eq!(out[0].output_tokens, 779);
        assert_eq!(out[0].cache_read_tokens, 101760);
        assert_eq!(out[0].cache_creation_tokens, 0);
    }

    #[test]
    fn disabled_skips_insert() {
        let store = mem_store();
        store.set_enabled(false);
        store.insert(&sample(TraceSample {
            trace_id: "t1",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        let out = store.query(&TraceQuery {
            limit: 50,
            ..Default::default()
        });
        assert_eq!(out.len(), 0, "trace 关闭时不应写入");
        // 重新开启后写入恢复
        store.set_enabled(true);
        store.insert(&sample(TraceSample {
            trace_id: "t2",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        assert_eq!(
            store
                .query(&TraceQuery {
                    limit: 50,
                    ..Default::default()
                })
                .len(),
            1
        );
    }

    #[test]
    fn attempt_started_ms_round_trips() {
        let store = mem_store();
        store.insert(&sample(TraceSample {
            trace_id: "t-off",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        let out = store.query(&TraceQuery {
            limit: 10,
            ..Default::default()
        });
        let attempts = &out[0].attempts;
        assert_eq!(attempts[0].started_ms, Some(0));
        // 第 1 跳起点 900 > 第 0 跳终点 400 —— 中间 500ms 是重试 backoff，
        // 前端据此画出空隙段。丢了这一列就只能顺序堆叠，backoff 会被抹掉。
        assert_eq!(attempts[1].started_ms, Some(900));
        assert!(
            attempts[1].started_ms.unwrap()
                > attempts[0].started_ms.unwrap() + attempts[0].duration_ms,
            "两跳之间应存在可识别的 backoff 空隙"
        );
    }

    /// 老库（无 started_ms 列）迁移后仍可读写，历史行该列为 None。
    #[test]
    fn migrate_adds_attempt_started_ms_and_keeps_old_rows_readable() {
        let conn = Connection::open_in_memory().unwrap();
        // 复刻该列存在前的 trace_attempts 定义
        conn.execute_batch(
            "CREATE TABLE traces (
                trace_id TEXT PRIMARY KEY, ts TEXT NOT NULL, ts_epoch INTEGER NOT NULL,
                key_id INTEGER NOT NULL, model TEXT NOT NULL, is_stream INTEGER NOT NULL,
                final_status TEXT NOT NULL, final_credential_id INTEGER NOT NULL,
                error_type TEXT, error_message TEXT, total_attempts INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL, interrupted_after_bytes INTEGER);
             CREATE TABLE trace_attempts (
                trace_id TEXT NOT NULL, attempt INTEGER NOT NULL, credential_id INTEGER NOT NULL,
                endpoint TEXT NOT NULL, http_status INTEGER, outcome TEXT NOT NULL,
                error_snippet TEXT, duration_ms INTEGER NOT NULL,
                PRIMARY KEY (trace_id, attempt));
             INSERT INTO traces VALUES
                ('t-old','2026-07-30T00:00:00Z',1785369600,0,'m1',1,'success',5,NULL,NULL,1,700,NULL);
             INSERT INTO trace_attempts VALUES ('t-old',0,5,'ide',200,'success',NULL,700);",
        )
        .unwrap();
        TraceStore::migrate(&conn).unwrap();
        // 幂等：重复迁移不应报 duplicate column
        TraceStore::migrate(&conn).unwrap();
        conn.execute_batch(SCHEMA).unwrap();

        let store = TraceStore {
            conn: Mutex::new(conn),
            enabled: AtomicBool::new(true),
            retention_days: AtomicU64::new(DEFAULT_RETENTION_DAYS),
        };
        let out = store.query(&TraceQuery {
            limit: 10,
            ..Default::default()
        });
        let old = out
            .iter()
            .find(|r| r.trace_id == "t-old")
            .expect("老行应可读");
        assert_eq!(
            old.attempts[0].started_ms, None,
            "历史行起点不可知，必须是 None 而非伪造的 0"
        );

        // 迁移后新写入仍带上偏移
        store.insert(&sample(TraceSample {
            trace_id: "t-new",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        let out = store.query(&TraceQuery {
            limit: 10,
            ..Default::default()
        });
        let new = out.iter().find(|r| r.trace_id == "t-new").unwrap();
        assert_eq!(new.attempts[0].started_ms, Some(0));
    }

    #[test]
    fn delete_for_credential_removes_failure_stats() {
        let store = mem_store();
        store.insert(&sample(TraceSample {
            trace_id: "old",
            status: "error",
            credential_id: 5,
            model: "m1",
        }));
        store.insert(&sample(TraceSample {
            trace_id: "keep",
            status: "error",
            credential_id: 6,
            model: "m1",
        }));

        assert!(store.failure_stats().contains_key(&5));
        store.delete_for_credential(5);

        let stats = store.failure_stats();
        assert!(!stats.contains_key(&5));
        assert!(stats.contains_key(&6));
        assert!(
            store
                .query(&TraceQuery {
                    credential_id: Some(5),
                    limit: 50,
                    ..Default::default()
                })
                .is_empty(),
            "deleted credential traces should not attach to a future account with the same id"
        );
    }

    #[test]
    fn filter_only_failed_and_status() {
        let store = mem_store();
        store.insert(&sample(TraceSample {
            trace_id: "ok",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        store.insert(&sample(TraceSample {
            trace_id: "bad",
            status: "error",
            credential_id: 6,
            model: "m1",
        }));
        store.insert(&sample(TraceSample {
            trace_id: "cut",
            status: "interrupted",
            credential_id: 7,
            model: "m2",
        }));

        let failed = store.query(&TraceQuery {
            only_failed: true,
            limit: 50,
            ..Default::default()
        });
        assert_eq!(failed.len(), 2);
        assert!(failed.iter().all(|r| r.final_status != "success"));

        let by_status = store.query(&TraceQuery {
            status: Some("interrupted".to_string()),
            limit: 50,
            ..Default::default()
        });
        assert_eq!(by_status.len(), 1);
        assert_eq!(by_status[0].trace_id, "cut");

        let by_model = store.query(&TraceQuery {
            model: Some("m2".to_string()),
            limit: 50,
            ..Default::default()
        });
        assert_eq!(by_model.len(), 1);
        assert_eq!(by_model[0].trace_id, "cut");
    }

    #[test]
    fn attempt_proxy_url_roundtrip() {
        let store = mem_store();
        store.insert(&sample(TraceSample {
            trace_id: "t-proxy",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        let out = store.query(&TraceQuery {
            limit: 50,
            ..Default::default()
        });
        assert_eq!(
            out[0].attempts[0].proxy_url.as_deref(),
            Some("socks5://p1:1080")
        );
        assert_eq!(out[0].attempts[1].proxy_url, None);
    }

    #[test]
    fn failure_stats_counts_stream_interruptions_from_traces() {
        let store = mem_store();
        // 流中断：attempts 里唯一一跳是 success（2xx 已拿到），错误只在 trace 顶层
        let mut rec = sample(TraceSample {
            trace_id: "t-int",
            status: "interrupted",
            credential_id: 5,
            model: "m1",
        });
        rec.error_type = Some(outcome::STREAM_INTERRUPTED.to_string());
        rec.attempts = vec![TraceAttempt {
            attempt: 0,
            credential_id: 5,
            endpoint: "ide".to_string(),
            http_status: Some(200),
            outcome: outcome::SUCCESS.to_string(),
            error_snippet: None,
            duration_ms: 800,
            started_ms: Some(0),
            proxy_url: None,
        }];
        store.insert(&rec);
        // 上游截断也应计入同一分类
        let mut rec2 = rec.clone();
        rec2.trace_id = "t-trunc".to_string();
        rec2.final_status = "error".to_string();
        rec2.error_type = Some(outcome::UPSTREAM_TRUNCATED.to_string());
        store.insert(&rec2);

        let stats = store.failure_stats();
        let s = stats.get(&5).expect("凭据 5 应有统计");
        assert_eq!(s.interrupted, 2, "流中断与上游截断都应计入 interrupted");
        assert_eq!(s.auth + s.throttle + s.other, 0, "attempt 全 success 不应产生其他失败计数");
    }

    #[test]
    fn cleanup_removes_old() {
        let store = mem_store();
        store.insert(&sample(TraceSample {
            trace_id: "recent",
            status: "success",
            credential_id: 5,
            model: "m1",
        }));
        // 手动塞一条 8 天前的记录
        {
            let conn = store.conn.lock();
            let old = (Utc::now() - chrono::Duration::days(8)).timestamp();
            conn.execute(
                "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
                 final_status, final_credential_id, total_attempts, duration_ms) \
                 VALUES ('old','2020',?1,1,'clientKey','m',1,'success',1,1,1)",
                [old],
            )
            .unwrap();
        }
        store.cleanup();
        let out = store.query(&TraceQuery {
            limit: 50,
            ..Default::default()
        });
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trace_id, "recent");
    }

    #[test]
    fn query_inner_rejects_unknown_key_source() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
             final_status, final_credential_id, total_attempts, duration_ms) \
             VALUES ('bad-source','2020',1,1,'unknown','m',1,'success',1,1,1)",
            [],
        )
        .unwrap();

        let result = TraceStore::query_inner(
            &conn,
            &TraceQuery {
                limit: 50,
                ..Default::default()
            },
        );

        assert!(result.is_err());
    }

    #[test]
    fn truncate_snippet_respects_limit() {
        assert_eq!(truncate_snippet("  "), None);
        assert_eq!(truncate_snippet("hi"), Some("hi".to_string()));
        let long = "x".repeat(ERROR_SNIPPET_MAX + 100);
        let out = truncate_snippet(&long).unwrap();
        assert!(out.ends_with("…(truncated)"));
        assert!(out.len() <= ERROR_SNIPPET_MAX + 20);
    }

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
}

//! 运维（Ops）模块：上游问题与代理池问题的统一统计和自动处置
//!
//! 两个组件：
//! - [`OpsStore`]：SQLite 存储。处置事件写 `ops_events` 表；统计聚合直接对
//!   traces.db 里既有的 `traces` / `trace_attempts` 表做 SQL 聚合（同一文件、
//!   独立连接，WAL 模式下并发读写安全），不引入第二份数据。
//! - [`OpsRuntime`]：请求路径上的反馈编排。provider / 流处理把「网络错误、
//!   流中断」按所用代理上报到这里；连续失败达阈值的代理被自动禁用后，
//!   在此完成受影响凭据的解绑/换绑，并记录处置事件。
//!
//! 设计参考 sub2api 的 ops 体系（错误分类统计、自动冷却/禁用、处置留痕），
//! 按单二进制 + SQLite 的体量裁剪。

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use parking_lot::Mutex;
use rusqlite::Connection;
use serde::Serialize;

use super::proxy_pool::ProxyPoolManager;
use crate::kiro::token_manager::MultiTokenManager;

/// 事件默认查询条数
const DEFAULT_EVENTS_LIMIT: usize = 100;
/// 统计窗口上限（小时）：防御性限制，避免全表扫描过大窗口
const MAX_WINDOW_HOURS: i64 = 24 * 31;

/// 处置事件分类
pub mod event_category {
    /// 请求级反馈触发的代理自动禁用
    pub const PROXY_AUTO_DISABLE: &str = "proxy_auto_disable";
    /// 自动禁用后受影响凭据的解绑/换绑
    pub const PROXY_REASSIGN: &str = "proxy_reassign";
    /// 健康检查（探测）触发的代理自动禁用
    pub const PROXY_PROBE_DISABLE: &str = "proxy_probe_disable";
}

/// 一条处置/异常事件
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsEvent {
    pub id: u64,
    pub ts: String,
    pub category: String,
    /// info / warn / error
    pub severity: String,
    /// 事件主体（如 `proxy#3` / `credential#5`）
    pub subject: String,
    pub message: String,
}

/// 运维概览（窗口内）
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsOverview {
    pub window_hours: i64,
    pub total: u64,
    pub success: u64,
    pub error: u64,
    pub interrupted: u64,
    /// error_type → 数量（含 interrupted 类）
    pub by_error_type: Vec<ErrorTypeCount>,
    pub avg_duration_ms: u64,
    pub avg_first_token_ms: Option<u64>,
    /// 中断类请求（stream_interrupted / upstream_truncated）的平均持续时长。
    /// 多次中断集中在同一时长（如 ~245s）通常指向链路上的固定超时。
    pub interrupted_avg_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorTypeCount {
    pub error_type: String,
    pub count: u64,
}

/// 按小时的请求趋势点
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsTrendPoint {
    /// 桶起始（unix 秒，整点对齐）
    pub bucket_epoch: i64,
    pub total: u64,
    pub success: u64,
    pub error: u64,
    pub interrupted: u64,
}

/// 按凭据的窗口统计
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCredentialRow {
    pub credential_id: u64,
    /// email 由 handler 层根据 token_manager 补充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    pub total: u64,
    pub success: u64,
    pub error: u64,
    pub interrupted: u64,
    /// attempt 级失败分类
    pub auth_failed: u64,
    pub account_throttled: u64,
    pub network_error: u64,
    pub other_failed: u64,
}

/// 按代理的窗口统计（'' = 直连）
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsProxyRow {
    /// 代理 URL；空串表示直连
    pub proxy_url: String,
    pub attempts: u64,
    pub success: u64,
    pub network_error: u64,
    pub other_failed: u64,
    /// 最终中断/截断的 trace 数（按该 trace 成功跳所用代理归属）
    pub interrupted: u64,
}

/// Ops SQLite 存储。
///
/// 与 [`super::trace_db::TraceStore`] 共用同一个 traces.db 文件、各持独立连接；
/// WAL + busy_timeout 保证两个写入方互不阻塞。
pub struct OpsStore {
    conn: Mutex<Connection>,
}

impl OpsStore {
    /// 打开（或创建）ops_events 表。path 指向 traces.db。
    pub fn open(path: PathBuf) -> rusqlite::Result<Self> {
        let conn = Connection::open(&path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(EVENTS_SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// 内存兜底（traces.db 打开失败时，事件仅进程内可见，聚合返回空）
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(EVENTS_SCHEMA)?;
        // 内存库没有 traces 表，聚合查询会失败：建空表让查询稳定返回空集
        conn.execute_batch(super::trace_db::SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// 记录一条处置事件。失败仅 warn，不阻塞请求路径。
    pub fn record_event(&self, category: &str, severity: &str, subject: &str, message: &str) {
        let now = Utc::now();
        let res = self.conn.lock().execute(
            "INSERT INTO ops_events (ts, ts_epoch, category, severity, subject, message) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                now.to_rfc3339(),
                now.timestamp(),
                category,
                severity,
                subject,
                message
            ],
        );
        if let Err(e) = res {
            tracing::warn!("ops 事件写入失败: {}", e);
        }
    }

    /// 最近的处置事件（时间倒序）
    pub fn recent_events(&self, limit: usize) -> Vec<OpsEvent> {
        let limit = if limit == 0 { DEFAULT_EVENTS_LIMIT } else { limit.min(1000) };
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT id, ts, category, severity, subject, message FROM ops_events \
             ORDER BY ts_epoch DESC, id DESC LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 事件查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(OpsEvent {
                id: row.get::<_, i64>(0)? as u64,
                ts: row.get(1)?,
                category: row.get(2)?,
                severity: row.get(3)?,
                subject: row.get(4)?,
                message: row.get(5)?,
            })
        });
        match rows {
            Ok(r) => r.flatten().collect(),
            Err(e) => {
                tracing::warn!("ops 事件查询失败: {}", e);
                Vec::new()
            }
        }
    }

    /// 清理过期事件（与 trace 保留天数对齐，由 main 的每日清理循环调用）
    pub fn cleanup(&self, retention_days: u64) {
        let cutoff = (Utc::now() - chrono::Duration::days(retention_days.max(1) as i64)).timestamp();
        match self
            .conn
            .lock()
            .execute("DELETE FROM ops_events WHERE ts_epoch < ?1", [cutoff])
        {
            Ok(n) if n > 0 => tracing::info!("已清理 {} 条过期 ops 事件", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("ops 事件清理失败: {}", e),
        }
    }

    fn window_cutoff(hours: i64) -> i64 {
        let hours = hours.clamp(1, MAX_WINDOW_HOURS);
        Utc::now().timestamp() - hours * 3600
    }

    /// 窗口内总体概览
    pub fn overview(&self, hours: i64) -> OpsOverview {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut out = OpsOverview {
            window_hours: hours.clamp(1, MAX_WINDOW_HOURS),
            ..Default::default()
        };
        let head = conn.query_row(
            "SELECT COUNT(*), \
             COALESCE(SUM(final_status = 'success'), 0), \
             COALESCE(SUM(final_status = 'interrupted'), 0), \
             COALESCE(CAST(AVG(duration_ms) AS INTEGER), 0), \
             CAST(AVG(first_token_ms) AS INTEGER) \
             FROM traces WHERE ts_epoch >= ?1",
            [cutoff],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        );
        match head {
            Ok((total, success, interrupted, avg_dur, avg_ftt)) => {
                out.total = total as u64;
                out.success = success as u64;
                out.interrupted = interrupted as u64;
                out.error = (total - success - interrupted).max(0) as u64;
                out.avg_duration_ms = avg_dur.max(0) as u64;
                out.avg_first_token_ms = avg_ftt.map(|v| v.max(0) as u64);
            }
            Err(e) => {
                tracing::warn!("ops overview 查询失败: {}", e);
                return out;
            }
        }
        // 错误类型分布
        let mut stmt = match conn.prepare(
            "SELECT error_type, COUNT(*) FROM traces \
             WHERE ts_epoch >= ?1 AND error_type IS NOT NULL \
             GROUP BY error_type ORDER BY COUNT(*) DESC",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 错误分布查询失败: {}", e);
                return out;
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok(ErrorTypeCount {
                error_type: row.get(0)?,
                count: row.get::<_, i64>(1)? as u64,
            })
        }) {
            out.by_error_type = rows.flatten().collect();
        }
        // 中断类平均时长（固定超时特征探测）
        out.interrupted_avg_duration_ms = conn
            .query_row(
                "SELECT CAST(AVG(duration_ms) AS INTEGER) FROM traces \
                 WHERE ts_epoch >= ?1 \
                 AND error_type IN ('stream_interrupted', 'upstream_truncated')",
                [cutoff],
                |row| row.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
            .map(|v| v.max(0) as u64);
        out
    }

    /// 按小时的请求趋势
    pub fn error_trend(&self, hours: i64) -> Vec<OpsTrendPoint> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT (ts_epoch / 3600) * 3600 AS bucket, COUNT(*), \
             COALESCE(SUM(final_status = 'success'), 0), \
             COALESCE(SUM(final_status = 'interrupted'), 0) \
             FROM traces WHERE ts_epoch >= ?1 GROUP BY bucket ORDER BY bucket ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 趋势查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map([cutoff], |row| {
            let total: i64 = row.get(1)?;
            let success: i64 = row.get(2)?;
            let interrupted: i64 = row.get(3)?;
            Ok(OpsTrendPoint {
                bucket_epoch: row.get(0)?,
                total: total as u64,
                success: success as u64,
                interrupted: interrupted as u64,
                error: (total - success - interrupted).max(0) as u64,
            })
        });
        match rows {
            Ok(r) => r.flatten().collect(),
            Err(e) => {
                tracing::warn!("ops 趋势查询失败: {}", e);
                Vec::new()
            }
        }
    }

    /// 按凭据的窗口统计（trace 顶层 + attempt 级失败分类合并）
    pub fn by_credential(&self, hours: i64) -> Vec<OpsCredentialRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut map: std::collections::HashMap<u64, OpsCredentialRow> =
            std::collections::HashMap::new();

        let mut stmt = match conn.prepare(
            "SELECT final_credential_id, COUNT(*), \
             COALESCE(SUM(final_status = 'success'), 0), \
             COALESCE(SUM(error_type IN ('stream_interrupted', 'upstream_truncated')), 0) \
             FROM traces WHERE ts_epoch >= ?1 AND final_credential_id != 0 \
             GROUP BY final_credential_id",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 凭据统计查询失败: {}", e);
                return Vec::new();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64,
            ))
        }) {
            for (cred, total, success, interrupted) in rows.flatten() {
                let row = map.entry(cred).or_default();
                row.credential_id = cred;
                row.total = total;
                row.success = success;
                row.interrupted = interrupted;
                row.error = total.saturating_sub(success).saturating_sub(interrupted);
            }
        }

        let mut stmt = match conn.prepare(
            "SELECT a.credential_id, a.outcome, COUNT(*) FROM trace_attempts a \
             JOIN traces t ON t.trace_id = a.trace_id \
             WHERE t.ts_epoch >= ?1 AND a.credential_id != 0 AND a.outcome != 'success' \
             GROUP BY a.credential_id, a.outcome",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 凭据 attempt 统计查询失败: {}", e);
                return map.into_values().collect();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
            ))
        }) {
            for (cred, outcome, cnt) in rows.flatten() {
                let row = map.entry(cred).or_default();
                row.credential_id = cred;
                match outcome.as_str() {
                    "auth_failed" => row.auth_failed += cnt,
                    "account_throttled" => row.account_throttled += cnt,
                    "network_error" => row.network_error += cnt,
                    _ => row.other_failed += cnt,
                }
            }
        }

        let mut out: Vec<OpsCredentialRow> = map.into_values().collect();
        out.sort_by(|a, b| b.total.cmp(&a.total));
        out
    }

    /// 按代理的窗口统计。proxy_url 为 NULL 的 attempt 归入 ''（直连/未知）。
    pub fn by_proxy(&self, hours: i64) -> Vec<OpsProxyRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut map: std::collections::HashMap<String, OpsProxyRow> =
            std::collections::HashMap::new();

        let mut stmt = match conn.prepare(
            "SELECT COALESCE(a.proxy_url, ''), COUNT(*), \
             COALESCE(SUM(a.outcome = 'success'), 0), \
             COALESCE(SUM(a.outcome = 'network_error'), 0) \
             FROM trace_attempts a JOIN traces t ON t.trace_id = a.trace_id \
             WHERE t.ts_epoch >= ?1 GROUP BY COALESCE(a.proxy_url, '')",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 代理统计查询失败: {}", e);
                return Vec::new();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64,
            ))
        }) {
            for (url, attempts, success, network_error) in rows.flatten() {
                let row = map.entry(url.clone()).or_default();
                row.proxy_url = url;
                row.attempts = attempts;
                row.success = success;
                row.network_error = network_error;
                row.other_failed = attempts
                    .saturating_sub(success)
                    .saturating_sub(network_error);
            }
        }

        // 中断/截断 trace 按其成功跳（2xx 已拿到、随后断流）所用代理归属
        let mut stmt = match conn.prepare(
            "SELECT COALESCE(a.proxy_url, ''), COUNT(*) \
             FROM traces t JOIN trace_attempts a \
             ON a.trace_id = t.trace_id AND a.outcome = 'success' \
             WHERE t.ts_epoch >= ?1 \
             AND t.error_type IN ('stream_interrupted', 'upstream_truncated') \
             GROUP BY COALESCE(a.proxy_url, '')",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 代理中断统计查询失败: {}", e);
                return map.into_values().collect();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        }) {
            for (url, cnt) in rows.flatten() {
                let row = map.entry(url.clone()).or_default();
                row.proxy_url = url;
                row.interrupted += cnt;
            }
        }

        let mut out: Vec<OpsProxyRow> = map.into_values().collect();
        out.sort_by(|a, b| b.attempts.cmp(&a.attempts));
        out
    }

    /// 按 (段, 出口) 统计窗口内的失败率。
    /// 出口取该 trace 最后一跳的 proxy_url —— 流生命周期发生在最终成功建连的那一跳上。
    pub fn phase_baseline(&self, hours: i64) -> Vec<PhaseBaselineRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT p.phase, \
                    COALESCE((SELECT a.proxy_url FROM trace_attempts a \
                              WHERE a.trace_id = p.trace_id \
                              ORDER BY a.attempt DESC LIMIT 1), '') AS proxy, \
                    COUNT(*) AS total, \
                    SUM(CASE WHEN p.outcome NOT IN ('success', 'client_disconnected') THEN 1 ELSE 0 END) AS failed \
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

const EVENTS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS ops_events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       TEXT NOT NULL,
    ts_epoch INTEGER NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL,
    subject  TEXT NOT NULL,
    message  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ops_events_ts ON ops_events(ts_epoch DESC);
";

/// 请求路径上的运维反馈编排。
///
/// 挂在 provider（网络错误/成功）与流处理（中断）上；所有方法都不阻塞、
/// 不返回错误——运维反馈失败不能影响正常请求。
pub struct OpsRuntime {
    pool: Arc<ProxyPoolManager>,
    token_manager: Arc<MultiTokenManager>,
    events: Arc<OpsStore>,
}

impl OpsRuntime {
    pub fn new(
        pool: Arc<ProxyPoolManager>,
        token_manager: Arc<MultiTokenManager>,
        events: Arc<OpsStore>,
    ) -> Self {
        Self {
            pool,
            token_manager,
            events,
        }
    }

    pub fn events(&self) -> &Arc<OpsStore> {
        &self.events
    }

    /// 真实请求经代理成功：清零该代理的请求级失败计数
    pub fn report_proxy_success(&self, proxy_url: Option<&str>) {
        if let Some(url) = proxy_url {
            self.pool.report_request_success(url);
        }
    }

    /// 真实请求经代理失败（网络错误 / 流中断）：累计并按需自动禁用 + 解绑换绑
    pub fn report_proxy_failure(&self, proxy_url: Option<&str>, error: &str) {
        let Some(url) = proxy_url else { return };
        let Some(disabled) = self.pool.report_request_failure(url, error) else {
            return;
        };
        self.events.record_event(
            event_category::PROXY_AUTO_DISABLE,
            "error",
            &format!("proxy#{}", disabled.id),
            &format!(
                "代理 {} 连续 {} 次真实请求失败，已自动禁用（最近错误: {}）",
                disabled.url,
                disabled.request_failures,
                error.chars().take(200).collect::<String>()
            ),
        );
        self.reassign_after_disable(&disabled.url, disabled.id);
    }

    /// 流中断反馈：计入所用代理的请求级失败（直连中断只落 trace 统计，无处置动作）
    pub fn report_stream_interrupted(
        &self,
        credential_id: u64,
        proxy_url: Option<&str>,
        error: &str,
    ) {
        tracing::debug!(
            "流中断反馈: 凭据 #{} 代理 {:?}",
            credential_id,
            proxy_url
        );
        self.report_proxy_failure(proxy_url, error);
    }

    /// 健康检查自动禁用后的补充处置（由 AdminService 的探测循环调用）：
    /// 记事件并解绑受影响凭据。
    pub fn handle_probe_auto_disable(&self, proxy_id: u64, proxy_url: &str) {
        self.events.record_event(
            event_category::PROXY_PROBE_DISABLE,
            "error",
            &format!("proxy#{}", proxy_id),
            &format!("代理 {} 连续探测失败，已自动禁用", proxy_url),
        );
        self.reassign_after_disable(proxy_url, proxy_id);
    }

    /// 代理被自动禁用后：把绑定它的凭据换绑到池内其它可用代理（无可用则清除、
    /// 回退全局代理/直连），并记录处置事件。
    fn reassign_after_disable(&self, disabled_url: &str, proxy_id: u64) {
        let replacement = self
            .pool
            .assignable_urls()
            .into_iter()
            .find(|u| u != disabled_url);
        match self
            .token_manager
            .reassign_proxy_url(disabled_url, replacement.clone())
        {
            Ok(affected) if affected.is_empty() => {}
            Ok(affected) => {
                let target = replacement.as_deref().unwrap_or("（无可用代理，回退全局/直连）");
                tracing::warn!(
                    "代理 #{} 自动禁用：凭据 {:?} 已换绑到 {}",
                    proxy_id,
                    affected,
                    target
                );
                self.events.record_event(
                    event_category::PROXY_REASSIGN,
                    "warn",
                    &format!("proxy#{}", proxy_id),
                    &format!(
                        "受影响凭据 {} 已从 {} 换绑到 {}",
                        affected
                            .iter()
                            .map(|id| format!("#{}", id))
                            .collect::<Vec<_>>()
                            .join(", "),
                        disabled_url,
                        target
                    ),
                );
            }
            Err(e) => {
                tracing::warn!("代理 #{} 自动禁用后换绑凭据失败: {}", proxy_id, e);
                self.events.record_event(
                    event_category::PROXY_REASSIGN,
                    "error",
                    &format!("proxy#{}", proxy_id),
                    &format!("换绑凭据失败: {}", e),
                );
            }
        }
    }
}

/// 共享句柄
pub type SharedOpsRuntime = Arc<OpsRuntime>;

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_store_with_traces() -> OpsStore {
        OpsStore::open_in_memory().unwrap()
    }

    fn insert_trace(
        store: &OpsStore,
        trace_id: &str,
        epoch_offset_secs: i64,
        final_status: &str,
        error_type: Option<&str>,
        credential_id: u64,
        duration_ms: u64,
    ) {
        let epoch = Utc::now().timestamp() - epoch_offset_secs;
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
                 final_status, final_credential_id, error_type, total_attempts, duration_ms) \
                 VALUES (?1, '2026', ?2, 1, 'clientKey', 'm', 1, ?3, ?4, ?5, 1, ?6)",
                rusqlite::params![
                    trace_id,
                    epoch,
                    final_status,
                    credential_id as i64,
                    error_type,
                    duration_ms as i64
                ],
            )
            .unwrap();
    }

    fn insert_attempt(
        store: &OpsStore,
        trace_id: &str,
        outcome: &str,
        credential_id: u64,
        proxy_url: Option<&str>,
    ) {
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO trace_attempts (trace_id, attempt, credential_id, endpoint, \
                 outcome, duration_ms, proxy_url) VALUES (?1, 0, ?2, 'ide', ?3, 100, ?4)",
                rusqlite::params![trace_id, credential_id as i64, outcome, proxy_url],
            )
            .unwrap();
    }

    fn insert_phase(store: &OpsStore, trace_id: &str, seq: u32, phase: &str, outcome: &str) {
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

    #[test]
    fn events_roundtrip_and_cleanup() {
        let store = mem_store_with_traces();
        store.record_event(event_category::PROXY_AUTO_DISABLE, "error", "proxy#1", "msg");
        let events = store.recent_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].category, event_category::PROXY_AUTO_DISABLE);
        store.cleanup(30);
        assert_eq!(store.recent_events(10).len(), 1, "未过期事件不应被清理");
    }

    #[test]
    fn overview_counts_statuses_and_interrupted_duration() {
        let store = mem_store_with_traces();
        insert_trace(&store, "t1", 60, "success", None, 1, 1000);
        insert_trace(&store, "t2", 60, "interrupted", Some("stream_interrupted"), 1, 245_000);
        insert_trace(&store, "t3", 60, "error", Some("upstream_truncated"), 2, 247_000);
        insert_trace(&store, "t4", 60, "error", Some("auth_failed"), 2, 500);
        // 窗口外的记录不计
        insert_trace(&store, "t-old", 3600 * 48, "error", Some("auth_failed"), 2, 500);

        let o = store.overview(24);
        assert_eq!(o.total, 4);
        assert_eq!(o.success, 1);
        assert_eq!(o.interrupted, 1);
        assert_eq!(o.error, 2);
        // 中断类平均时长 = (245000 + 247000) / 2
        assert_eq!(o.interrupted_avg_duration_ms, Some(246_000));
        let truncated = o
            .by_error_type
            .iter()
            .find(|e| e.error_type == "upstream_truncated")
            .unwrap();
        assert_eq!(truncated.count, 1);
    }

    #[test]
    fn by_proxy_attributes_interruptions_to_proxy_of_success_hop() {
        let store = mem_store_with_traces();
        // 走代理 p1 的中断
        insert_trace(&store, "t1", 60, "interrupted", Some("stream_interrupted"), 1, 245_000);
        insert_attempt(&store, "t1", "success", 1, Some("socks5://p1:1080"));
        // 直连成功
        insert_trace(&store, "t2", 60, "success", None, 2, 1000);
        insert_attempt(&store, "t2", "success", 2, None);
        // 走 p1 的网络错误跳 + 换直连成功
        insert_trace(&store, "t3", 60, "success", None, 2, 1500);
        insert_attempt(&store, "t3", "network_error", 1, Some("socks5://p1:1080"));

        let rows = store.by_proxy(24);
        let p1 = rows.iter().find(|r| r.proxy_url == "socks5://p1:1080").unwrap();
        assert_eq!(p1.attempts, 2);
        assert_eq!(p1.success, 1);
        assert_eq!(p1.network_error, 1);
        assert_eq!(p1.interrupted, 1);
        let direct = rows.iter().find(|r| r.proxy_url.is_empty()).unwrap();
        assert_eq!(direct.attempts, 1);
        assert_eq!(direct.interrupted, 0);
    }

    #[test]
    fn by_credential_merges_trace_and_attempt_stats() {
        let store = mem_store_with_traces();
        insert_trace(&store, "t1", 60, "interrupted", Some("stream_interrupted"), 5, 245_000);
        insert_attempt(&store, "t1", "success", 5, None);
        insert_trace(&store, "t2", 60, "error", Some("auth_failed"), 5, 400);
        insert_attempt(&store, "t2", "auth_failed", 5, None);

        let rows = store.by_credential(24);
        let c5 = rows.iter().find(|r| r.credential_id == 5).unwrap();
        assert_eq!(c5.total, 2);
        assert_eq!(c5.interrupted, 1);
        assert_eq!(c5.error, 1);
        assert_eq!(c5.auth_failed, 1);
    }

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
}

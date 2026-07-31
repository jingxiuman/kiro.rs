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
/// 交叉表每个 error_type 最多回多少个维度桶。
/// 判断"是否集中"只需要看头部几个，全量回传对结论无增益。
const CROSSTAB_TOP_BUCKETS: usize = 6;

/// 处置事件分类
pub mod event_category {
    /// 请求级反馈触发的代理自动禁用
    pub const PROXY_AUTO_DISABLE: &str = "proxy_auto_disable";
    /// 自动禁用后受影响凭据的解绑/换绑
    pub const PROXY_REASSIGN: &str = "proxy_reassign";
    /// 健康检查（探测）触发的代理自动禁用
    pub const PROXY_PROBE_DISABLE: &str = "proxy_probe_disable";
    /// 恢复探针把自动禁用的代理放回账号池
    pub const PROXY_AUTO_RECOVER: &str = "proxy_auto_recover";
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
    /// 平均首 token 延迟，**仅统计流式请求**。
    ///
    /// 非流式自 0.8.7 起也有 `first_token_ms`，但混进来会让这个指标的口径从
    /// 「流式首 token」变成「流式+非流式混合平均」，与历史窗口不可比 ——
    /// 非流式的首字节含义（等上游吐第一口，之后还要收完整段）与流式（此后即可
    /// 边收边发给客户端）对运维决策的指向也不同。故显式钉死在流式上。
    pub avg_first_token_ms: Option<u64>,
    /// 中断类请求（stream_interrupted / upstream_truncated）的耗时分位。
    /// 多个中断集中在同一时长通常指向链路上的固定超时。
    pub interrupted_duration: Option<DurationPercentiles>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorTypeCount {
    pub error_type: String,
    pub count: u64,
}

/// 耗时分位。
///
/// 替代原先的平均值：平均值会给出一个任何真实样本都不在的数字。实测中断同时
/// 存在 ~240s 与 ~720s 两簇（720s 恰是 `MCP_TOTAL_TIMEOUT_SECS`），平均落在
/// 中间的 ~300s —— 两簇里都不存在,反而把"存在两个不同固定超时"这个结论抹掉。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DurationPercentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    /// 样本数。实测 7 天窗口仅约 20 条，此时 p99 就是最大值本身；
    /// 面板必须一并显示 n，否则 p99 会被当成有统计意义的分位读。
    pub n: u64,
}

/// 从样本算分位。就地排序后按最近秩取值。
///
/// 不用 SQL 算：SQLite 无 `PERCENTILE_CONT`，而中断样本量在百级以下，
/// 取回来排序比在 SQL 里拼窗口函数更简单也更好测。
fn percentiles(mut v: Vec<u64>) -> Option<DurationPercentiles> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let at = |p: f64| -> u64 {
        let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
        v[idx.min(v.len() - 1)]
    };
    Some(DurationPercentiles {
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        n: v.len() as u64,
    })
}

/// 交叉表的维度
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrosstabDimension {
    Credential,
    Proxy,
    Model,
    /// 上游端点（`cli` / `ide`）。
    ///
    /// 加这个维度的直接原因：实测 transient 类错误（占全部错误约一半）在控制模型
    /// 后，`cli` 端点的失败率是 `ide` 的两个数量级以上，而 credential 与 proxy
    /// 两个维度上看到的信号都只是它的下游投影——那张凭据恰好绑着 cli。
    /// 少了这个维度，面板会把人引向「禁用某张凭据/某个代理」，而真正的杠杆在端点。
    Endpoint,
}

impl CrosstabDimension {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "credential" => Some(Self::Credential),
            "proxy" => Some(Self::Proxy),
            "model" => Some(Self::Model),
            "endpoint" => Some(Self::Endpoint),
            _ => None,
        }
    }

    /// 可接受取值，供 handler 拼错误消息用。
    /// 单独抽出来是为了新增维度时不会漏改那条 400 文案（漏改的话，
    /// 用户会被告知一个已经支持的维度不被支持）。
    pub const ALL: [&'static str; 4] = ["credential", "proxy", "model", "endpoint"];

    fn as_str(self) -> &'static str {
        match self {
            Self::Credential => "credential",
            Self::Proxy => "proxy",
            Self::Model => "model",
            Self::Endpoint => "endpoint",
        }
    }
}

/// 交叉表里的一个维度桶
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrosstabBucket {
    /// 维度取值。credential 为 id 字符串；proxy 为 URL（`direct` = 直连，
    /// 空串 = 未知）；model 为模型名
    pub key: String,
    pub count: u64,
    /// 该桶在窗口内的**全部**请求数（成功+失败），即流量基线
    pub traffic: u64,
    /// 超额倍数 = （本桶错误数 / 该 error_type 总错误数）÷（本桶流量 / 窗口总流量）
    ///
    /// 为什么必须有这个数：裸计数和裸集中度都会被流量分布带偏。实测
    /// `claude-opus-5` 占全流量 63%，于是**每一种**错误都"集中"在它身上，
    /// 看着像它有问题，其实只是它承载得多。lift ≈ 1 表示该桶的错误份额与它的
    /// 流量份额相称（没有异常）；lift 明显 > 1 才是"这个对象错得不成比例"。
    ///
    /// `None` = 该桶流量为 0（窗口内只有错误没有流量记录，理论上不该出现），
    /// 此时无法计算比值，不填 0 以免被误读成"低于预期"。
    pub lift: Option<f64>,
}

/// 一个 error_type 在某维度上的分布
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrosstabRow {
    pub error_type: String,
    /// 该 error_type 在窗口内的总数
    pub total: u64,
    /// 头部维度桶，降序
    pub buckets: Vec<CrosstabBucket>,
    /// 集中度 = 最大桶 / total，范围 (0, 1]。
    ///
    /// 接近 1 说明该错误压在单个对象上；接近 1/维度基数 说明均匀散开。
    ///
    /// **单看这个数会误判**：流量本身不均匀时，占流量最大的对象在每种错误上都会
    /// 显得"集中"。必须与桶上的 [`CrosstabBucket::lift`] 一起读 —— 集中度高
    /// 且 lift ≈ 1 只是流量使然；集中度高**且** lift 明显 > 1 才是真信号。
    pub concentration: f64,
    /// 该 error_type 覆盖到的不同维度取值数，用来判断 concentration 的分母量级
    pub distinct_keys: u64,
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
             CAST(AVG(CASE WHEN is_stream = 1 THEN first_token_ms END) AS INTEGER) \
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
        // 中断类耗时分位（固定超时特征探测）。
        // 口径与原先的平均值完全一致（同一 error_type 过滤），只是换成分位，
        // 保证与历史窗口可比。
        out.interrupted_duration = conn
            .prepare(
                "SELECT duration_ms FROM traces \
                 WHERE ts_epoch >= ?1 \
                 AND error_type IN ('stream_interrupted', 'upstream_truncated')",
            )
            .and_then(|mut stmt| {
                let rows = stmt.query_map([cutoff], |row| row.get::<_, i64>(0))?;
                Ok(rows.flatten().map(|v| v.max(0) as u64).collect::<Vec<u64>>())
            })
            .map_err(|e| tracing::warn!("ops 中断耗时分位查询失败: {}", e))
            .ok()
            .and_then(percentiles);
        out
    }

    /// error_type × 维度 的交叉表。
    ///
    /// 回答「这个错误是全局的还是压在某个对象上」—— 一维边际统计
    /// （[`Self::overview`] 的 `by_error_type`）答不出这个，而两种情况的处置动作相反。
    ///
    /// **代理归属口径**：按该 trace 的最后一跳所用代理。已用真实数据核实过，
    /// 中断类 trace 的最后一跳 outcome 全为 `success`，即最后一跳就是成功跳，
    /// 因此这一条规则同时满足「失败归给失败发生处」与 [`Self::by_proxy`] 里
    /// 「中断归给成功跳」两个口径，无需按 error_type 分叉。
    pub fn error_crosstab(&self, hours: i64, dim: CrosstabDimension) -> Vec<CrosstabRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();

        // 最后一跳的关联。proxy 与 endpoint 都只存在于 trace_attempts 上，
        // 共用这一段；credential 与 model 直接取 traces 列，不需要 join。
        const LAST_HOP_JOIN: &str = " JOIN trace_attempts a ON a.trace_id = t.trace_id \
             AND a.attempt = (SELECT MAX(a2.attempt) FROM trace_attempts a2 \
                              WHERE a2.trace_id = t.trace_id)";
        let (key_expr, join) = match dim {
            CrosstabDimension::Credential => ("CAST(t.final_credential_id AS TEXT)", ""),
            CrosstabDimension::Model => ("t.model", ""),
            CrosstabDimension::Proxy => ("COALESCE(a.proxy_url, '')", LAST_HOP_JOIN),
            CrosstabDimension::Endpoint => ("COALESCE(a.endpoint, '')", LAST_HOP_JOIN),
        };
        let sql = format!(
            "SELECT t.error_type, {key} AS k, COUNT(*) AS c \
             FROM traces t{join} \
             WHERE t.ts_epoch >= ?1 AND t.error_type IS NOT NULL \
             GROUP BY t.error_type, k ORDER BY t.error_type ASC, c DESC",
            key = key_expr,
            join = join,
        );

        // 流量基线：同一维度上**全部**请求（不筛 error_type）的分布。
        // 没有它就无法区分"这个对象错得不成比例"与"这个对象承载得最多"。
        let baseline_sql = format!(
            "SELECT {key} AS k, COUNT(*) FROM traces t{join} \
             WHERE t.ts_epoch >= ?1 GROUP BY k",
            key = key_expr,
            join = join,
        );
        let mut traffic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        match conn.prepare(&baseline_sql).and_then(|mut stmt| {
            let rows = stmt.query_map([cutoff], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?;
            Ok(rows.flatten().collect::<Vec<_>>())
        }) {
            Ok(rows) => traffic.extend(rows),
            Err(e) => {
                // 基线拿不到时不假装能算 lift：后面 lift 会是 None，
                // 前端据此隐藏该列，而不是显示一个错的倍数。
                tracing::warn!("ops 交叉表基线查询失败（dim={}）: {}", dim.as_str(), e);
            }
        }
        let total_traffic: u64 = traffic.values().sum();

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 交叉表查询失败（dim={}）: {}", dim.as_str(), e);
                return Vec::new();
            }
        };
        let rows = match stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
            ))
        }) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("ops 交叉表查询失败（dim={}）: {}", dim.as_str(), e);
                return Vec::new();
            }
        };

        // SQL 已按 (error_type, count DESC) 排好，这里顺序聚成每个 error_type 一行
        let mut out: Vec<CrosstabRow> = Vec::new();
        for (error_type, key, count) in rows.flatten() {
            let bucket = CrosstabBucket {
                traffic: traffic.get(&key).copied().unwrap_or(0),
                key,
                count,
                lift: None, // total 未知，等下面补
            };
            match out.last_mut() {
                Some(row) if row.error_type == error_type => {
                    row.total += count;
                    row.distinct_keys += 1;
                    if row.buckets.len() < CROSSTAB_TOP_BUCKETS {
                        row.buckets.push(bucket);
                    }
                }
                _ => out.push(CrosstabRow {
                    error_type,
                    total: count,
                    buckets: vec![bucket],
                    concentration: 0.0,
                    distinct_keys: 1,
                }),
            }
        }
        // 集中度与 lift 的分母都要等该 error_type 全部桶累完才知道
        for row in &mut out {
            let top = row.buckets.first().map(|b| b.count).unwrap_or(0);
            row.concentration = if row.total == 0 {
                0.0
            } else {
                top as f64 / row.total as f64
            };
            for b in &mut row.buckets {
                // lift = 错误份额 ÷ 流量份额
                b.lift = if row.total == 0 || total_traffic == 0 || b.traffic == 0 {
                    None
                } else {
                    let err_share = b.count as f64 / row.total as f64;
                    let traffic_share = b.traffic as f64 / total_traffic as f64;
                    Some(err_share / traffic_share)
                };
            }
        }
        out.sort_by_key(|row| std::cmp::Reverse(row.total));
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
        out.sort_by_key(|row| std::cmp::Reverse(row.total));
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
        out.sort_by_key(|row| std::cmp::Reverse(row.attempts));
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

    /// 恢复探针把代理放回账号池后：只记事件。
    ///
    /// **不换绑凭据**——当初被 [`Self::reassign_after_disable`] 迁走的凭据留在新代理上。
    /// 恢复的是「可用性」（重新进入可分配池），不是「谁绑在谁上」。它们已经在新代理上
    /// 跑得好好的，再搬一次只是多一次扰动。
    pub fn handle_probe_auto_recover(&self, proxy_id: u64, proxy_url: &str) {
        tracing::info!("代理 #{} 已通过恢复探测，放回账号池: {}", proxy_id, proxy_url);
        self.events.record_event(
            event_category::PROXY_AUTO_RECOVER,
            "info",
            &format!("proxy#{}", proxy_id),
            &format!("代理 {} 连续探测成功，已自动放回账号池（凭据绑定不变）", proxy_url),
        );
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
                let target = replacement.as_deref().unwrap_or(
                if crate::http_client::require_proxy() {
                    "（无可用代理；requireProxy 已开启，这些凭据的请求将被拒绝而非裸连）"
                } else {
                    "（无可用代理，回退全局/直连）"
                },
            );
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
        insert_attempt_on(store, trace_id, outcome, credential_id, proxy_url, "ide");
    }

    /// 可指定端点的变体。真实库里目前只有 `ide` 一个端点，端点维度的行为
    /// 无法用真实数据覆盖，只能在测试里构造多端点场景。
    fn insert_attempt_on(
        store: &OpsStore,
        trace_id: &str,
        outcome: &str,
        credential_id: u64,
        proxy_url: Option<&str>,
        endpoint: &str,
    ) {
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO trace_attempts (trace_id, attempt, credential_id, endpoint, \
                 outcome, duration_ms, proxy_url) VALUES (?1, 0, ?2, ?3, ?4, 100, ?5)",
                rusqlite::params![trace_id, credential_id as i64, endpoint, outcome, proxy_url],
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
        // 中断类耗时分位：样本 [245000, 247000]，n=2。
        // 最近秩取值（无插值）：p50 的秩 = (2-1)*0.5 = 0.5，round 进位到 1 → 取上界。
        // 偶数样本没有真正的中位数，不插值就必然偏向一侧，这是刻意取舍。
        let d = o.interrupted_duration.expect("应有中断样本");
        assert_eq!(d.n, 2);
        assert_eq!(d.p50, 247_000);
        assert_eq!(d.p99, 247_000);
        let truncated = o
            .by_error_type
            .iter()
            .find(|e| e.error_type == "upstream_truncated")
            .unwrap();
        assert_eq!(truncated.count, 1);
    }

    /// 分位替代平均值的理由必须可执行验证：双簇样本下平均值落在两簇之间的
    /// 空档（任何真实样本都不在那里），而 p50/p99 各自落在真实簇上。
    /// 这正是实测 ~240s 与 ~720s 两簇的情形。
    #[test]
    fn percentiles_expose_bimodal_clusters_that_average_would_hide() {
        // 6 个 240s 簇 + 3 个 720s 簇
        let mut v: Vec<u64> = vec![240_000; 6];
        v.extend(vec![720_000; 3]);
        let avg = v.iter().sum::<u64>() / v.len() as u64;
        let d = percentiles(v).unwrap();

        assert_eq!(d.n, 9);
        assert_eq!(d.p50, 240_000, "p50 落在主簇");
        assert_eq!(d.p99, 720_000, "p99 暴露次簇");
        // 平均值 400s 既不在 240 簇也不在 720 簇 —— 它描述的是不存在的请求
        assert_eq!(avg, 400_000);
        assert!(
            avg != d.p50 && avg != d.p99,
            "平均值落在两簇之间的空档，会抹掉'存在两个固定超时'这个结论"
        );
    }

    /// 交叉表的全部价值在于区分「压在单个对象上」与「均匀散开」，
    /// 因为两者的处置动作相反。这个测试把两种形态放在同一窗口里对比。
    #[test]
    fn crosstab_separates_concentrated_from_spread_errors() {
        let store = mem_store_with_traces();
        // network_error：4 条全压在凭据 7 上 → 集中，处置=换掉这个凭据/它的代理
        for (i, id) in [(0, 7), (1, 7), (2, 7), (3, 7)] {
            insert_trace(
                &store,
                &format!("conc{}", i),
                60,
                "error",
                Some("network_error"),
                id,
                500,
            );
        }
        // transient：4 条散在 4 个不同凭据 → 系统性，换单个凭据无效
        for (i, id) in [(0, 1), (1, 2), (2, 3), (3, 4)] {
            insert_trace(
                &store,
                &format!("spread{}", i),
                60,
                "error",
                Some("transient"),
                id,
                500,
            );
        }

        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let conc = rows
            .iter()
            .find(|r| r.error_type == "network_error")
            .expect("应有 network_error 行");
        let spread = rows
            .iter()
            .find(|r| r.error_type == "transient")
            .expect("应有 transient 行");

        assert_eq!(conc.total, 4);
        assert_eq!(conc.distinct_keys, 1);
        assert!(
            (conc.concentration - 1.0).abs() < 1e-9,
            "全压在一个凭据上，集中度应为 1"
        );
        assert_eq!(conc.buckets[0].key, "7");

        assert_eq!(spread.total, 4);
        assert_eq!(spread.distinct_keys, 4);
        assert!(
            (spread.concentration - 0.25).abs() < 1e-9,
            "均匀散在 4 个凭据上，集中度应为 1/4"
        );

        assert!(
            conc.concentration > spread.concentration,
            "集中度必须能把两种形态分开，否则这张表没有意义"
        );
    }

    /// endpoint 维度：错误集中在某个端点时必须能被区分出来。
    ///
    /// 这个维度在当前生产数据上没有区分力（实测 `trace_attempts.endpoint` 只有
    /// `ide` 与空串两个取值，全部 transient 错误都在 ide 上、lift 恰为 1.00），
    /// 所以行为只能靠构造数据覆盖 —— 这也正是加测试而非依赖线上观察的理由。
    #[test]
    fn crosstab_endpoint_dimension_separates_endpoints() {
        let store = mem_store_with_traces();
        // cli 端点：3 条 transient（少量流量、全是错）
        for i in 0..3 {
            let id = format!("cli{}", i);
            insert_trace(&store, &id, 60, "error", Some("transient"), 1, 100);
            insert_attempt_on(&store, &id, "transient", 1, None, "cli");
        }
        // ide 端点：1 条 transient + 6 条成功
        insert_trace(&store, "ide-err", 60, "error", Some("transient"), 1, 100);
        insert_attempt_on(&store, "ide-err", "transient", 1, None, "ide");
        for i in 0..6 {
            let id = format!("ide-ok{}", i);
            insert_trace(&store, &id, 60, "success", None, 1, 100);
            insert_attempt_on(&store, &id, "success", 1, None, "ide");
        }

        let rows = store.error_crosstab(24, CrosstabDimension::Endpoint);
        let tr = rows
            .iter()
            .find(|r| r.error_type == "transient")
            .expect("应有 transient 行");
        assert_eq!(tr.total, 4);

        let cli = tr.buckets.iter().find(|b| b.key == "cli").unwrap();
        let ide = tr.buckets.iter().find(|b| b.key == "ide").unwrap();
        assert_eq!(cli.traffic, 3, "cli 端点流量只有那 3 条");
        assert_eq!(ide.traffic, 7, "ide 端点流量含成功的 6 条");
        // cli：错误份额 3/4=75%，流量份额 3/10=30% → lift 2.5
        let cli_lift = cli.lift.unwrap();
        assert!(
            (cli_lift - 2.5).abs() < 1e-9,
            "cli lift 应为 2.5，实际 {}",
            cli_lift
        );
        assert!(
            cli_lift > ide.lift.unwrap(),
            "错误集中的端点 lift 必须高于另一端点"
        );
    }

    /// 端点为空串（请求未走到上游、没有端点可记）时不能崩、也不能被当成一个
    /// 正常端点混进排名。实测生产库里这类行有 72 条，全部属于凭据 0。
    #[test]
    fn crosstab_endpoint_handles_empty_endpoint() {
        let store = mem_store_with_traces();
        insert_trace(&store, "noep", 60, "error", Some("unknown"), 0, 100);
        insert_attempt_on(&store, "noep", "unknown", 0, None, "");

        let rows = store.error_crosstab(24, CrosstabDimension::Endpoint);
        let unk = rows.iter().find(|r| r.error_type == "unknown").unwrap();
        assert_eq!(unk.buckets[0].key, "", "空端点保持空串，由前端决定如何展示");
        assert_eq!(unk.total, 1);
    }

    /// lift 存在的理由必须可执行验证：流量分布不均时，裸集中度会把"承载最多的
    /// 对象"误报成"有问题的对象"。这里构造两个凭据 —— A 承载 90% 流量、错误份额
    /// 也正好 90%（相称，不该报警），B 只承载 10% 流量却吃下 50% 的另一类错误
    /// （不成比例，才是真信号）。集中度会给出相反的排序，lift 才纠正过来。
    #[test]
    fn lift_separates_disproportionate_errors_from_high_traffic_ones() {
        let store = mem_store_with_traces();
        // 凭据 1：90 条成功流量 + 9 条 transient
        for i in 0..90 {
            insert_trace(&store, &format!("ok1-{}", i), 60, "success", None, 1, 100);
        }
        for i in 0..9 {
            insert_trace(
                &store,
                &format!("tr1-{}", i),
                60,
                "error",
                Some("transient"),
                1,
                100,
            );
        }
        // 凭据 2：10 条成功流量 + 1 条 transient（与凭据1同比例）
        for i in 0..10 {
            insert_trace(&store, &format!("ok2-{}", i), 60, "success", None, 2, 100);
        }
        insert_trace(&store, "tr2-0", 60, "error", Some("transient"), 2, 100);

        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let tr = rows.iter().find(|r| r.error_type == "transient").unwrap();

        // 集中度 0.9：看着像"高度集中在凭据 1"
        assert!(
            (tr.concentration - 0.9).abs() < 1e-9,
            "集中度应为 9/10 = 0.9"
        );

        // 但 lift ≈ 1：凭据 1 的错误份额(90%)与它的流量份额(99/110≈90%)相称，
        // 集中只是因为它承载得多，不是它有问题
        let b1 = tr.buckets.iter().find(|b| b.key == "1").unwrap();
        let lift1 = b1.lift.expect("有流量应能算 lift");
        assert!(
            (lift1 - 1.0).abs() < 0.05,
            "承载最多的凭据 lift 应≈1（相称），实际 {:.3}",
            lift1
        );
        assert_eq!(b1.traffic, 99, "traffic 须含成功+失败的全部请求");

        // 凭据 2 同比例 → lift 也应 ≈1
        let b2 = tr.buckets.iter().find(|b| b.key == "2").unwrap();
        let lift2 = b2.lift.unwrap();
        assert!(
            (lift2 - 1.0).abs() < 0.05,
            "同比例的凭据 lift 也应≈1，实际 {:.3}",
            lift2
        );
    }

    /// 与上一个测试互补：错误份额明显超出流量份额时 lift 必须显著 > 1。
    /// 这是真正该报警的形态。
    #[test]
    fn lift_flags_low_traffic_object_with_outsized_errors() {
        let store = mem_store_with_traces();
        // 凭据 1：99 条成功，0 错误（承载绝大部分流量但很健康）
        for i in 0..99 {
            insert_trace(&store, &format!("ok-{}", i), 60, "success", None, 1, 100);
        }
        // 凭据 9：1 条成功 + 5 条 network_error（流量极少却错得多）
        insert_trace(&store, "ok9", 60, "success", None, 9, 100);
        for i in 0..5 {
            insert_trace(
                &store,
                &format!("ne9-{}", i),
                60,
                "error",
                Some("network_error"),
                9,
                100,
            );
        }

        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let ne = rows
            .iter()
            .find(|r| r.error_type == "network_error")
            .unwrap();
        let b9 = ne.buckets.iter().find(|b| b.key == "9").unwrap();
        let lift9 = b9.lift.unwrap();

        // 错误份额 5/5 = 100%，流量份额 6/105 ≈ 5.7% → lift ≈ 17.5
        assert!(
            lift9 > 5.0,
            "低流量高错误的对象 lift 必须显著>1 才能被发现，实际 {:.2}",
            lift9
        );
    }

    /// 桶数超过 CROSSTAB_TOP_BUCKETS 时，total 与 distinct_keys 必须仍按全量计，
    /// 只截断 buckets 列表 —— 否则集中度的分母会被截断后的和污染，
    /// 让"散开"的错误看起来像"集中"。
    #[test]
    fn crosstab_truncates_buckets_without_corrupting_totals() {
        let store = mem_store_with_traces();
        // 10 个不同凭据各 1 条，超过 CROSSTAB_TOP_BUCKETS(6)
        for id in 1..=10u64 {
            insert_trace(
                &store,
                &format!("t{}", id),
                60,
                "error",
                Some("transient"),
                id,
                500,
            );
        }
        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let row = rows.iter().find(|r| r.error_type == "transient").unwrap();

        assert_eq!(row.buckets.len(), CROSSTAB_TOP_BUCKETS, "桶列表应被截断");
        assert_eq!(row.total, 10, "total 必须是全量，不能只算被保留的桶");
        assert_eq!(row.distinct_keys, 10, "distinct_keys 必须是全量");
        assert!(
            (row.concentration - 0.1).abs() < 1e-9,
            "集中度分母须用全量 total(10)，否则 1/6 会被误报成集中"
        );
    }

    #[test]
    fn percentiles_handles_empty_and_single() {
        assert!(percentiles(Vec::new()).is_none(), "无样本应返回 None");
        let one = percentiles(vec![5_000]).unwrap();
        assert_eq!((one.p50, one.p95, one.p99, one.n), (5_000, 5_000, 5_000, 1));
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

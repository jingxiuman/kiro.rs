//! 余额快照存储（DuckDB）
//!
//! 余额后台刷新（默认 5 分钟一轮）时把每个凭据的当前余额落一行，形成时序。
//! 与 `kiro_balance_cache.json` 的分工：那个文件只存"当前值"供服务重启后立即可用；
//! 本表存"历史值"，用来回答 JSON 快照答不了的问题——某账号是持续烧还是突然掉、
//! 按当前速率还能撑几天。
//!
//! 桶列 `hour_ts` 与 usage_records 同样在 Rust 侧预计算（DuckDB 时区运算依赖 icu
//! 扩展，musl 静态二进制加载不了，见 [`super::duck`] 模块注释）。

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Local, TimeZone, Timelike, Utc};
use parking_lot::Mutex;
use serde::Serialize;

/// 单条余额快照（写入用）
#[derive(Debug, Clone)]
pub struct BalanceSnapshot {
    pub credential_id: u64,
    pub subscription_title: String,
    pub current_usage: f64,
    pub usage_limit: f64,
    pub remaining: f64,
    pub usage_percentage: f64,
    /// 下次账单重置时间（Unix 秒）；None = 上游未提供
    pub next_reset_at: Option<i64>,
}

/// 余额历史中的一个时间点（导出给前端）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BalancePoint {
    /// 桶起始时间（RFC3339）
    pub ts: String,
    pub credential_id: u64,
    /// 桶内均值——同一小时可能有多次刷新（默认 5 分钟一次共 12 条）
    pub current_usage: f64,
    pub remaining: f64,
    pub usage_percentage: f64,
    pub usage_limit: f64,
}

/// 某账号的余额消耗速率与耗尽预测
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BurnRate {
    pub credential_id: u64,
    /// 窗口内每小时平均消耗额度
    pub per_hour: f64,
    /// 当前剩余额度
    pub remaining: f64,
    /// 按当前速率预计耗尽还需多少小时；None = 速率非正（没在烧）或数据不足
    pub hours_to_exhaust: Option<f64>,
    /// 本次估算基于多少个快照点——太少时前端应标注「样本不足」
    pub sample_points: usize,
}

/// 余额快照存储
pub struct BalanceStore {
    conn: Mutex<duckdb::Connection>,
    /// 保留天数。固定值——余额历史没有「按需调长调短」的需求，
    /// 需要时再接进日志治理配置（那时才值得引入运行时可变）。
    retention_days: i64,
}

pub type SharedBalanceStore = Arc<BalanceStore>;

/// 本地小时桶起始（Unix 秒）
fn hour_bucket_of(ts: i64) -> i64 {
    let local = DateTime::<Utc>::from_timestamp(ts, 0)
        .unwrap_or_else(Utc::now)
        .with_timezone(&Local);
    Local
        .with_ymd_and_hms(
            local.year(),
            local.month(),
            local.day(),
            local.hour(),
            0,
            0,
        )
        .single()
        .map(|d| d.timestamp())
        .unwrap_or(0)
}

fn ts_to_rfc3339(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

impl BalanceStore {
    pub fn open(db_path: &Path, retention_days: i64) -> duckdb::Result<Self> {
        Ok(Self {
            conn: Mutex::new(super::duck::open_shared(db_path)?),
            retention_days: retention_days.max(1),
        })
    }

    /// 批量写入一轮刷新的快照。失败仅 warn——余额历史是观测数据，
    /// 写不进去不该影响余额刷新本身。
    pub fn record_batch(&self, snapshots: &[BalanceSnapshot]) {
        if snapshots.is_empty() {
            return;
        }
        let now = Utc::now().timestamp();
        let hour_ts = hour_bucket_of(now);
        let conn = self.conn.lock();
        for s in snapshots {
            let r = conn.execute(
                "INSERT INTO balance_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                duckdb::params![
                    now,
                    hour_ts,
                    s.credential_id as i64,
                    s.subscription_title,
                    s.current_usage,
                    s.usage_limit,
                    s.remaining,
                    s.usage_percentage,
                    s.next_reset_at,
                ],
            );
            if let Err(e) = r {
                tracing::warn!("余额快照写入失败 (凭据 #{}): {}", s.credential_id, e);
            }
        }
    }

    /// 清理超过保留期的快照
    pub fn cleanup(&self) {
        let cutoff = Utc::now().timestamp() - self.retention_days * 24 * 3600;
        let conn = self.conn.lock();
        match conn.execute("DELETE FROM balance_snapshots WHERE ts_epoch < ?", [cutoff]) {
            Ok(n) if n > 0 => tracing::info!("已清理过期余额快照: {} 行", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("清理余额快照失败: {}", e),
        }
    }

    /// 查询余额历史（按小时桶聚合取均值）
    ///
    /// `credential_id = None` 时返回全部账号的点，前端按 credentialId 分组画多条线。
    pub fn query_history(&self, start_ts: i64, credential_id: Option<u64>) -> Vec<BalancePoint> {
        let mut sql = String::from(
            "SELECT hour_ts, credential_id, \
                    avg(current_usage), avg(remaining), avg(usage_percentage), avg(usage_limit) \
             FROM balance_snapshots WHERE ts_epoch >= ?",
        );
        let mut params: Vec<i64> = vec![start_ts];
        if let Some(id) = credential_id {
            sql.push_str(" AND credential_id = ?");
            params.push(id as i64);
        }
        sql.push_str(" GROUP BY hour_ts, credential_id ORDER BY hour_ts, credential_id");

        let conn = self.conn.lock();
        let run = || -> duckdb::Result<Vec<BalancePoint>> {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(duckdb::params_from_iter(params.iter()), |r| {
                Ok(BalancePoint {
                    ts: ts_to_rfc3339(r.get::<_, i64>(0)?),
                    credential_id: r.get::<_, i64>(1)? as u64,
                    current_usage: r.get::<_, f64>(2)?,
                    remaining: r.get::<_, f64>(3)?,
                    usage_percentage: r.get::<_, f64>(4)?,
                    usage_limit: r.get::<_, f64>(5)?,
                })
            })?;
            rows.collect()
        };
        run().unwrap_or_else(|e| {
            tracing::warn!("余额历史查询失败: {}", e);
            Vec::new()
        })
    }

    /// 计算各账号的消耗速率与耗尽预测
    ///
    /// 速率取窗口内首末两点的差值除以时间跨度。为什么不用线性回归：账单周期
    /// 重置会让 remaining 突然跳回满额，回归会被这个断点带偏；首末差值配合
    /// 下面的重置检测更稳。
    ///
    /// 重置检测：窗口内若出现 remaining 上升（后一点比前一点多），说明跨了账单
    /// 重置，只取最后一次重置之后的区间算速率——否则会算出「负消耗」。
    pub fn burn_rates(&self, start_ts: i64) -> Vec<BurnRate> {
        let conn = self.conn.lock();
        let sql = "SELECT credential_id, ts_epoch, remaining \
                   FROM balance_snapshots WHERE ts_epoch >= ? \
                   ORDER BY credential_id, ts_epoch";
        let rows: Vec<(u64, i64, f64)> = (|| -> duckdb::Result<Vec<(u64, i64, f64)>> {
            let mut stmt = conn.prepare(sql)?;
            let it = stmt.query_map([start_ts], |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)?, r.get::<_, f64>(2)?))
            })?;
            it.collect()
        })()
        .unwrap_or_else(|e| {
            tracing::warn!("余额速率查询失败: {}", e);
            Vec::new()
        });
        drop(conn);

        let mut out: Vec<BurnRate> = Vec::new();
        let mut idx = 0usize;
        while idx < rows.len() {
            let cred = rows[idx].0;
            let end = rows[idx..].partition_point(|r| r.0 == cred) + idx;
            let series = &rows[idx..end];
            idx = end;

            // 找最后一次重置点（remaining 相比前一点上升 = 账单周期翻新）
            let mut seg_start = 0usize;
            for i in 1..series.len() {
                if series[i].2 > series[i - 1].2 + f64::EPSILON {
                    seg_start = i;
                }
            }
            let seg = &series[seg_start..];
            let last_remaining = seg.last().map(|r| r.2).unwrap_or(0.0);
            let (per_hour, sample_points) = if seg.len() < 2 {
                (0.0, seg.len())
            } else {
                let (t0, r0) = (seg[0].1, seg[0].2);
                let (t1, r1) = (seg[seg.len() - 1].1, seg[seg.len() - 1].2);
                let hours = (t1 - t0) as f64 / 3600.0;
                if hours <= 0.0 {
                    (0.0, seg.len())
                } else {
                    (((r0 - r1) / hours).max(0.0), seg.len())
                }
            };
            let hours_to_exhaust = if per_hour > 0.0 && last_remaining > 0.0 {
                Some(last_remaining / per_hour)
            } else {
                None
            };
            out.push(BurnRate {
                credential_id: cred,
                per_hour,
                remaining: last_remaining,
                hours_to_exhaust,
                sample_points,
            });
        }
        out.sort_by(|a, b| b.per_hour.total_cmp(&a.per_hour).then(a.credential_id.cmp(&b.credential_id)));
        out
    }
}

/// 便捷构造：窗口起点 = 现在往前 N 小时
pub fn window_start(hours: i64) -> i64 {
    Utc::now().timestamp() - hours.max(1) * 3600
}

/// 保留天数默认值：余额是低频数据（每 5 分钟 1 行/账号），留 90 天也才几十万行
pub const DEFAULT_BALANCE_RETENTION_DAYS: i64 = 90;

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_store() -> BalanceStore {
        let dir = std::env::temp_dir().join(format!(
            "balduck-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        BalanceStore::open(&dir.join("kiro.duckdb"), 90).unwrap()
    }

    fn snap(cred: u64, usage: f64, limit: f64) -> BalanceSnapshot {
        BalanceSnapshot {
            credential_id: cred,
            subscription_title: "KIRO POWER".into(),
            current_usage: usage,
            usage_limit: limit,
            remaining: limit - usage,
            usage_percentage: usage / limit * 100.0,
            next_reset_at: Some(1788220800),
        }
    }

    /// 写入的快照必须能按账号读回，且同一小时多次刷新聚合为一个点
    #[test]
    fn record_and_query_history() {
        let s = mk_store();
        s.record_batch(&[snap(1, 100.0, 10000.0), snap(2, 500.0, 10000.0)]);
        s.record_batch(&[snap(1, 200.0, 10000.0), snap(2, 600.0, 10000.0)]);

        let all = s.query_history(window_start(24), None);
        // 两个账号各一个小时桶（同一小时内的两次刷新被 avg 聚合）
        assert_eq!(all.len(), 2, "同小时多次刷新应聚合为每账号一点");
        let c1 = all.iter().find(|p| p.credential_id == 1).unwrap();
        assert!((c1.current_usage - 150.0).abs() < 1e-9, "应取均值 (100+200)/2");
        assert!((c1.remaining - 9850.0).abs() < 1e-9);

        // 按账号过滤
        let only2 = s.query_history(window_start(24), Some(2));
        assert_eq!(only2.len(), 1);
        assert_eq!(only2[0].credential_id, 2);
    }

    /// 窗口外的数据不能进结果
    #[test]
    fn history_respects_window() {
        let s = mk_store();
        s.record_batch(&[snap(1, 100.0, 10000.0)]);
        // 未来时间起点 → 应查不到刚写的点
        let future = Utc::now().timestamp() + 3600;
        assert!(s.query_history(future, None).is_empty());
    }

    /// 消耗速率：直接构造带时间戳的行，验证 per_hour 与耗尽预测
    #[test]
    fn burn_rate_computes_per_hour_and_exhaust() {
        let s = mk_store();
        let now = Utc::now().timestamp();
        {
            let conn = s.conn.lock();
            // 4 小时内从 剩余 1000 烧到 600 → 100/小时；剩 600 → 还能撑 6 小时
            for (i, remaining) in [1000.0, 900.0, 700.0, 600.0].iter().enumerate() {
                let ts = now - (3 - i as i64) * 3600;
                conn.execute(
                    "INSERT INTO balance_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        ts, hour_bucket_of(ts), 1i64, "KIRO POWER",
                        1000.0 - remaining, 1000.0, *remaining, 0.0, None::<i64>
                    ],
                ).unwrap();
            }
        }
        let rates = s.burn_rates(window_start(24));
        assert_eq!(rates.len(), 1);
        let r = &rates[0];
        assert!((r.per_hour - 133.33).abs() < 1.0, "3 小时烧 400 ≈ 133/h, 实际 {}", r.per_hour);
        assert!((r.remaining - 600.0).abs() < 1e-9);
        let h = r.hours_to_exhaust.unwrap();
        assert!((h - 4.5).abs() < 0.2, "600 / 133.3 ≈ 4.5h, 实际 {}", h);
        assert_eq!(r.sample_points, 4);
    }

    /// 账单重置（remaining 跳回满额）后，速率只算重置之后的区间——
    /// 否则首末差值会得出负消耗，把「刚重置完」误报成「没在烧」
    #[test]
    fn burn_rate_ignores_pre_reset_segment() {
        let s = mk_store();
        let now = Utc::now().timestamp();
        {
            let conn = s.conn.lock();
            // 前两点烧到很低，第三点重置回满额，之后继续烧
            for (i, remaining) in [200.0, 100.0, 1000.0, 800.0].iter().enumerate() {
                let ts = now - (3 - i as i64) * 3600;
                conn.execute(
                    "INSERT INTO balance_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        ts, hour_bucket_of(ts), 1i64, "KIRO POWER",
                        0.0, 1000.0, *remaining, 0.0, None::<i64>
                    ],
                ).unwrap();
            }
        }
        let rates = s.burn_rates(window_start(24));
        let r = &rates[0];
        // 只看重置后的 1000→800（1 小时 200）
        assert!((r.per_hour - 200.0).abs() < 1.0, "应只算重置后区间, 实际 {}", r.per_hour);
        assert_eq!(r.sample_points, 2, "重置后只有 2 个点");
    }

    /// 没在消耗的账号不该给出耗尽预测（否则前端会显示「还剩 inf 小时」）
    #[test]
    fn burn_rate_no_prediction_when_idle() {
        let s = mk_store();
        s.record_batch(&[snap(1, 0.0, 10000.0)]);
        s.record_batch(&[snap(1, 0.0, 10000.0)]);
        let rates = s.burn_rates(window_start(24));
        assert_eq!(rates[0].per_hour, 0.0);
        assert!(rates[0].hours_to_exhaust.is_none(), "零消耗不该预测耗尽");
    }

    #[test]
    fn cleanup_removes_old_snapshots() {
        let s = mk_store();
        let now = Utc::now().timestamp();
        {
            let conn = s.conn.lock();
            for days_ago in [100i64, 1] {
                let ts = now - days_ago * 24 * 3600;
                conn.execute(
                    "INSERT INTO balance_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    duckdb::params![
                        ts, hour_bucket_of(ts), 1i64, "K", 0.0, 100.0, 100.0, 0.0, None::<i64>
                    ],
                ).unwrap();
            }
        }
        s.cleanup();
        let left = s.query_history(now - 200 * 24 * 3600, None);
        assert_eq!(left.len(), 1, "只应剩 90 天内的那条");
    }
}

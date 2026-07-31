//! 请求用量记录 + 时序聚合
//!
//! 记录每次 `/v1/messages` 请求的 token 消耗与命中信息：
//! - 落盘：`usage_log.YYYY-MM-DD.jsonl`，每行一条 [`UsageRecord`]，按本地日期滚动
//! - 内存：[`UsageAggregator`] 维护近 31 天的小时桶 + 近 31 天的天桶，按需查询
//!
//! 启动时扫描历史 JSONL 文件重建聚合，保证重启后趋势图不丢数据。

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, Local, NaiveDate, TimeZone, Timelike, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// JSONL 文件保留天数
const RETENTION_DAYS: i64 = 31;
/// 小时桶数量（31 天）
const HOUR_BUCKETS: usize = 24 * 31;
/// 天桶数量（31 天）
const DAY_BUCKETS: usize = 31;

/// 单次请求的用量记录（与 JSONL 一行一一对应）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageRecord {
    /// 请求结束时间（RFC3339）
    pub ts: String,
    /// 客户端 Key id；0 表示用 master apiKey 调用
    pub key_id: u64,
    /// 实际命中的上游凭据 id；0 表示请求未走到上游
    pub credential_id: u64,
    /// 模型名（请求里声明的，可能含 -thinking 后缀）
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    /// 上游 meteringEvent.usage 上报的 credit 计费量（浮点）
    #[serde(default)]
    pub credits: f64,
    /// 端到端耗时（毫秒）
    #[serde(default)]
    pub duration_ms: u64,
    /// "success" 或 "error"
    pub status: String,
}

/// 按天 rotate 的 JSONL writer
pub struct UsageRecorder {
    inner: Mutex<RecorderState>,
    dir: PathBuf,
    /// 保留天数（运行时可改），cleanup_old_logs 时读取。
    retention_days: std::sync::atomic::AtomicI64,
}

struct RecorderState {
    /// 当前打开的 writer 与对应日期
    current_date: Option<NaiveDate>,
    writer: Option<BufWriter<File>>,
}

impl UsageRecorder {
    /// 指定初始保留天数构造
    pub fn with_retention(dir: PathBuf, retention_days: i64) -> Self {
        // 兜底：调用方传入空路径时归一为 "."，避免 join 出无目录前缀的路径导致写入 CWD
        let dir = if dir.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            dir
        };
        if !dir.exists()
            && let Err(e) = std::fs::create_dir_all(&dir) {
                tracing::warn!("创建 usage_log 目录失败 {}: {}", dir.display(), e);
            }
        Self {
            inner: Mutex::new(RecorderState {
                current_date: None,
                writer: None,
            }),
            dir,
            retention_days: std::sync::atomic::AtomicI64::new(retention_days.max(1)),
        }
    }

    fn log_path(&self, date: NaiveDate) -> PathBuf {
        self.dir
            .join(format!("usage_log.{}.jsonl", date.format("%Y-%m-%d")))
    }

    /// 同步写入一条记录。失败仅 warn，不阻塞请求。
    pub fn record(&self, rec: &UsageRecord) {
        let line = match serde_json::to_string(rec) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("usage_log 序列化失败: {}", e);
                return;
            }
        };
        let today = Local::now().date_naive();
        let mut state = self.inner.lock();
        if state.current_date != Some(today) || state.writer.is_none() {
            // 切换到当日文件
            let path = self.log_path(today);
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => {
                    state.writer = Some(BufWriter::new(file));
                    state.current_date = Some(today);
                }
                Err(e) => {
                    tracing::warn!("打开 usage_log {} 失败: {}", path.display(), e);
                    return;
                }
            }
        }
        if let Some(w) = state.writer.as_mut() {
            if let Err(e) = writeln!(w, "{}", line) {
                tracing::warn!("写入 usage_log 失败: {}", e);
                return;
            }
            // 立即 flush，保证崩溃时不丢失最近一条
            let _ = w.flush();
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

    /// 清理超过保留期的旧文件
    pub fn cleanup_old_logs(&self) {
        let cutoff = Local::now().date_naive() - Duration::days(self.retention_days());
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(it) => it,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if let Some(date) = parse_usage_log_filename(&name)
                && date < cutoff {
                    let _ = std::fs::remove_file(entry.path());
                    tracing::info!("已清理过期 usage_log: {}", name);
                }
        }
    }
}

fn parse_usage_log_filename(name: &str) -> Option<NaiveDate> {
    // 形如 usage_log.2026-05-22.jsonl
    let body = name.strip_prefix("usage_log.")?.strip_suffix(".jsonl")?;
    NaiveDate::parse_from_str(body, "%Y-%m-%d").ok()
}

/// 单个时间桶的统计
#[derive(Debug, Default, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketStats {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
    pub calls: u64,
    pub errors: u64,
    pub credits: f64,
}

impl BucketStats {
    fn add(&mut self, rec: &UsageRecord) {
        self.input_tokens += rec.input_tokens;
        self.output_tokens += rec.output_tokens;
        self.cache_creation_tokens += rec.cache_creation_tokens;
        self.cache_read_tokens += rec.cache_read_tokens;
        self.credits += rec.credits;
        self.calls += 1;
        if rec.status != "success" {
            self.errors += 1;
        }
    }

    /// 把另一个 stats 累加到自己上（用于 group 过滤后重新汇总）
    fn add_stats(&mut self, other: &BucketStats) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_creation_tokens += other.cache_creation_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.credits += other.credits;
        self.calls += other.calls;
        self.errors += other.errors;
    }
}

/// 单个时间桶含分组数据
#[derive(Debug, Default, Clone)]
struct BucketEntry {
    /// 桶起始时间戳（小时桶为整点 Unix 秒；天桶为本地 0 点 Unix 秒）
    ts: i64,
    overall: BucketStats,
    by_key: HashMap<u64, BucketStats>,
    by_model: HashMap<String, BucketStats>,
    by_credential: HashMap<u64, BucketStats>,
    by_key_model: HashMap<u64, HashMap<String, BucketStats>>,
    by_key_credential: HashMap<u64, HashMap<u64, BucketStats>>,
}

/// 时间维度聚合器
pub struct UsageAggregator {
    inner: parking_lot::RwLock<AggregatorInner>,
}

struct AggregatorInner {
    /// 小时桶（环形数组按桶起始时间索引），最近 31 天
    hour_buckets: Vec<BucketEntry>,
    /// 天桶（按本地日期），最近 31 天
    day_buckets: Vec<BucketEntry>,
}

/// 预设聚合查询时间范围
#[derive(Debug, Clone, Copy)]
pub enum Range {
    Last24h,
    Last7d,
    Last30d,
}

impl Range {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "24h" => Some(Range::Last24h),
            "7d" => Some(Range::Last7d),
            "30d" => Some(Range::Last30d),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsGranularity {
    Hour,
    Day,
}

impl StatsGranularity {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "hour" => Some(StatsGranularity::Hour),
            "day" => Some(StatsGranularity::Day),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StatsQueryWindow {
    pub start_ts: i64,
    pub end_ts: i64,
    pub granularity: StatsGranularity,
}

impl StatsQueryWindow {
    pub fn preset(range: Range, granularity: StatsGranularity) -> Self {
        let now = Utc::now().timestamp();
        let start_ts = match range {
            Range::Last24h => now - 24 * 3600,
            Range::Last7d => now - 7 * 24 * 3600,
            Range::Last30d => now - 30 * 24 * 3600,
        };
        Self {
            start_ts,
            end_ts: now,
            granularity,
        }
    }
}

/// 时序点（导出给前端）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeSeriesPoint {
    /// 桶起始时间（RFC3339）
    pub ts: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
    pub calls: u64,
    pub errors: u64,
    pub credits: f64,
}

/// 模型分布
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDistribution {
    pub model: String,
    pub calls: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
}

/// 上游凭据分布
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialDistribution {
    pub credential_id: u64,
    pub calls: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
    pub errors: u64,
}

/// 各账号积分消耗时序中，单个账号的系列元信息（图例 + 排序依据）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreditSeriesMeta {
    pub credential_id: u64,
    /// 窗口内该账号的积分合计，用于挑 Top N 与图例排序
    pub total_credits: f64,
}

/// 各账号积分消耗时序
///
/// 为什么不复用 `TimeSeriesPoint`：那个是「一个时间点一行、字段固定」的结构，
/// 装不下「账号数不定」的第二维度。这里把维度显式拆成 series（有哪些账号）
/// 与 points（每桶各账号多少），前端据此 pivot 成宽表。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreditsByCredential {
    /// 入选的账号系列，按窗口内总积分降序
    pub series: Vec<CreditSeriesMeta>,
    pub points: Vec<CreditPoint>,
    /// 窗口内有积分消耗的账号总数。> series.len() 说明被 Top N 截断了，
    /// 前端必须如实标注，否则「图上就这几个账号」是错的。
    pub total_credentials: usize,
}

/// 单个时间桶内各账号的积分
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreditPoint {
    pub ts: String,
    /// credential_id（转成字符串做 JSON key）→ 该桶该账号的积分。
    /// 稀疏存储：没有消耗的账号不出现，避免 N 账号 × M 桶 全量补零。
    pub credits: HashMap<String, f64>,
}

/// 概览：今日 + 累计
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewStats {
    /// 今日（本地 0 点起）的调用次数
    pub today_calls: u64,
    pub today_input_tokens: u64,
    pub today_output_tokens: u64,
    pub today_errors: u64,
    pub today_credits: f64,
    /// 最近 7 天累计
    pub week_calls: u64,
    pub week_input_tokens: u64,
    pub week_output_tokens: u64,
    pub week_credits: f64,
}

impl UsageAggregator {
    pub fn new() -> Self {
        Self {
            inner: parking_lot::RwLock::new(AggregatorInner {
                hour_buckets: Vec::new(),
                day_buckets: Vec::new(),
            }),
        }
    }

    /// 启动时从历史 JSONL 重建聚合
    pub fn rebuild_from_logs(&self, dir: &Path) {
        // 兜底：空路径归一为 "."，否则 read_dir("") 会失败导致重建为 0
        let dir_buf;
        let dir = if dir.as_os_str().is_empty() {
            dir_buf = PathBuf::from(".");
            dir_buf.as_path()
        } else {
            dir
        };
        let entries = match std::fs::read_dir(dir) {
            Ok(it) => it,
            Err(e) => {
                tracing::warn!("读取 usage_log 目录失败 {}: {}", dir.display(), e);
                return;
            }
        };
        let cutoff = Local::now().date_naive() - Duration::days(RETENTION_DAYS);
        let mut count = 0u64;
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            let Some(date) = parse_usage_log_filename(&name) else {
                continue;
            };
            if date < cutoff {
                continue;
            }
            let file = match File::open(entry.path()) {
                Ok(f) => f,
                Err(_) => continue,
            };
            for line in BufReader::new(file).lines().map_while(Result::ok) {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(rec) = serde_json::from_str::<UsageRecord>(&line) {
                    self.ingest(&rec);
                    count += 1;
                }
            }
        }
        tracing::info!(
            "UsageAggregator 重建完成：从 {} 装载 {} 条历史记录",
            dir.display(),
            count
        );
    }

    /// 接收一条记录并落入对应桶
    pub fn ingest(&self, rec: &UsageRecord) {
        let dt: DateTime<Utc> = match DateTime::parse_from_rfc3339(&rec.ts) {
            Ok(d) => d.with_timezone(&Utc),
            Err(_) => Utc::now(),
        };
        let local = dt.with_timezone(&Local);

        // 小时桶起始：当地小时整点 → 转回 UTC unix 秒
        let hour_start = Local
            .with_ymd_and_hms(local.year(), local.month(), local.day(), local.hour(), 0, 0)
            .single();
        // 天桶起始：本地 0 点 → 转回 UTC unix 秒
        let day_start = Local
            .with_ymd_and_hms(local.year(), local.month(), local.day(), 0, 0, 0)
            .single();

        let hour_ts = hour_start.map(|d| d.timestamp()).unwrap_or(0);
        let day_ts = day_start.map(|d| d.timestamp()).unwrap_or(0);

        let mut inner = self.inner.write();

        upsert_bucket(&mut inner.hour_buckets, hour_ts, rec, HOUR_BUCKETS);
        upsert_bucket(&mut inner.day_buckets, day_ts, rec, DAY_BUCKETS);
    }

    /// 时序数据查询
    pub fn query_timeseries(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
        cred_filter: Option<&std::collections::HashSet<u64>>,
    ) -> Vec<TimeSeriesPoint> {
        let inner = self.inner.read();
        let buckets = select_buckets(&inner, window.granularity);

        let mut points: Vec<TimeSeriesPoint> = buckets
            .iter()
            .filter(|b| bucket_in_window(b, window))
            .filter(|b| bucket_matches_key(b, key_id))
            .map(|b| {
                // 不带 group 过滤 → 走老逻辑（更快，命中预聚合 by_key/overall 桶）
                let stats = match cred_filter {
                    None => stats_for_key(b, key_id),
                    Some(allow) => credential_group_for_key(b, key_id)
                        .map(|group| {
                            let mut s = BucketStats::default();
                            for (cid, cs) in group {
                                if allow.contains(cid) {
                                    s.add_stats(cs);
                                }
                            }
                            s
                        })
                        .unwrap_or_default(),
                };
                TimeSeriesPoint {
                    ts: ts_to_rfc3339(b.ts),
                    input_tokens: stats.input_tokens,
                    output_tokens: stats.output_tokens,
                    cache_creation_tokens: stats.cache_creation_tokens,
                    cache_read_tokens: stats.cache_read_tokens,
                    calls: stats.calls,
                    errors: stats.errors,
                    credits: stats.credits,
                }
            })
            .collect();
        points.sort_by_key(|p| p.ts.clone());
        points
    }

    /// 模型分布
    pub fn query_by_model(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
    ) -> Vec<ModelDistribution> {
        let inner = self.inner.read();
        let buckets = select_buckets(&inner, window.granularity);
        let mut acc: HashMap<String, BucketStats> = HashMap::new();
        for b in buckets.iter().filter(|b| bucket_in_window(b, window)) {
            let Some(group) = model_group_for_key(b, key_id) else {
                continue;
            };
            for (model, stats) in group {
                let entry = acc.entry(model.clone()).or_default();
                entry.input_tokens += stats.input_tokens;
                entry.output_tokens += stats.output_tokens;
                entry.cache_creation_tokens += stats.cache_creation_tokens;
                entry.cache_read_tokens += stats.cache_read_tokens;
                entry.calls += stats.calls;
            }
        }
        let mut out: Vec<ModelDistribution> = acc
            .into_iter()
            .map(|(model, stats)| ModelDistribution {
                model,
                calls: stats.calls,
                input_tokens: stats.input_tokens,
                output_tokens: stats.output_tokens,
                cache_creation_tokens: stats.cache_creation_tokens,
                cache_read_tokens: stats.cache_read_tokens,
            })
            .collect();
        out.sort_by_key(|row| std::cmp::Reverse(row.calls));
        out
    }

    /// 上游凭据分布
    pub fn query_by_credential(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
        cred_filter: Option<&std::collections::HashSet<u64>>,
    ) -> Vec<CredentialDistribution> {
        let inner = self.inner.read();
        let buckets = select_buckets(&inner, window.granularity);
        let mut acc: HashMap<u64, BucketStats> = HashMap::new();
        for b in buckets.iter().filter(|b| bucket_in_window(b, window)) {
            let Some(group) = credential_group_for_key(b, key_id) else {
                continue;
            };
            for (id, stats) in group {
                if let Some(allow) = cred_filter
                    && !allow.contains(id) {
                        continue;
                    }
                let entry = acc.entry(*id).or_default();
                entry.input_tokens += stats.input_tokens;
                entry.output_tokens += stats.output_tokens;
                entry.cache_creation_tokens += stats.cache_creation_tokens;
                entry.cache_read_tokens += stats.cache_read_tokens;
                entry.calls += stats.calls;
                entry.errors += stats.errors;
            }
        }
        let mut out: Vec<CredentialDistribution> = acc
            .into_iter()
            .map(|(id, stats)| CredentialDistribution {
                credential_id: id,
                calls: stats.calls,
                input_tokens: stats.input_tokens,
                output_tokens: stats.output_tokens,
                cache_creation_tokens: stats.cache_creation_tokens,
                cache_read_tokens: stats.cache_read_tokens,
                errors: stats.errors,
            })
            .collect();
        out.sort_by_key(|row| std::cmp::Reverse(row.calls));
        out
    }

    /// 各账号积分消耗时序（Top `limit` 个账号，按窗口内总积分降序）
    ///
    /// 与 [`Self::query_by_credential`] 的分工：那个把整个窗口压成每账号一个总数，
    /// 这个保留时间维度，用来看「某账号是持续烧还是某一小时突刺」。
    ///
    /// 桶保留策略与 [`Self::query_timeseries`] 一致：只产出实际有记录的桶
    /// （`upsert_bucket` 仅在收到记录时物化桶），完全无流量的时段不会出现在结果里。
    /// 前端 pivot 时对入选账号缺失的桶补 0——积分是流量型指标，没消耗就是 0，
    /// 不是「无数据」。
    pub fn query_credits_by_credential(
        &self,
        window: StatsQueryWindow,
        key_id: Option<u64>,
        cred_filter: Option<&std::collections::HashSet<u64>>,
        limit: usize,
    ) -> CreditsByCredential {
        let inner = self.inner.read();
        let buckets = select_buckets(&inner, window.granularity);

        // 第一遍：窗口内每账号的积分合计，用来定 Top N
        let mut totals: HashMap<u64, f64> = HashMap::new();
        for b in buckets.iter().filter(|b| bucket_in_window(b, window)) {
            let Some(group) = credential_group_for_key(b, key_id) else {
                continue;
            };
            for (id, stats) in group {
                if let Some(allow) = cred_filter
                    && !allow.contains(id)
                {
                    continue;
                }
                if stats.credits > 0.0 {
                    *totals.entry(*id).or_default() += stats.credits;
                }
            }
        }
        let total_credentials = totals.len();

        let mut ranked: Vec<(u64, f64)> = totals.into_iter().collect();
        // 积分降序；并列时按 id 升序，保证同样数据每次返回同样顺序（否则图例会闪）
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        ranked.truncate(limit);
        let selected: std::collections::HashSet<u64> = ranked.iter().map(|(id, _)| *id).collect();

        // 第二遍：只为入选账号铺时序
        let mut points: Vec<CreditPoint> = buckets
            .iter()
            .filter(|b| bucket_in_window(b, window))
            .map(|b| {
                let mut credits: HashMap<String, f64> = HashMap::new();
                if let Some(group) = credential_group_for_key(b, key_id) {
                    for (id, stats) in group {
                        if selected.contains(id) && stats.credits > 0.0 {
                            credits.insert(id.to_string(), stats.credits);
                        }
                    }
                }
                CreditPoint {
                    ts: ts_to_rfc3339(b.ts),
                    credits,
                }
            })
            .collect();
        points.sort_by(|a, b| a.ts.cmp(&b.ts));

        CreditsByCredential {
            series: ranked
                .into_iter()
                .map(|(credential_id, total_credits)| CreditSeriesMeta {
                    credential_id,
                    total_credits,
                })
                .collect(),
            points,
            total_credentials,
        }
    }

    /// 概览（今日 + 最近 7 天）
    pub fn overview(&self) -> OverviewStats {
        let inner = self.inner.read();
        let today_start = Local
            .with_ymd_and_hms(
                Local::now().year(),
                Local::now().month(),
                Local::now().day(),
                0,
                0,
                0,
            )
            .single()
            .map(|d| d.timestamp())
            .unwrap_or(0);

        let mut today = BucketStats::default();
        for b in inner.hour_buckets.iter().filter(|b| b.ts >= today_start) {
            today.input_tokens += b.overall.input_tokens;
            today.output_tokens += b.overall.output_tokens;
            today.calls += b.overall.calls;
            today.errors += b.overall.errors;
            today.credits += b.overall.credits;
        }

        let week_cutoff = Utc::now().timestamp() - 7 * 24 * 3600;
        let mut week = BucketStats::default();
        for b in inner.hour_buckets.iter().filter(|b| b.ts >= week_cutoff) {
            week.input_tokens += b.overall.input_tokens;
            week.output_tokens += b.overall.output_tokens;
            week.calls += b.overall.calls;
            week.credits += b.overall.credits;
        }

        OverviewStats {
            today_calls: today.calls,
            today_input_tokens: today.input_tokens,
            today_output_tokens: today.output_tokens,
            today_errors: today.errors,
            today_credits: today.credits,
            week_calls: week.calls,
            week_input_tokens: week.input_tokens,
            week_output_tokens: week.output_tokens,
            week_credits: week.credits,
        }
    }
}

impl Default for UsageAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// 把记录写入对应桶；不存在则插入并按时间排序，超过容量时移除最旧的
fn upsert_bucket(buckets: &mut Vec<BucketEntry>, ts: i64, rec: &UsageRecord, max: usize) {
    if let Some(b) = buckets.iter_mut().find(|b| b.ts == ts) {
        add_record_to_bucket(b, rec);
        return;
    }
    let mut entry = BucketEntry {
        ts,
        ..Default::default()
    };
    add_record_to_bucket(&mut entry, rec);
    buckets.push(entry);
    buckets.sort_by_key(|b| b.ts);
    while buckets.len() > max {
        buckets.remove(0);
    }
}

fn add_record_to_bucket(bucket: &mut BucketEntry, rec: &UsageRecord) {
    bucket.overall.add(rec);
    bucket.by_key.entry(rec.key_id).or_default().add(rec);
    bucket
        .by_model
        .entry(rec.model.clone())
        .or_default()
        .add(rec);
    bucket
        .by_key_model
        .entry(rec.key_id)
        .or_default()
        .entry(rec.model.clone())
        .or_default()
        .add(rec);
    if rec.credential_id == 0 {
        return;
    }
    bucket
        .by_credential
        .entry(rec.credential_id)
        .or_default()
        .add(rec);
    bucket
        .by_key_credential
        .entry(rec.key_id)
        .or_default()
        .entry(rec.credential_id)
        .or_default()
        .add(rec);
}

fn bucket_matches_key(bucket: &BucketEntry, key_id: Option<u64>) -> bool {
    key_id
        .map(|id| bucket.by_key.contains_key(&id))
        .unwrap_or(true)
}

fn credential_group_for_key(
    bucket: &BucketEntry,
    key_id: Option<u64>,
) -> Option<&HashMap<u64, BucketStats>> {
    match key_id {
        Some(id) => bucket.by_key_credential.get(&id),
        None => Some(&bucket.by_credential),
    }
}

fn model_group_for_key(
    bucket: &BucketEntry,
    key_id: Option<u64>,
) -> Option<&HashMap<String, BucketStats>> {
    match key_id {
        Some(id) => bucket.by_key_model.get(&id),
        None => Some(&bucket.by_model),
    }
}

fn bucket_in_window(bucket: &BucketEntry, window: StatsQueryWindow) -> bool {
    bucket.ts >= window.start_ts && bucket.ts < window.end_ts
}

fn select_buckets(inner: &AggregatorInner, granularity: StatsGranularity) -> &[BucketEntry] {
    match granularity {
        StatsGranularity::Hour => &inner.hour_buckets,
        StatsGranularity::Day => &inner.day_buckets,
    }
}

fn stats_for_key(bucket: &BucketEntry, key_id: Option<u64>) -> BucketStats {
    match key_id {
        Some(id) => bucket.by_key.get(&id).copied().unwrap_or_default(),
        None => bucket.overall,
    }
}

fn ts_to_rfc3339(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default()
}

pub type SharedRecorder = Arc<UsageRecorder>;
pub type SharedAggregator = Arc<UsageAggregator>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_log_filename() {
        assert!(parse_usage_log_filename("usage_log.2026-05-22.jsonl").is_some());
        assert!(parse_usage_log_filename("foo.bar").is_none());
    }

    #[test]
    fn aggregator_basic_ingest_and_overview() {
        let agg = UsageAggregator::new();
        let now = Utc::now();
        let rec = UsageRecord {
            ts: now.to_rfc3339(),
            key_id: 1,
            credential_id: 5,
            model: "claude-opus-4-7".to_string(),
            input_tokens: 1000,
            output_tokens: 200,
            cache_creation_tokens: 300,
            cache_read_tokens: 4000,
            credits: 0.05,
            duration_ms: 1500,
            status: "success".to_string(),
        };
        agg.ingest(&rec);
        agg.ingest(&rec);

        let ov = agg.overview();
        assert_eq!(ov.today_calls, 2);
        assert_eq!(ov.today_input_tokens, 2000);

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let series = agg.query_timeseries(window, None, None);
        assert!(!series.is_empty());

        let by_model = agg.query_by_model(window, None);
        assert_eq!(by_model.len(), 1);
        assert_eq!(by_model[0].model, "claude-opus-4-7");
        assert_eq!(by_model[0].calls, 2);
        assert_eq!(by_model[0].cache_creation_tokens, 600);
        assert_eq!(by_model[0].cache_read_tokens, 8000);

        let by_cred = agg.query_by_credential(window, None, None);
        assert_eq!(by_cred.len(), 1);
        assert_eq!(by_cred[0].credential_id, 5);
        assert_eq!(by_cred[0].cache_creation_tokens, 600);
        assert_eq!(by_cred[0].cache_read_tokens, 8000);
    }

    /// 构造一条指定账号/时间/积分的记录，其余字段取无关默认值
    #[cfg(test)]
    fn credit_rec(ts: DateTime<Utc>, credential_id: u64, credits: f64) -> UsageRecord {
        UsageRecord {
            ts: ts.to_rfc3339(),
            key_id: 1,
            credential_id,
            model: "m".to_string(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits,
            duration_ms: 0,
            status: "success".to_string(),
        }
    }

    /// 积分时序必须保留时间维度：同一账号跨两个小时桶的消耗不能被压成一个总数，
    /// 否则「持续烧」和「某一小时突刺」在图上无法区分——这正是加这个图的目的。
    #[test]
    fn credits_by_credential_keeps_time_dimension() {
        let agg = UsageAggregator::new();
        let now = Utc::now();
        let two_hours_ago = now - Duration::hours(2);
        agg.ingest(&credit_rec(two_hours_ago, 5, 1.0));
        agg.ingest(&credit_rec(now, 5, 0.25));
        agg.ingest(&credit_rec(now, 7, 4.0));

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = agg.query_credits_by_credential(window, None, None, 10);

        assert_eq!(out.total_credentials, 2);
        // 按总积分降序：7 号 4.0 在 5 号 1.25 之前
        assert_eq!(
            out.series.iter().map(|s| s.credential_id).collect::<Vec<_>>(),
            vec![7, 5]
        );
        assert!((out.series[0].total_credits - 4.0).abs() < 1e-9);
        assert!((out.series[1].total_credits - 1.25).abs() < 1e-9);

        // 5 号的 1.0 与 0.25 必须落在不同的桶里
        let buckets_with_5: Vec<f64> = out
            .points
            .iter()
            .filter_map(|p| p.credits.get("5").copied())
            .collect();
        assert_eq!(
            buckets_with_5.len(),
            2,
            "跨两小时的消耗应占两个桶，不能被压成一个"
        );
        assert!(buckets_with_5.iter().any(|v| (v - 1.0).abs() < 1e-9));
        assert!(buckets_with_5.iter().any(|v| (v - 0.25).abs() < 1e-9));

        // 桶按时间升序，前端直接照序画
        let ts: Vec<&str> = out.points.iter().map(|p| p.ts.as_str()).collect();
        let mut sorted = ts.clone();
        sorted.sort_unstable();
        assert_eq!(ts, sorted, "points 必须按时间升序");

        // 只有实际有记录的两个小时桶，中间那一小时没有流量、不物化
        assert_eq!(out.points.len(), 2, "只产出有记录的桶");
    }

    /// Top N 截断时 total_credentials 必须报全量，否则 UI 会把「图上 1 个」
    /// 说成「只有 1 个账号在消耗」。
    #[test]
    fn credits_by_credential_reports_truncation() {
        let agg = UsageAggregator::new();
        let now = Utc::now();
        for id in 1..=5u64 {
            agg.ingest(&credit_rec(now, id, id as f64));
        }
        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = agg.query_credits_by_credential(window, None, None, 2);

        assert_eq!(out.series.len(), 2, "limit 应生效");
        assert_eq!(out.total_credentials, 5, "总数须报全量而非截断后的数量");
        // 取的是积分最高的两个
        assert_eq!(
            out.series.iter().map(|s| s.credential_id).collect::<Vec<_>>(),
            vec![5, 4]
        );
        // 未入选账号不得出现在 points 里
        assert!(
            out.points
                .iter()
                .all(|p| p.credits.keys().all(|k| k == "5" || k == "4")),
            "points 只应包含入选账号"
        );
    }

    /// 零积分账号不该占一条线（图例里全是平地会挤掉真正在烧的账号）
    #[test]
    fn credits_by_credential_skips_zero_credit_accounts() {
        let agg = UsageAggregator::new();
        let now = Utc::now();
        agg.ingest(&credit_rec(now, 5, 0.0));
        agg.ingest(&credit_rec(now, 7, 2.0));

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = agg.query_credits_by_credential(window, None, None, 10);
        assert_eq!(out.total_credentials, 1);
        assert_eq!(
            out.series.iter().map(|s| s.credential_id).collect::<Vec<_>>(),
            vec![7]
        );
    }

    /// group 白名单必须同时约束 Top N 排名与 points，否则筛了分组还能看到组外账号
    #[test]
    fn credits_by_credential_respects_cred_filter() {
        let agg = UsageAggregator::new();
        let now = Utc::now();
        agg.ingest(&credit_rec(now, 5, 1.0));
        agg.ingest(&credit_rec(now, 7, 9.0));

        let allow: std::collections::HashSet<u64> = [5u64].into_iter().collect();
        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let out = agg.query_credits_by_credential(window, None, Some(&allow), 10);

        assert_eq!(out.total_credentials, 1);
        assert_eq!(out.series[0].credential_id, 5);
        assert!(
            out.points.iter().all(|p| !p.credits.contains_key("7")),
            "组外账号不得出现在 points"
        );
    }

    #[test]
    fn aggregator_filters_by_client_key() {
        let agg = UsageAggregator::new();
        let now = Utc::now().to_rfc3339();
        let rec_a = UsageRecord {
            ts: now.clone(),
            key_id: 1,
            credential_id: 5,
            model: "m-a".to_string(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits: 0.01,
            duration_ms: 100,
            status: "success".to_string(),
        };
        let rec_b = UsageRecord {
            ts: now,
            key_id: 2,
            credential_id: 6,
            model: "m-b".to_string(),
            input_tokens: 300,
            output_tokens: 40,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits: 0.02,
            duration_ms: 200,
            status: "error".to_string(),
        };
        agg.ingest(&rec_a);
        agg.ingest(&rec_b);

        let window = StatsQueryWindow::preset(Range::Last24h, StatsGranularity::Hour);
        let series = agg.query_timeseries(window, Some(1), None);
        assert_eq!(series.iter().map(|p| p.calls).sum::<u64>(), 1);
        assert_eq!(series.iter().map(|p| p.input_tokens).sum::<u64>(), 100);

        let by_model = agg.query_by_model(window, Some(1));
        assert_eq!(by_model.len(), 1);
        assert_eq!(by_model[0].model, "m-a");

        let by_cred = agg.query_by_credential(window, Some(1), None);
        assert_eq!(by_cred.len(), 1);
        assert_eq!(by_cred[0].credential_id, 5);
    }

    #[test]
    fn aggregator_filters_by_custom_window_and_granularity() {
        let agg = UsageAggregator::new();
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
        let rec_yesterday = UsageRecord {
            ts: yesterday_noon,
            key_id: 0,
            credential_id: 5,
            model: "m-yesterday".to_string(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits: 0.01,
            duration_ms: 100,
            status: "success".to_string(),
        };
        let rec_today = UsageRecord {
            ts: today_noon,
            key_id: 0,
            credential_id: 5,
            model: "m-today".to_string(),
            input_tokens: 300,
            output_tokens: 40,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits: 0.02,
            duration_ms: 100,
            status: "success".to_string(),
        };
        agg.ingest(&rec_yesterday);
        agg.ingest(&rec_today);

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

        let hourly = agg.query_timeseries(hour_window, None, None);
        assert_eq!(hourly.iter().map(|p| p.calls).sum::<u64>(), 1);
        assert_eq!(hourly.iter().map(|p| p.input_tokens).sum::<u64>(), 300);

        let daily = agg.query_timeseries(day_window, None, None);
        assert_eq!(daily.iter().map(|p| p.calls).sum::<u64>(), 1);
        assert_eq!(daily.iter().map(|p| p.output_tokens).sum::<u64>(), 40);
    }

    #[test]
    fn error_record_increments_errors() {
        let agg = UsageAggregator::new();
        let rec = UsageRecord {
            ts: Utc::now().to_rfc3339(),
            key_id: 0,
            credential_id: 0,
            model: "claude-opus-4-7".to_string(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            credits: 0.0,
            duration_ms: 100,
            status: "error".to_string(),
        };
        agg.ingest(&rec);
        let ov = agg.overview();
        assert_eq!(ov.today_errors, 1);
    }
}

//! 请求用量的记录结构与统计查询参数/输出类型
//!
//! 存储与查询实现在 [`super::usage_store`]（DuckDB）。本模块只保留：
//! - [`UsageRecord`]：单次请求的用量记录（camelCase 序列化，与历史 JSONL 行同构）
//! - 统计端点的查询参数（[`Range`] / [`StatsGranularity`] / [`StatsQueryWindow`]）
//!   与输出结构（时序点、模型/凭据分布、积分时序、概览）
//! - [`parse_usage_log_filename`]：识别历史 `usage_log.YYYY-MM-DD.jsonl` 文件名
//!   （启动时一次性导入用）

use std::collections::HashMap;

use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// 单次请求的用量记录（与历史 JSONL 一行一一对应）
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

/// 识别历史 usage_log 文件名（形如 usage_log.2026-05-22.jsonl）
pub(crate) fn parse_usage_log_filename(name: &str) -> Option<NaiveDate> {
    let body = name.strip_prefix("usage_log.")?.strip_suffix(".jsonl")?;
    NaiveDate::parse_from_str(body, "%Y-%m-%d").ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_log_filename() {
        assert!(parse_usage_log_filename("usage_log.2026-05-22.jsonl").is_some());
        assert!(parse_usage_log_filename("foo.bar").is_none());
    }
}

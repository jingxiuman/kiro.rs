//! 代理 IP 池管理
//!
//! 独立于凭据管理，存储为 proxy_pool.json
//!
//! 除增删改查外，还提供主动健康检查：周期性（或按需）通过每个代理请求一个
//! 轻量公网探测端点，记录连通性与延迟；连续探测失败达阈值的代理会被自动禁用。

use crate::http_client::{ProxyConfig, build_client};
use crate::model::config::TlsBackend;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// 健康检查探测端点：返回 204 No Content 的轻量公网地址，不依赖上游 Kiro。
const PROXY_HEALTH_CHECK_URL: &str = "https://www.gstatic.com/generate_204";
/// 单次探测超时（秒）
const PROXY_PROBE_TIMEOUT_SECS: u64 = 8;
/// 连续探测失败阈值：达到后自动禁用（与凭据的 MAX_FAILURES_PER_CREDENTIAL 对齐）
const MAX_PROXY_PROBE_FAILURES: u32 = 3;
/// 请求级连续失败阈值：真实上游请求（含流中断）经该代理连续失败达到此值时自动禁用。
/// 比探测阈值宽松：真实流量的失败包含上游自身抖动，误伤代价（换绑凭据）也更高。
const MAX_PROXY_REQUEST_FAILURES: u32 = 5;

/// 恢复探针基础间隔（秒），与主健康检查同频。退避档位 0 即此值。
const PROXY_RECOVERY_BASE_INTERVAL_SECS: u64 = 300;
/// 放回账号池所需的连续探测成功次数。
/// 比禁用阈值（3 次失败）严格：下线宽容、上线严格，一次偶然成功不足以证明恢复。
const PROXY_RECOVERY_SUCCESSES: u32 = 2;
/// 恢复探针退避上限（秒，4 小时）。防止坏代理被反复放回祸害生产流量。
const PROXY_RECOVERY_MAX_INTERVAL_SECS: u64 = 4 * 3600;
/// 同场故障判定窗口（秒，24 小时）。
/// 放回后在此窗口内又挂 = 上次恢复是假的，退避档位递增；超出则视为独立故障，档位归零。
const PROXY_RECOVERY_INCIDENT_WINDOW_SECS: i64 = 24 * 3600;

/// 代理健康状态
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProxyHealth {
    /// 尚未探测
    #[default]
    Unknown,
    /// 最近一次探测成功
    Healthy,
    /// 最近一次探测失败
    Unhealthy,
}

/// 持久化的代理条目
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyEntry {
    pub id: u64,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 健康状态（健康检查结果）
    #[serde(default)]
    pub health: ProxyHealth,
    /// 最近一次成功探测的延迟（毫秒）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u32>,
    /// 最近一次探测时间（RFC3339）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_at: Option<String>,
    /// 连续探测失败计数（成功后清零）
    #[serde(default)]
    pub consecutive_failures: u32,
    /// 是否由健康检查自动禁用（区别于用户手动禁用）
    #[serde(default)]
    pub auto_disabled: bool,
    /// 请求级连续失败计数：真实上游请求经该代理失败（网络错误 / 流中断）累加，
    /// 任一真实请求成功即清零。与探测计数独立——能通探测端点但连不上上游的
    /// 代理只有这条通道能暴露。
    #[serde(default)]
    pub request_failures: u32,
    /// 最近一次请求级失败原因（截断）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_request_error: Option<String>,
    /// 自动恢复探针状态。仅对 `auto_disabled` 的条目有意义。
    #[serde(default)]
    pub recovery: RecoveryState,
}

/// 自动恢复探针的状态。
///
/// 单独成结构而不是往 [`ProxyEntry`] 上再摊四个字段：恢复逻辑自成一体，
/// 独立结构让它能脱离整个 `ProxyPoolManager` 单测。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RecoveryState {
    /// 连续探测成功次数。放回后清零；中途任何一次失败也清零。
    pub consecutive_successes: u32,
    /// 退避档位。探针放回后又在窗口内被自动禁用则递增。
    pub backoff_level: u32,
    /// 下次允许探测的时间（RFC3339）。`None` = 立即可探。
    pub next_probe_at: Option<String>,
    /// 最近一次被探针放回的时间，用于判定「是不是同一场故障」。
    pub last_recovered_at: Option<String>,
}

/// 退避档位对应的探测间隔（秒）。`min(300 × 2^level, 4h)`。
fn recovery_backoff_secs(level: u32) -> u64 {
    // level 上界钳制：避免 1u64 << level 在大档位上溢出（档位本身有 4h 封顶，
    // 但溢出是 UB 级别的问题，不能靠"档位不会涨那么高"来兜）
    let shift = level.min(32);
    PROXY_RECOVERY_BASE_INTERVAL_SECS
        .saturating_mul(1u64 << shift)
        .min(PROXY_RECOVERY_MAX_INTERVAL_SECS)
}

/// 自动禁用时装配恢复探针：推进退避档位、安排下次探测。
///
/// 两条自动禁用路径（探测失败 / 请求级失败）共用，保证退避语义只有一处实现。
fn arm_recovery_on_auto_disable(entry: &mut ProxyEntry, now: chrono::DateTime<chrono::Utc>) {
    let same_incident = entry
        .recovery
        .last_recovered_at
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .is_some_and(|t| {
            (now - t.with_timezone(&chrono::Utc)).num_seconds() < PROXY_RECOVERY_INCIDENT_WINDOW_SECS
        });
    entry.recovery.backoff_level = if same_incident {
        entry.recovery.backoff_level.saturating_add(1)
    } else {
        // 独立故障从 0 起步：level 0 = 基础间隔，即「与主检查同频」这个既定参数
        0
    };
    entry.recovery.consecutive_successes = 0;
    entry.recovery.next_probe_at = Some(next_probe_at(entry.recovery.backoff_level, now));
}

/// 按当前退避档位算出下次探测时刻（RFC3339）
fn next_probe_at(level: u32, now: chrono::DateTime<chrono::Utc>) -> String {
    (now + chrono::Duration::seconds(recovery_backoff_secs(level) as i64)).to_rfc3339()
}

/// 该条目本轮是否该做恢复探测。
///
/// `next_probe_at` 为 `None`（历史数据没有这个字段，或刚从旧版本升上来）视为立即可探——
/// 宁可多探一次，也不要因为缺字段让代理永远卡在禁用态。解析失败同理。
fn recovery_probe_due(entry: &ProxyEntry, now: chrono::DateTime<chrono::Utc>) -> bool {
    match entry.recovery.next_probe_at.as_deref() {
        None => true,
        Some(raw) => match chrono::DateTime::parse_from_rfc3339(raw) {
            Ok(t) => now >= t.with_timezone(&chrono::Utc),
            Err(_) => true,
        },
    }
}

fn default_true() -> bool {
    true
}

/// 代理分配结果
pub enum GetUrlResult {
    /// 代理存在且已启用，返回 URL
    Ok(String),
    /// 代理不存在
    NotFound,
    /// 代理存在但已被禁用
    Disabled,
}

/// 一次全量健康检查的摘要
#[derive(Debug, Clone, Default)]
pub struct CheckSummary {
    /// 探测成功数
    pub healthy: usize,
    /// 探测失败数
    pub unhealthy: usize,
    /// 本轮新增的自动禁用数
    pub auto_disabled: usize,
    /// 本轮新自动禁用的条目 (id, url)，供上层记事件 / 解绑凭据
    pub newly_disabled: Vec<(u64, String)>,
    /// 本轮被恢复探针探测的条目数（已自动禁用、且退避时间已到的）
    pub recovery_probed: usize,
    /// 本轮被探针放回账号池的条目 (id, url)，供上层记事件
    pub newly_recovered: Vec<(u64, String)>,
}

/// 单个代理探测结果
enum ProbeResult {
    Ok { latency_ms: u32 },
    Err { error: String },
}

pub struct ProxyPoolManager {
    entries: Mutex<Vec<ProxyEntry>>,
    // 仅需原子自增，不需要与 entries 联锁；约定独立使用，无锁顺序问题
    next_id: AtomicU64,
    path: Option<PathBuf>,
    /// TLS 后端，构建探测用 HTTP client 时需要
    tls_backend: TlsBackend,
    /// 全量健康检查重入保护：后台定时与手动触发可能重叠，若并发探测同一瞬时故障
    /// 会被计成多次连续失败并误达阈值。置位期间的重复调用直接跳过。
    check_in_progress: AtomicBool,
}

/// 校验代理 URL 的 scheme 是否合法
fn validate_proxy_url(url: &str) -> anyhow::Result<()> {
    let valid_schemes = ["http://", "https://", "socks5://", "socks4://"];
    if !valid_schemes.iter().any(|s| url.starts_with(s)) {
        anyhow::bail!(
            "代理 URL scheme 无效，支持: http/https/socks4/socks5（收到: {}）",
            url
        );
    }
    // 简单检查 host:port 存在
    let after_scheme = valid_schemes
        .iter()
        .find(|s| url.starts_with(*s))
        .map(|s| &url[s.len()..])
        .unwrap_or(url);
    // after_scheme 可能是 user:pass@host:port 或 host:port
    let host_part = after_scheme.rsplit('@').next().unwrap_or(after_scheme);
    if !host_part.contains(':') {
        anyhow::bail!("代理 URL 缺少端口号: {}", url);
    }
    Ok(())
}

impl ProxyPoolManager {
    pub fn new(path: Option<PathBuf>, tls_backend: TlsBackend) -> Self {
        let entries = path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|s| serde_json::from_str::<Vec<ProxyEntry>>(&s).ok())
            .unwrap_or_default();

        let next_id = entries.iter().map(|e| e.id).max().unwrap_or(0) + 1;

        Self {
            entries: Mutex::new(entries),
            next_id: AtomicU64::new(next_id),
            path,
            tls_backend,
            check_in_progress: AtomicBool::new(false),
        }
    }

    pub fn list(&self) -> Vec<ProxyEntry> {
        self.entries.lock().clone()
    }

    pub fn add(&self, url: String, label: Option<String>) -> anyhow::Result<ProxyEntry> {
        let url = url.trim().to_string();
        if url.is_empty() {
            anyhow::bail!("代理 URL 不能为空");
        }
        validate_proxy_url(&url)?;

        let mut entries = self.entries.lock();

        if entries.iter().any(|e| e.url == url) {
            anyhow::bail!("代理 URL 已存在: {}", url);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = ProxyEntry {
            id,
            url,
            label,
            enabled: true,
            health: ProxyHealth::Unknown,
            latency_ms: None,
            last_checked_at: None,
            consecutive_failures: 0,
            auto_disabled: false,
            request_failures: 0,
            last_request_error: None,
            recovery: RecoveryState::default(),
        };
        entries.push(entry.clone());
        drop(entries);

        self.persist()?;
        Ok(entry)
    }

    /// 批量添加：在单次加锁内完成所有插入，最后统一持久化一次
    pub fn batch_add(&self, urls: Vec<String>) -> (Vec<ProxyEntry>, Vec<String>) {
        let mut added = vec![];
        let mut errors = vec![];

        let mut entries = self.entries.lock();
        for url in urls {
            let url = url.trim().to_string();
            if url.is_empty() || url.starts_with('#') {
                continue;
            }
            if let Err(e) = validate_proxy_url(&url) {
                errors.push(e.to_string());
                continue;
            }
            if entries.iter().any(|e| e.url == url) {
                errors.push(format!("代理 URL 已存在: {}", url));
                continue;
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let entry = ProxyEntry {
                id,
                url,
                label: None,
                enabled: true,
                health: ProxyHealth::Unknown,
                latency_ms: None,
                last_checked_at: None,
                consecutive_failures: 0,
                auto_disabled: false,
                request_failures: 0,
                last_request_error: None,
                recovery: RecoveryState::default(),
            };
            entries.push(entry.clone());
            added.push(entry);
        }
        drop(entries);

        if !added.is_empty() {
            if let Err(e) = self.persist() {
                tracing::warn!("批量添加代理后持久化失败: {}", e);
            }
        }

        (added, errors)
    }

    pub fn delete(&self, id: u64) -> anyhow::Result<()> {
        let mut entries = self.entries.lock();
        let len_before = entries.len();
        entries.retain(|e| e.id != id);
        if entries.len() == len_before {
            anyhow::bail!("代理不存在: {}", id);
        }
        drop(entries);
        self.persist()?;
        Ok(())
    }

    /// 设置代理启用/禁用状态
    ///
    /// 用户手动启用时清除「健康检查自动禁用」标记与连续失败计数，
    /// 让该代理重新参与健康检查与分配。
    pub fn set_enabled(&self, id: u64, enabled: bool) -> anyhow::Result<()> {
        let mut entries = self.entries.lock();
        let entry = entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| anyhow::anyhow!("代理不存在: {}", id))?;
        entry.enabled = enabled;
        if enabled {
            entry.auto_disabled = false;
            entry.consecutive_failures = 0;
            entry.request_failures = 0;
            entry.last_request_error = None;
            // 人工介入即重置：包括退避档位。管理员手动放回，不应背负历史抖动的惩罚。
            entry.recovery = RecoveryState::default();
        }
        drop(entries);
        self.persist()?;
        Ok(())
    }

    /// 获取代理 URL，区分"不存在"和"已禁用"两种情况
    pub fn get_url(&self, id: u64) -> GetUrlResult {
        match self.entries.lock().iter().find(|e| e.id == id) {
            None => GetUrlResult::NotFound,
            Some(e) if !e.enabled => GetUrlResult::Disabled,
            Some(e) => GetUrlResult::Ok(e.url.clone()),
        }
    }

    /// 获取所有「可用于分配」的代理 URL：已启用且非 Unhealthy
    pub fn assignable_urls(&self) -> Vec<String> {
        self.entries
            .lock()
            .iter()
            .filter(|e| e.enabled && e.health != ProxyHealth::Unhealthy)
            .map(|e| e.url.clone())
            .collect()
    }

    fn persist(&self) -> anyhow::Result<()> {
        let path = match &self.path {
            Some(p) => p,
            None => return Ok(()),
        };
        let entries = self.entries.lock();
        let json = serde_json::to_string_pretty(&*entries)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

// ============ 健康检查 ============

impl ProxyPoolManager {
    /// 探测单个代理 URL 的连通性与延迟。
    ///
    /// 通过该代理请求 `PROXY_HEALTH_CHECK_URL`，成功（HTTP 2xx/3xx）即视为连通，
    /// 返回往返延迟；任何网络错误或非预期状态码视为失败。
    async fn probe_one(&self, url: &str) -> ProbeResult {
        let proxy = ProxyConfig::new(url);
        let client = match build_client(Some(&proxy), PROXY_PROBE_TIMEOUT_SECS, self.tls_backend) {
            Ok(c) => c,
            Err(e) => {
                return ProbeResult::Err {
                    error: format!("构建探测 client 失败: {}", e),
                };
            }
        };

        let started = Instant::now();
        match client.get(PROXY_HEALTH_CHECK_URL).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() || status.is_redirection() {
                    ProbeResult::Ok {
                        latency_ms: started.elapsed().as_millis().min(u32::MAX as u128) as u32,
                    }
                } else {
                    ProbeResult::Err {
                        error: format!("探测端点返回非预期状态: {}", status),
                    }
                }
            }
            Err(e) => ProbeResult::Err {
                error: e.to_string(),
            },
        }
    }

    /// 将一次探测结果回写到指定条目，并按需触发自动禁用。
    ///
    /// 返回 `(变为不健康, 本次新自动禁用)` 供摘要统计。
    fn apply_probe_result(
        entry: &mut ProxyEntry,
        result: &ProbeResult,
        now: chrono::DateTime<chrono::Utc>,
    ) -> (bool, bool) {
        entry.last_checked_at = Some(now.to_rfc3339());
        match result {
            ProbeResult::Ok { latency_ms } => {
                entry.health = ProxyHealth::Healthy;
                entry.latency_ms = Some(*latency_ms);
                entry.consecutive_failures = 0;
                (false, false)
            }
            ProbeResult::Err { error } => {
                entry.health = ProxyHealth::Unhealthy;
                entry.latency_ms = None;
                entry.consecutive_failures += 1;
                tracing::warn!(
                    "代理 #{} 探测失败（{}/{}）: {}",
                    entry.id,
                    entry.consecutive_failures,
                    MAX_PROXY_PROBE_FAILURES,
                    error
                );
                let mut newly_disabled = false;
                if entry.consecutive_failures >= MAX_PROXY_PROBE_FAILURES && entry.enabled {
                    entry.enabled = false;
                    entry.auto_disabled = true;
                    arm_recovery_on_auto_disable(entry, now);
                    newly_disabled = true;
                    tracing::error!(
                        "代理 #{} 连续探测失败 {} 次，已自动禁用（{}s 后开始恢复探测）",
                        entry.id,
                        entry.consecutive_failures,
                        recovery_backoff_secs(entry.recovery.backoff_level)
                    );
                }
                (true, newly_disabled)
            }
        }
    }

    /// 应用一次**恢复探测**的结果，返回该条目是否在本次被放回账号池。
    ///
    /// 与 [`Self::apply_probe_result`] 分开的理由：两者的判据方向相反。在线探测
    /// 累加失败计数、向下线收敛；恢复探测累加成功计数、向上线收敛。塞进一个函数
    /// 会变成一堆 `if entry.enabled` 分支，且 `consecutive_failures` 的语义会被
    /// 两个方向争抢。
    ///
    /// 恢复探测**不**触碰 `consecutive_failures`：那是在线探测的账本，
    /// 一条已经下线的代理再累加它没有意义。
    fn apply_recovery_probe_result(
        entry: &mut ProxyEntry,
        result: &ProbeResult,
        now: chrono::DateTime<chrono::Utc>,
    ) -> bool {
        entry.last_checked_at = Some(now.to_rfc3339());
        match result {
            ProbeResult::Ok { latency_ms } => {
                entry.health = ProxyHealth::Healthy;
                entry.latency_ms = Some(*latency_ms);
                entry.recovery.consecutive_successes += 1;
                if entry.recovery.consecutive_successes < PROXY_RECOVERY_SUCCESSES {
                    tracing::info!(
                        "代理 #{} 恢复探测成功（{}/{}），未达放回阈值",
                        entry.id,
                        entry.recovery.consecutive_successes,
                        PROXY_RECOVERY_SUCCESSES
                    );
                    entry.recovery.next_probe_at =
                        Some(next_probe_at(entry.recovery.backoff_level, now));
                    return false;
                }
                // 放回：两个失败计数都必须清零。request_failures 不清零的话，
                // 恢复后第一个真实请求失败就会撞上 MAX_PROXY_REQUEST_FAILURES 的旧值。
                entry.enabled = true;
                entry.auto_disabled = false;
                entry.consecutive_failures = 0;
                entry.request_failures = 0;
                entry.last_request_error = None;
                entry.recovery.consecutive_successes = 0;
                entry.recovery.next_probe_at = None;
                entry.recovery.last_recovered_at = Some(now.to_rfc3339());
                // backoff_level 有意保留：下次再挂时才按 24h 窗口决定升还是归零
                tracing::info!(
                    "代理 #{} 连续 {} 次恢复探测成功，已放回账号池",
                    entry.id,
                    PROXY_RECOVERY_SUCCESSES
                );
                true
            }
            ProbeResult::Err { error } => {
                entry.health = ProxyHealth::Unhealthy;
                entry.latency_ms = None;
                entry.recovery.consecutive_successes = 0;
                entry.recovery.next_probe_at = Some(next_probe_at(entry.recovery.backoff_level, now));
                tracing::debug!(
                    "代理 #{} 恢复探测仍失败（{}s 后重试）: {}",
                    entry.id,
                    recovery_backoff_secs(entry.recovery.backoff_level),
                    error
                );
                false
            }
        }
    }

    /// 全量健康检查：并发探测所有「已启用」代理，回写结果并持久化一次。
    ///
    /// 仅探测当前 enabled 的条目；用户/自动禁用的条目跳过（手动重新启用会清零计数）。
    pub async fn check_all(&self) -> CheckSummary {
        // 重入保护：已有检查在跑就跳过，避免并发探测把一次瞬时故障计成多次连续失败
        if self
            .check_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::debug!("代理健康检查已在进行中，跳过本次重入");
            return CheckSummary::default();
        }
        // 守卫：无论后续如何返回都复位标记
        struct Guard<'a>(&'a AtomicBool);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _guard = Guard(&self.check_in_progress);

        let now = chrono::Utc::now();

        // 快照待探测的 (id, url, 是否恢复探测)，避免长时间持锁。
        // 两个集合互斥：在线的走 enabled，待恢复的走 auto_disabled && !enabled，
        // 所以一次并发探测能同时覆盖，不会对同一条目重复探。
        let targets: Vec<(u64, String, bool)> = self
            .entries
            .lock()
            .iter()
            .filter_map(|e| {
                if e.enabled {
                    Some((e.id, e.url.clone(), false))
                } else if e.auto_disabled && recovery_probe_due(e, now) {
                    Some((e.id, e.url.clone(), true))
                } else {
                    // 用户手动禁用（auto_disabled == false）永不进入恢复探测；
                    // 退避时间未到的也跳过本轮
                    None
                }
            })
            .collect();

        if targets.is_empty() {
            return CheckSummary::default();
        }

        let probes = targets
            .iter()
            .map(|(id, url, is_recovery)| async move {
                (*id, *is_recovery, self.probe_one(url).await)
            });
        let results = futures::future::join_all(probes).await;

        let mut summary = CheckSummary::default();
        {
            let mut entries = self.entries.lock();
            for (id, is_recovery, result) in &results {
                let Some(entry) = entries.iter_mut().find(|e| e.id == *id) else {
                    continue;
                };
                if *is_recovery {
                    summary.recovery_probed += 1;
                    if Self::apply_recovery_probe_result(entry, result, now) {
                        summary.newly_recovered.push((entry.id, entry.url.clone()));
                    }
                    continue;
                }
                let (unhealthy, newly_disabled) = Self::apply_probe_result(entry, result, now);
                if unhealthy {
                    summary.unhealthy += 1;
                } else {
                    summary.healthy += 1;
                }
                if newly_disabled {
                    summary.auto_disabled += 1;
                    summary.newly_disabled.push((entry.id, entry.url.clone()));
                }
            }
        }

        if let Err(e) = self.persist() {
            tracing::warn!("健康检查后持久化失败: {}", e);
        }
        summary
    }

    /// 请求级成功反馈：真实上游请求经该代理成功，清零请求级失败计数。
    /// 仅在计数非零时才写状态与持久化，避免每个成功请求都触发磁盘写。
    pub fn report_request_success(&self, url: &str) {
        let mut changed = false;
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.url == url) {
                if entry.request_failures != 0 || entry.last_request_error.is_some() {
                    entry.request_failures = 0;
                    entry.last_request_error = None;
                    changed = true;
                }
            }
        }
        if changed {
            if let Err(e) = self.persist() {
                tracing::warn!("请求级反馈持久化失败: {}", e);
            }
        }
    }

    /// 请求级失败反馈：真实上游请求经该代理失败（网络错误 / 流中断）。
    ///
    /// 连续失败达 [`MAX_PROXY_REQUEST_FAILURES`] 且仍启用时自动禁用，
    /// 返回被禁用条目的快照（供上层解绑凭据、记录处置事件）；否则返回 None。
    pub fn report_request_failure(&self, url: &str, error: &str) -> Option<ProxyEntry> {
        let disabled = {
            let mut entries = self.entries.lock();
            let entry = entries.iter_mut().find(|e| e.url == url)?;
            entry.request_failures += 1;
            entry.last_request_error =
                Some(error.chars().take(200).collect::<String>());
            tracing::warn!(
                "代理 #{} 请求级失败（{}/{}）: {}",
                entry.id,
                entry.request_failures,
                MAX_PROXY_REQUEST_FAILURES,
                error
            );
            if entry.request_failures >= MAX_PROXY_REQUEST_FAILURES && entry.enabled {
                entry.enabled = false;
                entry.auto_disabled = true;
                arm_recovery_on_auto_disable(entry, chrono::Utc::now());
                tracing::error!(
                    "代理 #{} 连续 {} 次真实请求失败，已自动禁用",
                    entry.id,
                    entry.request_failures
                );
                Some(entry.clone())
            } else {
                None
            }
        };
        if let Err(e) = self.persist() {
            tracing::warn!("请求级反馈持久化失败: {}", e);
        }
        disabled
    }

    /// 单个代理即时探测（供 UI「测试」按钮调用），回写结果并持久化。
    /// 返回 (条目快照, 本次是否新触发自动禁用)。newly_disabled 供上层处置（记事件 + 换绑）。
    pub async fn check_one(&self, id: u64) -> anyhow::Result<(ProxyEntry, bool)> {
        let url = self
            .entries
            .lock()
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.url.clone())
            .ok_or_else(|| anyhow::anyhow!("代理不存在: {}", id))?;

        let result = self.probe_one(&url).await;

        let (entry, newly_disabled) = {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("代理不存在: {}", id))?;
            let (_, newly_disabled) = Self::apply_probe_result(entry, &result, chrono::Utc::now());
            (entry.clone(), newly_disabled)
        };

        self.persist()?;
        Ok((entry, newly_disabled))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(url: &str) -> ProxyEntry {
        ProxyEntry {
            id: 1,
            url: url.to_string(),
            label: None,
            enabled: true,
            health: ProxyHealth::Unknown,
            latency_ms: None,
            last_checked_at: None,
            consecutive_failures: 0,
            auto_disabled: false,
            request_failures: 0,
            last_request_error: None,
            recovery: RecoveryState::default(),
        }
    }

    #[test]
    fn old_json_without_new_fields_deserializes() {
        // 旧格式 JSON 只有 id/url/label/enabled，新字段应由 serde default 补全
        let json = r#"[{"id":1,"url":"socks5://127.0.0.1:1080","enabled":true}]"#;
        let entries: Vec<ProxyEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.health, ProxyHealth::Unknown);
        assert_eq!(e.latency_ms, None);
        assert_eq!(e.consecutive_failures, 0);
        assert!(!e.auto_disabled);
    }

    #[test]
    fn probe_failure_increments_and_auto_disables_at_threshold() {
        let mut entry = make_entry("socks5://127.0.0.1:1080");
        let err = ProbeResult::Err {
            error: "connection refused".to_string(),
        };
        // 前两次失败：计数累加，仍启用
        for n in 1..MAX_PROXY_PROBE_FAILURES {
            let (unhealthy, disabled) = ProxyPoolManager::apply_probe_result(&mut entry, &err, chrono::Utc::now());
            assert!(unhealthy);
            assert!(!disabled);
            assert_eq!(entry.consecutive_failures, n);
            assert!(entry.enabled);
            assert!(!entry.auto_disabled);
        }
        // 第 N 次失败：自动禁用
        let (_, disabled) = ProxyPoolManager::apply_probe_result(&mut entry, &err, chrono::Utc::now());
        assert!(disabled);
        assert_eq!(entry.consecutive_failures, MAX_PROXY_PROBE_FAILURES);
        assert!(!entry.enabled);
        assert!(entry.auto_disabled);
    }

    #[test]
    fn probe_success_clears_failures_and_marks_healthy() {
        let mut entry = make_entry("socks5://127.0.0.1:1080");
        entry.consecutive_failures = 2;
        entry.health = ProxyHealth::Unhealthy;
        let ok = ProbeResult::Ok { latency_ms: 123 };
        let (unhealthy, disabled) = ProxyPoolManager::apply_probe_result(&mut entry, &ok, chrono::Utc::now());
        assert!(!unhealthy);
        assert!(!disabled);
        assert_eq!(entry.consecutive_failures, 0);
        assert_eq!(entry.health, ProxyHealth::Healthy);
        assert_eq!(entry.latency_ms, Some(123));
    }

    #[test]
    fn request_failures_auto_disable_at_threshold_and_success_resets() {
        let mgr = ProxyPoolManager::new(None, TlsBackend::Rustls);
        let entry = mgr.add("socks5://127.0.0.1:1080".to_string(), None).unwrap();
        let url = entry.url.clone();

        // 前 N-1 次失败：计数累加，不禁用
        for _ in 1..MAX_PROXY_REQUEST_FAILURES {
            assert!(mgr.report_request_failure(&url, "error decoding response body").is_none());
        }
        // 一次成功清零
        mgr.report_request_success(&url);
        let e = mgr.list().into_iter().find(|e| e.id == entry.id).unwrap();
        assert_eq!(e.request_failures, 0);
        assert!(e.enabled);

        // 连续 N 次失败：第 N 次返回被禁用的快照
        for i in 1..=MAX_PROXY_REQUEST_FAILURES {
            let disabled = mgr.report_request_failure(&url, "connect timeout");
            if i < MAX_PROXY_REQUEST_FAILURES {
                assert!(disabled.is_none());
            } else {
                let d = disabled.expect("达到阈值应自动禁用");
                assert!(!d.enabled);
                assert!(d.auto_disabled);
            }
        }
        // 已禁用后继续失败不重复返回
        assert!(mgr.report_request_failure(&url, "again").is_none());
    }

    #[test]
    fn request_feedback_unknown_url_is_noop() {
        let mgr = ProxyPoolManager::new(None, TlsBackend::Rustls);
        assert!(mgr.report_request_failure("socks5://nope:1", "x").is_none());
        mgr.report_request_success("socks5://nope:1");
    }

    #[test]
    fn set_enabled_true_clears_auto_disable_state() {
        let mgr = ProxyPoolManager::new(None, TlsBackend::Rustls);
        let entry = mgr.add("socks5://127.0.0.1:1080".to_string(), None).unwrap();
        // 模拟自动禁用状态
        {
            let mut entries = mgr.entries.lock();
            let e = entries.iter_mut().find(|e| e.id == entry.id).unwrap();
            e.enabled = false;
            e.auto_disabled = true;
            e.consecutive_failures = MAX_PROXY_PROBE_FAILURES;
        }
        mgr.set_enabled(entry.id, true).unwrap();
        let list = mgr.list();
        let e = list.iter().find(|e| e.id == entry.id).unwrap();
        assert!(e.enabled);
        assert!(!e.auto_disabled);
        assert_eq!(e.consecutive_failures, 0);
    }

    // ===== 自动恢复探针 =====

    fn auto_disabled_entry(url: &str) -> ProxyEntry {
        let mut e = make_entry(url);
        e.enabled = false;
        e.auto_disabled = true;
        e.health = ProxyHealth::Unhealthy;
        e.consecutive_failures = MAX_PROXY_PROBE_FAILURES;
        e
    }

    fn ok_probe() -> ProbeResult {
        ProbeResult::Ok { latency_ms: 42 }
    }

    fn err_probe() -> ProbeResult {
        ProbeResult::Err {
            error: "connect timeout".to_string(),
        }
    }

    /// 用户手动禁用的条目（auto_disabled == false）永远不进恢复探测集合。
    /// 这是「自动下线、人工上线」语义的硬边界：管理员关掉的东西不许自己爬回来。
    #[test]
    fn manually_disabled_proxy_is_never_probed_for_recovery() {
        let mut manual = make_entry("socks5://127.0.0.1:1080");
        manual.enabled = false;
        manual.auto_disabled = false;

        // 筛选条件与 check_all 中一致
        let now = chrono::Utc::now();
        assert!(
            !(manual.auto_disabled && recovery_probe_due(&manual, now)),
            "手动禁用的代理不得进入恢复探测集合"
        );

        let auto = auto_disabled_entry("socks5://127.0.0.1:1081");
        assert!(
            auto.auto_disabled && recovery_probe_due(&auto, now),
            "自动禁用且未安排过下次探测的代理应立即可探"
        );
    }

    /// 连续成功 1 次不放回，第 2 次才放回。
    #[test]
    fn recovery_requires_two_consecutive_successes() {
        let mut e = auto_disabled_entry("socks5://127.0.0.1:1080");
        let now = chrono::Utc::now();

        assert!(
            !ProxyPoolManager::apply_recovery_probe_result(&mut e, &ok_probe(), now),
            "第 1 次成功不得放回"
        );
        assert!(!e.enabled, "未达阈值前必须保持禁用");
        assert_eq!(e.recovery.consecutive_successes, 1);
        assert!(
            e.recovery.next_probe_at.is_some(),
            "未达阈值时要安排下次探测，否则下一轮会被 due 判定挡住或永久卡住"
        );

        assert!(
            ProxyPoolManager::apply_recovery_probe_result(&mut e, &ok_probe(), now),
            "第 2 次成功必须放回"
        );
        assert!(e.enabled);
    }

    /// 中途失败清零成功计数：1 次成功 + 1 次失败 + 1 次成功 ≠ 放回。
    #[test]
    fn failed_probe_resets_success_streak() {
        let mut e = auto_disabled_entry("socks5://127.0.0.1:1080");
        let now = chrono::Utc::now();

        ProxyPoolManager::apply_recovery_probe_result(&mut e, &ok_probe(), now);
        assert_eq!(e.recovery.consecutive_successes, 1);

        ProxyPoolManager::apply_recovery_probe_result(&mut e, &err_probe(), now);
        assert_eq!(e.recovery.consecutive_successes, 0, "失败必须清零连续成功计数");

        assert!(
            !ProxyPoolManager::apply_recovery_probe_result(&mut e, &ok_probe(), now),
            "清零后的第 1 次成功不得放回"
        );
        assert!(!e.enabled);
    }

    /// 放回时的完整终态。request_failures 尤其关键：不清零的话恢复后第一个
    /// 真实请求失败就会撞上 MAX_PROXY_REQUEST_FAILURES 的旧值，立刻又被禁用。
    #[test]
    fn recovery_clears_both_failure_counters_and_stamps_time() {
        let mut e = auto_disabled_entry("socks5://127.0.0.1:1080");
        e.request_failures = MAX_PROXY_REQUEST_FAILURES;
        e.last_request_error = Some("stream aborted".to_string());
        e.recovery.backoff_level = 3;
        let now = chrono::Utc::now();

        ProxyPoolManager::apply_recovery_probe_result(&mut e, &ok_probe(), now);
        assert!(ProxyPoolManager::apply_recovery_probe_result(
            &mut e,
            &ok_probe(),
            now
        ));

        assert!(e.enabled, "必须放回");
        assert!(!e.auto_disabled, "自动禁用标记必须清除");
        assert_eq!(e.consecutive_failures, 0);
        assert_eq!(e.request_failures, 0, "请求级计数必须清零");
        assert!(e.last_request_error.is_none());
        assert_eq!(e.recovery.consecutive_successes, 0);
        assert!(e.recovery.next_probe_at.is_none(), "放回后不该再排探测");
        assert!(e.recovery.last_recovered_at.is_some(), "必须打放回时间戳");
        assert_eq!(
            e.recovery.backoff_level, 3,
            "退避档位在放回时保留，下次再挂才按 24h 窗口决定升降"
        );
    }

    /// 放回后在 24h 窗口内又挂 → 档位递增（上次恢复是假的）。
    /// 超出窗口 → 档位归零（独立故障，重新按 5 分钟同频起步）。
    #[test]
    fn backoff_level_advances_within_incident_window_and_resets_outside() {
        let now = chrono::Utc::now();

        let mut recent = auto_disabled_entry("socks5://127.0.0.1:1080");
        recent.recovery.backoff_level = 2;
        recent.recovery.last_recovered_at =
            Some((now - chrono::Duration::hours(1)).to_rfc3339());
        arm_recovery_on_auto_disable(&mut recent, now);
        assert_eq!(recent.recovery.backoff_level, 3, "窗口内复发必须升档");

        let mut stale = auto_disabled_entry("socks5://127.0.0.1:1081");
        stale.recovery.backoff_level = 5;
        stale.recovery.last_recovered_at =
            Some((now - chrono::Duration::hours(25)).to_rfc3339());
        arm_recovery_on_auto_disable(&mut stale, now);
        assert_eq!(stale.recovery.backoff_level, 0, "超窗口视为独立故障，档位归零");

        let mut fresh = auto_disabled_entry("socks5://127.0.0.1:1082");
        arm_recovery_on_auto_disable(&mut fresh, now);
        assert_eq!(
            fresh.recovery.backoff_level, 0,
            "从未恢复过的代理首次禁用从 0 起步（= 与主检查同频）"
        );
    }

    /// 退避间隔翻倍并在 4 小时封顶。
    #[test]
    fn backoff_interval_doubles_and_caps() {
        assert_eq!(recovery_backoff_secs(0), PROXY_RECOVERY_BASE_INTERVAL_SECS);
        assert_eq!(recovery_backoff_secs(1), 600);
        assert_eq!(recovery_backoff_secs(2), 1200);
        assert_eq!(recovery_backoff_secs(5), 9600);
        assert_eq!(
            recovery_backoff_secs(6),
            PROXY_RECOVERY_MAX_INTERVAL_SECS,
            "第 6 档（19200s）应被 4h 上限压住"
        );
        assert_eq!(
            recovery_backoff_secs(u32::MAX),
            PROXY_RECOVERY_MAX_INTERVAL_SECS,
            "极端档位不得溢出，必须落在上限"
        );
    }

    /// next_probe_at 未到时本轮跳过；到了才探。
    #[test]
    fn recovery_probe_respects_next_probe_at() {
        let now = chrono::Utc::now();
        let mut e = auto_disabled_entry("socks5://127.0.0.1:1080");

        e.recovery.next_probe_at = Some((now + chrono::Duration::minutes(3)).to_rfc3339());
        assert!(!recovery_probe_due(&e, now), "退避时间未到必须跳过");

        e.recovery.next_probe_at = Some((now - chrono::Duration::seconds(1)).to_rfc3339());
        assert!(recovery_probe_due(&e, now), "退避时间已过必须探测");

        e.recovery.next_probe_at = Some("not-a-timestamp".to_string());
        assert!(
            recovery_probe_due(&e, now),
            "时间戳损坏时宁可多探一次，也不能让代理永久卡在禁用态"
        );
    }

    /// 手动启用清空整个恢复状态，包括退避档位——人工介入不该背负历史抖动的惩罚。
    #[test]
    fn manual_enable_clears_recovery_state() {
        let mgr = ProxyPoolManager::new(None, TlsBackend::default());
        let entry = mgr.add("socks5://127.0.0.1:1080".to_string(), None).unwrap();
        {
            let mut entries = mgr.entries.lock();
            let e = entries.iter_mut().find(|e| e.id == entry.id).unwrap();
            e.enabled = false;
            e.auto_disabled = true;
            e.recovery.backoff_level = 4;
            e.recovery.consecutive_successes = 1;
            e.recovery.next_probe_at = Some(chrono::Utc::now().to_rfc3339());
            e.recovery.last_recovered_at = Some(chrono::Utc::now().to_rfc3339());
        }

        mgr.set_enabled(entry.id, true).unwrap();

        let e = mgr.list().into_iter().find(|e| e.id == entry.id).unwrap();
        assert!(e.enabled);
        assert!(!e.auto_disabled);
        assert_eq!(
            e.recovery,
            RecoveryState::default(),
            "手动启用必须把恢复状态整体归位，含退避档位"
        );
    }

    /// 旧 proxy_pool.json 没有 recovery 字段，必须能反序列化并落到默认值。
    #[test]
    fn old_json_without_recovery_field_deserializes() {
        let json = r#"[{"id":1,"url":"socks5://127.0.0.1:1080","enabled":false,"autoDisabled":true}]"#;
        let entries: Vec<ProxyEntry> = serde_json::from_str(json).unwrap();
        assert_eq!(entries[0].recovery, RecoveryState::default());
        assert!(
            recovery_probe_due(&entries[0], chrono::Utc::now()),
            "旧数据没有 next_probe_at，应视为立即可探"
        );
    }
}

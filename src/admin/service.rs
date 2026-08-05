//! Admin API 业务逻辑服务

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Duration, Timelike, Utc};
use parking_lot::Mutex;
use serde::Deserialize;
use uuid::Uuid;

use crate::admin::balance_cache::{
    BalanceCache, CachedBalance, SharedBalanceCache, BALANCE_CACHE_TTL_SECS,
};
use crate::http_client::ProxyConfig;
use crate::kiro::auth::idc::{self, BUILDER_ID_START_URL};
use crate::kiro::auth::social;
use crate::kiro::error::UpstreamRateLimitError;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::model::credentials::{normalize_import_auth_method, validate_external_idp_endpoint};
use crate::kiro::token_manager::{
    CredentialUpdate, MultiTokenManager, RefreshTokenInvalidError,
};
use crate::model::config::Config;

use super::error::AdminServiceError;
use super::proxy_pool::{GetUrlResult, ProxyPoolManager};
use super::types::{
    AccountRpmLimitConfigResponse, AccountThrottleConfigResponse, AddCredentialRequest,
    AddCredentialResponse,
    AssignProxyRequest, AssignRoundRobinResponse, AvailableModelItem, AvailableModelsResponse,
    BalanceResponse, BatchAddProxyRequest, BatchImportEvent,
    CheckRateLimitRequest, CredentialStatusItem, CredentialsStatusResponse, EnableOverageAllResult,
    GitHubRateLimitInfo, ImageUpdateResponse, ExportedAccount, ExportedCredentials,
    CredentialsExportResponse,
    LoadBalancingModeResponse, LogGovernanceConfigResponse, ModelTestRequest, ModelTestResponse,
    PollIdcLoginResponse,
    ProxyCheckAllResponse, ProxyCheckResponse, ProxyPoolEntry, ProxyPoolResponse,
    QuotaExceededResult, SetAccountRpmLimitConfigRequest, SetAccountThrottleConfigRequest, SetLoadBalancingModeRequest,
    SetLogGovernanceConfigRequest, SetModelSyncSettingsRequest, SetUpdateConfigRequest,
    StartIdcLoginRequest, StartIdcLoginResponse, StartSocialLoginRequest,
    StartSocialLoginResponse, UpdateCheckInfo, UpdateConfigResponse, UpdateCredentialRequest,
    UpdateRefreshTokenRequest,
};

/// 在线检查更新结果缓存时间（秒），30 分钟。
/// 在线检查更新结果缓存时间（秒），30 分钟。
/// Docker Hub 的 tags 接口对匿名访问有 IP 维度的限流，30 分钟 TTL 既能让用户
/// 看到红点提醒，又能避免短时间内重复请求被限流。
const UPDATE_CHECK_TTL_SECS: i64 = 1800;

/// 单条凭据导入结果（服务端内部用，映射为 SSE 事件）
pub(crate) enum ImportStatus {
    Verified,
    /// 直接导入（未验活）成功
    Imported,
    Duplicate,
    Failed,
}

pub(crate) struct ImportItemResult {
    pub status: ImportStatus,
    pub credential_id: Option<u64>,
    pub email: Option<String>,
    pub balance: Option<BalanceResponse>,
    pub error: Option<String>,
    pub rolled_back: bool,
}

impl ImportItemResult {
    /// 转换为 SSE 事件（携带在数组中的下标）
    pub fn into_event(self, index: usize) -> BatchImportEvent {
        let status = match self.status {
            ImportStatus::Verified => "verified",
            ImportStatus::Imported => "imported",
            ImportStatus::Duplicate => "duplicate",
            ImportStatus::Failed => "failed",
        }
        .to_string();
        BatchImportEvent {
            index: Some(index),
            status,
            credential_id: self.credential_id,
            email: self.email,
            usage: self.balance.as_ref().map(|b| {
                format!(
                    "{:.0}/{:.0}",
                    b.current_usage.round(),
                    b.usage_limit.round()
                )
            }),
            subscription: self.balance.and_then(|b| b.subscription_title),
            error: self.error,
            rolled_back: if self.rolled_back { Some(true) } else { None },
            summary: None,
        }
    }
}

/// 缓存的"检查更新"结果
#[derive(Debug, Clone)]
struct CachedUpdateCheck {
    /// 缓存时间
    cached_at: DateTime<Utc>,
    /// 拉取到的更新信息
    info: UpdateCheckInfo,
}

#[derive(Debug, Clone)]
struct RuntimeUpdateConfig {
    previous_version: Option<String>,
    last_applied_at: Option<String>,
    github_token: Option<String>,
    auto_apply: bool,
    auto_apply_time: String,
}

impl RuntimeUpdateConfig {
    fn from_config(config: &Config) -> Self {
        Self {
            previous_version: config.update_previous_version.clone(),
            last_applied_at: config.update_last_applied_at.clone(),
            github_token: config.github_token.clone(),
            auto_apply: config.update_auto_apply,
            auto_apply_time: config.update_auto_apply_time.clone(),
        }
    }

    fn response(&self) -> UpdateConfigResponse {
        UpdateConfigResponse {
            previous_version: self.previous_version.clone(),
            last_applied_at: self.last_applied_at.clone(),
            github_token_set: self
                .github_token
                .as_deref()
                .map(|t| !t.trim().is_empty())
                .unwrap_or(false),
            auto_apply: self.auto_apply,
            auto_apply_time: self.auto_apply_time.clone(),
        }
    }
}

/// 模型同步的运行时配置。不放进不可变的 Config clone
/// （MultiTokenManager 持有的是 clone，见 token_manager.rs 中 `config:` 字段），
/// 否则 PATCH 无法热生效。
#[derive(Debug, Clone)]
pub struct ModelSyncSettings {
    pub enabled: bool,
    pub time: String,
    pub probe_credential_id: Option<u64>,
    pub allow_passthrough: bool,
}

impl ModelSyncSettings {
    /// pub(crate)：main.rs 需要在 admin 分支之外先建出共享 holder 给调度器用。
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            enabled: config.model_sync_enabled,
            time: config.model_sync_time.clone(),
            probe_credential_id: config.model_sync_probe_credential_id,
            allow_passthrough: config.allow_unknown_model_passthrough,
        }
    }
}

/// Admin 服务
///
/// 封装所有 Admin API 的业务逻辑
pub struct AdminService {
    token_manager: Arc<MultiTokenManager>,
    balance_cache: SharedBalanceCache,
    /// 余额快照存储（DuckDB 时序），用于余额趋势与消耗速率。未注入时不记录历史。
    balance_store: Option<crate::admin::balance_store::SharedBalanceStore>,
    /// 已注册的端点名称集合（用于 add_credential 校验）
    known_endpoints: HashSet<String>,
    /// 代理 IP 池管理器（与 KiroProvider 的请求级反馈共享同一实例）
    proxy_pool: Arc<ProxyPoolManager>,
    /// 运维反馈编排（可选）：探测自动禁用时记事件 + 解绑受影响凭据
    ops: Option<crate::admin::ops::SharedOpsRuntime>,
    /// 在线镜像更新运行时配置
    update_config: Mutex<RuntimeUpdateConfig>,
    /// 最近一次"检查更新"结果（带 TTL，用于减少 GitHub API 调用）
    update_check_cache: Mutex<Option<CachedUpdateCheck>>,
    /// 进行中的 IdC 设备授权会话
    idc_sessions: Arc<Mutex<HashMap<String, IdcAuthSession>>>,
    /// 进行中的 Social 登录会话
    social_sessions: Arc<Mutex<HashMap<String, SocialAuthSession>>>,
    /// 请求链路追踪存储（用于日志治理：开关 + 保留天数运行时可改）
    trace_store: Option<crate::admin::trace_db::SharedTraceStore>,
    /// 用量存储（用于日志治理：保留天数运行时可改）
    usage_recorder: Option<crate::admin::usage_store::SharedUsageStore>,
    /// 模型同步运行时配置（可热改）。
    /// `Arc` 是因为同步调度器创建在 admin 分支之外（spec §6.1），它和这里必须
    /// 读同一份 holder，否则 UI 改了开关调度器看不到，得重启才生效。
    model_sync: Arc<parking_lot::RwLock<ModelSyncSettings>>,
    /// config.json 写锁。既有的 `update_config_file` 是无保护的
    /// load-modify-save，本锁用于串行化本任务新增的写路径，避免并发丢失更新。
    config_write_lock: tokio::sync::Mutex<()>,
    /// models.json 存储层。由启动流程注入（见 `with_model_registry`）；
    /// 未注入时 `/models` 相关端点返回明确的未初始化错误，而不是假装成功。
    model_registry_store: Option<Arc<crate::anthropic::model_registry_store::ModelRegistryStore>>,
    /// 手动同步用的同步服务。同上，由启动流程注入。
    model_sync_service: Option<Arc<crate::anthropic::model_sync::ModelSyncService>>,
    /// Kiro Provider。`POST /models/test` 要发真实上游请求，必须与生产流量
    /// 走同一个 provider（同一份账号池 / 代理 / 端点解析），否则测出来的
    /// 不是「本代理实际会发生什么」。未注入时该端点返回明确的未配置错误。
    kiro_provider: Option<Arc<crate::kiro::provider::KiroProvider>>,
}

/// Social 登录会话状态
struct SocialAuthSession {
    auth_endpoint: String,
    /// 发起时生成的 state，用于 CSRF 验证
    state: String,
    code_verifier: String,
    redirect_uri: String,
    expires_at: DateTime<Utc>,
    /// 收到 OAuth 回调时的数据（code + login_option + path）
    callback_rx: tokio::sync::Mutex<tokio::sync::oneshot::Receiver<social::OAuthCallbackData>>,
    cred_template: KiroCredentials,
    proxy: Option<ProxyConfig>,
    /// Drop 时自动关闭回调服务器并释放端口
    _server_handle: social::ServerHandle,
    /// 重新登录时更新此凭据的 Token（非 None 时更新已有凭据而非创建新凭据）
    relogin_target_id: Option<u64>,
}

/// IdC 设备授权会话状态
struct IdcAuthSession {
    region: String,
    client_id: String,
    client_secret: String,
    device_code: String,
    expires_at: DateTime<Utc>,
    poll_interval: i64,
    /// 登录成功后写入的凭据配置
    cred_template: KiroCredentials,
    /// 用于发起 token 请求的代理
    proxy: Option<ProxyConfig>,
    /// 重新登录时更新此凭据的 Token（非 None 时更新已有凭据而非创建新凭据）
    relogin_target_id: Option<u64>,
}

/// 解析自动更新触发时间（`HH:MM`，本地 24 小时制）。允许 `H:M` 简写，
/// 例如 `3:0`；解析失败时返回原字符串，便于错误信息提示。
/// pub(crate)：模型同步调度器建在 admin 分支之外（main.rs），需要同一套 HH:MM 解析。
pub(crate) fn parse_auto_apply_time(value: &str) -> Result<(u32, u32), AdminServiceError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AdminServiceError::InvalidCredential(
            "自动更新时间不能为空".to_string(),
        ));
    }
    let mut parts = trimmed.splitn(2, ':');
    let hour_str = parts.next().unwrap_or("");
    let minute_str = parts.next().unwrap_or("");
    let hour: u32 = hour_str.parse().map_err(|_| {
        AdminServiceError::InvalidCredential(format!(
            "自动更新时间格式无效：{}（应为 HH:MM）",
            value
        ))
    })?;
    let minute: u32 = minute_str.parse().map_err(|_| {
        AdminServiceError::InvalidCredential(format!(
            "自动更新时间格式无效：{}（应为 HH:MM）",
            value
        ))
    })?;
    if hour > 23 || minute > 59 {
        return Err(AdminServiceError::InvalidCredential(format!(
            "自动更新时间超出范围：{}（HH 0-23，MM 0-59）",
            value
        )));
    }
    Ok((hour, minute))
}

/// 把 HH:MM 规范化成 `HH:MM`（两位补零），方便存储和比较。
fn normalize_auto_apply_time(value: &str) -> Result<String, AdminServiceError> {
    let (h, m) = parse_auto_apply_time(value)?;
    Ok(format!("{:02}:{:02}", h, m))
}

/// 校验全局代理 URL
///
/// 与凭据级的区别：不放行 "direct" —— 全局代理用 `None` 表达「不走代理」，
/// 存进 `"direct"` 只会得到一个连不上的代理配置。
fn validate_global_proxy_url(url: &str) -> Result<(), AdminServiceError> {
    crate::http_client::validate_proxy_url(url)
        .map_err(|e| AdminServiceError::InvalidCredential(e.to_string()))
}

/// 校验凭据级 proxy_url（放行 "direct"），并映射成 admin 的错误类型
fn validate_credential_proxy_url(url: &str) -> Result<(), AdminServiceError> {
    KiroCredentials::validate_proxy_url(url)
        .map_err(|e| AdminServiceError::InvalidCredential(e.to_string()))
}

/// 解析登录流程要用的代理：请求级 > 全局 > 无。
///
/// 语义必须与 [`KiroCredentials::effective_proxy`] 一致——尤其是 "direct" 表示
/// **显式不走代理**（而非回退全局）。此前这里直接 `ProxyConfig::new(url)`，
/// 传 "direct" 会构造出一个连不上的代理。
fn resolve_login_proxy(
    req_proxy_url: Option<&str>,
    global_proxy: Option<ProxyConfig>,
) -> Option<ProxyConfig> {
    match req_proxy_url.filter(|u| !u.is_empty()) {
        Some(u) if u.eq_ignore_ascii_case(KiroCredentials::PROXY_DIRECT) => None,
        Some(u) => Some(ProxyConfig::new(u)),
        None => global_proxy,
    }
}

/// GitHub `repos/{owner}/{repo}/releases/tags/{tag}` 返回 JSON 中我们关心
/// 的字段，用于在「检查更新」结果里附带本次发布的 changelog。
#[derive(Debug, Deserialize)]
struct GitHubRelease {
    #[serde(default)]
    name: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    html_url: String,
    #[serde(default)]
    published_at: String,
    #[serde(default)]
    tag_name: String,
}

/// 比较两个 semver 字符串。仅按 `MAJOR.MINOR.PATCH` 三段数字比较，忽略
/// 预发布后缀；解析失败的段当作 0 处理（最坏情况下"无更新"）。
fn compare_semver(current: &str, latest: &str) -> std::cmp::Ordering {
    parse_semver_core(current).cmp(&parse_semver_core(latest))
}

/// 解析 semver 三段数字，解析失败的段作 0；用于 latest tag 的稳定排序。
fn parse_semver_core(value: &str) -> [u32; 3] {
    let core = value
        .trim_start_matches('v')
        .split(['-', '+'])
        .next()
        .unwrap_or("");
    let mut out = [0u32; 3];
    for (i, part) in core.splitn(3, '.').enumerate() {
        if i >= 3 {
            break;
        }
        out[i] = part.parse::<u32>().unwrap_or(0);
    }
    out
}

/// 当前构建类型。在线更新走"下载 GitHub Releases 二进制 + 进程退出由
/// docker restart policy 接管重启"的方案。
const BUILD_TYPE: &str = "binary";

/// 暂存路径：下载到 `<exe>.staged`，原子替换前再 mv 到 `<exe>`。
/// 暂存路径：下载到 `<exe>.staged-<version>`，原子替换前再 mv 到 `<exe>`。
/// 文件名中带版本号，便于 apply 复用 pull 已下载的二进制（命中时跳过重新下载）。
fn staged_binary_path(exe: &std::path::Path, version: &str) -> std::path::PathBuf {
    let mut s = exe.as_os_str().to_os_string();
    s.push(format!(".staged-{}", version.trim().trim_start_matches('v')));
    std::path::PathBuf::from(s)
}

/// 清理目标版本之外的所有 staged 文件，避免之前下载的旧版本残留干扰。
fn cleanup_other_staged(exe: &std::path::Path, keep_version: &str) {
    let dir = match exe.parent() {
        Some(d) => d,
        None => return,
    };
    let exe_name = match exe.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return,
    };
    let keep = format!(
        "{}.staged-{}",
        exe_name,
        keep_version.trim().trim_start_matches('v')
    );
    let prefix = format!("{}.staged-", exe_name);
    let entries = match std::fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        if name.starts_with(&prefix) && name != keep {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// 将单个凭据映射为嵌套 `Account` 结构
///
/// API Key 凭据无 refreshToken，导出格式无对应字段，跳过。
/// 空字符串字段会被过滤，保持导出 JSON 整洁。
fn credential_to_export_account(cred: KiroCredentials) -> Option<ExportedAccount> {
    let refresh_token = cred
        .refresh_token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)?;

    fn non_empty(value: Option<String>) -> Option<String> {
        value
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    // authMethod 规范化："idc" → "IdC"，其余按 social 处理
    // authMethod 规范化："idc" → "IdC"，"external_idp" 保留，其余按 social 处理
    let auth_method = non_empty(cred.auth_method.clone()).map(|m| {
        if m.eq_ignore_ascii_case("idc")
            || m.eq_ignore_ascii_case("builder-id")
            || m.eq_ignore_ascii_case("iam")
        {
            "IdC".to_string()
        } else if cred.is_external_idp_credential() {
            "external_idp".to_string()
        } else {
            "social".to_string()
        }
    });
    let is_idc = auth_method.as_deref() == Some("IdC");
    let is_external_idp = auth_method.as_deref() == Some("external_idp");

    let provider = non_empty(cred.provider.clone());
    // idp 与 provider 同义；缺失时按认证方式回退到合法的身份提供商
    let idp = provider.clone().unwrap_or_else(|| {
        if is_external_idp {
            "AzureAD"
        } else if is_idc {
            "BuilderId"
        } else {
            "Google"
        }
        .to_string()
    });

    let status = if cred.disabled {
        "unknown".to_string()
    } else {
        "active".to_string()
    };

    // expiresAt → 毫秒时间戳（解析失败或缺失时为 0）
    let expires_at_ms = cred
        .expires_at
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0);

    // 订阅：最小可用结构（type + 原始 title）
    let subscription = serde_json::json!({
        "type": subscription_type_from_title(cred.subscription_title.as_deref()),
        "title": cred.subscription_title,
    });
    let now_ms = Utc::now().timestamp_millis();
    let usage = serde_json::json!({
        "current": 0,
        "limit": 0,
        "percentUsed": 0,
        "lastUpdated": now_ms,
    });

    // 仅导出真实 profileArn，跳过 BuilderID 占位符
    let profile_arn = cred.effective_profile_arn().map(str::to_string);

    let credentials = ExportedCredentials {
        access_token: non_empty(cred.access_token).unwrap_or_default(),
        csrf_token: String::new(),
        refresh_token: Some(refresh_token),
        client_id: non_empty(cred.client_id),
        client_secret: non_empty(cred.client_secret),
        region: non_empty(cred.region.clone())
            .or_else(|| non_empty(cred.auth_region.clone()))
            .or_else(|| non_empty(cred.api_region.clone())),
        start_url: non_empty(cred.start_url.clone()),
        token_endpoint: non_empty(cred.token_endpoint.clone()),
        issuer_url: non_empty(cred.issuer_url.clone()),
        scopes: non_empty(cred.scopes.clone()),
        expires_at: expires_at_ms,
        auth_method,
        provider: provider.clone(),
    };

    Some(ExportedAccount {
        id: uuid::Uuid::new_v4().to_string(),
        email: non_empty(cred.email).unwrap_or_default(),
        nickname: None,
        idp,
        user_id: None,
        profile_arn,
        machine_id: non_empty(cred.machine_id),
        credentials,
        subscription,
        usage,
        tags: Vec::new(),
        status,
        created_at: now_ms,
        last_used_at: now_ms,
    })
}

/// 由订阅标题推断 `SubscriptionType`（粗粒度，导入方刷新后会自行校正）
fn subscription_type_from_title(title: Option<&str>) -> &'static str {
    let Some(title) = title else { return "Free" };
    let u = title.to_uppercase();
    if u.contains("FREE") {
        "Free"
    } else if u.contains("PRO+") || u.contains("PRO PLUS") || u.contains("PRO_PLUS") {
        "Pro_Plus"
    } else if u.contains("POWER") || u.contains("ENTERPRISE") || u.contains("TEAM") {
        "Enterprise"
    } else if u.contains("PRO") {
        "Pro"
    } else {
        "Free"
    }
}

/// GitHub Release 仓库名（owner/repo）。
/// 在线更新所需的版本号、changelog、二进制资产都从这里取。
/// 与 [`binary_update::GITHUB_REPO`] 必须一致，且指向本 fork——否则自动更新会用
/// 上游二进制覆盖本 fork 的改动。
const GITHUB_RELEASES_REPO: &str = "jingxiuman/kiro.rs";

impl AdminService {
    pub fn new(
        token_manager: Arc<MultiTokenManager>,
        known_endpoints: impl IntoIterator<Item = String>,
        proxy_pool: Arc<ProxyPoolManager>,
        balance_cache: SharedBalanceCache,
    ) -> Self {
        let update_config = RuntimeUpdateConfig::from_config(token_manager.config());
        let model_sync = ModelSyncSettings::from_config(token_manager.config());

        let svc = Self {
            token_manager,
            balance_cache,
            balance_store: None,
            known_endpoints: known_endpoints.into_iter().collect(),
            proxy_pool,
            ops: None,
            update_config: Mutex::new(update_config),
            update_check_cache: Mutex::new(None),
            idc_sessions: Arc::new(Mutex::new(HashMap::new())),
            social_sessions: Arc::new(Mutex::new(HashMap::new())),
            trace_store: None,
            usage_recorder: None,
            model_sync: Arc::new(parking_lot::RwLock::new(model_sync)),
            config_write_lock: tokio::sync::Mutex::new(()),
            model_registry_store: None,
            model_sync_service: None,
            kiro_provider: None,
        };

        // 后台任务：每 5 分钟清理过期的登录会话，防止内存泄漏
        {
            let idc = Arc::clone(&svc.idc_sessions);
            let social = Arc::clone(&svc.social_sessions);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
                loop {
                    interval.tick().await;
                    let now = Utc::now();
                    idc.lock().retain(|_, s| now < s.expires_at);
                    social.lock().retain(|_, s| now < s.expires_at);
                }
            });
        }

        svc
    }

    /// 暴露 TokenManager 给 handlers（分组管理需要 count / rename / remove 凭据 groups 字段）
    pub fn token_manager(&self) -> &Arc<MultiTokenManager> {
        &self.token_manager
    }

    /// 注入运维反馈编排（探测自动禁用时记事件 + 解绑凭据；ops 统计 API 也从这里取）
    pub fn with_ops(mut self, ops: crate::admin::ops::SharedOpsRuntime) -> Self {
        self.ops = Some(ops);
        self
    }

    /// 运维编排句柄（供 handlers 访问 ops 统计）
    pub fn ops(&self) -> Option<&crate::admin::ops::SharedOpsRuntime> {
        self.ops.as_ref()
    }

    /// 注入余额快照存储（DuckDB）。不注入则只有 JSON 当前值缓存，无历史趋势。
    pub fn with_balance_store(
        mut self,
        store: Option<crate::admin::balance_store::SharedBalanceStore>,
    ) -> Self {
        self.balance_store = store;
        self
    }

    /// 余额快照存储句柄（供 handlers 查历史/速率）
    pub fn balance_store(
        &self,
    ) -> Option<&crate::admin::balance_store::SharedBalanceStore> {
        self.balance_store.as_ref()
    }

    /// 注入日志治理句柄（trace 存储 + 用量记录器），用于运行时改保留期/开关。
    pub fn with_log_governance(
        mut self,
        trace_store: Option<crate::admin::trace_db::SharedTraceStore>,
        usage_recorder: Option<crate::admin::usage_store::SharedUsageStore>,
    ) -> Self {
        self.trace_store = trace_store;
        self.usage_recorder = usage_recorder;
        self
    }

    /// 获取所有凭据状态
    pub fn get_all_credentials(&self) -> CredentialsStatusResponse {
        let snapshot = self.token_manager.snapshot();
        let default_endpoint = self.token_manager.config().default_endpoint.clone();

        // 一次性快照余额缓存，避免 N 次加锁
        let balance_snapshot: HashMap<u64, CachedBalance> = self.balance_cache.snapshot().entries;
        let now_ts = Utc::now().timestamp() as f64;

        // balanced 模式下 `current_id` 只是内部调度指针（每次请求重新选号），
        // 不存在「当前活跃账号」这个概念；对外暴露它等于显示假信息。
        // 固定为 0，与「无当前凭据」的既有取值（凭据全空时的初值）一致。
        let exposed_current_id = if matches!(
            self.token_manager.get_load_balancing_mode().as_str(),
            "balanced" | "weighted"
        ) {
            0
        } else {
            snapshot.current_id
        };

        let mut credentials: Vec<CredentialStatusItem> = snapshot
            .entries
            .into_iter()
            .map(|entry| {
                let (balance, balance_updated_at) = balance_snapshot
                    .get(&entry.id)
                    .filter(|c| (now_ts - c.cached_at) < BALANCE_CACHE_TTL_SECS as f64)
                    .map(|c| (Some(c.data.clone()), Some(c.cached_at)))
                    .unwrap_or((None, None));

                CredentialStatusItem {
                    id: entry.id,
                    priority: entry.priority,
                    disabled: entry.disabled,
                    failure_count: entry.failure_count,
                    total_failure_count: entry.total_failure_count,
                    is_current: exposed_current_id != 0 && entry.id == exposed_current_id,
                    expires_at: entry.expires_at,
                    auth_method: entry.auth_method,
                    provider: entry.provider,
                    has_profile_arn: entry.has_profile_arn,
                    refresh_token_hash: entry.refresh_token_hash,
                    api_key_hash: entry.api_key_hash,
                    masked_api_key: entry.masked_api_key,
                    email: entry.email,
                    success_count: entry.success_count,
                    last_used_at: entry.last_used_at.clone(),
                    has_proxy: entry.has_proxy,
                    proxy_url: entry.proxy_url,
                    refresh_failure_count: entry.refresh_failure_count,
                    disabled_reason: entry.disabled_reason,
                    endpoint: entry.endpoint.unwrap_or_else(|| default_endpoint.clone()),
                    groups: entry.groups,
                    source_channel: entry.source_channel,
                    balance,
                    balance_updated_at,
                }
            })
            .collect();

        // 按优先级排序（数字越小优先级越高）
        credentials.sort_by_key(|c| c.priority);

        CredentialsStatusResponse {
            total: snapshot.total,
            available: snapshot.available,
            current_id: exposed_current_id,
            credentials,
        }
    }

    /// 导出凭据为兼容 JSON（嵌套 `Account` 格式）
    ///
    /// 返回的结构体含 refreshToken、accessToken、clientSecret 等敏感字段，
    /// 调用方需自行保证传输与存储安全；按 priority 升序排序，与 UI 列表一致。
    /// `id_filter` 为 None 时导出全部凭据；为 Some 时仅导出集合内的 ID。
    pub fn export_credentials(
        &self,
        id_filter: Option<&HashSet<u64>>,
    ) -> CredentialsExportResponse {
        let mut credentials = self.token_manager.clone_all_credentials();
        if let Some(filter) = id_filter {
            credentials.retain(|c| c.id.map(|id| filter.contains(&id)).unwrap_or(false));
        }
        credentials.sort_by_key(|c| c.priority);

        let accounts = credentials
            .into_iter()
            .filter_map(credential_to_export_account)
            .collect();

        CredentialsExportResponse {
            version: "1.8.3".to_string(),
            exported_at: Utc::now().timestamp_millis(),
            accounts,
            groups: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// 一键禁用所有"已超额"的凭据（remaining ≤ 0 或 usage_percentage ≥ 100）
    ///
    /// 数据来源是 `balance_cache`，所以前端在调用前最好先触发一次"查询信息"
    /// 或等待后台调度器完成首次刷新。返回 (禁用数量, 跳过数量, 已超额未禁用名单)。
    pub fn disable_quota_exceeded(&self) -> QuotaExceededResult {
        let snapshot = self.token_manager.snapshot();
        let current_id = snapshot.current_id;

        let cache_snapshot: HashMap<u64, CachedBalance> = self.balance_cache.snapshot().entries;
        let now_ts = Utc::now().timestamp() as f64;

        let mut disabled_ids: Vec<u64> = Vec::new();
        let mut skipped_ids: Vec<u64> = Vec::new();
        let mut switched_current = false;

        for entry in snapshot.entries.iter() {
            if entry.disabled {
                continue;
            }
            let cached = match cache_snapshot.get(&entry.id) {
                Some(c) if (now_ts - c.cached_at) < BALANCE_CACHE_TTL_SECS as f64 => c,
                _ => continue,
            };
            let exceeded = cached.data.remaining <= 0.0 || cached.data.usage_percentage >= 100.0;
            if !exceeded {
                continue;
            }
            match self.token_manager.disable_quota_exceeded(entry.id) {
                Ok(()) => {
                    disabled_ids.push(entry.id);
                    if entry.id == current_id {
                        switched_current = true;
                    }
                }
                Err(e) => {
                    tracing::warn!("一键超额：禁用凭据 #{} 失败: {}", entry.id, e);
                    skipped_ids.push(entry.id);
                }
            }
        }

        if switched_current {
            let _ = self.token_manager.switch_to_next();
        }

        QuotaExceededResult {
            disabled_ids,
            skipped_ids,
        }
    }

    /// 设置凭据禁用状态
    pub fn set_disabled(&self, id: u64, disabled: bool) -> Result<(), AdminServiceError> {
        // 先获取当前凭据 ID，用于判断是否需要切换
        let snapshot = self.token_manager.snapshot();
        let current_id = snapshot.current_id;

        self.token_manager
            .set_disabled(id, disabled)
            .map_err(|e| self.classify_error(e, id))?;

        // 只有禁用的是当前凭据时才尝试切换到下一个
        if disabled && id == current_id {
            let _ = self.token_manager.switch_to_next();
        }
        Ok(())
    }

    /// 设置凭据优先级
    pub fn set_priority(&self, id: u64, priority: u32) -> Result<(), AdminServiceError> {
        self.token_manager
            .set_priority(id, priority)
            .map_err(|e| self.classify_error(e, id))
    }

    /// 重置失败计数并重新启用
    pub fn reset_and_enable(&self, id: u64) -> Result<(), AdminServiceError> {
        self.token_manager
            .reset_and_enable(id)
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn clear_throttle(&self, id: u64) -> Result<(), AdminServiceError> {
        self.token_manager
            .clear_throttle(id)
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn reset_success_count(&self, id: Option<u64>) -> Result<u32, AdminServiceError> {
        self.token_manager
            .reset_success_count(id)
            .map_err(|e| self.classify_error(e, id.unwrap_or(0)))
    }

    /// 获取凭据余额（带缓存）
    pub async fn get_balance(&self, id: u64) -> Result<BalanceResponse, AdminServiceError> {
        // 先查缓存
        {
            let cache = self.balance_cache.snapshot().entries;
            if let Some(cached) = cache.get(&id) {
                let now = Utc::now().timestamp() as f64;
                if (now - cached.cached_at) < BALANCE_CACHE_TTL_SECS as f64 {
                    tracing::debug!("凭据 #{} 余额命中缓存", id);
                    return Ok(cached.data.clone());
                }
            }
        }

        // 缓存未命中或已过期，从上游获取
        let balance = self.fetch_balance(id).await?;

        // 更新缓存
        self.balance_cache.upsert_one(
            id,
            CachedBalance {
                cached_at: Utc::now().timestamp() as f64,
                data: balance.clone(),
            },
        );

        Ok(balance)
    }

    /// 从上游获取余额（无缓存）
    async fn fetch_balance(&self, id: u64) -> Result<BalanceResponse, AdminServiceError> {
        let usage = self
            .token_manager
            .get_usage_limits_for(id)
            .await
            .map_err(|e| self.classify_balance_error(e, id))?;

        let current_usage = usage.current_usage();
        let usage_limit = usage.usage_limit();
        // 允许 remaining 显示为负值：开启超额后实际使用可能超过限额，
        // 直接保留差值便于在 UI 中体现"已欠多少"。
        let remaining = usage_limit - current_usage;
        // usage_percentage 同理保留真实值，超额时 > 100%。
        let usage_percentage = if usage_limit > 0.0 {
            current_usage / usage_limit * 100.0
        } else {
            0.0
        };

        Ok(BalanceResponse {
            id,
            subscription_title: usage.subscription_title().map(|s| s.to_string()),
            current_usage,
            usage_limit,
            remaining,
            usage_percentage,
            next_reset_at: usage.next_date_reset,
            overage_enabled: usage.overage_enabled(),
            overage_capable: usage.overage_capable(),
            overage_capability_raw: usage
                .subscription_info
                .as_ref()
                .and_then(|s| s.overage_capability.clone()),
        })
    }

    /// 获取指定凭据当前可用的模型列表（按需实时查询上游，不缓存）
    pub async fn get_available_models(
        &self,
        id: u64,
    ) -> Result<AvailableModelsResponse, AdminServiceError> {
        let resp = self
            .token_manager
            .get_available_models_for(id)
            .await
            .map_err(|e| self.classify_balance_error(e, id))?;

        let models = resp
            .models
            .into_iter()
            .map(|m| AvailableModelItem {
                model_id: m.model_id,
                model_name: m.model_name,
                description: m.description,
                max_input_tokens: m.token_limits.and_then(|t| t.max_input_tokens),
            })
            .collect();

        Ok(AvailableModelsResponse { id, models })
    }

    /// 批量刷新所有非禁用凭据的余额（用于后台调度）
    ///
    /// 串行执行以避免对上游产生瞬时高并发，每次成功的查询都会更新内存缓存
    /// 与磁盘缓存。失败的条目不会清空旧缓存，调用方可在下次轮询时重试。
    pub async fn refresh_all_balances(&self) -> (usize, usize) {
        let snapshot = self.token_manager.snapshot();
        let mut success = 0_usize;
        let mut failure = 0_usize;
        // 本轮拿到的余额，批量落一次时序库（失败不影响余额刷新本身）
        let mut history: Vec<crate::admin::balance_store::BalanceSnapshot> = Vec::new();
        // 以现有快照打底：失败的账号沿用旧值，成功的整轮结束后一次性覆盖发布
        let mut collected: HashMap<u64, CachedBalance> = self.balance_cache.snapshot().entries;

        for entry in snapshot.entries.into_iter() {
            if entry.disabled {
                continue;
            }
            match self.fetch_balance(entry.id).await {
                Ok(balance) => {
                    history.push(crate::admin::balance_store::BalanceSnapshot {
                        credential_id: entry.id,
                        subscription_title: balance
                            .subscription_title
                            .clone()
                            .unwrap_or_default(),
                        current_usage: balance.current_usage,
                        usage_limit: balance.usage_limit,
                        remaining: balance.remaining,
                        usage_percentage: balance.usage_percentage,
                        next_reset_at: balance.next_reset_at.map(|v| v as i64),
                    });
                    collected.insert(
                        entry.id,
                        CachedBalance {
                            cached_at: Utc::now().timestamp() as f64,
                            data: balance,
                        },
                    );
                    success += 1;
                }
                Err(e) => {
                    tracing::warn!("后台刷新凭据 #{} 余额失败: {}", entry.id, e);
                    failure += 1;
                }
            }
            // 节流，避免上游限流
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        }

        if success > 0 {
            self.balance_cache.publish(collected);
            if let Some(store) = &self.balance_store {
                store.record_batch(&history);
            }
        }
        (success, failure)
    }

    /// 启动余额后台刷新调度器
    ///
    /// - 启动后立刻执行一次刷新
    /// - 之后按 `interval` 周期循环刷新
    /// - 调用方持有 `Arc<Self>` 即可，任务在后台 tokio runtime 上运行
    pub fn start_balance_refresher(self: &Arc<Self>, interval: std::time::Duration) {
        let svc = Arc::clone(self);
        tokio::spawn(async move {
            // 启动后稍等片刻，让上游/Token Manager 准备就绪
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            loop {
                let started = std::time::Instant::now();
                let (ok, err) = svc.refresh_all_balances().await;
                tracing::info!(
                    "余额后台刷新完成：成功 {}，失败 {}，耗时 {:.1}s",
                    ok,
                    err,
                    started.elapsed().as_secs_f32()
                );
                tokio::time::sleep(interval).await;
            }
        });
    }

    /// 启动代理池后台健康检查调度器
    ///
    /// - 启动后稍等片刻再执行首次探测
    /// - 之后按 `interval` 周期循环，对所有已启用代理并发探测
    /// - 连续探测失败达阈值的代理由 `check_all` 内部自动禁用
    pub fn start_proxy_health_checker(self: &Arc<Self>, interval: std::time::Duration) {
        let svc = Arc::clone(self);
        tokio::spawn(async move {
            // 启动后稍等片刻，让网络/代理就绪
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            loop {
                let started = std::time::Instant::now();
                let summary = svc.proxy_pool.check_all().await;
                tracing::info!(
                    "代理池健康检查完成：健康 {}，异常 {}，本轮自动禁用 {}，恢复探测 {}，放回 {}，耗时 {:.1}s",
                    summary.healthy,
                    summary.unhealthy,
                    summary.auto_disabled,
                    summary.recovery_probed,
                    summary.newly_recovered.len(),
                    started.elapsed().as_secs_f32()
                );
                // 探测触发的自动禁用同样走处置流程：记事件 + 解绑受影响凭据
                if let Some(ops) = &svc.ops {
                    for (id, url) in &summary.newly_disabled {
                        ops.handle_probe_auto_disable(*id, url);
                    }
                    // 放回只记事件，不动凭据绑定（见 handle_probe_auto_recover）
                    for (id, url) in &summary.newly_recovered {
                        ops.handle_probe_auto_recover(*id, url);
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    /// 启动无人值守自动更新调度器。
    ///
    /// 任务始终运行，每分钟唤醒一次：
    /// - `update_auto_apply` 关闭时只是记录"未到点"，不做任何远端调用。
    /// - 开启时，比较当前本地时间与 `update_auto_apply_time`，命中目标分钟
    ///   就触发一次 `apply_image_update`。同一目标版本只会被自动应用一次。
    pub fn start_auto_update_scheduler(self: &Arc<Self>) {
        let svc = Arc::clone(self);
        tokio::spawn(async move {
            // 给 Docker socket / compose 元数据探测留点准备时间
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;

            // 同一分钟避免重复触发；记录最近一次应用过的"日期 + 版本"
            let mut last_run_marker: Option<String> = None;
            let mut last_applied_version: Option<String> = None;

            loop {
                let runtime = svc.update_config.lock().clone();
                if runtime.auto_apply {
                    let target = parse_auto_apply_time(&runtime.auto_apply_time).ok();
                    if let Some((target_hour, target_minute)) = target {
                        let now = chrono::Local::now();
                        let date_minute_marker = format!(
                            "{}-{:02}:{:02}",
                            now.format("%Y-%m-%d"),
                            now.hour(),
                            now.minute()
                        );

                        let hit = now.hour() == target_hour && now.minute() == target_minute;
                        let already_ran_this_minute = last_run_marker.as_deref()
                            == Some(date_minute_marker.as_str());

                        if hit && !already_ran_this_minute {
                            last_run_marker = Some(date_minute_marker);
                            let info = svc.check_update(true).await;
                            if info.has_update
                                && !info.latest_version.is_empty()
                                && last_applied_version.as_deref()
                                    != Some(info.latest_version.as_str())
                            {
                                tracing::info!(
                                    "自动更新：到达计划时间 {}，发现新版本 {}（当前 {}），开始应用",
                                    runtime.auto_apply_time,
                                    info.latest_version,
                                    info.current_version
                                );
                            match svc.apply_image_update().await {
                                    Ok(res) => {
                                        tracing::info!("自动更新完成：{}", res.message);
                                        last_applied_version = Some(info.latest_version);
                                    }
                                    Err(e) => {
                                        tracing::warn!("自动更新失败：{}", e);
                                    }
                                }
                            } else {
                                tracing::info!(
                                    "自动更新：到达计划时间 {}，但当前已是最新版本（{}）",
                                    runtime.auto_apply_time,
                                    info.current_version
                                );
                            }
                        }
                    } else {
                        tracing::warn!(
                            "自动更新时间配置无效：{}，跳过本轮检查",
                            runtime.auto_apply_time
                        );
                    }
                }

                // 30 秒粒度足以可靠命中目标分钟，又不会在系统时间漂移下错过
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });
    }

    /// 添加新凭据
    pub async fn add_credential(
        &self,
        req: AddCredentialRequest,
    ) -> Result<AddCredentialResponse, AdminServiceError> {
        // 默认获取余额（保持单条添加 / 登录路径的既有行为：加完即可见订阅等级）
        self.add_credential_inner(req, true).await
    }

    /// 添加凭据的核心实现。
    ///
    /// - `fetch_balance = true`：添加后主动拉取余额（含订阅等级 / 邮箱）并写入缓存，
    ///   既是"加完即可见"，也作为 API Key 的有效性校验（即"验活"）。
    /// - `fetch_balance = false`：跳过余额拉取，仅落库（"直接导入"路径），
    ///   订阅信息留待首次请求时按需获取。
    async fn add_credential_inner(
        &self,
        req: AddCredentialRequest,
        fetch_balance: bool,
    ) -> Result<AddCredentialResponse, AdminServiceError> {
        // 校验凭据级代理 URL（空串等价于未设置，交由后续逻辑处理）
        if let Some(ref u) = req.proxy_url
            && !u.is_empty()
        {
            validate_credential_proxy_url(u)?;
        }

        // 校验端点名：未指定则默认合法，指定则必须已注册
        if let Some(ref name) = req.endpoint
            && !self.known_endpoints.contains(name) {
                let mut known: Vec<&str> =
                    self.known_endpoints.iter().map(|s| s.as_str()).collect();
                known.sort();
                return Err(AdminServiceError::InvalidCredential(format!(
                    "未知端点 \"{}\"，已注册端点: {:?}",
                    name, known
                )));
            }

        // 规范化 auth_method：识别企业 SSO 别名；带 tokenEndpoint 但未声明时推断为 external_idp。
        let auth_method =
            normalize_import_auth_method(&req.auth_method, req.token_endpoint.as_deref());

        // 企业 SSO 导入校验（安全边界）：
        // - 必须同时具备 clientId 与 tokenEndpoint（refresh_external_idp_token 的前提）；
        // - tokenEndpoint / issuerUrl 必须过 Microsoft allow-list，防止把 refreshToken
        //   外发到内网 / 攻击者控制的主机（SSRF / 凭据外泄）。
        if auth_method == "external_idp" {
            let client_id_ok = req
                .client_id
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());
            let token_endpoint = req
                .token_endpoint
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let Some(token_endpoint) = token_endpoint else {
                return Err(AdminServiceError::InvalidCredential(
                    "企业 SSO (external_idp) 需要 clientId 和 tokenEndpoint".to_string(),
                ));
            };
            if !client_id_ok {
                return Err(AdminServiceError::InvalidCredential(
                    "企业 SSO (external_idp) 需要 clientId 和 tokenEndpoint".to_string(),
                ));
            }
            validate_external_idp_endpoint(token_endpoint).map_err(|e| {
                AdminServiceError::InvalidCredential(format!("tokenEndpoint 被拒绝: {}", e))
            })?;
            if let Some(issuer) = req
                .issuer_url
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                validate_external_idp_endpoint(issuer).map_err(|e| {
                    AdminServiceError::InvalidCredential(format!("issuerUrl 被拒绝: {}", e))
                })?;
            }
        }

        // 构建凭据对象
        let email = req.email.clone();
        let new_cred = KiroCredentials {
            id: None,
            access_token: req.access_token,
            refresh_token: req.refresh_token,
            profile_arn: req.profile_arn,
            expires_at: req.expires_at,
            auth_method: Some(auth_method),
            provider: req.provider,
            client_id: req.client_id,
            client_secret: req.client_secret,
            start_url: req.start_url,
            token_endpoint: req.token_endpoint,
            issuer_url: req.issuer_url,
            scopes: req.scopes,
            priority: req.priority,
            region: req.region,
            auth_region: req.auth_region,
            api_region: req.api_region,
            machine_id: req.machine_id,
            email: req.email,
            subscription_title: None, // 将在首次获取使用额度时自动更新
            proxy_url: req.proxy_url,
            proxy_username: req.proxy_username,
            proxy_password: req.proxy_password,
            disabled: false, // 新添加的凭据默认启用
            kiro_api_key: req.kiro_api_key,
            endpoint: req.endpoint,
            groups: req.groups,
            source_channel: req.source_channel,
        };

        // 调用 token_manager 添加凭据
        let credential_id = self
            .token_manager
            .add_credential(new_cred)
            .await
            .map_err(|e| self.classify_add_error(e))?;

        // 主动获取余额（含订阅等级 / 邮箱）并写入缓存，添加后立即可见，
        // 同时避免首次请求时 Free 账号绕过 Opus 模型过滤。
        // 仅验活路径需要；"直接导入"路径跳过以省掉这次上游往返。
        if fetch_balance
            && let Err(e) = self.get_balance(credential_id).await {
                tracing::warn!("添加凭据后刷新余额失败（不影响凭据添加）: {}", e);
            }

        Ok(AddCredentialResponse {
            success: true,
            message: format!("凭据添加成功，ID: {}", credential_id),
            credential_id,
            email,
        })
    }

    /// 批量导入的单条处理。
    ///
    /// - `verify = true`（验活路径）：add（内部 refresh + 缓存 balance）→ 显式取余额验活
    ///   → 失败回滚删除。镜像前端旧流程的"add → getCredentialBalance → 失败回滚"。
    /// - `verify = false`（直接导入路径）：仅 add 落库，不取余额、不回滚。
    ///
    /// 全部在服务端完成，便于在 `buffer_unordered` 下有界并发。
    pub async fn import_one_credential(
        &self,
        req: AddCredentialRequest,
        verify: bool,
    ) -> ImportItemResult {
        // 1. add：去重 / 未知端点 / token 刷新失败在此暴露，未插入即无需回滚。
        //    verify=false 时跳过内部余额拉取。
        let resp = match self.add_credential_inner(req, verify).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                let is_duplicate =
                    msg.contains("凭据已存在") || msg.contains("重复");
                return ImportItemResult {
                    status: if is_duplicate {
                        ImportStatus::Duplicate
                    } else {
                        ImportStatus::Failed
                    },
                    credential_id: None,
                    email: None,
                    balance: None,
                    error: Some(msg),
                    rolled_back: false,
                };
            }
        };

        // 2. 直接导入：add 成功即完成，不做余额验活、不回滚。
        if !verify {
            return ImportItemResult {
                status: ImportStatus::Imported,
                credential_id: Some(resp.credential_id),
                email: resp.email.clone(),
                balance: None,
                error: None,
                rolled_back: false,
            };
        }

        // 3. 验活路径：显式取余额验活（OAuth 正常路径命中 add 内缓存；
        //    API Key 无 token 刷新，余额拉取即真正的验活，失败则回滚）。
        match self.get_balance(resp.credential_id).await {
            Ok(balance) => ImportItemResult {
                status: ImportStatus::Verified,
                credential_id: Some(resp.credential_id),
                email: resp.email.clone(),
                balance: Some(balance),
                error: None,
                rolled_back: false,
            },
            Err(e) => {
                let msg = e.to_string();
                tracing::warn!(
                    "批量导入凭据 #{} 验活失败，回滚删除: {}",
                    resp.credential_id,
                    msg
                );
                // 回滚：直接删除（delete_credential 会清理 balance 缓存与 trace）。
                // 不先 disable——delete 是整条移除，无 enabled 守卫，足够原子。
                let rolled_back = self.delete_credential(resp.credential_id).is_ok();
                ImportItemResult {
                    status: ImportStatus::Failed,
                    credential_id: Some(resp.credential_id),
                    email: resp.email,
                    balance: None,
                    error: Some(msg),
                    rolled_back,
                }
            }
        }
    }

    /// 更新凭据的可编辑字段（email、proxy 等）
    pub fn update_credential(
        &self,
        id: u64,
        req: UpdateCredentialRequest,
    ) -> Result<(), AdminServiceError> {
        // 只校验本次实际传入的 proxy_url：不传则不校验，避免改邮箱时被存量非法值挡住；
        // 空串表示清除，同样跳过。
        if let Some(ref u) = req.proxy_url
            && !u.is_empty()
        {
            validate_credential_proxy_url(u)?;
        }

        self.token_manager
            .update_credential(
                id,
                CredentialUpdate {
                    email: req.email.map(|v| if v.is_empty() { None } else { Some(v) }),
                    proxy_url: req.proxy_url.map(|v| if v.is_empty() { None } else { Some(v) }),
                    proxy_username: req
                        .proxy_username
                        .map(|v| if v.is_empty() { None } else { Some(v) }),
                    proxy_password: req
                        .proxy_password
                        .map(|v| if v.is_empty() { None } else { Some(v) }),
                    groups: req.groups,
                    source_channel: req
                        .source_channel
                        .map(|v| if v.is_empty() { None } else { Some(v) }),
                },
            )
            .map_err(|e| self.classify_error(e, id))
    }

    /// 删除凭据
    pub fn delete_credential(&self, id: u64) -> Result<(), AdminServiceError> {
        self.token_manager
            .delete_credential(id)
            .map_err(|e| self.classify_delete_error(e, id))?;

        // 清理已删除凭据的余额缓存
        self.balance_cache.remove(id);

        if let Some(trace_store) = &self.trace_store {
            trace_store.delete_for_credential(id);
        }

        Ok(())
    }

    /// 从磁盘加载最新配置并应用更新，再写回磁盘。
    ///
    /// 每次读最新文件再写，避免多次调用之间字段互相覆盖。
    fn update_config_file(&self, updater: impl FnOnce(&mut Config)) {
        let base = self.token_manager.config();
        let Some(path) = base.config_path() else {
            return;
        };
        match Config::load(path) {
            Ok(mut fresh) => {
                updater(&mut fresh);
                if let Err(e) = fresh.save() {
                    tracing::warn!("保存配置文件失败: {}", e);
                }
            }
            Err(e) => tracing::warn!("读取配置文件失败（跳过持久化）: {}", e),
        }
    }

    /// 注入模型注册表依赖（models.json 存储层 + 同步服务）。
    ///
    /// 与 `with_log_governance` 同样的注入风格。**调度器不在这里** ——
    /// 定时同步必须在 admin 分支之外创建（spec §6.1：AdminService 仅在
    /// adminApiKey 非空时创建），这里只承接手动触发与表编辑。
    pub fn with_model_registry(
        mut self,
        store: Option<Arc<crate::anthropic::model_registry_store::ModelRegistryStore>>,
        sync_service: Option<Arc<crate::anthropic::model_sync::ModelSyncService>>,
    ) -> Self {
        self.model_registry_store = store;
        self.model_sync_service = sync_service;
        self
    }

    /// 采用启动流程创建的共享同步配置 holder。
    ///
    /// 不这样做的话，`AdminService::new` 会自己从 config 复制一份，
    /// `set_model_sync_settings` 只改到自己那份，admin 分支之外的调度器
    /// 永远看的是启动瞬间的快照 —— 开关/时间/探针的修改都得重启才生效。
    pub fn with_model_sync_settings(
        mut self,
        settings: Arc<parking_lot::RwLock<ModelSyncSettings>>,
    ) -> Self {
        self.model_sync = settings;
        self
    }

    /// 注入 Kiro Provider（`POST /models/test` 发真实请求用）。
    ///
    /// 必须与 `/v1/messages` 用同一个实例：模型测试要回答的问题是「客户端发这个
    /// 模型名时**本代理**实际会发生什么」，另起一个 provider 就换了账号池与
    /// client 缓存，测的不再是生产链路。
    pub fn with_kiro_provider(
        mut self,
        provider: Arc<crate::kiro::provider::KiroProvider>,
    ) -> Self {
        self.kiro_provider = Some(provider);
        self
    }

    /// `POST /models/test`：对指定模型发送一次真实、最小化的 Kiro 请求。
    ///
    /// 先过本地注册表再决定发不发：这样测出来的是「客户端发这个模型名时本代理
    /// 实际会发生什么」——含别名映射、禁用判定、thinking 变体与透传开关，而不是
    /// 「把这串字符原样丢给上游会怎样」。被注册表拒绝的请求一次都不发出去。
    ///
    /// 这是一次真实请求，成功/失败照常计入凭据统计（`report_success` /
    /// `report_failure` 由 provider 内部完成），不做只读豁免。
    pub async fn test_model(
        &self,
        request: ModelTestRequest,
    ) -> Result<ModelTestResponse, AdminServiceError> {
        use crate::anthropic::model_registry::{
            RejectReason, Resolution, allow_passthrough, current_registry,
        };
        use crate::kiro::model::events::{Event, strip_tool_use_xml_leaks};
        use crate::kiro::model::requests::conversation::{
            ConversationState, CurrentMessage, UserInputMessage,
        };
        use crate::kiro::model::requests::kiro::KiroRequest;
        use crate::kiro::parser::decoder::EventStreamDecoder;

        let model_id = request.model_id.trim().to_string();
        if model_id.is_empty() {
            return Err(AdminServiceError::InvalidModelField(
                "modelId 不能为空".to_string(),
            ));
        }

        // 第 1 步：注册表解析。Rejected 直接返回，不发任何请求。
        let resolved_model_id = match current_registry().resolve(&model_id, allow_passthrough()) {
            Resolution::Mapped { upstream_id, .. } | Resolution::Passthrough { upstream_id, .. } => {
                upstream_id
            }
            // 「没配」→ 404 语义
            Resolution::Rejected(RejectReason::Unknown) => {
                return Err(AdminServiceError::ModelNotFound(model_id));
            }
            // 「配了但被人工禁用」→ 400 语义：这是本地配置问题，不是模型不存在
            Resolution::Rejected(RejectReason::Disabled) => {
                return Err(AdminServiceError::InvalidModelField(format!(
                    "模型 {} 已在本地模型表中被禁用，不会下发到上游",
                    model_id
                )));
            }
        };
        // 请求名带 -thinking 后缀且解析通过 → 本次走的是 thinking 变体
        let thinking = model_id.to_ascii_lowercase().ends_with("-thinking");

        let provider = self
            .kiro_provider
            .as_ref()
            .ok_or_else(|| AdminServiceError::InternalError("Kiro Provider 未配置".to_string()))?;

        // 第 2 步：构造最小请求体
        let conversation_state = ConversationState::new(Uuid::new_v4().to_string())
            .with_agent_continuation_id(Uuid::new_v4().to_string())
            .with_agent_task_type("vibe")
            .with_chat_trigger_type("MANUAL")
            .with_current_message(CurrentMessage::new(
                UserInputMessage::new("Reply with exactly: OK", resolved_model_id.as_str())
                    .with_origin("AI_EDITOR"),
            ));
        let body = serde_json::to_string(&KiroRequest {
            conversation_state,
            profile_arn: None,
            additional_model_request_fields: None,
        })
        .map_err(|error| AdminServiceError::InternalError(error.to_string()))?;

        // 第 3 步：发送。指定了凭据就钉死它（不跨凭据故障转移），否则走正常账号池。
        let pinned = request.credential_id;
        let started = std::time::Instant::now();
        let (credential_id, bytes) = tokio::time::timeout(
            std::time::Duration::from_secs(90),
            async {
                let call = match pinned {
                    Some(id) => provider.call_api_pinned(id, &body).await?,
                    None => provider.call_api(&body, None, None).await?,
                };
                let credential_id = call.credential_id;
                let bytes = call.response.bytes().await?;
                Ok::<_, anyhow::Error>((credential_id, bytes))
            },
        )
        .await
        .map_err(|_| AdminServiceError::UpstreamError("模型测试请求超时".to_string()))?
        .map_err(|error| AdminServiceError::UpstreamError(error.to_string()))?;

        // 第 4 步：解帧
        let mut decoder = EventStreamDecoder::new();
        decoder
            .feed(&bytes)
            .map_err(|error| AdminServiceError::UpstreamError(error.to_string()))?;
        let mut response_text = String::new();
        let mut credit_usage = 0.0_f64;
        let mut credit_unit = None;

        for frame in decoder.decode_iter() {
            let frame = frame.map_err(|error| {
                AdminServiceError::UpstreamError(format!("模型响应解析失败: {error}"))
            })?;
            let event = Event::from_frame(frame).map_err(|error| {
                AdminServiceError::UpstreamError(format!("模型事件解析失败: {error}"))
            })?;
            match event {
                Event::AssistantResponse(response) => response_text.push_str(&response.content),
                Event::Metering(metering) => {
                    credit_usage += metering.usage;
                    if !metering.unit.is_empty() {
                        credit_unit = Some(metering.unit);
                    }
                }
                Event::Error {
                    error_code,
                    error_message,
                } => {
                    return Err(AdminServiceError::UpstreamError(format!(
                        "{error_code}: {error_message}"
                    )));
                }
                Event::Exception {
                    exception_type,
                    message,
                } => {
                    return Err(AdminServiceError::UpstreamError(format!(
                        "{exception_type}: {message}"
                    )));
                }
                _ => {}
            }
        }

        let response_text = strip_tool_use_xml_leaks(&response_text);
        if response_text.trim().is_empty() {
            return Err(AdminServiceError::UpstreamError(
                "模型返回了空响应".to_string(),
            ));
        }

        Ok(ModelTestResponse {
            model_id,
            resolved_model_id,
            thinking,
            credential_id,
            latency_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            response_text,
            credit_usage: (credit_usage > 0.0).then_some(credit_usage),
            credit_unit,
        })
    }

    fn registry_store(
        &self,
    ) -> Result<&Arc<crate::anthropic::model_registry_store::ModelRegistryStore>, AdminServiceError>
    {
        self.model_registry_store.as_ref().ok_or_else(|| {
            AdminServiceError::InternalError("模型注册表未初始化（models.json 存储层缺失）".to_string())
        })
    }

    /// 启用中的凭据 id（覆盖率分母）。禁用凭据不参与调度，统计它没有意义。
    fn enabled_credential_ids(&self) -> Vec<u64> {
        self.token_manager
            .snapshot()
            .entries
            .iter()
            .filter(|e| !e.disabled)
            .map(|e| e.id)
            .collect()
    }

    /// `GET /models`
    pub fn model_registry(
        &self,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError> {
        let store = self.registry_store()?;
        Ok(build_model_registry_response(
            store,
            self.model_sync_settings(),
            &self.enabled_credential_ids(),
        ))
    }

    /// 所有写路径的公共骨架：store.mutate（内部串行 + 原子落盘 + 落盘前整表校验）
    /// → 成功后才热替换内存注册表 → 返回最新快照供 UI 直接刷新。
    async fn mutate_registry<F>(
        &self,
        f: F,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError>
    where
        F: FnOnce(
            &mut crate::anthropic::model_registry::ModelRegistryFile,
        ) -> Result<(), AdminServiceError>,
    {
        let store = self.registry_store()?;
        let file = store
            .mutate(|file| f(file).map_err(encode_model_error))
            .await
            .map_err(decode_model_error)?;

        // 落盘成功才 swap，失败则内存保持旧值（spec §6.5）
        match crate::anthropic::model_registry::ModelRegistry::from_file(file) {
            Ok(registry) => crate::anthropic::model_registry::install_registry(registry),
            Err(e) => {
                tracing::error!("写入后的 models.json 校验失败: {}，保持内存中旧表", e);
                return Err(AdminServiceError::InternalError(format!(
                    "模型表已写盘但校验失败: {}",
                    e
                )));
            }
        }
        self.model_registry()
    }

    /// `POST /models` 手动新增一行（origin = manual）
    pub async fn create_model(
        &self,
        req: crate::admin::types::CreateModelRequest,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError> {
        self.mutate_registry(|file| create_model_in_file(file, &req)).await
    }

    /// `PATCH /models/{upstreamId}`
    pub async fn patch_model(
        &self,
        upstream_id: &str,
        req: crate::admin::types::PatchModelRequest,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError> {
        self.mutate_registry(|file| patch_model_in_file(file, upstream_id, &req)).await
    }

    /// `DELETE /models/{upstreamId}`
    pub async fn delete_model(
        &self,
        upstream_id: &str,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError> {
        self.mutate_registry(|file| delete_model_in_file(file, upstream_id)).await
    }

    /// `POST /models/aliases`
    pub async fn upsert_alias(
        &self,
        req: crate::admin::types::UpsertAliasRequest,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError> {
        self.mutate_registry(|file| upsert_alias_in_file(file, &req)).await
    }

    /// `DELETE /models/aliases`
    pub async fn delete_alias(
        &self,
        from: &str,
    ) -> Result<crate::admin::types::ModelRegistryResponse, AdminServiceError> {
        self.mutate_registry(|file| delete_alias_in_file(file, from)).await
    }

    /// `POST /models/sync` 手动触发一轮同步，返回 diff 摘要。
    ///
    /// 失败（无可用凭据 / 全部凭据返回空列表 / 乱序丢弃）时 models.json 不被改动，
    /// 原因原样带回给 UI —— 这些都是需要人看到具体原因才能处理的情况。
    pub async fn sync_models(
        &self,
        force_disappearance_check: bool,
    ) -> Result<crate::admin::types::SyncSummaryResponse, AdminServiceError> {
        use crate::anthropic::model_sync::{RoundKind, SyncOptions};

        let service = self.model_sync_service.as_ref().ok_or_else(|| {
            AdminServiceError::InternalError("模型同步服务未初始化".to_string())
        })?;
        let probe = self.model_sync_settings().probe_credential_id;
        if force_disappearance_check {
            // 强制放行会真的把模型标成 deprecated，留一条谁都能查到的痕迹。
            tracing::warn!("运维显式强制放行一次消失判定（POST /models/sync?force=true）");
        }
        let summary = service
            .sync_once_with(
                probe,
                Utc::now(),
                SyncOptions { force_disappearance_check },
            )
            .await
            .map_err(|e| AdminServiceError::InternalError(format!("模型同步失败: {}", e)))?;

        // 同步顺带刷新了 credentialSupport，落盘后要灌回调度层，否则手动同步之后
        // 凭据过滤仍按上一份记录走（新记录到下次重启才生效）。
        if let Some(store) = &self.model_registry_store {
            self.token_manager
                .set_credential_support(store.load().file.credential_support);
        }

        Ok(crate::admin::types::SyncSummaryResponse {
            round: match summary.round {
                RoundKind::Authoritative => "authoritative".to_string(),
                RoundKind::Advisory => "advisory".to_string(),
            },
            added: summary.added,
            updated: summary.updated,
            deprecated: summary.deprecated,
            trusted: summary.trusted,
            source: summary.source,
            disappearance_check_skipped: summary.disappearance_check_skipped,
            missing_ratio: summary.missing_ratio,
        })
    }

    /// 获取模型同步运行时配置
    pub fn model_sync_settings(&self) -> ModelSyncSettings {
        self.model_sync.read().clone()
    }

    /// 更新模型同步运行时配置（字段缺省表示不修改），持久化到 config.json 并热生效。
    pub async fn set_model_sync_settings(
        &self,
        req: SetModelSyncSettingsRequest,
    ) -> Result<ModelSyncSettings, AdminServiceError> {
        // M1：白名单之外的键一律拒绝。serde 默认静默丢弃未知字段，实测
        // `{"allowUnknownModelPassthrough":true}`（config.json 里的真实键名）
        // 返回 200 却什么都没改 —— 用户会以为开关已经打开。
        if !req.extra.is_empty() {
            let names: Vec<&str> = req.extra.keys().map(|k| k.as_str()).collect();
            return Err(AdminServiceError::InvalidModelField(format!(
                "以下字段未知: {}。可写字段: enabled、time、probeCredentialId\
                 （配合 probeCredentialIdSet）、allowPassthrough",
                names.join("、")
            )));
        }

        // 校验时间格式，复用既有解析器。
        // M2：解析器的错误文案属于「二进制自动更新」那条路径（凭据无效: 自动更新时间…），
        // 用在这里会把「模型同步时间」说成「凭据无效」，排查方向直接被带偏。
        // 这里改挂 §8 为模型字段准备的 InvalidModelField，并换成本路径的措辞。
        if let Some(time) = req.time.as_deref() {
            parse_auto_apply_time(time).map_err(|_| {
                AdminServiceError::InvalidModelField(format!(
                    "模型同步时间无效：{}（应为 HH:MM，HH 0-23，MM 0-59）",
                    time
                ))
            })?;
        }

        // 串行化 config.json 的 load-modify-save，避免并发写丢失更新
        let _guard = self.config_write_lock.lock().await;

        let mut next = self.model_sync_settings();
        if let Some(v) = req.enabled {
            next.enabled = v;
        }
        if let Some(v) = req.time.clone() {
            next.time = v;
        }
        if req.probe_credential_id_set {
            next.probe_credential_id = req.probe_credential_id;
        }
        if let Some(v) = req.allow_passthrough {
            next.allow_passthrough = v;
        }

        // 持久化：从磁盘加载最新后再写，避免覆盖其他字段
        let base = self.token_manager.config();
        let path = base.config_path().ok_or_else(|| {
            AdminServiceError::InternalError("配置文件路径未知，无法保存配置".to_string())
        })?;
        let mut config = Config::load(path)
            .map_err(|e| AdminServiceError::InternalError(format!("加载配置失败: {}", e)))?;
        config.model_sync_enabled = next.enabled;
        config.model_sync_time = next.time.clone();
        config.model_sync_probe_credential_id = next.probe_credential_id;
        config.allow_unknown_model_passthrough = next.allow_passthrough;
        config
            .save()
            .map_err(|e| AdminServiceError::InternalError(format!("保存配置失败: {}", e)))?;

        *self.model_sync.write() = next.clone();
        crate::anthropic::model_registry::set_allow_passthrough(next.allow_passthrough);
        Ok(next)
    }

    /// 获取全局代理 URL
    pub fn get_global_proxy(&self) -> Option<String> {
        self.token_manager.proxy().map(|p| p.url.clone())
    }

    /// 设置全局代理 URL（None 表示清除）并持久化到配置文件
    ///
    /// 校验见 [`validate_global_proxy_url`]。
    pub fn set_global_proxy(&self, url: Option<String>) -> Result<(), AdminServiceError> {
        if let Some(ref u) = url {
            validate_global_proxy_url(u)?;
        }

        let proxy = url.as_deref().map(ProxyConfig::new);
        self.token_manager.set_global_proxy(proxy);

        // 从磁盘加载最新 config 再写，避免覆盖其他字段的并发修改
        let url_for_save = url;
        self.update_config_file(move |c| c.proxy_url = url_for_save);
        Ok(())
    }

    /// 持久化新的登录API密钥（adminApiKey）到配置文件（内存中的 key 由 handler 层负责更新）
    pub fn persist_admin_key(&self, new_key: &str) {
        let key = new_key.to_string();
        self.update_config_file(move |c| c.admin_api_key = Some(key));
    }

    /// 将系统密钥写回 `config.json`。
    pub fn persist_api_key(&self, new_key: &str) {
        let key = new_key.to_string();
        self.update_config_file(move |c| c.api_key = Some(key));
    }

    /// 获取在线更新配置（GitHub Token 只返回是否已配置）
    pub fn get_update_config(&self) -> UpdateConfigResponse {
        self.update_config.lock().response()
    }

    /// 更新在线更新配置。
    pub fn set_update_config(
        &self,
        req: SetUpdateConfigRequest,
    ) -> Result<UpdateConfigResponse, AdminServiceError> {
        // 在写入运行时之前先校验时间格式，并规范化成两位补零的 HH:MM
        let normalized_time = match req.auto_apply_time.as_deref() {
            Some(value) => Some(normalize_auto_apply_time(value)?),
            None => None,
        };

        // GitHub Token：空字符串表示清除，None 表示保持原值
        let token_update: Option<Option<String>> = req.github_token.as_ref().map(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });

        {
            let mut runtime = self.update_config.lock();
            if let Some(auto_apply) = req.auto_apply {
                runtime.auto_apply = auto_apply;
            }
            if let Some(time) = &normalized_time {
                runtime.auto_apply_time = time.clone();
            }
            if let Some(token) = &token_update {
                runtime.github_token = token.clone();
            }
        }

        self.update_config_file(move |c| {
            if let Some(auto_apply) = req.auto_apply {
                c.update_auto_apply = auto_apply;
            }
            if let Some(time) = normalized_time {
                c.update_auto_apply_time = time;
            }
            if let Some(token) = token_update {
                c.github_token = token;
            }
        });

        Ok(self.get_update_config())
    }

    /// 下载新版二进制并通过校验和验证（对应前端「拉取镜像」按钮）。
    /// 不替换当前可执行文件，便于用户在正式应用前先确认下载成功。
    /// 下载产物保存到 `<exe>.staged-<version>`，下次 apply 命中同版本时复用。
    pub async fn pull_update_image(&self) -> Result<ImageUpdateResponse, AdminServiceError> {
        let (proxy, token) = {
            let runtime = self.update_config.lock();
            (
                self.token_manager.proxy().map(|p| p.url.clone()),
                runtime.github_token.clone(),
            )
        };
        let exe = super::binary_update::current_executable()?;

        let version = self.resolve_target_version(false).await?;
        let staged = staged_binary_path(&exe, &version);

        // 已经下载过同版本时直接复用，避免重复网络请求
        let reused = staged.exists();
        if !reused {
            super::binary_update::download_release_binary(
                &version,
                proxy.as_deref(),
                token.as_deref(),
                &staged,
            )
            .await?;
        }
        // 清理其它版本的旧 staged 文件，避免占用磁盘
        cleanup_other_staged(&exe, &version);

        Ok(ImageUpdateResponse {
            success: true,
            message: if reused {
                format!("v{} 已下载并校验，可直接执行「更新并重启」", version)
            } else {
                format!(
                    "已下载并校验 v{} 二进制，可直接执行「更新并重启」",
                    version
                )
            },
            output: Some(format!(
                "{}: v{}\nstaged: {}",
                if reused { "reused" } else { "downloaded" },
                version,
                staged.display()
            )),
            applied: false,
            need_restart: false,
        })
    }

    /// 下载新版二进制并替换当前可执行文件，随后让进程退出由
    /// `restart: unless-stopped` 接管重启（对应前端「更新并重启」按钮）。
    /// 若 pull 已经把目标版本下载到 `<exe>.staged-<version>`，跳过重复下载。
    pub async fn apply_image_update(&self) -> Result<ImageUpdateResponse, AdminServiceError> {
        let (proxy, token) = {
            let runtime = self.update_config.lock();
            (
                self.token_manager.proxy().map(|p| p.url.clone()),
                runtime.github_token.clone(),
            )
        };
        let exe = super::binary_update::current_executable()?;

        let version = self.resolve_target_version(true).await?;
        let staged = staged_binary_path(&exe, &version);

        let reused = staged.exists();
        if !reused {
            super::binary_update::download_release_binary(
                &version,
                proxy.as_deref(),
                token.as_deref(),
                &staged,
            )
            .await?;
        }
        cleanup_other_staged(&exe, &version);

        // 记录当前版本作为「上一版本」，供前端展示「回退」按钮
        let previous_version = env!("CARGO_PKG_VERSION").to_string();
        super::binary_update::install_binary(&exe, &staged)?;

        let prev_label = format!("v{}", previous_version);
        let applied_at = chrono::Utc::now().to_rfc3339();
        {
            let mut runtime = self.update_config.lock();
            runtime.previous_version = Some(prev_label.clone());
            runtime.last_applied_at = Some(applied_at.clone());
        }
        let prev_to_persist = prev_label.clone();
        let applied_at_to_persist = applied_at.clone();
        self.update_config_file(move |c| {
            c.update_previous_version = Some(prev_to_persist);
            c.update_last_applied_at = Some(applied_at_to_persist);
        });

        super::binary_update::schedule_self_exit(std::time::Duration::from_secs(2));

        Ok(ImageUpdateResponse {
            success: true,
            message: format!(
                "已替换为 v{}，进程将在 2 秒后退出，由容器重启策略接管",
                version
            ),
            output: Some(format!(
                "previous: v{}\n{}: v{}",
                previous_version,
                if reused { "reused-staged" } else { "installed" },
                version
            )),
            applied: true,
            need_restart: true,
        })
    }

    /// 把可执行文件回退到 `<exe>.backup`，再重启进程。
    pub async fn rollback_image_update(&self) -> Result<ImageUpdateResponse, AdminServiceError> {
        let previous_label = self
            .update_config
            .lock()
            .previous_version
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "尚未记录可回退的版本，请先执行一次在线更新".to_string(),
                )
            })?
            .to_string();

        let exe = super::binary_update::current_executable()?;
        super::binary_update::restore_backup(&exe)?;
        // 回退后清掉所有 staged：用户已表态"上一次更新是错的"，残留只会误导
        cleanup_other_staged(&exe, "");

        // 回退视为撤销最近一次更新：清空 previous_version 和 last_applied_at
        {
            let mut runtime = self.update_config.lock();
            runtime.previous_version = None;
            runtime.last_applied_at = None;
        }
        self.update_config_file(|c| {
            c.update_previous_version = None;
            c.update_last_applied_at = None;
        });

        super::binary_update::schedule_self_exit(std::time::Duration::from_secs(2));

        Ok(ImageUpdateResponse {
            success: true,
            message: format!(
                "已回退到 {}，进程将在 2 秒后退出，由容器重启策略接管",
                previous_label
            ),
            output: Some(format!("rolled back to: {}", previous_label)),
            applied: true,
            need_restart: true,
        })
    }

    /// 返回 GitHub Releases 上的最新可用版本号（无 `v` 前缀）。
    /// 失败时返回 `InternalError`，调用方应直接返回给前端。
    /// 返回 GitHub Releases 上的最新可用版本号（无 `v` 前缀）。
    /// 失败时返回 `InternalError`，调用方应直接返回给前端。
    ///
    /// `require_update` 为 true 时，若当前版本已经是最新（无更新可用），
    /// 直接返回错误而不是返回相同版本号——避免 apply 流程下载并替换同一版本。
    async fn resolve_target_version(
        &self,
        require_update: bool,
    ) -> Result<String, AdminServiceError> {
        let info = self.check_update(true).await;
        if let Some(warn) = info.warning {
            return Err(AdminServiceError::InternalError(warn));
        }
        if info.latest_version.is_empty() {
            return Err(AdminServiceError::InternalError(
                "无法解析最新版本号（GitHub Releases 返回空）".to_string(),
            ));
        }
        if require_update && !info.has_update {
            return Err(AdminServiceError::InvalidCredential(format!(
                "当前已是最新版本 v{}，无需更新",
                info.current_version
            )));
        }
        Ok(info.latest_version)
    }

    /// 检查 GitHub Releases 上是否存在新版本。
    ///
    /// `force=false` 时优先返回 30 分钟内的缓存结果；`force=true` 时强制查询
    /// 远端。查询失败但有旧缓存时，返回旧缓存并附带 warning。
    pub async fn check_update(&self, force: bool) -> UpdateCheckInfo {
        if !force
            && let Some(cached) = self.update_check_cache.lock().clone() {
                let age = Utc::now()
                    .signed_duration_since(cached.cached_at)
                    .num_seconds();
                if age < UPDATE_CHECK_TTL_SECS {
                    let mut info = cached.info.clone();
                    info.cached = true;
                    return info;
                }
            }

        match self.fetch_latest_release().await {
            Ok(info) => {
                self.update_check_cache.lock().replace(CachedUpdateCheck {
                    cached_at: Utc::now(),
                    info: info.clone(),
                });
                info
            }
            Err(err) => {
                let warning = format!("检查更新失败：{}", err);
                if let Some(cached) = self.update_check_cache.lock().clone() {
                    let mut info = cached.info.clone();
                    info.cached = true;
                    info.warning = Some(warning);
                    return info;
                }
                UpdateCheckInfo {
                    current_version: env!("CARGO_PKG_VERSION").to_string(),
                    latest_version: String::new(),
                    has_update: false,
                    build_type: BUILD_TYPE.to_string(),
                    release_name: None,
                    release_notes: None,
                    release_url: None,
                    published_at: None,
                    checked_at: Utc::now().to_rfc3339(),
                    cached: false,
                    warning: Some(warning),
                }
            }
        }
    }

    async fn fetch_latest_release(&self) -> Result<UpdateCheckInfo, AdminServiceError> {
        let url = format!(
            "https://api.github.com/repos/{}/releases/latest",
            GITHUB_RELEASES_REPO
        );
        let token = self.update_config.lock().github_token.clone();
        let mut req = reqwest::Client::new()
            .get(&url)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "kiro-rs-update-checker")
            .timeout(std::time::Duration::from_secs(15));
        if let Some(t) = token.as_deref() {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                req = req.header("Authorization", format!("Bearer {}", trimmed));
            }
        }
        let resp = req.send().await.map_err(|e| {
            AdminServiceError::InternalError(format!("请求 GitHub API 失败: {}", e))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AdminServiceError::InternalError(format!(
                "GitHub API 返回 {}: {}",
                status,
                body.chars().take(200).collect::<String>()
            )));
        }

        let release: GitHubRelease = resp.json().await.map_err(|e| {
            AdminServiceError::InternalError(format!("解析 GitHub release 失败: {}", e))
        })?;

        let current = env!("CARGO_PKG_VERSION").to_string();
        let latest_version = release.tag_name.trim().trim_start_matches('v').to_string();
        let has_update =
            !latest_version.is_empty() && compare_semver(&current, &latest_version).is_lt();

        Ok(UpdateCheckInfo {
            current_version: current,
            latest_version,
            has_update,
            build_type: BUILD_TYPE.to_string(),
            release_name: Some(release.name).filter(|v| !v.is_empty()),
            release_notes: Some(release.body).filter(|v| !v.is_empty()),
            release_url: Some(release.html_url).filter(|v| !v.is_empty()),
            published_at: Some(release.published_at).filter(|v| !v.is_empty()),
            checked_at: Utc::now().to_rfc3339(),
            cached: false,
            warning: None,
        })
    }

    /// 查询 GitHub API 当前限流配额。
    ///
    /// `req.github_token` 不为空时使用该 token 验证（用于"保存前先试一下"），
    /// 否则使用配置中已保存的 `config.github_token`，再缺则匿名查询。
    /// `/rate_limit` 端点本身不消耗任何配额。
    pub async fn check_rate_limit(
        &self,
        req: CheckRateLimitRequest,
    ) -> GitHubRateLimitInfo {
        // 优先用入参 token；空字符串视作"尝试匿名"；缺省回退到已保存 token
        let token = req
            .github_token
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .or_else(|| {
                self.update_config
                    .lock()
                    .github_token
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
            });
        let authenticated = token.is_some();

        let proxy = self.token_manager.proxy().map(|p| p.url.clone());
        let client = match super::binary_update::build_http_client(proxy.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                return GitHubRateLimitInfo {
                    valid: false,
                    authenticated,
                    limit: 0,
                    remaining: 0,
                    used: 0,
                    reset: 0,
                    login: None,
                    warning: Some(format!("构造 HTTP 客户端失败: {}", e)),
                };
            }
        };

        let mut req_builder = client
            .get("https://api.github.com/rate_limit")
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "kiro-rs-update-checker")
            .timeout(std::time::Duration::from_secs(10));
        if let Some(t) = token.as_deref() {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", t));
        }

        let resp = match req_builder.send().await {
            Ok(r) => r,
            Err(e) => {
                return GitHubRateLimitInfo {
                    valid: false,
                    authenticated,
                    limit: 0,
                    remaining: 0,
                    used: 0,
                    reset: 0,
                    login: None,
                    warning: Some(format!("请求 GitHub API 失败: {}", e)),
                };
            }
        };

        let status = resp.status();

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return GitHubRateLimitInfo {
                valid: false,
                authenticated,
                limit: 0,
                remaining: 0,
                used: 0,
                reset: 0,
                login: None,
                warning: Some("GitHub Token 无效或已过期".to_string()),
            };
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return GitHubRateLimitInfo {
                valid: false,
                authenticated,
                limit: 0,
                remaining: 0,
                used: 0,
                reset: 0,
                login: None,
                warning: Some(format!(
                    "GitHub API 返回 {}: {}",
                    status,
                    body.chars().take(200).collect::<String>()
                )),
            };
        }

        let payload: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                return GitHubRateLimitInfo {
                    valid: false,
                    authenticated,
                    limit: 0,
                    remaining: 0,
                    used: 0,
                    reset: 0,
                    login: None,
                    warning: Some(format!("解析 GitHub 响应失败: {}", e)),
                };
            }
        };

        // /rate_limit 返回结构：{ resources: { core: { limit, remaining, used, reset } }, rate: {...} }
        // 其中 `core` 是 REST API 整体配额，最贴合在线更新的实际消耗
        let core = payload
            .get("resources")
            .and_then(|r| r.get("core"))
            .or_else(|| payload.get("rate"));
        let limit = core.and_then(|c| c.get("limit")).and_then(|v| v.as_u64()).unwrap_or(0);
        let remaining = core
            .and_then(|c| c.get("remaining"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let used = core.and_then(|c| c.get("used")).and_then(|v| v.as_u64()).unwrap_or(0);
        let reset = core.and_then(|c| c.get("reset")).and_then(|v| v.as_u64()).unwrap_or(0);

        // 同时尝试拿 token 对应的用户名；失败不影响主结果
        let login = if authenticated {
            self.fetch_github_login(&client, token.as_deref()).await
        } else {
            None
        };

        GitHubRateLimitInfo {
            valid: true,
            authenticated,
            limit,
            remaining,
            used,
            reset,
            login,
            warning: None,
        }
    }

    async fn fetch_github_login(
        &self,
        client: &reqwest::Client,
        token: Option<&str>,
    ) -> Option<String> {
        let mut req = client
            .get("https://api.github.com/user")
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "kiro-rs-update-checker")
            .timeout(std::time::Duration::from_secs(10));
        if let Some(t) = token {
            req = req.header("Authorization", format!("Bearer {}", t));
        }
        let resp = req.send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let payload: serde_json::Value = resp.json().await.ok()?;
        payload
            .get("login")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// 获取负载均衡模式
    pub fn get_load_balancing_mode(&self) -> LoadBalancingModeResponse {
        LoadBalancingModeResponse {
            mode: self.token_manager.get_load_balancing_mode(),
        }
    }

    /// 设置负载均衡模式
    pub fn set_load_balancing_mode(
        &self,
        req: SetLoadBalancingModeRequest,
    ) -> Result<LoadBalancingModeResponse, AdminServiceError> {
        // 验证模式值
        if !matches!(req.mode.as_str(), "priority" | "balanced" | "weighted") {
            return Err(AdminServiceError::InvalidCredential(
                "mode 必须是 'priority'、'balanced' 或 'weighted'".to_string(),
            ));
        }

        self.token_manager
            .set_load_balancing_mode(req.mode.clone())
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        Ok(LoadBalancingModeResponse { mode: req.mode })
    }

    /// 获取账号级风控故障转移配置
    pub fn get_account_throttle_config(&self) -> AccountThrottleConfigResponse {
        AccountThrottleConfigResponse {
            failover: self.token_manager.get_account_throttle_failover(),
            cooldown_secs: self.token_manager.get_account_throttle_cooldown_secs(),
        }
    }

    /// 更新账号级风控故障转移配置
    pub fn set_account_throttle_config(
        &self,
        req: SetAccountThrottleConfigRequest,
    ) -> Result<AccountThrottleConfigResponse, AdminServiceError> {
        if req.failover.is_none() && req.cooldown_secs.is_none() {
            return Err(AdminServiceError::InvalidCredential(
                "至少提供 failover 或 cooldownSecs 一个字段".to_string(),
            ));
        }

        self.token_manager
            .set_account_throttle_config(req.failover, req.cooldown_secs)
            .map_err(|e| AdminServiceError::InvalidCredential(e.to_string()))?;

        Ok(self.get_account_throttle_config())
    }

    /// 获取单账号 RPM 主动限流配置
    pub fn get_account_rpm_limit_config(&self) -> AccountRpmLimitConfigResponse {
        AccountRpmLimitConfigResponse {
            enabled: self.token_manager.get_account_rpm_limit_enabled(),
            limit: self.token_manager.get_account_rpm_limit(),
        }
    }

    /// 更新单账号 RPM 主动限流配置
    pub fn set_account_rpm_limit_config(
        &self,
        req: SetAccountRpmLimitConfigRequest,
    ) -> Result<AccountRpmLimitConfigResponse, AdminServiceError> {
        if req.enabled.is_none() && req.limit.is_none() {
            return Err(AdminServiceError::InvalidCredential(
                "至少提供 enabled 或 limit 一个字段".to_string(),
            ));
        }

        self.token_manager
            .set_account_rpm_limit_config(req.enabled, req.limit)
            .map_err(|e| AdminServiceError::InvalidCredential(e.to_string()))?;

        Ok(self.get_account_rpm_limit_config())
    }

    /// 读取日志治理配置（trace 开关 / trace 保留天数 / usage 保留天数）
    pub fn get_log_governance_config(&self) -> LogGovernanceConfigResponse {
        let cfg = self.token_manager.config();
        LogGovernanceConfigResponse {
            trace_enabled: self
                .trace_store
                .as_ref()
                .map(|s| s.is_enabled())
                .unwrap_or(cfg.trace_enabled),
            trace_retention_days: self
                .trace_store
                .as_ref()
                .map(|s| s.retention_days() as u32)
                .unwrap_or(cfg.trace_retention_days),
            usage_log_retention_days: self
                .usage_recorder
                .as_ref()
                .map(|r| r.retention_days() as u32)
                .unwrap_or(cfg.usage_log_retention_days),
        }
    }

    /// 更新日志治理配置：改运行时原子值 + 持久化到 config.json。
    /// 任一字段缺省表示不修改。
    pub fn set_log_governance_config(
        &self,
        req: SetLogGovernanceConfigRequest,
    ) -> Result<LogGovernanceConfigResponse, AdminServiceError> {
        if req.trace_enabled.is_none()
            && req.trace_retention_days.is_none()
            && req.usage_log_retention_days.is_none()
        {
            return Err(AdminServiceError::InvalidCredential(
                "至少提供 traceEnabled / traceRetentionDays / usageLogRetentionDays 一个字段"
                    .to_string(),
            ));
        }
        // 校验范围：保留天数 1..=365
        for (name, v) in [
            ("traceRetentionDays", req.trace_retention_days),
            ("usageLogRetentionDays", req.usage_log_retention_days),
        ] {
            if let Some(d) = v
                && !(1..=365).contains(&d) {
                    return Err(AdminServiceError::InvalidCredential(format!(
                        "{} 必须在 1..=365 内: {}",
                        name, d
                    )));
                }
        }

        // 先改运行时原子值
        if let Some(enabled) = req.trace_enabled
            && let Some(s) = &self.trace_store {
                s.set_enabled(enabled);
            }
        if let Some(days) = req.trace_retention_days
            && let Some(s) = &self.trace_store {
                s.set_retention_days(days);
            }
        if let Some(days) = req.usage_log_retention_days
            && let Some(r) = &self.usage_recorder {
                r.set_retention_days(days as i64);
            }

        // 持久化到 config.json
        if let Err(e) = self.persist_log_governance_config(&req) {
            tracing::warn!("持久化日志治理配置失败（运行时已生效）: {}", e);
        }

        Ok(self.get_log_governance_config())
    }

    fn persist_log_governance_config(
        &self,
        req: &SetLogGovernanceConfigRequest,
    ) -> anyhow::Result<()> {
        use anyhow::Context;
        let config_path = match self.token_manager.config().config_path() {
            Some(p) => p.to_path_buf(),
            None => {
                tracing::warn!("配置文件路径未知，日志治理配置仅在当前进程生效");
                return Ok(());
            }
        };
        let mut config = crate::model::config::Config::load(&config_path)
            .with_context(|| format!("重新加载配置失败: {}", config_path.display()))?;
        if let Some(v) = req.trace_enabled {
            config.trace_enabled = v;
        }
        if let Some(v) = req.trace_retention_days {
            config.trace_retention_days = v;
        }
        if let Some(v) = req.usage_log_retention_days {
            config.usage_log_retention_days = v;
        }
        config
            .save()
            .with_context(|| format!("持久化日志治理配置失败: {}", config_path.display()))?;
        Ok(())
    }

    /// 更新指定凭据的 refreshToken（仅限已禁用凭据）
    pub fn update_refresh_token(
        &self,
        id: u64,
        req: UpdateRefreshTokenRequest,
    ) -> Result<(), AdminServiceError> {
        self.token_manager
            .update_refresh_token(id, req.refresh_token, req.access_token, req.expires_at)
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("不存在") {
                    AdminServiceError::NotFound { id }
                } else if msg.contains("只能为已禁用")
                    || msg.contains("refreshToken 重复")
                    || msg.contains("已被截断")
                    || msg.contains("refreshToken 为空")
                    || msg.contains("缺少 refreshToken")
                {
                    AdminServiceError::InvalidCredential(msg)
                } else {
                    AdminServiceError::InternalError(msg)
                }
            })
    }

    /// 一键开启所有"可开启超额且当前未开启"凭据的超额
    /// 数据来源是 balance_cache（5 分钟有效）；若缓存缺失或 capable 状态未知则乐观尝试，
    /// 由上游 setUserPreference 接口本身决定是否成功（不支持的订阅会返回 4xx 失败）。
    pub async fn enable_overage_for_all_capable(&self) -> EnableOverageAllResult {
        let snapshot = self.token_manager.snapshot();
        let cache_snapshot: HashMap<u64, CachedBalance> = self.balance_cache.snapshot().entries;
        let now_ts = Utc::now().timestamp() as f64;

        // 选出需要操作的 ID 列表
        let mut targets: Vec<u64> = Vec::new();
        let mut skipped: Vec<u64> = Vec::new();
        for entry in snapshot.entries.iter() {
            if entry.disabled {
                skipped.push(entry.id);
                continue;
            }
            let cached = cache_snapshot.get(&entry.id).filter(|c| {
                (now_ts - c.cached_at) < BALANCE_CACHE_TTL_SECS as f64
            });

            match cached {
                // 缓存命中：明确不可开启，跳过
                Some(c) if c.data.overage_capable == Some(false) => {
                    skipped.push(entry.id);
                    continue;
                }
                // 缓存命中：明确已开启，跳过
                Some(c) if c.data.overage_enabled == Some(true) => {
                    skipped.push(entry.id);
                    continue;
                }
                // 其它（缓存缺失 / 状态未知 / 明确可开启未开启）— 乐观尝试
                _ => targets.push(entry.id),
            }
        }

        let mut enabled_ids: Vec<u64> = Vec::new();
        let mut failed_ids: Vec<u64> = Vec::new();
        let mut failure_messages: Vec<String> = Vec::new();

        for id in targets {
            match self.token_manager.set_user_preference_for(id, "ENABLED").await {
                Ok(()) => {
                    enabled_ids.push(id);
                    // 失效本地缓存
                    self.balance_cache.remove(id);
                }
                Err(e) => {
                    tracing::warn!("一键开启超额：凭据 #{} 失败: {}", id, e);
                    failed_ids.push(id);
                    failure_messages.push(e.to_string());
                }
            }
            // 节流
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }

        EnableOverageAllResult {
            enabled_ids,
            skipped_ids: skipped,
            failed_ids,
            failure_messages,
        }
    }

    /// 强制刷新指定凭据的 Token
    pub async fn force_refresh_token(&self, id: u64) -> Result<(), AdminServiceError> {
        self.token_manager
            .force_refresh_token_for(id)
            .await
            .map_err(|e| self.classify_balance_error(e, id))
    }

    /// 设置凭据的"超额"开关（ENABLED / DISABLED）
    /// 成功后会主动失效本地余额缓存，让下次列表刷新展示最新 overage 状态
    pub async fn set_overage(&self, id: u64, enabled: bool) -> Result<(), AdminServiceError> {
        let status = if enabled { "ENABLED" } else { "DISABLED" };
        self.token_manager
            .set_user_preference_for(id, status)
            .await
            .map_err(|e| self.classify_balance_error(e, id))?;

        // 让本地缓存的 overage 状态失效（下次刷新时重新拉）
        self.balance_cache.remove(id);

        // 异步触发一次新的余额查询（不阻塞响应）
        let svc_handle = self.token_manager.clone();
        tokio::spawn(async move {
            if let Err(e) = svc_handle.get_usage_limits_for(id).await {
                tracing::warn!("超额状态变更后预热余额失败 #{}: {}", id, e);
            }
        });

        Ok(())
    }

    // ============ 代理池管理 ============

    /// 获取代理池列表（含凭据引用计数）
    pub fn get_proxy_pool(&self) -> ProxyPoolResponse {
        let proxies = self.proxy_pool.list();
        let credentials = {
            let snapshot = self.token_manager.snapshot();
            snapshot.entries
        };

        let pool: Vec<ProxyPoolEntry> = proxies
            .into_iter()
            .map(|p| {
                let count = credentials
                    .iter()
                    .filter(|c| c.proxy_url.as_deref().map(|u| u == p.url).unwrap_or(false))
                    .count() as u32;
                ProxyPoolEntry {
                    id: p.id,
                    url: p.url,
                    label: p.label,
                    enabled: p.enabled,
                    credential_count: count,
                    health: p.health,
                    latency_ms: p.latency_ms,
                    last_checked_at: p.last_checked_at,
                    consecutive_failures: p.consecutive_failures,
                    auto_disabled: p.auto_disabled,
                    request_failures: p.request_failures,
                    last_request_error: p.last_request_error,
                }
            })
            .collect();

        ProxyPoolResponse {
            total: pool.len(),
            proxies: pool,
        }
    }

    /// 添加代理到池中
    pub fn add_proxy(
        &self,
        url: String,
        label: Option<String>,
    ) -> Result<ProxyPoolEntry, AdminServiceError> {
        let entry = self
            .proxy_pool
            .add(url, label)
            .map_err(|e| AdminServiceError::InvalidCredential(e.to_string()))?;
        Ok(ProxyPoolEntry {
            id: entry.id,
            url: entry.url,
            label: entry.label,
            enabled: entry.enabled,
            credential_count: 0,
            health: entry.health,
            latency_ms: entry.latency_ms,
            last_checked_at: entry.last_checked_at,
            consecutive_failures: entry.consecutive_failures,
            auto_disabled: entry.auto_disabled,
            request_failures: entry.request_failures,
            last_request_error: entry.last_request_error,
        })
    }

    /// 批量添加代理
    pub fn batch_add_proxies(
        &self,
        req: BatchAddProxyRequest,
    ) -> (Vec<ProxyPoolEntry>, Vec<String>) {
        let (added, errors) = self.proxy_pool.batch_add(req.urls);
        let result = added
            .into_iter()
            .map(|e| ProxyPoolEntry {
                id: e.id,
                url: e.url,
                label: e.label,
                enabled: e.enabled,
                credential_count: 0,
                health: e.health,
                latency_ms: e.latency_ms,
                last_checked_at: e.last_checked_at,
                consecutive_failures: e.consecutive_failures,
                auto_disabled: e.auto_disabled,
                request_failures: e.request_failures,
                last_request_error: e.last_request_error,
            })
            .collect();
        (result, errors)
    }

    /// 删除代理池中的代理
    pub fn delete_proxy(&self, id: u64) -> Result<(), AdminServiceError> {
        self.proxy_pool.delete(id).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("不存在") {
                AdminServiceError::NotFound { id }
            } else {
                AdminServiceError::InternalError(msg)
            }
        })
    }

    /// 设置代理启用/禁用状态
    pub fn set_proxy_enabled(&self, id: u64, enabled: bool) -> Result<(), AdminServiceError> {
        self.proxy_pool
            .set_enabled(id, enabled)
            .map_err(|_| AdminServiceError::NotFound { id })
    }

    /// 将代理池中的代理分配给指定凭据
    pub fn assign_proxy_to_credential(
        &self,
        credential_id: u64,
        req: AssignProxyRequest,
    ) -> Result<(), AdminServiceError> {
        let proxy_url = match req.proxy_id {
            Some(proxy_id) => {
                let url = match self.proxy_pool.get_url(proxy_id) {
                    GetUrlResult::Ok(url) => url,
                    GetUrlResult::NotFound => {
                        return Err(AdminServiceError::NotFound { id: proxy_id });
                    }
                    GetUrlResult::Disabled => {
                        return Err(AdminServiceError::InvalidCredential(format!(
                            "代理 #{} 已被禁用，请先启用后再分配",
                            proxy_id
                        )));
                    }
                };
                Some(url)
            }
            None => None, // 清除代理
        };

        self.token_manager
            .update_credential(
                credential_id,
                CredentialUpdate {
                    // Some(None) = 清除，Some(Some(url)) = 设置。
                    proxy_url: Some(proxy_url),
                    ..CredentialUpdate::default()
                },
            )
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("不存在") {
                    AdminServiceError::NotFound { id: credential_id }
                } else {
                    AdminServiceError::InternalError(msg)
                }
            })
    }

    /// 即时探测单个代理的连通性（供 UI「测试」按钮调用）
    pub async fn check_proxy(&self, id: u64) -> Result<ProxyCheckResponse, AdminServiceError> {
        let (entry, newly_disabled) = self
            .proxy_pool
            .check_one(id)
            .await
            .map_err(|_| AdminServiceError::NotFound { id })?;
        // 与后台检查一致：探测触发的自动禁用也记事件 + 解绑受影响凭据
        if newly_disabled && let Some(ops) = &self.ops {
            ops.handle_probe_auto_disable(entry.id, &entry.url);
        }
        Ok(ProxyCheckResponse {
            id: entry.id,
            health: entry.health,
            latency_ms: entry.latency_ms,
            last_checked_at: entry.last_checked_at,
            enabled: entry.enabled,
            auto_disabled: entry.auto_disabled,
        })
    }

    /// 触发全部代理的健康检查
    pub async fn check_all_proxies(&self) -> ProxyCheckAllResponse {
        let summary = self.proxy_pool.check_all().await;
        // 与后台调度器一致：手动全量检查触发的自动禁用也记事件 + 解绑受影响凭据
        if let Some(ops) = &self.ops {
            for (id, url) in &summary.newly_disabled {
                ops.handle_probe_auto_disable(*id, url);
            }
        }
        ProxyCheckAllResponse {
            healthy: summary.healthy,
            unhealthy: summary.unhealthy,
            auto_disabled: summary.auto_disabled,
        }
    }

    /// 将可用代理（已启用且非 Unhealthy）按轮询方式批量分配给凭据
    ///
    /// - `credential_ids` 为 None 时对全部凭据分配
    /// - 无可用代理时返回错误
    pub fn assign_proxies_round_robin(
        &self,
        credential_ids: Option<Vec<u64>>,
    ) -> Result<AssignRoundRobinResponse, AdminServiceError> {
        let urls = self.proxy_pool.assignable_urls();
        if urls.is_empty() {
            return Err(AdminServiceError::InvalidCredential(
                "没有可用代理（需已启用且健康检查未失败）".to_string(),
            ));
        }

        let target_ids: Vec<u64> = match credential_ids {
            Some(ids) if !ids.is_empty() => ids,
            _ => self
                .token_manager
                .snapshot()
                .entries
                .iter()
                .map(|c| c.id)
                .collect(),
        };

        let mut assigned = 0;
        for (i, cred_id) in target_ids.iter().enumerate() {
            let url = urls[i % urls.len()].clone();
            if self
                .token_manager
                .update_credential(
                    *cred_id,
                    CredentialUpdate {
                        proxy_url: Some(Some(url)),
                        ..CredentialUpdate::default()
                    },
                )
                .is_ok()
            {
                assigned += 1;
            }
        }

        Ok(AssignRoundRobinResponse {
            assigned,
            proxy_count: urls.len(),
        })
    }

    // ============ 错误分类 ============

    /// 分类简单操作错误（set_disabled, set_priority, reset_and_enable）
    fn classify_error(&self, e: anyhow::Error, id: u64) -> AdminServiceError {
        if let Some(error) = classify_rate_limit(&e) {
            return error;
        }
        let msg = e.to_string();
        if msg.contains("不存在") {
            AdminServiceError::NotFound { id }
        } else {
            AdminServiceError::InternalError(msg)
        }
    }

    /// 分类余额查询错误（可能涉及上游 API 调用）
    fn classify_balance_error(&self, e: anyhow::Error, id: u64) -> AdminServiceError {
        if let Some(error) = classify_rate_limit(&e) {
            return error;
        }
        if e.downcast_ref::<RefreshTokenInvalidError>().is_some() {
            return AdminServiceError::InvalidCredential(
                "refreshToken 已失效，请重新登录或更新凭据".to_string(),
            );
        }
        let msg = e.to_string();

        // 1. 凭据不存在
        if msg.contains("不存在") {
            return AdminServiceError::NotFound { id };
        }

        // 2. API Key 凭据不支持刷新：客户端请求错误，映射为 400
        if msg.contains("API Key 凭据不支持刷新") {
            return AdminServiceError::InvalidCredential(msg);
        }

        // 3. 上游明确指出凭据缺少或携带了错误的 Profile ARN，属于导入凭据不完整/无效。
        if msg.contains("Invalid profileArn") {
            return AdminServiceError::InvalidCredential(
                "凭据缺少或包含无效 profileArn，无法查询余额；请重新登录获取 profileArn，或导入包含 profileArn 的完整凭据"
                    .to_string(),
            );
        }

        // 3. 上游服务错误特征：HTTP 响应错误或网络错误
        let is_upstream_error = msg.contains("获取使用额度失败") ||
            msg.contains("获取可用模型失败") ||
            msg.contains("设置用户偏好失败") ||
            // HTTP 响应错误（来自 refresh_*_token 的错误消息）
            msg.contains("凭证已过期或无效") ||
            msg.contains("权限不足") ||
            msg.contains("已被限流") ||
            msg.contains("服务器错误") ||
            msg.contains("Token 刷新失败") ||
            msg.contains("暂时不可用") ||
            // 网络错误（reqwest 错误格式）
            msg.contains("error sending request") ||
            msg.contains("error trying to connect") ||
            msg.contains("connection") ||
            msg.contains("timeout") ||
            msg.contains("timed out") ||
            msg.contains("proxy") ||
            msg.contains("SOCKS") ||
            msg.contains("dns") ||
            msg.contains("DNS");

        if is_upstream_error {
            AdminServiceError::UpstreamError(msg)
        } else {
            // 4. 默认归类为内部错误（本地验证失败、配置错误等）
            // 包括：缺少 refreshToken、refreshToken 已被截断、无法生成 machineId 等
            AdminServiceError::InternalError(msg)
        }
    }

    /// 分类添加凭据错误
    fn classify_add_error(&self, e: anyhow::Error) -> AdminServiceError {
        if let Some(error) = classify_rate_limit(&e) {
            return error;
        }
        let msg = e.to_string();

        // 凭据验证失败（refreshToken 无效、格式错误等）
        let is_invalid_credential = msg.contains("缺少 refreshToken")
            || msg.contains("refreshToken 为空")
            || msg.contains("refreshToken 已被截断")
            || msg.contains("凭据已存在")
            || msg.contains("refreshToken 重复")
            || msg.contains("kiroApiKey 重复")
            || msg.contains("缺少 kiroApiKey")
            || msg.contains("kiroApiKey 为空")
            || msg.contains("凭证已过期或无效")
            || msg.contains("权限不足")
            || msg.contains("已被限流");

        if is_invalid_credential {
            AdminServiceError::InvalidCredential(msg)
        } else if msg.contains("error trying to connect")
            || msg.contains("connection")
            || msg.contains("timeout")
        {
            AdminServiceError::UpstreamError(msg)
        } else {
            AdminServiceError::InternalError(msg)
        }
    }

    // ── Social 登录（Portal PKCE OAuth）────────────────────────────────────────

    /// 发起 Social 登录，返回 portal URL 供用户在浏览器打开
    ///
    /// 始终在服务端本机启动临时 TCP 回调服务器，redirect_uri 为 `http://127.0.0.1:{port}`。
    /// 本机浏览器授权后自动完成；远程访问时用户从地址栏复制回调 URL，经 `complete_social_login` 手动完成。
    pub async fn start_social_login(
        &self,
        req: StartSocialLoginRequest,
    ) -> Result<StartSocialLoginResponse, AdminServiceError> {
        if let Some(ref u) = req.proxy_url
            && !u.is_empty()
        {
            validate_credential_proxy_url(u)?;
        }

        let global_proxy = self.token_manager.proxy();
        let proxy = resolve_login_proxy(req.proxy_url.as_deref(), global_proxy);

        let auth_endpoint = req
            .auth_endpoint
            .unwrap_or_else(|| social::KIRO_AUTH_ENDPOINT.to_string());

        let (code_verifier, code_challenge) = social::generate_pkce();
        let state = uuid::Uuid::new_v4().to_string();

        let (tx, rx) = tokio::sync::oneshot::channel::<social::OAuthCallbackData>();

        // 启动本地 TCP 回调服务器（本地模式）
        // 远程访问时用户须从浏览器地址栏复制回调 URL，通过 complete_social_login 接口手动完成
        let (port, server_handle) = social::start_callback_server(tx)
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let redirect_uri = format!("http://127.0.0.1:{}", port);
        let portal_url = social::build_portal_url(&state, &code_challenge, &redirect_uri);

        let expires_at = Utc::now() + Duration::minutes(10);
        let session_id = uuid::Uuid::new_v4().to_string();

        let cred_template = KiroCredentials {
            auth_method: Some("social".to_string()),
            priority: req.priority,
            email: req.email,
            proxy_url: req.proxy_url,
            ..Default::default()
        };

        let session = SocialAuthSession {
            auth_endpoint,
            state,
            code_verifier,
            redirect_uri,
            expires_at,
            callback_rx: tokio::sync::Mutex::new(rx),
            cred_template,
            proxy,
            _server_handle: server_handle,
            relogin_target_id: None,
        };

        self.social_sessions
            .lock()
            .insert(session_id.clone(), session);

        Ok(StartSocialLoginResponse {
            session_id,
            portal_url,
            expires_at: expires_at.to_rfc3339(),
        })
    }

    /// 轮询一次 Social 登录状态
    pub async fn poll_social_login(
        &self,
        session_id: &str,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        use tokio::sync::oneshot::error::TryRecvError;

        // 一次加锁同时完成：过期检查 + 非阻塞回调接收，消除 TOCTOU
        enum PollOutcome {
            Expired,
            Closed,
            Pending,
            Received(social::OAuthCallbackData),
        }

        let outcome = {
            let sessions = self.social_sessions.lock();
            let Some(session) = sessions.get(session_id) else {
                return Err(AdminServiceError::NotFound { id: 0 });
            };

            if Utc::now() >= session.expires_at {
                PollOutcome::Expired
            } else {
                match session.callback_rx.try_lock() {
                    Ok(mut rx) => match rx.try_recv() {
                        Ok(data) => PollOutcome::Received(data),
                        Err(TryRecvError::Empty) => PollOutcome::Pending,
                        Err(TryRecvError::Closed) => PollOutcome::Closed,
                    },
                    Err(_) => PollOutcome::Pending,
                }
            }
        };

        match outcome {
            PollOutcome::Pending => Ok(PollIdcLoginResponse::Pending),
            PollOutcome::Expired => {
                self.social_sessions.lock().remove(session_id);
                Ok(PollIdcLoginResponse::Expired)
            }
            PollOutcome::Closed => {
                self.social_sessions.lock().remove(session_id);
                Err(AdminServiceError::InternalError(
                    "Social 登录回调服务器已关闭，请重新发起登录".to_string(),
                ))
            }
            PollOutcome::Received(callback) => {
                self.do_complete_social_login(session_id, callback).await
            }
        }
    }

    /// 内部：完成 Social 登录的 token 兑换和凭据创建（供轮询回调和手动完成共用）
    ///
    /// 调用前须确认 session 存在且未过期。会在内部做 state CSRF 校验。
    async fn do_complete_social_login(
        &self,
        session_id: &str,
        callback: social::OAuthCallbackData,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        // 先做 CSRF 校验（不移除 session，校验失败时保持 session 可继续轮询）
        {
            let sessions = self.social_sessions.lock();
            let s = sessions
                .get(session_id)
                .ok_or(AdminServiceError::NotFound { id: 0 })?;
            if callback.state != s.state {
                tracing::warn!(
                    "Social 登录 state 不匹配（期望 {}, 收到 {}），已拒绝",
                    s.state,
                    callback.state
                );
                return Err(AdminServiceError::InternalError(
                    "OAuth state 不匹配，请重新发起登录".to_string(),
                ));
            }
        }

        // 移除 session（含 code_verifier 等敏感数据）
        let session = self
            .social_sessions
            .lock()
            .remove(session_id)
            .ok_or(AdminServiceError::NotFound { id: 0 })?;

        let config = self.token_manager.config();

        // 构建完整的 redirect_uri（与 IDE 行为一致）
        let full_redirect_uri = if callback.login_option.is_empty() {
            format!("{}{}", session.redirect_uri, callback.path)
        } else {
            format!(
                "{}{}?login_option={}",
                session.redirect_uri,
                callback.path,
                urlencoding::encode(&callback.login_option),
            )
        };

        let token = social::exchange_code_for_token(
            &session.auth_endpoint,
            &callback.code,
            &session.code_verifier,
            &full_redirect_uri,
            config,
            session.proxy.as_ref(),
        )
        .await
        .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        // 重新登录模式：更新已有凭据而非创建新凭据
        if let Some(target_id) = session.relogin_target_id {
            let refresh_token = token.refresh_token.ok_or_else(|| {
                AdminServiceError::InternalError(
                    "Social 登录未返回 refreshToken，无法更新凭据".to_string(),
                )
            })?;
            self.do_relogin_update(target_id, refresh_token)
                .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
            tracing::info!("Social 重新登录成功，凭据 #{} Token 已更新", target_id);
            return Ok(PollIdcLoginResponse::Success {
                credential_id: target_id,
            });
        }

        let mut new_cred = session.cred_template;
        new_cred.access_token = Some(token.access_token);
        new_cred.refresh_token = token.refresh_token;
        new_cred.expires_at = token.expires_at.or_else(|| {
            token
                .expires_in
                .map(|secs| (Utc::now() + Duration::seconds(secs)).to_rfc3339())
        });
        if let Some(arn) = token.profile_arn {
            new_cred.profile_arn = Some(arn);
        }

        let credential_id = self
            .token_manager
            .add_credential(new_cred)
            .await
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        // 主动刷新余额（含订阅等级 / 邮箱）并写入缓存，登录后立即可见
        if let Err(e) = self.get_balance(credential_id).await {
            tracing::warn!("Social 登录后刷新余额失败（不影响登录）: {}", e);
        }

        tracing::info!("Social 登录成功，已添加凭据 #{}", credential_id);
        Ok(PollIdcLoginResponse::Success { credential_id })
    }

    /// 手动完成 Social 登录：远程访问时从浏览器地址栏粘贴的回调 URL 中提取参数，直接完成 token 兑换
    pub async fn complete_social_login(
        &self,
        session_id: &str,
        code: String,
        state: String,
        login_option: String,
        path: String,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        // 过期检查
        {
            let sessions = self.social_sessions.lock();
            let s = sessions
                .get(session_id)
                .ok_or(AdminServiceError::NotFound { id: 0 })?;
            if Utc::now() >= s.expires_at {
                return Ok(PollIdcLoginResponse::Expired);
            }
        }

        let callback = social::OAuthCallbackData {
            code,
            login_option,
            path,
            state,
        };
        self.do_complete_social_login(session_id, callback).await
    }

    /// 分类删除凭据错误
    fn classify_delete_error(&self, e: anyhow::Error, id: u64) -> AdminServiceError {
        let msg = e.to_string();
        if msg.contains("不存在") {
            AdminServiceError::NotFound { id }
        } else if msg.contains("只能删除已禁用的凭据") || msg.contains("请先禁用凭据")
        {
            AdminServiceError::InvalidCredential(msg)
        } else {
            AdminServiceError::InternalError(msg)
        }
    }

    // ── IdC 设备授权登录 ──────────────────────────────────────────────────────

    /// 发起 IdC 设备授权，返回验证码和 URL
    pub async fn start_idc_login(
        &self,
        req: StartIdcLoginRequest,
    ) -> Result<StartIdcLoginResponse, AdminServiceError> {
        if let Some(ref u) = req.proxy_url
            && !u.is_empty()
        {
            validate_credential_proxy_url(u)?;
        }

        let config = self.token_manager.config();
        let global_proxy = self.token_manager.proxy();

        // 代理：优先用请求级，否则回退全局
        let proxy = resolve_login_proxy(req.proxy_url.as_deref(), global_proxy);

        let start_url = req.start_url.as_deref().unwrap_or(BUILDER_ID_START_URL);

        // 1. 注册 OIDC 客户端
        let reg = idc::register_client(&req.region, start_url, config, proxy.as_ref())
            .await
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        // 2. 发起设备授权
        let device = idc::start_device_authorization(
            &req.region,
            start_url,
            &reg.client_id,
            &reg.client_secret,
            config,
            proxy.as_ref(),
        )
        .await
        .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let expires_at = Utc::now() + Duration::seconds(device.expires_in);
        let session_id = Uuid::new_v4().to_string();

        // 身份提供商：默认 Start URL 为 AWS Builder ID，自定义 Start URL 为企业 IAM Identity Center
        let provider = if start_url == BUILDER_ID_START_URL {
            "BuilderId"
        } else {
            "Enterprise"
        };

        // 构建登录成功后写入的凭据模板
        let cred_template = KiroCredentials {
            auth_method: Some("idc".to_string()),
            provider: Some(provider.to_string()),
            client_id: Some(reg.client_id.clone()),
            client_secret: Some(reg.client_secret.clone()),
            start_url: Some(start_url.to_string()),
            region: Some(req.region.clone()),
            priority: req.priority,
            email: req.email,
            proxy_url: req.proxy_url,
            ..Default::default()
        };

        let session = IdcAuthSession {
            region: req.region,
            client_id: reg.client_id,
            client_secret: reg.client_secret,
            device_code: device.device_code,
            expires_at,
            poll_interval: device.interval.max(5),
            cred_template,
            proxy,
            relogin_target_id: None,
        };

        let poll_interval = session.poll_interval;
        self.idc_sessions.lock().insert(session_id.clone(), session);

        Ok(StartIdcLoginResponse {
            session_id,
            user_code: device.user_code,
            verification_uri: device.verification_uri,
            verification_uri_complete: device.verification_uri_complete,
            expires_at: expires_at.to_rfc3339(),
            poll_interval,
        })
    }

    /// 轮询一次 IdC 登录状态
    pub async fn poll_idc_login(
        &self,
        session_id: &str,
    ) -> Result<PollIdcLoginResponse, AdminServiceError> {
        let (
            region,
            client_id,
            client_secret,
            device_code,
            _expires_at,
            proxy,
            cred_template,
            relogin_target_id,
        ) = {
            let sessions = self.idc_sessions.lock();
            let s = sessions
                .get(session_id)
                .ok_or(AdminServiceError::NotFound { id: 0 })?;

            if Utc::now() >= s.expires_at {
                return Ok(PollIdcLoginResponse::Expired);
            }

            (
                s.region.clone(),
                s.client_id.clone(),
                s.client_secret.clone(),
                s.device_code.clone(),
                s.expires_at,
                s.proxy.clone(),
                s.cred_template.clone(),
                s.relogin_target_id,
            )
        };

        let config = self.token_manager.config();

        match idc::poll_token(
            &region,
            &client_id,
            &client_secret,
            &device_code,
            config,
            proxy.as_ref(),
        )
        .await
        {
            idc::PollResult::Pending => Ok(PollIdcLoginResponse::Pending),
            idc::PollResult::Expired => {
                self.idc_sessions.lock().remove(session_id);
                Ok(PollIdcLoginResponse::Expired)
            }
            idc::PollResult::Error(e) => Err(AdminServiceError::InternalError(e.to_string())),
            idc::PollResult::Success(token) => {
                self.idc_sessions.lock().remove(session_id);

                // 重新登录模式：更新已有凭据而非创建新凭据
                if let Some(target_id) = relogin_target_id {
                    if let Some(refresh_token) = token.refresh_token {
                        self.do_relogin_update(target_id, refresh_token)
                            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
                    }
                    tracing::info!("IdC 重新登录成功，凭据 #{} Token 已更新", target_id);
                    return Ok(PollIdcLoginResponse::Success {
                        credential_id: target_id,
                    });
                }

                // 写入凭据
                let mut new_cred = cred_template;
                new_cred.access_token = Some(token.access_token);
                new_cred.refresh_token = token.refresh_token;
                if let Some(secs) = token.expires_in {
                    new_cred.expires_at = Some((Utc::now() + Duration::seconds(secs)).to_rfc3339());
                }

                let credential_id = self
                    .token_manager
                    .add_credential(new_cred)
                    .await
                    .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

                // 主动刷新余额（含订阅等级 / 邮箱）并写入缓存，登录后立即可见
                if let Err(e) = self.get_balance(credential_id).await {
                    tracing::warn!("IdC 登录后刷新余额失败（不影响登录）: {}", e);
                }

                tracing::info!("IdC 设备授权登录成功，已添加凭据 #{}", credential_id);
                Ok(PollIdcLoginResponse::Success { credential_id })
            }
        }
    }

    /// 内部：重新登录完成后更新已有凭据的 Token（禁用→更新→重置→启用）
    fn do_relogin_update(&self, target_id: u64, refresh_token: String) -> anyhow::Result<()> {
        // 先禁用（update_refresh_token 要求凭据处于禁用状态）
        self.token_manager.set_disabled(target_id, true)?;
        // 更新 refreshToken（同时清空 accessToken 和 expiresAt，系统会在下次使用时自动刷新）
        self.token_manager
            .update_refresh_token(target_id, refresh_token, None, None)?;
        // 重置失败计数并重新启用
        self.token_manager.reset_and_enable(target_id)?;
        Ok(())
    }

    /// 发起 Social 重新登录（更新已有凭据的 Token 而非创建新凭据）
    pub async fn start_social_relogin(
        &self,
        target_id: u64,
        req: StartSocialLoginRequest,
    ) -> Result<StartSocialLoginResponse, AdminServiceError> {
        // 验证目标凭据存在
        {
            let snapshot = self.token_manager.snapshot();
            if !snapshot.entries.iter().any(|e| e.id == target_id) {
                return Err(AdminServiceError::NotFound { id: target_id });
            }
        }

        if let Some(ref u) = req.proxy_url
            && !u.is_empty()
        {
            validate_credential_proxy_url(u)?;
        }

        let global_proxy = self.token_manager.proxy();
        let proxy = resolve_login_proxy(req.proxy_url.as_deref(), global_proxy);

        let auth_endpoint = req
            .auth_endpoint
            .unwrap_or_else(|| social::KIRO_AUTH_ENDPOINT.to_string());

        let (code_verifier, code_challenge) = social::generate_pkce();
        let state = uuid::Uuid::new_v4().to_string();

        let (tx, rx) = tokio::sync::oneshot::channel::<social::OAuthCallbackData>();

        let (port, server_handle) = social::start_callback_server(tx)
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let redirect_uri = format!("http://127.0.0.1:{}", port);
        let portal_url = social::build_portal_url(&state, &code_challenge, &redirect_uri);

        let expires_at = Utc::now() + Duration::minutes(10);
        let session_id = uuid::Uuid::new_v4().to_string();

        let session = SocialAuthSession {
            auth_endpoint,
            state,
            code_verifier,
            redirect_uri,
            expires_at,
            callback_rx: tokio::sync::Mutex::new(rx),
            cred_template: KiroCredentials::default(),
            proxy,
            _server_handle: server_handle,
            relogin_target_id: Some(target_id),
        };

        self.social_sessions
            .lock()
            .insert(session_id.clone(), session);

        Ok(StartSocialLoginResponse {
            session_id,
            portal_url,
            expires_at: expires_at.to_rfc3339(),
        })
    }

    /// 发起 IdC 重新登录（更新已有凭据的 Token 而非创建新凭据）
    pub async fn start_idc_relogin(
        &self,
        target_id: u64,
        req: StartIdcLoginRequest,
    ) -> Result<StartIdcLoginResponse, AdminServiceError> {
        // 验证目标凭据存在
        {
            let snapshot = self.token_manager.snapshot();
            if !snapshot.entries.iter().any(|e| e.id == target_id) {
                return Err(AdminServiceError::NotFound { id: target_id });
            }
        }

        if let Some(ref u) = req.proxy_url
            && !u.is_empty()
        {
            validate_credential_proxy_url(u)?;
        }

        let config = self.token_manager.config();
        let global_proxy = self.token_manager.proxy();

        let proxy = resolve_login_proxy(req.proxy_url.as_deref(), global_proxy);

        let start_url = req.start_url.as_deref().unwrap_or(BUILDER_ID_START_URL);

        let reg = idc::register_client(&req.region, start_url, config, proxy.as_ref())
            .await
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let device = idc::start_device_authorization(
            &req.region,
            start_url,
            &reg.client_id,
            &reg.client_secret,
            config,
            proxy.as_ref(),
        )
        .await
        .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        let expires_at = Utc::now() + Duration::seconds(device.expires_in);
        let session_id = Uuid::new_v4().to_string();

        let session = IdcAuthSession {
            region: req.region,
            client_id: reg.client_id,
            client_secret: reg.client_secret,
            device_code: device.device_code,
            expires_at,
            poll_interval: device.interval.max(5),
            cred_template: KiroCredentials::default(),
            proxy,
            relogin_target_id: Some(target_id),
        };

        let poll_interval = session.poll_interval;
        self.idc_sessions.lock().insert(session_id.clone(), session);

        Ok(StartIdcLoginResponse {
            session_id,
            user_code: device.user_code,
            verification_uri: device.verification_uri,
            verification_uri_complete: device.verification_uri_complete,
            expires_at: expires_at.to_rfc3339(),
            poll_interval,
        })
    }
}

// ============ 模型注册表：纯函数层 ============
//
// 这些函数只操作 `ModelRegistryFile`，不碰 I/O、不碰全局状态，因此可以脱离
// AdminService（构造它需要一个真实的 MultiTokenManager）单独单测。
// AdminService 上的方法只负责「取 store → mutate → 热替换 → 返回快照」。

/// 可写字段中「会被自动 pin」的那部分 = **同步会覆盖的字段**，直接复用
/// `SYNC_MANAGED_FIELDS`，不在这里另抄一份（抄第二份就会漂移）。
///
/// 这条等式就是 pin 的定义本身：pin 的全部意义是挡住同步，所以「值得 pin 的字段」
/// 恰好是「同步会覆盖的字段」。反过来说，`enabled` / `sortOrder` / `matchKind` 是
/// 本地展示与准入策略，同步流程根本不碰，pin 了只会在 UI 上多出永远解不掉的锁标记。
use crate::anthropic::model_registry::SYNC_MANAGED_FIELDS as PATCHABLE_PINNED_FIELDS;

/// 领域错误穿过 `ModelRegistryStore::mutate` 的 `String` 通道时用的标签。
/// mutate 的闭包只能返回 `String`，直接丢失错误类型会让「模型不存在」变成 500。
const TAG_NOT_FOUND: &str = "[model-not-found] ";
const TAG_CONFLICT: &str = "[model-conflict] ";
const TAG_INVALID: &str = "[model-invalid-field] ";

fn encode_model_error(err: AdminServiceError) -> String {
    match err {
        AdminServiceError::ModelNotFound(m) => format!("{}{}", TAG_NOT_FOUND, m),
        AdminServiceError::ModelConflict(m) => format!("{}{}", TAG_CONFLICT, m),
        AdminServiceError::InvalidModelField(m) => format!("{}{}", TAG_INVALID, m),
        other => other.to_string(),
    }
}

fn decode_model_error(raw: String) -> AdminServiceError {
    if let Some(m) = raw.strip_prefix(TAG_NOT_FOUND) {
        AdminServiceError::ModelNotFound(m.to_string())
    } else if let Some(m) = raw.strip_prefix(TAG_CONFLICT) {
        AdminServiceError::ModelConflict(m.to_string())
    } else if let Some(m) = raw.strip_prefix(TAG_INVALID) {
        AdminServiceError::InvalidModelField(m.to_string())
    } else {
        // 剩下的都是 store 自己的失败：读写 / 序列化 / 落盘前的整表校验。
        // 前两类是服务端问题；整表校验失败在这里理论上不会发生（每个 in_file
        // 函数返回前都已自校验），保守归为 InternalError 而不是伪装成 4xx。
        AdminServiceError::InternalError(raw)
    }
}

/// 整表校验：任何一次写入后的文件都必须能被 `from_file` 加载，
/// 否则下次启动会整体退回内置默认（degraded）。
fn validate_registry_file(
    file: &crate::anthropic::model_registry::ModelRegistryFile,
) -> Result<crate::anthropic::model_registry::ModelRegistry, AdminServiceError> {
    crate::anthropic::model_registry::ModelRegistry::from_file(file.clone())
        .map_err(AdminServiceError::ModelConflict)
}

/// 有效行集 = 内置默认 ∪ 覆盖层（叠加 syncState.modelMeta）。
/// **查行必须走这里**，不能只看 `file.models`：内置行不在覆盖层里。
fn effective_registry(
    file: &crate::anthropic::model_registry::ModelRegistryFile,
) -> Result<crate::anthropic::model_registry::ModelRegistry, AdminServiceError> {
    validate_registry_file(file)
}

/// 应用 PATCH，被编辑的字段自动进 pinned。
///
/// 语义要点：**白名单之外的字段一律报错**。serde 默认静默丢弃未知字段，
/// 用户改 `origin` 会拿到 200 却什么都没发生；而 `origin` 恰恰是删除保护的
/// 依据，静默失败在这里是安全问题的一半。
pub fn apply_model_patch(
    row: &mut crate::anthropic::model_registry::ModelRow,
    req: &crate::admin::types::PatchModelRequest,
) -> Result<(), AdminServiceError> {
    if !req.extra.is_empty() {
        let names: Vec<&str> = req.extra.keys().map(|k| k.as_str()).collect();
        return Err(AdminServiceError::InvalidModelField(format!(
            "以下字段不可写（只读或未知）: {}。可写字段: exposedId、displayName、\
             contextWindow、maxOutputTokens、exposeThinkingVariant、enabled、sortOrder、\
             matchKind、supportsReasoning",
            names.join("、")
        )));
    }

    // 先全部校验再落值：中途失败不能留下改了一半的行。
    if let Some(v) = req.exposed_id.as_deref()
        && v.trim().is_empty() {
            return Err(AdminServiceError::InvalidModelField(
                "exposedId 不能为空".to_string(),
            ));
        }
    if let Some(v) = req.display_name.as_deref()
        && v.trim().is_empty() {
            return Err(AdminServiceError::InvalidModelField(
                "displayName 不能为空".to_string(),
            ));
        }
    if let Some(v) = req.context_window
        && v <= 0 {
            return Err(AdminServiceError::InvalidModelField(
                "contextWindow 必须为正数".to_string(),
            ));
        }
    if let Some(v) = req.max_output_tokens
        && v <= 0 {
            return Err(AdminServiceError::InvalidModelField(
                "maxOutputTokens 必须为正数".to_string(),
            ));
        }

    let pin = |row: &mut crate::anthropic::model_registry::ModelRow, field: &str| {
        if PATCHABLE_PINNED_FIELDS.contains(&field) && !row.pinned.iter().any(|p| p == field) {
            row.pinned.push(field.to_string());
        }
    };

    if let Some(v) = req.exposed_id.clone() {
        row.exposed_id = v.trim().to_ascii_lowercase();
        pin(row, "exposedId");
    }
    if let Some(v) = req.display_name.clone() {
        row.display_name = v;
        pin(row, "displayName");
    }
    if let Some(v) = req.context_window {
        row.context_window = v;
        pin(row, "contextWindow");
    }
    if let Some(v) = req.max_output_tokens {
        row.max_output_tokens = v;
        pin(row, "maxOutputTokens");
    }
    if let Some(v) = req.expose_thinking_variant {
        row.expose_thinking_variant = v;
        pin(row, "exposeThinkingVariant");
    }
    // 以下四个字段同步流程不覆盖，不需要 pin（见 PATCHABLE_PINNED_FIELDS 注释）：
    // enabled/sortOrder/matchKind 是既有的本地策略；supportsReasoning 同理——
    // 同步的数据源（ListAvailableModels）不返回这个信息，加进 pin 名单只会让
    // 它永远显示解不开的锁，且没有任何东西会去解它。
    if let Some(v) = req.enabled {
        row.enabled = v;
    }
    if let Some(v) = req.sort_order {
        row.sort_order = v;
    }
    if let Some(v) = req.match_kind {
        row.match_kind = v;
    }
    // 用 `supportsReasoningSet` 标出「本次确实要动这个字段」，而不是直接看
    // `supports_reasoning.is_some()`——否则「清回跟随内置默认」（值本身是 None）
    // 就永远无法通过 PATCH 表达，只能靠直接改 models.json。
    if req.supports_reasoning_set {
        row.supports_reasoning = req.supports_reasoning;
    }

    for field in &req.unpin {
        row.pinned.retain(|p| p != field);
    }
    Ok(())
}

/// builtin 行永不可删。
pub fn ensure_deletable(
    row: &crate::anthropic::model_registry::ModelRow,
) -> Result<(), AdminServiceError> {
    if row.origin == crate::anthropic::model_registry::ModelOrigin::Builtin {
        return Err(AdminServiceError::InvalidModelField(format!(
            "内置模型不可删除: {}",
            row.upstream_id
        )));
    }
    Ok(())
}

/// PATCH 一行。行可能只存在于内置默认里（覆盖层没有它），此时把它**下沉**成一条
/// manual 覆盖行。
///
/// 为什么下沉后 origin 记 `Manual` 而不是保留 `Builtin`：`origin=builtin` 的行
/// 不允许出现在覆盖层（见 model_sync 里 `retain` 维持的不变量），一旦写进去，
/// 它就成了内置定义的冻结快照。删除保护因此**不能**只看覆盖层里的 origin ——
/// `delete_model_in_file` 改为对照 `builtin_rows()` 判定，见那里的注释。
fn patch_model_in_file(
    file: &mut crate::anthropic::model_registry::ModelRegistryFile,
    upstream_id: &str,
    req: &crate::admin::types::PatchModelRequest,
) -> Result<(), AdminServiceError> {
    use crate::anthropic::model_registry::{ModelOrigin, ModelStatus};

    // 改动先做在副本上，校验通过才提交，保证「失败不留半成品」
    let mut next = file.clone();
    let effective = effective_registry(&next)?;

    match next.models.iter_mut().find(|r| r.upstream_id == upstream_id) {
        Some(row) => apply_model_patch(row, req)?,
        None => {
            let Some(base) = effective
                .rows()
                .iter()
                .find(|r| r.upstream_id == upstream_id)
                .cloned()
            else {
                return Err(AdminServiceError::ModelNotFound(upstream_id.to_string()));
            };
            let mut row = base;
            row.origin = ModelOrigin::Manual;
            // 同步元数据的权威源是 syncState.modelMeta，覆盖层不该带着它的副本，
            // 否则文件里会出现两份互相打架的 status/missingSyncRounds。
            //
            // 注意这里清掉行上的 last_seen_at **并不能**清掉 modelMeta 里的那份：
            // 加载器（overlay_onto_builtin）判定「同步是否写过这一行」时两处都算数，
            // 而 PATCH 不该去动同步元数据。因此在同步已开启的部署里，本行 4 个非
            // pinned 的同步管辖字段会取「编辑这一刻的内置定义」，直到下一轮同步把
            // 它们刷成上游值（窗口 ≤ 一个同步周期）。已知限制，见 overlay_onto_builtin。
            row.status = ModelStatus::Active;
            row.missing_sync_rounds = 0;
            row.last_seen_at = None;
            apply_model_patch(&mut row, req)?;
            next.models.push(row);
        }
    }

    validate_registry_file(&next)?;
    *file = next;
    Ok(())
}

/// 新增一行手动模型。
fn create_model_in_file(
    file: &mut crate::anthropic::model_registry::ModelRegistryFile,
    req: &crate::admin::types::CreateModelRequest,
) -> Result<(), AdminServiceError> {
    use crate::anthropic::model_registry::{MatchKind, ModelOrigin, ModelRow, ModelStatus};
    use crate::anthropic::model_sync::{derive_exposed_id, derive_thinking_variant};

    // M1：未知字段一律拒绝（与 PATCH 同一处理）。静默丢弃会让「字段名写错」
    // 表现成 200 成功，而值根本没进表 —— 用户下次看到的是「我明明设过了」。
    if !req.extra.is_empty() {
        let names: Vec<&str> = req.extra.keys().map(|k| k.as_str()).collect();
        return Err(AdminServiceError::InvalidModelField(format!(
            "以下字段不可写（只读或未知）: {}。可写字段: upstreamId、exposedId、displayName、\
             contextWindow、maxOutputTokens、exposeThinkingVariant、enabled、sortOrder、matchKind",
            names.join("、")
        )));
    }

    let upstream_id = req.upstream_id.trim().to_string();
    if upstream_id.is_empty() {
        return Err(AdminServiceError::InvalidModelField(
            "upstreamId 不能为空".to_string(),
        ));
    }

    let mut next = file.clone();
    let effective = effective_registry(&next)?;
    if effective.rows().iter().any(|r| r.upstream_id == upstream_id) {
        return Err(AdminServiceError::ModelConflict(format!(
            "已存在同名 upstreamId: {}",
            upstream_id
        )));
    }

    let context_window = req
        .context_window
        .unwrap_or(crate::anthropic::model_registry::PASSTHROUGH_CONTEXT_WINDOW);
    let max_output_tokens = req.max_output_tokens.unwrap_or(64_000);
    if context_window <= 0 || max_output_tokens <= 0 {
        return Err(AdminServiceError::InvalidModelField(
            "contextWindow / maxOutputTokens 必须为正数".to_string(),
        ));
    }

    // sortOrder 基线取**有效行集**的最大值，否则会与内置行（占 [0,130]）撞号，
    // 同值行之间的列表顺序就不确定了。
    let max_sort = effective
        .rows()
        .iter()
        .map(|r| r.sort_order)
        .max()
        .unwrap_or(0);
    let match_kind = req.match_kind.unwrap_or(MatchKind::Exact);

    let row = ModelRow {
        exposed_id: req
            .exposed_id
            .clone()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| derive_exposed_id(&upstream_id)),
        display_name: req
            .display_name
            .clone()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| upstream_id.clone()),
        owned_by: if upstream_id.starts_with("claude-") {
            "anthropic".to_string()
        } else {
            "openai".to_string()
        },
        model_type: "chat".to_string(),
        created: Utc::now().timestamp(),
        context_window,
        max_output_tokens,
        // prefix 行不得开启 thinking 变体（§4.5 第 6 条），这里直接按类型定死，
        // 免得写出一份自己加载不了的文件。
        expose_thinking_variant: match match_kind {
            MatchKind::Prefix => false,
            MatchKind::Exact => req
                .expose_thinking_variant
                .unwrap_or_else(|| derive_thinking_variant(&upstream_id)),
        },
        enabled: req.enabled.unwrap_or(true),
        listed: match_kind == MatchKind::Exact,
        status: ModelStatus::Active,
        origin: ModelOrigin::Manual,
        sort_order: req.sort_order.unwrap_or(max_sort + 10),
        pinned: Vec::new(),
        missing_sync_rounds: 0,
        last_seen_at: None,
        match_substrings: Vec::new(),
        supports_reasoning: None,
        upstream_id,
        match_kind,
    };

    next.models.push(row);
    validate_registry_file(&next)?;
    *file = next;
    Ok(())
}

/// 删除一行。
fn delete_model_in_file(
    file: &mut crate::anthropic::model_registry::ModelRegistryFile,
    upstream_id: &str,
) -> Result<(), AdminServiceError> {
    // 删除保护对照的是**代码里的内置集**，不是覆盖层行上的 origin：
    // PATCH 一个内置行会在覆盖层落一条 manual 行（覆盖层不允许 builtin），
    // 若只看 origin，「先改一下再删」就绕过了保护，而且删掉后内置行会重新
    // 出现在列表里——用户看到的是「删除按钮没用」。同源判据见
    // `is_builtin_upstream_id`（响应里的 `deletable` 字段也调它）。
    if crate::anthropic::model_registry::is_builtin_upstream_id(upstream_id) {
        return Err(AdminServiceError::InvalidModelField(format!(
            "内置模型不可删除: {}",
            upstream_id
        )));
    }

    let Some(idx) = file.models.iter().position(|r| r.upstream_id == upstream_id) else {
        return Err(AdminServiceError::ModelNotFound(upstream_id.to_string()));
    };
    // 老文件里可能残留 origin=builtin 的行（本分支中间版本写出的快照），继续挡住。
    ensure_deletable(&file.models[idx])?;

    if let Some(alias) = file.aliases.iter().find(|a| a.to == upstream_id) {
        return Err(AdminServiceError::ModelConflict(format!(
            "别名 {} 指向该模型，请先删除别名",
            alias.from
        )));
    }

    let mut next = file.clone();
    next.models.remove(idx);
    // 行没了，它的同步元数据就是孤儿记录，一并清掉
    next.sync_state.model_meta.remove(upstream_id);
    validate_registry_file(&next)?;
    *file = next;
    Ok(())
}

/// 新增或覆盖一条别名。
fn upsert_alias_in_file(
    file: &mut crate::anthropic::model_registry::ModelRegistryFile,
    req: &crate::admin::types::UpsertAliasRequest,
) -> Result<(), AdminServiceError> {
    use crate::anthropic::model_registry::ModelAlias;

    let from = req.from.trim().to_ascii_lowercase();
    let to = req.to.trim().to_string();
    if from.is_empty() || to.is_empty() {
        return Err(AdminServiceError::InvalidModelField(
            "别名的 from / to 都不能为空".to_string(),
        ));
    }

    let mut next = file.clone();
    let effective = effective_registry(&next)?;
    if !effective.rows().iter().any(|r| r.upstream_id == to) {
        return Err(AdminServiceError::ModelConflict(format!(
            "别名指向不存在的 upstreamId: {}",
            to
        )));
    }

    match next.aliases.iter_mut().find(|a| a.from == from) {
        Some(existing) => existing.to = to,
        None => next.aliases.push(ModelAlias { from, to }),
    }
    validate_registry_file(&next)?;
    *file = next;
    Ok(())
}

/// 删除一条别名。
fn delete_alias_in_file(
    file: &mut crate::anthropic::model_registry::ModelRegistryFile,
    from: &str,
) -> Result<(), AdminServiceError> {
    let from = from.trim().to_ascii_lowercase();
    let Some(idx) = file.aliases.iter().position(|a| a.from == from) else {
        return Err(AdminServiceError::ModelNotFound(format!("别名 {}", from)));
    };
    let mut next = file.clone();
    next.aliases.remove(idx);
    validate_registry_file(&next)?;
    *file = next;
    Ok(())
}

/// 组装 `GET /models` 响应。从 store 重新 load 而不是读全局 REGISTRY：
/// 只有 load 能给出 `degraded_reason` 与 `syncState` 原文。
fn build_model_registry_response(
    store: &crate::anthropic::model_registry_store::ModelRegistryStore,
    settings: ModelSyncSettings,
    enabled_credential_ids: &[u64],
) -> crate::admin::types::ModelRegistryResponse {
    use crate::admin::types::{ModelRegistryResponse, ModelRowResponse, ModelSyncSettingsResponse};
    use crate::anthropic::model_registry::is_builtin_upstream_id;

    let out = store.load();
    // rows() 已经叠加了 syncState.modelMeta（status / missingSyncRounds /
    // lastSeenAt），所以状态列直接取这里，不要去读 file.models。
    let mut models = out.registry.rows().to_vec();
    models.sort_by(|a, b| {
        a.sort_order
            .cmp(&b.sort_order)
            .then_with(|| a.upstream_id.cmp(&b.upstream_id))
    });
    // deletable 与 delete_model_in_file 同源判据，不能看 row.origin（见
    // ModelRowResponse 文档）。
    let models: Vec<ModelRowResponse> = models
        .into_iter()
        .map(|row| ModelRowResponse {
            deletable: !is_builtin_upstream_id(&row.upstream_id),
            row,
        })
        .collect();

    let credential_support_covered = enabled_credential_ids
        .iter()
        .filter(|id| {
            out.file
                .credential_support
                .get(&id.to_string())
                .is_some_and(|v| !v.is_empty())
        })
        .count();

    // 护栏结论编码在 syncState.source 里（定时同步不经 admin 层、重启会丢内存态，
    // 唯有落盘的字段能让 UI 持续看见「消失判定已停机」）。这里拆成两个显式字段，
    // 并把 source 还原成干净的来源串给 UI 展示。
    let mut sync_state = out.file.sync_state;
    let (clean_source, disappearance_check_skipped, missing_ratio) = sync_state
        .source
        .as_deref()
        .map(crate::anthropic::model_sync::decode_source)
        .unwrap_or_else(|| (String::new(), false, 0.0));
    if sync_state.source.is_some() {
        sync_state.source = Some(clean_source);
    }

    ModelRegistryResponse {
        models,
        aliases: out.registry.aliases().to_vec(),
        sync_state,
        settings: ModelSyncSettingsResponse {
            enabled: settings.enabled,
            time: settings.time,
            probe_credential_id: settings.probe_credential_id,
            allow_passthrough: settings.allow_passthrough,
        },
        degraded: out.degraded_reason.is_some(),
        degraded_reason: out.degraded_reason,
        credential_support_covered,
        credential_total: enabled_credential_ids.len(),
        disappearance_check_skipped,
        missing_ratio,
    }
}

fn classify_rate_limit(error: &anyhow::Error) -> Option<AdminServiceError> {
    error
        .downcast_ref::<UpstreamRateLimitError>()
        .map(|rate_limit| AdminServiceError::RateLimited {
            retry_after: rate_limit.retry_after().map(str::to_string),
        })
}

#[cfg(test)]
mod model_registry_tests {
    use super::*;
    use crate::admin::types::{CreateModelRequest, PatchModelRequest, UpsertAliasRequest};
    use crate::anthropic::model_registry::{
        builtin_rows, ModelAlias, ModelOrigin, ModelRegistry, ModelRegistryFile, ModelStatus,
    };
    use crate::anthropic::model_registry_store::ModelRegistryStore;
    use crate::anthropic::model_sync::{
        BoxFuture, ModelListFetcher, ModelSyncService, UpstreamModel,
    };

    fn empty_file() -> ModelRegistryFile {
        ModelRegistryFile::default()
    }

    /// PATCH 只能改白名单字段；被改字段自动进 pinned
    #[test]
    fn patch_pins_edited_fields_and_rejects_readonly() {
        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();

        let req = PatchModelRequest {
            context_window: Some(800_000),
            unpin: vec![],
            ..Default::default()
        };
        apply_model_patch(&mut row, &req).unwrap();
        assert_eq!(row.context_window, 800_000);
        assert!(
            row.pinned.contains(&"contextWindow".to_string()),
            "被编辑字段应自动 pin"
        );

        // unpin 后该字段回归自动同步
        let req = PatchModelRequest {
            unpin: vec!["contextWindow".to_string()],
            ..Default::default()
        };
        apply_model_patch(&mut row, &req).unwrap();
        assert!(!row.pinned.contains(&"contextWindow".to_string()));
    }

    /// builtin 行不可删
    #[test]
    fn builtin_row_cannot_be_deleted() {
        let rows = builtin_rows();
        let builtin = rows.iter().find(|r| r.exposed_id == "claude-opus-4-8").unwrap();
        assert!(ensure_deletable(builtin).is_err());
    }

    /// 白名单之外的字段必须**明确报错**，不能静默忽略——
    /// 静默忽略会让用户以为 origin/status 改成功了。
    #[test]
    fn patch_rejects_fields_outside_whitelist() {
        let raw = r#"{"contextWindow":500000,"origin":"manual","missingSyncRounds":0}"#;
        let req: PatchModelRequest = serde_json::from_str(raw).unwrap();
        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        let err = apply_model_patch(&mut row, &req).unwrap_err();
        match err {
            AdminServiceError::InvalidModelField(msg) => {
                assert!(msg.contains("origin"), "错误信息应点名被拒字段: {}", msg);
                assert!(msg.contains("missingSyncRounds"), "应列出全部被拒字段: {}", msg);
            }
            other => panic!("期望 InvalidModelField，实际 {:?}", other),
        }
        // 拒绝时不得留下部分生效的修改
        assert_eq!(row.context_window, 1_000_000, "被拒的 PATCH 不应改动任何字段");
    }

    /// 非正数窗口拒绝
    #[test]
    fn patch_rejects_non_positive_numbers() {
        let mut row = builtin_rows().into_iter().next().unwrap();
        let req = PatchModelRequest {
            context_window: Some(0),
            ..Default::default()
        };
        assert!(matches!(
            apply_model_patch(&mut row, &req),
            Err(AdminServiceError::InvalidModelField(_))
        ));
    }

    /// PATCH 一个内置行：覆盖层里落一行 **manual**（覆盖层不得出现 builtin 行），
    /// 但删除保护必须仍然生效——否则「改一下再删」就绕过了内置行不可删。
    #[test]
    fn patch_builtin_writes_manual_overlay_but_keeps_delete_protection() {
        let mut file = empty_file();
        let req = PatchModelRequest {
            context_window: Some(800_000),
            ..Default::default()
        };
        patch_model_in_file(&mut file, "claude-opus-4.8", &req).unwrap();

        assert_eq!(file.models.len(), 1);
        assert_eq!(file.models[0].origin, ModelOrigin::Manual);
        assert!(
            !file.models.iter().any(|r| r.origin == ModelOrigin::Builtin),
            "覆盖层不得写入 builtin 行"
        );
        assert_eq!(file.models[0].context_window, 800_000);
        assert!(file.models[0].pinned.contains(&"contextWindow".to_string()));

        // 仍然不可删
        let err = delete_model_in_file(&mut file, "claude-opus-4.8").unwrap_err();
        assert!(matches!(err, AdminServiceError::InvalidModelField(_)));
        assert_eq!(file.models.len(), 1, "删除失败不应改动文件");
    }

    /// 回归用例：PATCH 过的内置模型在覆盖层落成 origin=Manual，但响应里的
    /// `deletable` 必须仍然是 false —— 它对照的是 `is_builtin_upstream_id`，
    /// 不是行上的 origin。否则前端会照 origin 显示删除按钮，点击后端却拒绝。
    #[test]
    fn patched_builtin_row_stays_non_deletable_in_response() {
        let mut file = empty_file();
        let req = PatchModelRequest {
            context_window: Some(800_000),
            ..Default::default()
        };
        patch_model_in_file(&mut file, "claude-opus-4.8", &req).unwrap();

        // 落盘后走响应组装路径（与 GET /models 完全一致的代码路径）
        let mut path = std::env::temp_dir();
        path.push(format!(
            "kiro-admin-models-patched-deletable-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();
        let store = ModelRegistryStore::new(path.clone());
        let settings = ModelSyncSettings {
            enabled: false,
            time: "04:00".to_string(),
            probe_credential_id: None,
            allow_passthrough: false,
        };
        let resp = build_model_registry_response(&store, settings, &[]);
        let _ = std::fs::remove_file(&path);

        let row = resp
            .models
            .iter()
            .find(|r| r.row.upstream_id == "claude-opus-4.8")
            .expect("PATCH 过的行应出现在响应里");
        assert_eq!(row.row.origin, ModelOrigin::Manual, "覆盖层落地后 origin 应为 Manual");
        assert!(
            !row.deletable,
            "origin 已变成 Manual，但该行本质仍是内置模型，deletable 必须仍为 false"
        );
    }

    #[test]
    fn patch_unknown_model_is_not_found() {
        let mut file = empty_file();
        let err = patch_model_in_file(&mut file, "claude-nope", &PatchModelRequest::default())
            .unwrap_err();
        assert!(matches!(err, AdminServiceError::ModelNotFound(_)));
    }

    #[test]
    fn create_and_delete_manual_model() {
        let mut file = empty_file();
        let req = CreateModelRequest {
            upstream_id: "claude-opus-9.0".to_string(),
            ..Default::default()
        };
        create_model_in_file(&mut file, &req).unwrap();
        assert_eq!(file.models.len(), 1);
        assert_eq!(file.models[0].origin, ModelOrigin::Manual);
        // exposedId 按 §4.4 派生：claude-* 点号转连字符
        assert_eq!(file.models[0].exposed_id, "claude-opus-9-0");
        // sortOrder 必须避开内置行占用的号段
        assert!(file.models[0].sort_order > 130);

        // 重复 upstreamId → 冲突
        let err = create_model_in_file(&mut file, &req).unwrap_err();
        assert!(matches!(err, AdminServiceError::ModelConflict(_)));

        // manual 行可删
        delete_model_in_file(&mut file, "claude-opus-9.0").unwrap();
        assert!(file.models.is_empty());
        // 删不存在的行
        assert!(matches!(
            delete_model_in_file(&mut file, "claude-opus-9.0"),
            Err(AdminServiceError::ModelNotFound(_))
        ));
    }

    #[test]
    fn alias_upsert_and_delete() {
        let mut file = empty_file();
        upsert_alias_in_file(
            &mut file,
            &UpsertAliasRequest {
                from: "opus".to_string(),
                to: "claude-opus-4.8".to_string(),
            },
        )
        .unwrap();
        assert_eq!(file.aliases.len(), 1);

        // 同名 from 覆盖而非追加
        upsert_alias_in_file(
            &mut file,
            &UpsertAliasRequest {
                from: "opus".to_string(),
                to: "claude-sonnet-5".to_string(),
            },
        )
        .unwrap();
        assert_eq!(file.aliases.len(), 1);
        assert_eq!(file.aliases[0].to, "claude-sonnet-5");

        // 指向不存在的 upstreamId → 冲突（加载校验会拒绝整份文件，必须提前挡）
        let err = upsert_alias_in_file(
            &mut file,
            &UpsertAliasRequest {
                from: "x".to_string(),
                to: "claude-missing".to_string(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminServiceError::ModelConflict(_)));

        delete_alias_in_file(&mut file, "opus").unwrap();
        assert!(file.aliases.is_empty());
        assert!(matches!(
            delete_alias_in_file(&mut file, "opus"),
            Err(AdminServiceError::ModelNotFound(_))
        ));
    }

    /// 被别名指向的模型不得被删——否则写出去的文件下次加载时因 dangling alias 整体被拒
    #[test]
    fn model_referenced_by_alias_cannot_be_deleted() {
        let mut file = empty_file();
        create_model_in_file(
            &mut file,
            &CreateModelRequest {
                upstream_id: "claude-opus-9.0".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        file.aliases.push(ModelAlias {
            from: "o9".to_string(),
            to: "claude-opus-9.0".to_string(),
        });
        let err = delete_model_in_file(&mut file, "claude-opus-9.0").unwrap_err();
        assert!(matches!(err, AdminServiceError::ModelConflict(_)));
    }

    /// 空表 / 缺文件边界：GET /models 必须给出内置全量、非降级、别名为空
    #[test]
    fn registry_response_on_missing_file_is_builtin_and_not_degraded() {
        let mut path = std::env::temp_dir();
        path.push(format!("kiro-admin-models-missing-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = ModelRegistryStore::new(path);
        let settings = ModelSyncSettings {
            enabled: false,
            time: "04:00".to_string(),
            probe_credential_id: None,
            allow_passthrough: false,
        };
        let resp = build_model_registry_response(&store, settings, &[]);

        assert!(!resp.degraded, "文件不存在不是降级状态");
        assert!(resp.degraded_reason.is_none());
        assert_eq!(resp.models.len(), builtin_rows().len());
        assert!(resp.aliases.is_empty());
        assert_eq!(resp.credential_total, 0);
        assert_eq!(resp.credential_support_covered, 0);
    }

    /// 覆盖率统计：只有「已记录可用模型」的启用凭据才算被覆盖
    #[test]
    fn registry_response_reports_credential_support_coverage() {
        let mut path = std::env::temp_dir();
        path.push(format!("kiro-admin-models-cov-{}.json", std::process::id()));
        let mut file = empty_file();
        file.credential_support
            .insert("1".to_string(), vec!["claude-opus-4.8".to_string()]);
        std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();

        let store = ModelRegistryStore::new(path.clone());
        let settings = ModelSyncSettings {
            enabled: true,
            time: "04:00".to_string(),
            probe_credential_id: Some(1),
            allow_passthrough: false,
        };
        let resp = build_model_registry_response(&store, settings, &[1, 2, 3]);
        assert_eq!(resp.credential_total, 3);
        assert_eq!(resp.credential_support_covered, 1);
        let _ = std::fs::remove_file(&path);
    }

    struct StubFetcher {
        models: Vec<UpstreamModel>,
    }

    impl ModelListFetcher for StubFetcher {
        fn fetch(&self, _id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>> {
            let models = self.models.clone();
            Box::pin(async move { Ok(models) })
        }
        fn candidate_credential_ids(&self) -> Vec<u64> {
            vec![1]
        }
        fn is_credential_usable(&self, _id: u64) -> bool {
            true
        }
    }

    /// pinned 存在的**唯一理由**：手工改过的字段不被后续自动同步冲掉。
    /// 光测「PATCH 写进去了」不够，必须真跑一轮同步再看值。
    #[tokio::test]
    async fn pinned_field_survives_a_sync_round() {
        let _guard = crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let mut path = std::env::temp_dir();
        path.push(format!("kiro-admin-models-pinned-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(ModelRegistryStore::new(path.clone()));

        // 人工把窗口改成 800K（自动进 pinned），displayName 不动
        store
            .mutate(|f| {
                patch_model_in_file(
                    f,
                    "claude-opus-4.8",
                    &PatchModelRequest {
                        context_window: Some(800_000),
                        ..Default::default()
                    },
                )
                .map_err(|e| e.to_string())
            })
            .await
            .unwrap();

        // 上游报 1M 窗口 + 新名字
        let fetcher = Arc::new(StubFetcher {
            models: vec![UpstreamModel {
                model_id: "claude-opus-4.8".to_string(),
                model_name: Some("Claude Opus 4.8 (上游名)".to_string()),
                max_input_tokens: Some(1_000_000),
            }],
        });
        let sync = ModelSyncService::new(Arc::clone(&store), fetcher);
        sync.sync_once(Some(1), Utc::now()).await.unwrap();

        let out = store.load();
        let row = out
            .registry
            .rows()
            .iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        assert_eq!(row.context_window, 800_000, "pinned 字段被同步冲掉了");
        assert_eq!(
            row.display_name, "Claude Opus 4.8 (上游名)",
            "未 pinned 字段应跟随上游更新"
        );
        assert_eq!(row.status, ModelStatus::Active);
        let _ = std::fs::remove_file(&path);
        // sync_once 会 install_registry 到全局 holder：必须还原，
        // 否则 800K 的 opus 窗口会漏给后面依赖内置默认的测试（converter 侧）。
        crate::anthropic::model_registry::install_registry(ModelRegistry::builtin());
    }

    /// PATCH `supportsReasoning` 不进 pinned——它与 `enabled`/`sortOrder`/
    /// `matchKind` 同组：本地策略开关，同步没有数据源覆盖它，pin 了只会在 UI
    /// 上留一个永远解不开的锁。
    #[test]
    fn patch_supports_reasoning_does_not_pin() {
        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        assert_eq!(row.supports_reasoning, None, "前置条件：内置行默认未设置");

        let req = PatchModelRequest {
            supports_reasoning: Some(false),
            supports_reasoning_set: true,
            ..Default::default()
        };
        apply_model_patch(&mut row, &req).unwrap();

        assert_eq!(row.supports_reasoning, Some(false));
        assert!(
            !row.pinned.contains(&"supportsReasoning".to_string()),
            "supportsReasoning 不应自动进 pinned"
        );

        // supportsReasoningSet 标出「清回 None」，而不是 Option<bool> 字段本身
        // 缺省时的「不改」语义。
        let clear_req = PatchModelRequest {
            supports_reasoning: None,
            supports_reasoning_set: true,
            ..Default::default()
        };
        apply_model_patch(&mut row, &clear_req).unwrap();
        assert_eq!(row.supports_reasoning, None, "supportsReasoningSet 应能清回未设置");
    }

    /// PATCH `supportsReasoning` 后跑一轮同步，值不应被抹掉——同步的数据源
    /// （`ListAvailableModels`）根本不携带这个信息，因此它必须走「用户专属
    /// 字段覆盖层始终胜出」这条路径，而不是「同步管辖字段仅 pinned 时保留」。
    #[tokio::test]
    async fn supports_reasoning_survives_a_sync_round() {
        let _guard = crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let mut path = std::env::temp_dir();
        path.push(format!(
            "kiro-admin-models-supports-reasoning-sync-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(ModelRegistryStore::new(path.clone()));

        store
            .mutate(|f| {
                patch_model_in_file(
                    f,
                    "claude-opus-4.8",
                    &PatchModelRequest {
                        supports_reasoning: Some(false),
                        supports_reasoning_set: true,
                        ..Default::default()
                    },
                )
                .map_err(|e| e.to_string())
            })
            .await
            .unwrap();

        let fetcher = Arc::new(StubFetcher {
            models: vec![UpstreamModel {
                model_id: "claude-opus-4.8".to_string(),
                model_name: Some("Claude Opus 4.8".to_string()),
                max_input_tokens: Some(1_000_000),
            }],
        });
        let sync = ModelSyncService::new(Arc::clone(&store), fetcher);
        sync.sync_once(Some(1), Utc::now()).await.unwrap();

        let out = store.load();
        let row = out
            .registry
            .rows()
            .iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        assert_eq!(
            row.supports_reasoning,
            Some(false),
            "supportsReasoning 不应被同步抹掉"
        );

        let _ = std::fs::remove_file(&path);
        crate::anthropic::model_registry::install_registry(ModelRegistry::builtin());
    }

    // ===== 同步 → credentialSupport → 调度层过滤：整条链 =====

    /// 只有 1 号凭据能拉到列表，其余一律失败。用来产生「只覆盖一个凭据」的
    /// credentialSupport 记录 —— 这正是真实探针轮次的形状。
    struct ProbeOnlyFetcher {
        models: Vec<UpstreamModel>,
    }

    impl ModelListFetcher for ProbeOnlyFetcher {
        fn fetch(&self, id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>> {
            let models = if id == 1 { self.models.clone() } else { Vec::new() };
            let ok = id == 1;
            Box::pin(async move {
                if ok {
                    Ok(models)
                } else {
                    Err("本测试只允许探针凭据拉取".to_string())
                }
            })
        }
        fn candidate_credential_ids(&self) -> Vec<u64> {
            vec![1, 2]
        }
        fn is_credential_usable(&self, id: u64) -> bool {
            id == 1
        }
    }

    fn live_cred(id: u64, token: &str, priority: u32) -> KiroCredentials {
        let mut c = KiroCredentials::default();
        c.id = Some(id);
        c.access_token = Some(token.to_string());
        // 未过期 → acquire_context 不会去刷 token（测试环境没有上游）
        c.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
        c.priority = priority;
        c
    }

    /// **本轮收尾的重点：同步跑完之后，调度层的凭据过滤真的按新数据变了。**
    ///
    /// 此前这条链上每一段都有测试，唯独没有一条把它们串起来跑：
    /// `sync_once` 写 `credentialSupport` → `AdminService::sync_models` 把它灌回
    /// `MultiTokenManager` → `credential_matches_request` 依据它筛凭据。
    /// 段段都绿而接缝错位（例如同步写的键格式与过滤读的不一致）是发现不了的。
    ///
    /// 断言方式刻意**不是**「刷新函数被调用了」，而是**同一个凭据 + 同一个模型，
    /// 同步前后的过滤结果不同**：
    /// - 同步前：1 号凭据无记录 → 放行 → 按 priority 选中 1 号；
    /// - 同步后：1 号凭据有记录且不含该模型 → 拒绝 → 改选无记录的 2 号。
    ///
    /// 顺带钉住键/值的格式：键是凭据 id 的**字符串**，值是**上游 id**（带点号，
    /// 不是对外的连字符形式）—— 这两处任何一处漂移，过滤都会静默失效（永远放行）。
    #[tokio::test]
    async fn sync_refresh_changes_credential_filtering_end_to_end() {
        use crate::kiro::token_manager::credential_supports_model;

        let _guard = crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        // 探针（1 号）能看到的模型集里**没有** claude-sonnet-5。
        // 「看得见」的那个刻意选带点号的上游 id：它的对外名是 claude-sonnet-4-5，
        // 两者不同，于是下面「值必须是上游 id」的断言才真的能挡住键值格式漂移。
        const PROBE_SEES: &str = "claude-sonnet-4.5";
        const PROBE_DOES_NOT_SEE: &str = "claude-sonnet-5";

        let token_manager = Arc::new(
            MultiTokenManager::new(
                Config::default(),
                vec![live_cred(1, "tok-probe", 1), live_cred(2, "tok-other", 10)],
                None,
                None,
                true,
            )
            .unwrap(),
        );

        let mut path = std::env::temp_dir();
        path.push(format!("kiro-admin-models-e2e-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(ModelRegistryStore::new(path.clone()));

        let sync_service = Arc::new(ModelSyncService::new(
            Arc::clone(&store),
            Arc::new(ProbeOnlyFetcher {
                models: vec![UpstreamModel {
                    model_id: PROBE_SEES.to_string(),
                    model_name: Some("Claude Sonnet 4.5".to_string()),
                    max_input_tokens: Some(200_000),
                }],
            }),
        ));
        let settings = Arc::new(parking_lot::RwLock::new(ModelSyncSettings {
            enabled: true,
            time: "04:00".to_string(),
            probe_credential_id: Some(1),
            allow_passthrough: false,
        }));
        let service = AdminService::new(Arc::clone(&token_manager), Vec::<String>::new(), Arc::new(ProxyPoolManager::new(None, crate::model::config::TlsBackend::Rustls)), Arc::new(BalanceCache::new(None)))
            .with_model_registry(Some(Arc::clone(&store)), Some(sync_service))
            .with_model_sync_settings(settings);

        // ---- 同步前：无记录 → 放行 → 选到高优先级的 1 号 ----
        assert!(
            token_manager.credential_support().is_empty(),
            "前置条件：启动时没有任何 credentialSupport 记录"
        );
        assert!(
            credential_supports_model(1, PROBE_DOES_NOT_SEE, &token_manager.credential_support()),
            "无记录必须放行（保守，不误杀）"
        );
        let before = token_manager
            .acquire_context(Some(PROBE_DOES_NOT_SEE), None)
            .await
            .expect("同步前应能选到凭据");
        assert_eq!(before.id, 1, "同步前：1 号无记录、优先级更高，应被选中");

        // ---- 跑一轮真同步（走的是 /models/sync 的生产路径）----
        let summary = service.sync_models(false).await.expect("同步应成功");
        assert_eq!(summary.round, "authoritative");
        assert!(summary.trusted);

        // ---- 落盘内容的格式：键是 id 字符串，值是上游 id ----
        let support = token_manager.credential_support();
        assert_eq!(
            support.get("1").map(|v| v.as_slice()),
            Some([PROBE_SEES.to_string()].as_slice()),
            "刷新后的 credentialSupport 必须按「id 字符串 → 上游 id 列表」记录，实际: {:?}",
            support
        );
        assert!(
            !support.contains_key("2"),
            "拉取失败的凭据不得留下记录（否则会被当成「不支持任何模型」永久踢出轮换）"
        );

        // ---- 同步后：同一个凭据 + 同一个模型，过滤结论反转 ----
        assert!(
            !credential_supports_model(1, PROBE_DOES_NOT_SEE, &support),
            "1 号已有记录且不含该模型 → 必须被过滤掉"
        );
        assert!(
            credential_supports_model(1, PROBE_SEES, &support),
            "1 号对记录内的模型仍应放行"
        );
        let after = token_manager
            .acquire_context(Some(PROBE_DOES_NOT_SEE), None)
            .await
            .expect("2 号无记录，仍应能选到凭据");
        assert_eq!(
            after.id, 2,
            "同步后：1 号被 credentialSupport 过滤掉，调度层应改选 2 号 —— \
             这一条才证明刷新真的传导到了过滤行为，而不只是「刷新函数被调用了」"
        );
        // 说明：这里不再断言「换个模型又会选回 1 号」——priority 模式下
        // `acquire_context` 会优先复用 current_id（上一步已粘在 2 号），
        // 那是负载均衡的既有语义，与本测试要证明的过滤传导无关。
        // 「1 号对记录内的模型仍放行」由上面的 credential_supports_model 断言覆盖。

        let _ = std::fs::remove_file(&path);
    }

    /// 写入的文件必须能被 from_file 加载（落盘前校验的兜底断言）
    #[test]
    fn patched_file_still_loads() {
        let mut file = empty_file();
        patch_model_in_file(
            &mut file,
            "claude-opus-4.8",
            &PatchModelRequest {
                exposed_id: Some("claude-opus-4-8x".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        let registry = ModelRegistry::from_file(file).unwrap();
        assert!(registry
            .rows()
            .iter()
            .any(|r| r.exposed_id == "claude-opus-4-8x"));
    }

    /// M1：`POST /models` 的未知字段必须明确报错（400），不能被 serde 静默丢弃
    /// 后返回 200——用户会以为写进去了，实际值根本没进表。
    #[test]
    fn create_model_rejects_unknown_fields() {
        let raw = r#"{"upstreamId":"claude-opus-9.1","origin":"builtin","enabld":true}"#;
        let req: CreateModelRequest = serde_json::from_str(raw).unwrap();
        let mut file = empty_file();
        let err = create_model_in_file(&mut file, &req).unwrap_err();
        match err {
            AdminServiceError::InvalidModelField(msg) => {
                assert!(msg.contains("origin"), "应点名被拒字段: {}", msg);
                assert!(msg.contains("enabld"), "拼写错误的字段名也应被指出: {}", msg);
            }
            other => panic!("期望 InvalidModelField，实际 {:?}", other),
        }
        assert!(file.models.is_empty(), "被拒的请求不应留下任何部分写入");
    }

    fn model_sync_test_service() -> AdminService {
        let token_manager = Arc::new(
            MultiTokenManager::new(Config::default(), vec![live_cred(1, "tok", 1)], None, None, true)
                .unwrap(),
        );
        AdminService::new(token_manager, Vec::<String>::new(), Arc::new(ProxyPoolManager::new(None, crate::model::config::TlsBackend::Rustls)), Arc::new(BalanceCache::new(None)))
    }

    /// M1：`PATCH /models/settings` 的未知字段必须明确报错，不能静默丢弃。
    /// 实测 `allowUnknownModelPassthrough`（config.json 里的真实键名）与本接口的
    /// `allowPassthrough` 只差一个写法，静默丢弃会让用户以为开关已打开。
    #[tokio::test]
    async fn set_model_sync_settings_rejects_unknown_fields() {
        let service = model_sync_test_service();
        let raw = r#"{"allowUnknownModelPassthrough":true}"#;
        let req: SetModelSyncSettingsRequest = serde_json::from_str(raw).unwrap();
        let err = service.set_model_sync_settings(req).await.unwrap_err();
        match err {
            AdminServiceError::InvalidModelField(msg) => {
                assert!(msg.contains("allowUnknownModelPassthrough"), "应点名被拒字段: {}", msg)
            }
            other => panic!("期望 InvalidModelField，实际 {:?}", other),
        }
    }

    // ============ POST /models/test：注册表拒绝时一次请求都不发 ============
    //
    // 下面两条测试用的 service **没有注入 kiro_provider**：一旦解析放行，
    // 代码会在发请求前先撞上 `InternalError("Kiro Provider 未配置")`。
    // 断言拿到的是 ModelNotFound / InvalidModelField，就证明拒绝发生在发请求之前。

    /// 未收录且未开透传 → ModelNotFound（404 语义），不发请求
    #[tokio::test]
    async fn test_model_rejects_unknown_model_before_sending_request() {
        let service = model_sync_test_service();
        let err = service
            .test_model(ModelTestRequest {
                model_id: "  no-such-model-xyz  ".to_string(),
                credential_id: None,
            })
            .await
            .unwrap_err();
        match err {
            AdminServiceError::ModelNotFound(m) => {
                assert_eq!(m, "no-such-model-xyz", "回显的模型名应已 trim")
            }
            other => panic!("期望 ModelNotFound（且未发出请求），实际 {:?}", other),
        }
    }

    /// 配了但被人工禁用 → InvalidModelField（400 语义），不发请求。
    /// 与「没配」区分开：两者的排查方向完全不同。
    #[tokio::test]
    async fn test_model_rejects_disabled_model_before_sending_request() {
        let _guard = crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        row.enabled = false;
        let exposed = row.exposed_id.clone();
        let file = ModelRegistryFile {
            models: vec![row],
            ..ModelRegistryFile::default()
        };
        crate::anthropic::model_registry::install_registry(
            ModelRegistry::from_file(file).unwrap(),
        );

        let service = model_sync_test_service();
        let err = service
            .test_model(ModelTestRequest {
                model_id: exposed.clone(),
                credential_id: None,
            })
            .await
            .unwrap_err();
        match err {
            AdminServiceError::InvalidModelField(msg) => {
                assert!(msg.contains(&exposed), "错误信息应点名被禁用的模型: {}", msg)
            }
            other => panic!("期望 InvalidModelField（且未发出请求），实际 {:?}", other),
        }

        crate::anthropic::model_registry::install_registry(ModelRegistry::builtin());
    }

    /// 空 modelId 属于请求本身有问题，不是「模型不存在」
    #[tokio::test]
    async fn test_model_rejects_blank_model_id() {
        let service = model_sync_test_service();
        let err = service
            .test_model(ModelTestRequest {
                model_id: "   ".to_string(),
                credential_id: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, AdminServiceError::InvalidModelField(_)), "实际 {:?}", err);
    }

    /// M2：模型同步时间校验失败必须走 `InvalidModelField`，文案不得是「凭据无效」
    /// 或提及「自动更新」——那是另一条路径（二进制自动更新时间）复用解析器的副作用，
    /// 会把排查方向带偏到完全不相关的功能上。
    #[tokio::test]
    async fn set_model_sync_settings_invalid_time_uses_model_field_error_not_credential_wording() {
        let service = model_sync_test_service();
        let req = SetModelSyncSettingsRequest {
            enabled: None,
            time: Some("25:99".to_string()),
            probe_credential_id: None,
            probe_credential_id_set: false,
            allow_passthrough: None,
            extra: Default::default(),
        };
        let err = service.set_model_sync_settings(req).await.unwrap_err();
        match err {
            AdminServiceError::InvalidModelField(msg) => {
                assert!(
                    !msg.contains("凭据无效") && !msg.contains("自动更新"),
                    "文案不应带偏到凭据/自动更新路径: {}",
                    msg
                );
            }
            other => panic!("期望 InvalidModelField，实际 {:?}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_upstream_rate_limit_is_classified_without_losing_retry_after() {
        let error = anyhow::Error::new(UpstreamRateLimitError::new(Some("120".to_string())));
        let classified = classify_rate_limit(&error).expect("应识别类型化上游 429");

        match classified {
            AdminServiceError::RateLimited { retry_after } => {
                assert_eq!(retry_after.as_deref(), Some("120"));
            }
            other => panic!("预期 RateLimited，实际为 {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_grant_is_a_sanitized_bad_request_instead_of_gateway_error() {
        let manager = Arc::new(
            MultiTokenManager::new(Config::default(), vec![], None, None, false).unwrap(),
        );
        let service = AdminService::new(
            manager,
            Vec::<String>::new(),
            Arc::new(ProxyPoolManager::new(
                None,
                crate::model::config::TlsBackend::Rustls,
            )),
            Arc::new(BalanceCache::new(None)),
        );
        let raw = "invalid_grant: upstream-private-detail";
        let classified = service.classify_balance_error(
            anyhow::Error::new(RefreshTokenInvalidError {
                message: raw.to_string(),
            }),
            1,
        );

        assert_eq!(classified.status_code(), axum::http::StatusCode::BAD_REQUEST);
        match classified {
            AdminServiceError::InvalidCredential(message) => {
                assert!(!message.contains("upstream-private-detail"));
                assert!(message.contains("refreshToken 已失效"));
            }
            other => panic!("预期 InvalidCredential，实际为 {other:?}"),
        }
    }

    #[test]
    fn global_proxy_validation_accepts_every_supported_scheme() {
        // 防回归：全局代理曾经有一份独立的白名单副本，漏掉了 socks5h。
        // 现在它必须与代理池共用同一份 scheme 真源。
        for url in [
            "http://127.0.0.1:8080",
            "https://127.0.0.1:8443",
            "socks4://127.0.0.1:1080",
            "socks4a://127.0.0.1:1080",
            "socks5://127.0.0.1:1080",
            "socks5h://127.0.0.1:1080",
        ] {
            assert!(validate_global_proxy_url(url).is_ok(), "应接受 {}", url);
        }
    }

    #[test]
    fn global_proxy_validation_rejects_direct_sentinel() {
        // "direct" 只在凭据级有意义（覆盖全局代理）；全局代理用 None 表达不走代理，
        // 存进去会变成一个非法 URL 的代理配置
        assert!(validate_global_proxy_url("direct").is_err());
        assert!(validate_global_proxy_url("socks6://127.0.0.1:1080").is_err());
    }

    #[test]
    fn login_proxy_honors_direct_instead_of_falling_back_to_global() {
        let global = Some(ProxyConfig::new("http://global:8080"));

        // "direct" = 显式不走代理，不能回退全局
        assert_eq!(resolve_login_proxy(Some("direct"), global.clone()), None);
        assert_eq!(resolve_login_proxy(Some("DIRECT"), global.clone()), None);
        // 未传 / 空串 = 回退全局
        assert_eq!(resolve_login_proxy(None, global.clone()), global);
        assert_eq!(resolve_login_proxy(Some(""), global.clone()), global);
        // 传了就用传的
        assert_eq!(
            resolve_login_proxy(Some("socks5h://p:1080"), global),
            Some(ProxyConfig::new("socks5h://p:1080"))
        );
    }

    #[test]
    fn semver_compares_correctly() {
        use std::cmp::Ordering;
        assert_eq!(compare_semver("0.3.0", "0.3.1"), Ordering::Less);
        assert_eq!(compare_semver("v0.3.1", "0.3.1"), Ordering::Equal);
        assert_eq!(compare_semver("1.0.0", "0.99.99"), Ordering::Greater);
        assert_eq!(compare_semver("0.3.1-rc.1", "0.3.1"), Ordering::Equal);
    }

    #[test]
    fn export_uses_nested_account_format() {
        let mut cred = KiroCredentials::default();
        cred.refresh_token = Some("rt-123".to_string());
        cred.client_id = Some("cid".to_string());
        cred.client_secret = Some("csec".to_string());
        cred.auth_method = Some("idc".to_string());
        cred.provider = Some("Enterprise".to_string());
        cred.region = Some("us-east-1".to_string());
        cred.email = Some("e@example.com".to_string());
        cred.expires_at = Some("2026-06-06T00:00:00Z".to_string());
        // 占位符 profileArn 应在导出时被剥离
        cred.profile_arn = Some(
            crate::kiro::model::credentials::BUILDER_ID_PROFILE_ARN.to_string(),
        );

        let acc = credential_to_export_account(cred).expect("应生成账号");

        // 嵌套 credentials 结构
        assert_eq!(acc.credentials.refresh_token.as_deref(), Some("rt-123"));
        assert_eq!(acc.credentials.client_id.as_deref(), Some("cid"));
        // authMethod 规范化为 "IdC"
        assert_eq!(acc.credentials.auth_method.as_deref(), Some("IdC"));
        // expiresAt 解析为毫秒时间戳
        assert!(acc.credentials.expires_at > 0);
        // idp 取 provider
        assert_eq!(acc.idp, "Enterprise");
        // 占位符 profileArn 被跳过
        assert_eq!(acc.profile_arn, None);
        // 必填的 csrfToken 输出空串
        assert_eq!(acc.credentials.csrf_token, "");
    }

    #[test]
    fn export_skips_api_key_credentials() {
        let mut cred = KiroCredentials::default();
        cred.kiro_api_key = Some("ksk_abc".to_string());
        cred.auth_method = Some("api_key".to_string());
        // 无 refreshToken → 跳过
        assert!(credential_to_export_account(cred).is_none());
    }

    #[test]
    fn subscription_type_mapping() {
        assert_eq!(subscription_type_from_title(Some("KIRO FREE")), "Free");
        assert_eq!(subscription_type_from_title(Some("KIRO PRO+")), "Pro_Plus");
        assert_eq!(subscription_type_from_title(Some("KIRO PRO")), "Pro");
        assert_eq!(subscription_type_from_title(Some("KIRO POWER")), "Enterprise");
        assert_eq!(subscription_type_from_title(None), "Free");
    }

    // ============ 凭据状态：current_id / is_current 的模式相关语义 ============

    fn cred_with_priority(priority: u32) -> KiroCredentials {
        let mut c = KiroCredentials::default();
        c.access_token = Some("tok".to_string());
        // 未过期 → 不会触发刷新（测试环境没有上游）
        c.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
        c.priority = priority;
        c
    }

    /// 两条凭据（priority 0 / 1），负载均衡模式由参数决定。
    fn service_with_balancing_mode(mode: &str) -> AdminService {
        let mut config = Config::default();
        config.load_balancing_mode = mode.to_string();
        let manager = Arc::new(
            MultiTokenManager::new(
                config,
                vec![cred_with_priority(0), cred_with_priority(1)],
                None,
                None,
                false,
            )
            .unwrap(),
        );
        AdminService::new(
            manager,
            Vec::<String>::new(),
            Arc::new(ProxyPoolManager::new(
                None,
                crate::model::config::TlsBackend::Rustls,
            )),
            Arc::new(BalanceCache::new(None)),
        )
    }

    /// balanced 模式下 `current_id` 只是内部调度指针（每次请求都重新选号），
    /// 把它渲染成「当前活跃账号」是假信息。对外必须固定为 0 / 全 false。
    #[tokio::test]
    async fn balanced_mode_exposes_no_current_credential() {
        let service = service_with_balancing_mode("balanced");

        let response = service.get_all_credentials();

        assert_eq!(response.current_id, 0, "均衡模式不得对外暴露调度指针");
        assert!(
            response.credentials.iter().all(|item| !item.is_current),
            "均衡模式下没有「当前活跃账号」这个概念，全部必须为 false"
        );
    }

    /// priority 模式下该语义是真实的：最高优先级（priority 最小）的那条为当前凭据。
    #[tokio::test]
    async fn priority_mode_keeps_single_current_credential() {
        let service = service_with_balancing_mode("priority");

        let response = service.get_all_credentials();

        // 列表已按 priority 升序排序，首条即最高优先级
        assert_eq!(response.current_id, response.credentials[0].id);
        assert!(
            response.credentials[0].is_current,
            "最高优先级的那条应为当前凭据"
        );
        assert!(
            response.credentials[1..].iter().all(|item| !item.is_current),
            "当前凭据必须唯一"
        );
    }
}

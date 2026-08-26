use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum TlsBackend {
    #[default]
    Rustls,
    NativeTls,
}


/// 工具兼容模式。
///
/// - `ClaudeCode`（默认）：把 Claude Code 内置工具（Write/Edit/Bash/Read/Glob/Grep/LS/WebSearch）
///   的工具名与入参双向适配为 Kiro 内置工具（fs_write/str_replace/... ），并替换为 Kiro 内置 schema。
/// - `Raw`：保留旧行为，直接透传客户端工具名/schema，用于排障。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ToolCompatibilityMode {
    #[default]
    ClaudeCode,
    Raw,
}

/// 自定义模型定义。
///
/// 用户在 `config.json` 的 `customModels` 数组里声明客户端模型别名到 Kiro 后端
/// 模型 ID 的映射及元数据。运行期由 [`crate::model::custom_models`] 全局注册表按
/// `id`（大小写不敏感）精确匹配，优先于内置的模糊映射逻辑——既能新增模型，也能
/// 覆盖内置模型的映射。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomModel {
    /// 客户端请求时使用的模型名（别名）。匹配大小写不敏感。
    pub id: String,

    /// 映射到的 Kiro 后端模型 ID（实际下发给上游）。
    pub backend_id: String,

    /// `/v1/models` 展示名（可选，缺省用 `id`）。
    #[serde(default)]
    pub display_name: Option<String>,

    /// 上下文窗口大小（可选，缺省 200000）。
    #[serde(default)]
    pub context_window: Option<i32>,

    /// 单次响应最大 token 数，用于 `/v1/models` 展示（可选，缺省 64000）。
    #[serde(default)]
    pub max_tokens: Option<i32>,

    /// 是否支持原生 reasoning / `output_config`（可选，缺省 false）。
    /// 命中的自定义模型置 true 时，会按 backend_id 放行 `additionalModelRequestFields`。
    #[serde(default)]
    pub supports_reasoning: Option<bool>,

    /// `/v1/models` 的 `owned_by` 字段（可选，缺省 "custom"）。
    #[serde(default)]
    pub owned_by: Option<String>,
}

/// KNA 应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_region")]
    pub region: String,

    /// Auth Region（用于 Token 刷新），未配置时回退到 region
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_region: Option<String>,

    /// API Region（用于 API 请求），未配置时回退到 region
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_region: Option<String>,

    #[serde(default = "default_kiro_version")]
    pub kiro_version: String,

    #[serde(default)]
    pub machine_id: Option<String>,

    #[serde(default)]
    pub api_key: Option<String>,

    #[serde(default = "default_system_version")]
    pub system_version: String,

    #[serde(default = "default_node_version")]
    pub node_version: String,

    #[serde(default = "default_tls_backend")]
    pub tls_backend: TlsBackend,

    /// 外部 count_tokens API 地址（可选）
    #[serde(default)]
    pub count_tokens_api_url: Option<String>,

    /// count_tokens API 密钥（可选）
    #[serde(default)]
    pub count_tokens_api_key: Option<String>,

    /// count_tokens API 认证类型（可选，"x-api-key" 或 "bearer"，默认 "x-api-key"）
    #[serde(default = "default_count_tokens_auth_type")]
    pub count_tokens_auth_type: String,

    /// HTTP 代理地址（可选）
    /// 支持格式: http://host:port, https://host:port, socks5://host:port
    #[serde(default)]
    pub proxy_url: Option<String>,

    /// 代理认证用户名（可选）
    #[serde(default)]
    pub proxy_username: Option<String>,

    /// 代理认证密码（可选）
    #[serde(default)]
    pub proxy_password: Option<String>,

    /// 强制走代理：开启后，凭据与全局均无可用代理时**拒绝出网**，而不是降级直连。
    ///
    /// 关闭（默认）时行为完全不变，保持向后兼容。开启后受保护的是所有出网路径
    /// （API 调用 / token 刷新 / 余额与模型查询 / 登录），包括显式配置的
    /// `"proxyUrl": "direct"`——本开关的语义是「这个部署绝不裸连」。
    #[serde(default)]
    pub require_proxy: bool,

    /// 流式上游相邻数据帧之间的最长空闲时间（秒，默认 90）。
    #[serde(default = "default_stream_idle_timeout_secs")]
    pub stream_idle_timeout_secs: u64,

    /// 流式上游请求从建连到响应体读完的绝对总时限（秒，默认 1800）。
    #[serde(default = "default_stream_total_timeout_secs")]
    pub stream_total_timeout_secs: u64,

    /// Admin API 密钥（可选，启用 Admin API 功能）
    #[serde(default)]
    pub admin_api_key: Option<String>,

    /// 上一次成功更新前正在运行的版本号，用于在前端展示「回退到 vX.Y.Z」按钮。
    /// 实际回退动作通过 `<exe>.backup` 文件完成，无需访问网络。
    #[serde(default)]
    pub update_previous_version: Option<String>,

    /// GitHub Personal Access Token（可选）。设置后 GitHub Releases 接口会带上
    /// `Authorization: Bearer <token>`，把限流从匿名 60/h 提到认证 5000/h。
    /// 仅需 `public_repo` 读取权限即可。
    #[serde(default)]
    pub github_token: Option<String>,

    /// 上一次成功完成在线更新的时间（RFC3339）。前端用于显示「上次更新于 …」。
    #[serde(default)]
    pub update_last_applied_at: Option<String>,

    /// 是否启用无人值守自动更新。开启后服务会在每天的 `update_auto_apply_time`
    /// 时刻检查 GitHub Releases，发现新版本即自动下载二进制并替换重启。
    #[serde(default)]
    pub update_auto_apply: bool,

    /// 自动更新的每日触发时间（本地时区，`HH:MM` 24 小时制）。
    /// 默认 03:00 凌晨执行，对在线服务影响最小。
    #[serde(default = "default_update_auto_apply_time")]
    pub update_auto_apply_time: String,

    /// 负载均衡模式（"priority" 或 "balanced"）
    #[serde(default = "default_load_balancing_mode")]
    pub load_balancing_mode: String,

    /// 额度护栏的安全垫（credits，默认 200；0 = 退化为「remaining ≤ 0 才算超额」）。
    ///
    /// 有效剩余低于该值的凭据即视为已超额：自动冷冻并退出调度池，不再打上游。
    /// 为什么不是 0——余额快照最长可陈旧 [`crate::kiro::dispatch::MAX_STALE_SECS`]，
    /// 且本地消耗计数只覆盖本进程，所以「余额显示归零」时实际早已透支。生产上
    /// 曾观测到某凭据在开启上游 overage 的情况下透支到 remaining = -202，留垫
    /// 就是对这段观测延迟的补偿。
    #[serde(default = "default_quota_guard_reserve")]
    pub quota_guard_reserve: f64,

    /// 单凭证并发上限（默认 2；0 = 禁用门禁）。
    ///
    /// 满载时新请求先排队等原凭证（见下两项），排队失败才切换凭证——
    /// 平滑单凭证请求速率，降低触发上游 suspicious-activity 风控 429 的概率。
    /// 429 风控冷却/故障转移语义不受影响。
    #[serde(default = "default_credential_max_concurrent")]
    pub credential_max_concurrent: usize,

    /// 单凭证等待队列深度（默认 3）。排队人数达到上限时立即换凭证。
    #[serde(default = "default_credential_queue_depth")]
    pub credential_queue_depth: usize,

    /// 排队等待超时（秒，默认 60）。超时后切换凭证；所有候选凭证都
    /// 排队失败时兜底放行（不阻塞请求）。
    #[serde(default = "default_credential_queue_timeout_secs")]
    pub credential_queue_timeout_secs: u64,

    /// 账号级 429 风控触发时是否对当前凭据进入冷却并故障转移（默认 true）。
    ///
    /// 关闭后：429 + suspicious activity 仍按普通瞬态错误重试，不切换凭据。
    /// 开启后：识别到 suspicious activity 字符串时，把当前凭据冷却 `account_throttle_cooldown_secs` 秒，
    /// 立即切换到下一个可用凭据。
    #[serde(default = "default_account_throttle_failover")]
    pub account_throttle_failover: bool,

    /// 账号级风控冷却时长（秒，默认 1800 = 30 分钟）。
    #[serde(default = "default_account_throttle_cooldown_secs")]
    pub account_throttle_cooldown_secs: u64,

    /// 是否启用单账号每分钟请求次数（RPM）主动限流（默认 false）。
    ///
    /// 开启后：每个凭据独立维护最近 60 秒的滑动窗口计数，达到 `account_rpm_limit`
    /// 上限时，该凭据在窗口内被临时排除出候选，请求自动故障转移到下一个可用凭据；
    /// 所有凭据都超限时返回 429。窗口计数不持久化，进程重启后清空。
    /// 关闭时（默认）完全不计数、不影响调度，存量用户无感知。
    #[serde(default = "default_account_rpm_limit_enabled")]
    pub account_rpm_limit_enabled: bool,

    /// 单账号每分钟请求次数上限（默认 60）。仅在 `account_rpm_limit_enabled` 为 true 时生效。
    #[serde(default = "default_account_rpm_limit")]
    pub account_rpm_limit: u32,

    /// 是否开启非流式响应的 thinking 块提取（默认 true）
    ///
    /// 启用后，非流式响应中的 `<thinking>...</thinking>` 标签会被解析为
    /// 独立的 `{"type": "thinking", ...}` 内容块,与流式响应行为一致。
    #[serde(default = "default_extract_thinking")]
    pub extract_thinking: bool,

    /// 工具兼容模式。默认 `claude-code`：把 Claude Code 内置工具名/入参双向适配为
    /// Kiro 内置工具；`raw` 保留旧行为、直接透传客户端工具 schema，用于排障。
    #[serde(default = "default_tool_compatibility_mode")]
    pub tool_compatibility_mode: ToolCompatibilityMode,

    /// 默认端点名称（凭据未显式指定 endpoint 时使用，默认 "ide"）
    #[serde(default = "default_endpoint")]
    pub default_endpoint: String,

    /// 是否启用请求链路追踪（写 kiro.duckdb 的 traces 表）。默认 true。
    ///
    /// 关闭后：不再写入 trace 记录、不走 TraceSink，但 `GET /api/admin/traces`
    /// 仍可查询历史已存记录。适合隐私敏感或磁盘紧张的场景。
    #[serde(default = "default_trace_enabled")]
    pub trace_enabled: bool,

    /// 请求链路追踪记录保留天数（默认 7）。后台任务每天清理超期记录。
    #[serde(default = "default_trace_retention_days")]
    pub trace_retention_days: u32,

    /// 是否全量保留 /v1/messages 原始请求体（gzip 落盘 request_bodies/，
    /// 保留期跟随 traceRetentionDays）。默认 false：内容含用户源码与对话，
    /// 显式开启才存。用途：复盘「未知字段膨胀」类问题（如 208KB thinking 签名），
    /// serde 解析后的视图恰好会丢掉这类字段，必须存线上原始字节。
    #[serde(default)]
    pub store_request_bodies: bool,

    /// 请求用量日志（usage_log.*.jsonl + 聚合桶）保留天数（默认 31）。
    #[serde(default = "default_usage_log_retention_days")]
    pub usage_log_retention_days: u32,

    /// 端点特定的配置
    ///
    /// 键为端点名（如 "ide" / "cli"），值为该端点自由定义的参数对象。
    /// 未在此表出现的端点沿用实现内置默认值。
    #[serde(default)]
    pub endpoints: HashMap<String, serde_json::Value>,

    /// 是否启用「每日自动同步上游模型」。**默认 false** —— 保证不配置任何东西时
    /// 行为与改造前完全一致（零行为回归）。
    #[serde(default)]
    pub model_sync_enabled: bool,

    /// 每日同步触发时间（`HH:MM`，本地 24 小时制）。
    #[serde(default = "default_model_sync_time")]
    pub model_sync_time: String,

    /// 探针凭据 id。设置且可用时，该轮同步为「权威轮次」，可判定模型消失。
    /// 未设置或不可用时回退为采样 3 个凭据的「非权威轮次」，只做新增/更新。
    #[serde(default)]
    pub model_sync_probe_credential_id: Option<u64>,

    /// 未收录模型是否放行透传。**默认 false** —— 保留「模型名写错」的快速失败信号。
    #[serde(default)]
    pub allow_unknown_model_passthrough: bool,

    /// 自定义模型映射表（兼容旧版 config.json 配置项）。
    ///
    /// **已被模型注册表（`models.json` / admin UI）取代**：启动时一次性把这里
    /// 未在注册表中出现的条目导入为 Manual 行（见
    /// [`crate::model::custom_models_import`]），此后以注册表为准。保留此字段
    /// 只是为了不让老配置文件在升级后直接报错；新部署请直接用 admin UI 管理模型。
    #[serde(default)]
    pub custom_models: Vec<CustomModel>,

    /// 配置文件路径（运行时元数据，不写入 JSON）
    #[serde(skip)]
    config_path: Option<PathBuf>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_kiro_version() -> String {
    "2.3.0".to_string()
}

fn default_system_version() -> String {
    "macos".to_string()
}

fn default_node_version() -> String {
    "22.22.0".to_string()
}

fn default_count_tokens_auth_type() -> String {
    "x-api-key".to_string()
}

fn default_tls_backend() -> TlsBackend {
    TlsBackend::Rustls
}

fn default_stream_idle_timeout_secs() -> u64 {
    crate::http_client::DEFAULT_STREAM_IDLE_TIMEOUT_SECS
}

fn default_stream_total_timeout_secs() -> u64 {
    crate::http_client::DEFAULT_STREAM_TOTAL_TIMEOUT_SECS
}

fn default_load_balancing_mode() -> String {
    "priority".to_string()
}

fn default_quota_guard_reserve() -> f64 {
    200.0
}

fn default_credential_max_concurrent() -> usize {
    2
}

fn default_credential_queue_depth() -> usize {
    3
}

fn default_credential_queue_timeout_secs() -> u64 {
    60
}

fn default_account_throttle_failover() -> bool {
    true
}

fn default_account_throttle_cooldown_secs() -> u64 {
    30 * 60
}

fn default_account_rpm_limit_enabled() -> bool {
    false
}

fn default_account_rpm_limit() -> u32 {
    60
}

fn default_update_auto_apply_time() -> String {
    "03:00".to_string()
}

fn default_extract_thinking() -> bool {
    true
}

fn default_tool_compatibility_mode() -> ToolCompatibilityMode {
    ToolCompatibilityMode::ClaudeCode
}

fn default_endpoint() -> String {
    crate::kiro::endpoint::ide::IDE_ENDPOINT_NAME.to_string()
}

fn default_trace_enabled() -> bool {
    true
}

fn default_trace_retention_days() -> u32 {
    7
}

fn default_usage_log_retention_days() -> u32 {
    31
}

fn default_model_sync_time() -> String {
    "04:00".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            region: default_region(),
            auth_region: None,
            api_region: None,
            kiro_version: default_kiro_version(),
            machine_id: None,
            api_key: None,
            system_version: default_system_version(),
            node_version: default_node_version(),
            tls_backend: default_tls_backend(),
            count_tokens_api_url: None,
            count_tokens_api_key: None,
            count_tokens_auth_type: default_count_tokens_auth_type(),
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            require_proxy: false,
            stream_idle_timeout_secs: default_stream_idle_timeout_secs(),
            stream_total_timeout_secs: default_stream_total_timeout_secs(),
            admin_api_key: None,
            update_previous_version: None,
            github_token: None,
            update_last_applied_at: None,
            update_auto_apply: false,
            update_auto_apply_time: default_update_auto_apply_time(),
            load_balancing_mode: default_load_balancing_mode(),
            quota_guard_reserve: default_quota_guard_reserve(),
            credential_max_concurrent: default_credential_max_concurrent(),
            credential_queue_depth: default_credential_queue_depth(),
            credential_queue_timeout_secs: default_credential_queue_timeout_secs(),
            account_throttle_failover: default_account_throttle_failover(),
            account_throttle_cooldown_secs: default_account_throttle_cooldown_secs(),
            account_rpm_limit_enabled: default_account_rpm_limit_enabled(),
            account_rpm_limit: default_account_rpm_limit(),
            extract_thinking: default_extract_thinking(),
            tool_compatibility_mode: default_tool_compatibility_mode(),
            default_endpoint: default_endpoint(),
            trace_enabled: default_trace_enabled(),
            trace_retention_days: default_trace_retention_days(),
            store_request_bodies: false,
            usage_log_retention_days: default_usage_log_retention_days(),
            endpoints: HashMap::new(),
            model_sync_enabled: false,
            model_sync_time: default_model_sync_time(),
            model_sync_probe_credential_id: None,
            allow_unknown_model_passthrough: false,
            custom_models: Vec::new(),
            config_path: None,
        }
    }
}

impl Config {
    /// 获取默认配置文件路径
    pub fn default_config_path() -> &'static str {
        "config.json"
    }

    /// 获取有效的 Auth Region（用于 Token 刷新）
    /// 优先使用 auth_region，未配置时回退到 region
    pub fn effective_auth_region(&self) -> &str {
        self.auth_region.as_deref().unwrap_or(&self.region)
    }

    /// 获取有效的 API Region（用于 API 请求）
    /// 优先使用 api_region，未配置时回退到 region
    pub fn effective_api_region(&self) -> &str {
        self.api_region.as_deref().unwrap_or(&self.region)
    }

    /// 从文件加载配置
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            // 配置文件不存在，返回默认配置
            let config = Self { config_path: Some(path.to_path_buf()), ..Self::default() };
            return Ok(config);
        }

        let content = fs::read_to_string(path)?;
        let mut config: Config = serde_json::from_str(&content)?;
        config.config_path = Some(path.to_path_buf());

        // 用户手工把字符串字段清空（如 `"updateAutoApplyTime": ""`）时，serde 默认值不会
        // 介入；这里把"看起来像空"的关键字段回退到默认值，避免后续业务用到
        // 空字符串导致难以诊断的错误。
        if config.update_auto_apply_time.trim().is_empty() {
            config.update_auto_apply_time = default_update_auto_apply_time();
        }

        Ok(config)
    }

    /// 获取配置文件路径（如果有）
    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    /// 将当前配置写回原始配置文件
    pub fn save(&self) -> anyhow::Result<()> {
        let path = self
            .config_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("配置文件路径未知，无法保存配置"))?;

        let content = serde_json::to_string_pretty(self).context("序列化配置失败")?;
        fs::write(path, content)
            .with_context(|| format!("写入配置文件失败: {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod model_sync_config_tests {
    use super::*;

    /// 四个新字段必须全部可缺省，且 modelSyncEnabled 默认 false
    /// （否则破坏「零行为回归」）
    #[test]
    fn model_sync_fields_default_off() {
        let cfg: Config = serde_json::from_str(r#"{"apiKey":"sk-x"}"#).unwrap();
        assert!(!cfg.model_sync_enabled, "自动同步必须默认关闭");
        assert_eq!(cfg.model_sync_time, "04:00");
        assert_eq!(cfg.model_sync_probe_credential_id, None);
        assert!(!cfg.allow_unknown_model_passthrough, "透传必须默认关闭");
    }

    #[test]
    fn model_sync_fields_roundtrip_camel_case() {
        let cfg: Config = serde_json::from_str(
            r#"{"modelSyncEnabled":true,"modelSyncTime":"3:5","modelSyncProbeCredentialId":7,"allowUnknownModelPassthrough":true}"#,
        )
        .unwrap();
        assert!(cfg.model_sync_enabled);
        assert_eq!(cfg.model_sync_time, "3:5");
        assert_eq!(cfg.model_sync_probe_credential_id, Some(7));
        assert!(cfg.allow_unknown_model_passthrough);
    }
}

#[cfg(test)]
mod streaming_timeout_config_tests {
    use super::*;

    /// 缺字段的老配置必须仍能解析，并落到代码默认值上（向后兼容）。
    ///
    /// 断言跟随 `DEFAULT_STREAM_*` 常量而不写死数值：本测试的意图是「有默认值可用」，
    /// 具体取值是运维折中、会随实测分布调整（已从 90s 调过一次，因为 90s 实证会误杀
    /// 健康长生成）。把数值写死会让每次调阈值都要改这里，且失败信息指向"兼容性坏了"
    /// 这个错误方向。取值本身的合理性由 `http_client` 侧的常量注释与断言把关。
    #[test]
    fn streaming_timeouts_have_backward_compatible_defaults() {
        let cfg: Config = serde_json::from_str(r#"{"apiKey":"sk-x"}"#).unwrap();
        assert_eq!(
            cfg.stream_idle_timeout_secs,
            crate::http_client::DEFAULT_STREAM_IDLE_TIMEOUT_SECS
        );
        assert_eq!(
            cfg.stream_total_timeout_secs,
            crate::http_client::DEFAULT_STREAM_TOTAL_TIMEOUT_SECS
        );
    }

    #[test]
    fn streaming_timeouts_use_camel_case_config_names() {
        let cfg: Config =
            serde_json::from_str(r#"{"streamIdleTimeoutSecs":120,"streamTotalTimeoutSecs":2400}"#)
                .unwrap();
        assert_eq!(cfg.stream_idle_timeout_secs, 120);
        assert_eq!(cfg.stream_total_timeout_secs, 2400);
    }

    /// RPM 限流对存量 config.json 必须完全无感：缺字段时默认关闭。
    /// 这条断言守的是「升级不改变现有调度行为」——一旦默认值被误改成 true，
    /// 所有存量部署会突然开始按 60 RPM 卡账号。
    #[test]
    fn account_rpm_limit_defaults_for_existing_configs() {
        let config: Config = serde_json::from_str("{}").unwrap();
        assert!(!config.account_rpm_limit_enabled);
        assert_eq!(config.account_rpm_limit, 60);

        let default = Config::default();
        assert!(!default.account_rpm_limit_enabled);
        assert_eq!(default.account_rpm_limit, 60);
    }

    #[test]
    fn account_rpm_limit_accepts_explicit_values() {
        let config: Config = serde_json::from_str(
            r#"{
                "accountRpmLimitEnabled": true,
                "accountRpmLimit": 120
            }"#,
        )
        .unwrap();
        assert!(config.account_rpm_limit_enabled);
        assert_eq!(config.account_rpm_limit, 120);
    }
}

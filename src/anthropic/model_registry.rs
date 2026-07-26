//! 模型注册表：把「有哪些模型 / 映射到哪个上游 id / 输入窗口多大」从编译期
//! 硬编码改为「内置默认 ⊕ models.json 覆盖」。
//!
//! **本模块不含任何 I/O、不含任何时间概念。** 时间语义（deprecated 宽限、
//! lastSeenAt）属于 model_sync；文件读写属于 model_registry_store。
//! 这样模型解析逻辑完全确定性，可直接复用 converter 现有测试作为回归基线。

use super::types::Model;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// 匹配方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchKind {
    /// 精确匹配（默认）。
    Exact,
    /// 前缀匹配。用于复现 `gpt-5*` 通配透传，命中后上游 id 为「小写后的请求名原样」。
    Prefix,
}

impl Default for MatchKind {
    fn default() -> Self {
        Self::Exact
    }
}

/// 模型状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelStatus {
    Active,
    /// 上游已不再返回，但保留且仍可用（不打断在用客户端）。
    Deprecated,
}

impl Default for ModelStatus {
    fn default() -> Self {
        Self::Active
    }
}

/// 行来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelOrigin {
    /// 编译内置，永不可删。
    Builtin,
    /// 自动同步产生。
    Synced,
    /// 人工新增。
    Manual,
}

fn default_true() -> bool {
    true
}

/// 一行 = 一个上游模型。`-thinking` 变体由 `expose_thinking_variant` 派生，
/// 不单独占行——否则「改窗口要记得改两处」。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRow {
    /// 上游 modelId，行主键。上游使用点号（`claude-opus-4.8`）。
    pub upstream_id: String,
    #[serde(default)]
    pub match_kind: MatchKind,
    /// 对外名。claude-* 用连字符；其他（含 gpt-5*）原样保留。
    pub exposed_id: String,
    pub display_name: String,
    pub owned_by: String,
    pub model_type: String,
    pub created: i64,
    /// **输入**上下文窗口，供 get_context_window_size()。
    pub context_window: i32,
    /// **输出**上限，供 /v1/models 的 Model.max_tokens。与 context_window 语义不同。
    pub max_output_tokens: i32,
    #[serde(default)]
    pub expose_thinking_variant: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 是否出现在 /v1/models。prefix 行强制 false（它不是一个真实模型）。
    #[serde(default = "default_true")]
    pub listed: bool,
    #[serde(default)]
    pub status: ModelStatus,
    pub origin: ModelOrigin,
    /// 列表排序键，升序。替代当前硬编码 Vec 的隐式顺序。
    pub sort_order: i32,
    /// 已人工编辑、同步时逐字段跳过的字段名。
    #[serde(default)]
    pub pinned: Vec<String>,
    #[serde(default)]
    pub missing_sync_rounds: u32,
    #[serde(default)]
    pub last_seen_at: Option<String>,
    /// 额外的子串匹配关键字，用于复现旧 map_model 的「家族通吃」语义。
    /// 内置默认只给三行填值；**不得给 sonnet/opus 4.x 行填家族关键字**——
    /// 旧代码对它们要求版本匹配，填了会让 claude-3-5-sonnet 被误判
    /// （旧行为是 None）。
    #[serde(default)]
    pub match_substrings: Vec<String>,
}

/// 手动别名。生命周期与 models 不同：models 会被同步覆盖，aliases 只属于人工。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelAlias {
    pub from: String,
    /// 必须是某个存在的 upstream_id。不支持指向另一个 alias（无递归）。
    pub to: String,
}

/// 内置默认：由改造前的 available_models() / map_model() /
/// get_context_window_size() 三处硬编码合并而来。
///
/// context_window 取值依据改造前的 get_context_window_size：
/// claude-sonnet-4.6/4.8/5、claude-opus-4.6/4.7/4.8、claude-fable-5 为 1_000_000；
/// gpt* 为 272_000；其余 200_000。
/// max_output_tokens 全部为 64_000（改造前 available_models 中全部为 64000）。
pub fn builtin_rows() -> Vec<ModelRow> {
    let gpt = |exposed: &str, display: &str, sort: i32| ModelRow {
        upstream_id: exposed.to_string(),
        match_kind: MatchKind::Exact,
        exposed_id: exposed.to_string(),
        display_name: display.to_string(),
        owned_by: "openai".to_string(),
        model_type: "chat".to_string(),
        created: 1782000000,
        context_window: 272_000,
        max_output_tokens: 64_000,
        expose_thinking_variant: false,
        enabled: true,
        listed: true,
        status: ModelStatus::Active,
        origin: ModelOrigin::Builtin,
        sort_order: sort,
        pinned: Vec::new(),
        missing_sync_rounds: 0,
        last_seen_at: None,
        match_substrings: Vec::new(),
    };
    let claude = |upstream: &str,
                  exposed: &str,
                  display: &str,
                  created: i64,
                  window: i32,
                  sort: i32| ModelRow {
        upstream_id: upstream.to_string(),
        match_kind: MatchKind::Exact,
        exposed_id: exposed.to_string(),
        display_name: display.to_string(),
        owned_by: "anthropic".to_string(),
        model_type: "chat".to_string(),
        created,
        context_window: window,
        max_output_tokens: 64_000,
        expose_thinking_variant: true,
        enabled: true,
        listed: true,
        status: ModelStatus::Active,
        origin: ModelOrigin::Builtin,
        sort_order: sort,
        pinned: Vec::new(),
        missing_sync_rounds: 0,
        last_seen_at: None,
        match_substrings: match upstream {
            "claude-fable-5" => vec!["fable".to_string()],
            "claude-haiku-4.5" => vec!["haiku".to_string()],
            "claude-sonnet-5" => {
                vec!["sonnet-5".to_string(), "sonnet5".to_string(), "sonnet.5".to_string()]
            }
            _ => Vec::new(),
        },
    };

    vec![
        // ---- gpt-5 通配行：仅用于解析，不出现在 /v1/models ----
        ModelRow {
            upstream_id: "gpt-5".to_string(),
            match_kind: MatchKind::Prefix,
            exposed_id: "gpt-5".to_string(),
            display_name: "GPT-5 family (prefix)".to_string(),
            owned_by: "openai".to_string(),
            model_type: "chat".to_string(),
            created: 1782000000,
            context_window: 272_000,
            max_output_tokens: 64_000,
            expose_thinking_variant: false,
            enabled: true,
            listed: false,
            status: ModelStatus::Active,
            origin: ModelOrigin::Builtin,
            sort_order: 0,
            pinned: Vec::new(),
            missing_sync_rounds: 0,
            last_seen_at: None,
            match_substrings: Vec::new(),
        },
        // ---- 顺序与改造前 available_models() 完全一致 ----
        gpt("gpt-5.6-sol", "GPT-5.6 Sol", 10),
        gpt("gpt-5.6-terra", "GPT-5.6 Terra", 20),
        gpt("gpt-5.6-luna", "GPT-5.6 Luna", 30),
        claude("claude-fable-5", "claude-fable-5", "Claude Fable 5", 1781481600, 1_000_000, 40),
        claude("claude-sonnet-5", "claude-sonnet-5", "Claude Sonnet 5", 1781481600, 1_000_000, 50),
        claude("claude-opus-4.8", "claude-opus-4-8", "Claude Opus 4.8", 1779897600, 1_000_000, 60),
        claude("claude-sonnet-4.8", "claude-sonnet-4-8", "Claude Sonnet 4.8", 1779897600, 1_000_000, 70),
        claude("claude-opus-4.7", "claude-opus-4-7", "Claude Opus 4.7", 1776276000, 1_000_000, 80),
        claude("claude-opus-4.6", "claude-opus-4-6", "Claude Opus 4.6", 1770163200, 1_000_000, 90),
        claude("claude-sonnet-4.6", "claude-sonnet-4-6", "Claude Sonnet 4.6", 1771286400, 1_000_000, 100),
        claude("claude-opus-4.5", "claude-opus-4-5-20251101", "Claude Opus 4.5", 1763942400, 200_000, 110),
        claude("claude-sonnet-4.5", "claude-sonnet-4-5-20250929", "Claude Sonnet 4.5", 1759104000, 200_000, 120),
        claude("claude-haiku-4.5", "claude-haiku-4-5-20251001", "Claude Haiku 4.5", 1760486400, 200_000, 130),
    ]
}

/// 拒绝原因。「配了但禁用」与「没配」是不同的排查方向，必须区分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    Unknown,
    Disabled,
}

/// 解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Mapped { upstream_id: String, context_window: i32 },
    /// 未收录但放行透传，窗口按 200K 估算。
    Passthrough { upstream_id: String, context_window: i32 },
    Rejected(RejectReason),
}

/// 透传时的窗口回退值。
pub const PASSTHROUGH_CONTEXT_WINDOW: i32 = 200_000;

/// passthrough 命中时的 warn 去重集合容量上限。
/// `MessagesRequest.model` 由客户端控制，无界集合可被打爆内存。
const PASSTHROUGH_WARN_CACHE_CAP: usize = 64;

static PASSTHROUGH_WARN_CACHE: LazyLock<RwLock<Vec<String>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));

/// 记录一次 passthrough 命中；同名模型只 warn 一次，集合满则整体清空重来
/// （等价于粗粒度节流，避免无界增长）。
pub fn note_passthrough_model(model: &str) {
    let mut cache = PASSTHROUGH_WARN_CACHE.write();
    if cache.iter().any(|m| m == model) {
        return;
    }
    if cache.len() >= PASSTHROUGH_WARN_CACHE_CAP {
        cache.clear();
    }
    cache.push(model.to_string());
    tracing::warn!(
        "模型 {} 未在映射表中，走透传，窗口按 {} 估算",
        model,
        PASSTHROUGH_CONTEXT_WINDOW
    );
}

#[cfg(test)]
pub fn passthrough_warn_cache_len() -> usize {
    PASSTHROUGH_WARN_CACHE.read().len()
}

/// 规范化模型名。产物形态恰好是上游 id 的形态（点号版本段），
/// 因此解析的第 4 步用它去匹配 upstream_id。
///
/// **顺序不可颠倒**：必须先剥 `-thinking` 再剥日期。否则
/// `claude-sonnet-5-20260101-thinking` 的日期永远剥不掉
/// （因为尾部是 `-thinking`，日期后缀不在末尾）。
pub fn normalize_model_name(requested: &str) -> String {
    let mut s = requested.trim().to_ascii_lowercase();

    // 1. 剥 -thinking 后缀
    if let Some(stripped) = s.strip_suffix("-thinking") {
        s = stripped.to_string();
    }

    // 2. 剥日期后缀 -YYYYMMDD（8 位纯数字）
    if let Some(idx) = s.rfind('-') {
        let tail = &s[idx + 1..];
        if tail.len() == 8 && tail.bytes().all(|b| b.is_ascii_digit()) {
            s.truncate(idx);
        }
    }

    // 3. 版本段连字符转点号：结尾的 -<数字>-<数字> → -<数字>.<数字>
    //    仅作用于结尾，避免把 legacy 名字（claude-3-5-sonnet）改写。
    let parts: Vec<&str> = s.rsplitn(3, '-').collect();
    if parts.len() == 3 {
        let (last, mid, head) = (parts[0], parts[1], parts[2]);
        if !last.is_empty()
            && !mid.is_empty()
            && last.bytes().all(|b| b.is_ascii_digit())
            && mid.bytes().all(|b| b.is_ascii_digit())
        {
            s = format!("{}-{}.{}", head, mid, last);
        }
    }

    s
}

/// `models.json` 的 schema 版本。加载时必须精确等于此值。
/// 不做自动迁移：未来版本靠「只增字段」保持前向兼容。
pub const REGISTRY_SCHEMA_VERSION: u32 = 1;

/// 单个模型的**纯同步元数据**。
///
/// 为什么单独放一个结构、而不是写回 `models` 数组里的行：
/// `from_file` 的叠加语义是 `*existing = incoming`（**整行替换**，不是逐字段
/// 合并）。一旦为了承载 `missingSyncRounds` 把内置行整行写进 `models`，那一行
/// 就冻结成写入时刻的完整快照——后续版本在代码的 `builtin_rows()` 里改
/// `contextWindow` / `displayName`，对已有部署完全失效；`models.json` 也从
/// 「稀疏的人工覆盖层」退化成「内置表的全量副本」。
///
/// 这三个字段都由同步服务单方面写入、不属于「用户覆盖」，因此挂在
/// `syncState.modelMeta` 下，与 `models` 彻底解耦；解析时按 `upstreamId`
/// 叠加到有效行集上（内置行也能拿到状态）。
///
/// **只放当前确实需要的字段**：消失判定计数、状态、最后一次被上游返回的时间。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncMeta {
    #[serde(default)]
    pub missing_sync_rounds: u32,
    #[serde(default)]
    pub status: ModelStatus,
    #[serde(default)]
    pub last_seen_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncState {
    #[serde(default)]
    pub last_sync_at: Option<String>,
    /// 本轮 fetch 的起始时间，用于乱序丢弃（见 model_sync）。
    #[serde(default)]
    pub last_fetch_started_at: Option<String>,
    /// 数据来源标记，如 `probe:3` 或 `sample:1,4,7`。
    #[serde(default)]
    pub source: Option<String>,
    /// upstreamId → 同步元数据。见 `SyncMeta` 的说明。
    #[serde(default)]
    pub model_meta: HashMap<String, SyncMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRegistryFile {
    pub version: u32,
    #[serde(default)]
    pub sync_state: SyncState,
    #[serde(default)]
    pub models: Vec<ModelRow>,
    #[serde(default)]
    pub aliases: Vec<ModelAlias>,
    /// 凭据 id（字符串形式，JSON key 限制）→ 该凭据可用的 upstream_id 列表。
    /// 同步时顺带记录（零额外请求），供调度层过滤。
    #[serde(default)]
    pub credential_support: HashMap<String, Vec<String>>,
}

impl Default for ModelRegistryFile {
    fn default() -> Self {
        Self {
            version: REGISTRY_SCHEMA_VERSION,
            sync_state: SyncState::default(),
            models: Vec::new(),
            aliases: Vec::new(),
            credential_support: HashMap::new(),
        }
    }
}

/// 注册表。构造后不可变；热重载靠整体替换 Arc。
#[derive(Debug, Clone)]
pub struct ModelRegistry {
    rows: Vec<ModelRow>,
    aliases: Vec<ModelAlias>,
}

impl ModelRegistry {
    /// 仅内置默认（无覆盖层）。models.json 不存在或校验失败时使用。
    pub fn builtin() -> Self {
        Self {
            rows: builtin_rows(),
            aliases: Vec::new(),
        }
    }

    pub fn rows(&self) -> &[ModelRow] {
        &self.rows
    }

    pub fn aliases(&self) -> &[ModelAlias] {
        &self.aliases
    }

    /// 测试与同步服务用于就地修改行。
    pub fn rows_mut(&mut self) -> &mut Vec<ModelRow> {
        &mut self.rows
    }

    pub fn set_aliases(&mut self, aliases: Vec<ModelAlias>) {
        self.aliases = aliases;
    }

    fn row_by_upstream(&self, upstream_id: &str) -> Option<&ModelRow> {
        self.rows.iter().find(|r| r.upstream_id == upstream_id)
    }

    fn hit(row: &ModelRow, upstream_id: String) -> Resolution {
        if !row.enabled {
            return Resolution::Rejected(RejectReason::Disabled);
        }
        Resolution::Mapped { upstream_id, context_window: row.context_window }
    }

    /// 解析请求中的模型名。顺序：alias → exposedId → exposedId-thinking →
    /// prefix 行 → 规范化后匹配 upstreamId → match_substrings 家族匹配 →
    /// 家族+版本宽松匹配 → 透传 → 拒绝。
    ///
    /// prefix 匹配被放在「规范化匹配 upstreamId」**之前**：规范化会剥掉日期
    /// 后缀（如 `-20250929`），但 `gpt-5.6-sol-20250929` 这类 gpt-5* 请求
    /// 必须原样透传、日期不能丢（旧代码对 gpt-5* 不做任何规范化，直接
    /// contains 判断）。若规范化先跑，日期就已经被剥掉，prefix 步骤命中时
    /// 上游 id 就不再是原始请求名了。claude 系没有 prefix 行，不受此调整
    /// 影响。
    ///
    /// 「thinking 变体被关闭」的拒绝会被延后：第 3/5 步命中但变体关闭时，
    /// 先记下待定拒绝、继续往下走，只有后续步骤也未命中才真正拒绝——
    /// 这样 `gpt-5.6-sol-thinking` 这类请求能落到 prefix 透传（旧代码对
    /// gpt-5* 不剥 -thinking，原样透传）。`enabled == false` 的
    /// `Rejected(Disabled)` 不受影响，命中即返回。
    pub fn resolve(&self, requested: &str, allow_passthrough: bool) -> Resolution {
        let lower = requested.trim().to_ascii_lowercase();
        let mut pending_thinking_rejection = false;

        // 1. alias 精确命中
        if let Some(alias) = self.aliases.iter().find(|a| a.from.to_ascii_lowercase() == lower) {
            return match self.row_by_upstream(&alias.to) {
                Some(row) => Self::hit(row, row.upstream_id.clone()),
                // 加载校验已保证不会 dangling；防御性返回 Unknown。
                None => Resolution::Rejected(RejectReason::Unknown),
            };
        }

        // 2. exposed_id 精确命中
        if let Some(row) = self
            .rows
            .iter()
            .find(|r| r.match_kind == MatchKind::Exact && r.exposed_id == lower)
        {
            return Self::hit(row, row.upstream_id.clone());
        }

        // 3. {exposed_id}-thinking 命中，且该行开启 thinking 变体
        if let Some(base) = lower.strip_suffix("-thinking") {
            if let Some(row) = self
                .rows
                .iter()
                .find(|r| r.match_kind == MatchKind::Exact && r.exposed_id == base)
            {
                if !row.enabled {
                    // 禁用信号优先级高于「变体未开启」：「配了但禁用」与
                    // 「没配」是不同的排查方向，不能被 pending 兜底逻辑掩盖。
                    return Resolution::Rejected(RejectReason::Disabled);
                } else if !row.expose_thinking_variant {
                    // 该行关闭了 thinking 变体 → 先记下待定拒绝，继续往下找兜底
                    pending_thinking_rejection = true;
                } else {
                    return Self::hit(row, row.upstream_id.clone());
                }
            }
        }

        // 4. prefix 行，最长前缀优先；上游 id = 小写请求名原样。
        //    必须在规范化匹配之前：规范化会剥掉日期后缀，而 gpt-5* 这类
        //    prefix 命中要求「原样透传」，日期不能丢。
        if let Some(row) = self
            .rows
            .iter()
            .filter(|r| r.match_kind == MatchKind::Prefix && lower.starts_with(&r.exposed_id))
            .max_by_key(|r| r.exposed_id.len())
        {
            return Self::hit(row, lower.clone());
        }

        // 5. 规范化后匹配 upstream_id
        let normalized = normalize_model_name(&lower);
        if let Some(row) = self
            .rows
            .iter()
            .find(|r| r.match_kind == MatchKind::Exact && r.upstream_id == normalized)
        {
            if lower.ends_with("-thinking") && !row.enabled {
                // 禁用信号优先级高于「变体未开启」，见第 3 步同一处理。
                return Resolution::Rejected(RejectReason::Disabled);
            } else if lower.ends_with("-thinking") && !row.expose_thinking_variant {
                // 请求名带 thinking 但该行关闭了变体 → 先记下待定拒绝，继续往下找兜底
                pending_thinking_rejection = true;
            } else {
                return Self::hit(row, row.upstream_id.clone());
            }
        }

        // 6. match_substrings 家族匹配：复现旧 map_model 的 contains("haiku") /
        //    contains("fable") / sonnet 5 代三种拼法，不看版本号。
        if let Some(row) = self
            .rows
            .iter()
            .find(|r| r.match_kind == MatchKind::Exact && r.match_substrings.iter().any(|s| lower.contains(s.as_str())))
        {
            return Self::hit(row, row.upstream_id.clone());
        }

        // 7. 家族 + 版本宽松匹配：复现旧 map_model 对 sonnet/opus 4.x 系列的
        //    `contains(家族) && contains(版本)` 语义，用于容忍
        //    Bedrock/Vertex 风格前缀（`anthropic.claude-opus-4-8`）、
        //    `-latest` / `-preview` / `@日期` 等后缀、以及不带 `claude-`
        //    前缀的简写（`opus-4.8`）。
        //
        //    匹配规则完全从 upstream_id 派生，不新增数据字段：
        //    - 仅对 upstream_id 以 "claude-" 开头、且最后一段形如
        //      `<数字>.<数字>` 的行启用（如 claude-opus-4.8 的 "4.8"）。
        //      **刻意排除**无点号的版本段（如 claude-sonnet-5 的
        //      "5"、claude-fable-5 的 "5"）——旧代码对 sonnet/opus 4.x
        //      要求同时命中版本号，若把无点号的单段数字也当作版本，
        //      `contains("sonnet") && contains("5")` 会让
        //      `claude-3-5-sonnet` 被误判为 sonnet 5 代（旧行为是
        //      None）。这些行的宽松别名已由上面的 match_substrings 单独
        //      覆盖，不需要也不应该走这条通用规则。
        //    - 家族关键字 = upstream_id 按 "-" 切分的第 2 段
        //      （claude-opus-4.8 → "opus"）。
        //    - 版本形态两种：点号版（"4.8"）与连字符版（"4-8"），命中
        //      其一即可，因为规范化前的原始请求可能用任一形态书写。
        //    - 命中条件：请求名同时包含家族关键字与其中一种版本形态。
        //    - 多行都命中时按 sort_order 升序取第一个。
        //
        //    已知的可接受差异：旧代码 opus 分支的判断顺序是
        //    4-8 → 4-7 → 4-5 → 4-6（4-5 排在 4-6 之前，像笔误但是既有
        //    事实），这里按 sort_order 即 4.8 → 4.7 → 4.6 → 4.5。仅当输入
        //    同时包含两个不同版本号（如 `claude-opus-4-5-4-6` 这种病态
        //    输入）时结果才会不同，可接受。
        if let Some(row) = self
            .rows
            .iter()
            .filter(|r| r.match_kind == MatchKind::Exact)
            .filter(|r| {
                let Some(rest) = r.upstream_id.strip_prefix("claude-") else { return false };
                let segments: Vec<&str> = rest.split('-').collect();
                if segments.len() < 2 {
                    return false;
                }
                let family = segments[0];
                let version_dot = *segments.last().unwrap();
                let Some((major, minor)) = version_dot.split_once('.') else { return false };
                if major.is_empty()
                    || minor.is_empty()
                    || !major.bytes().all(|b| b.is_ascii_digit())
                    || !minor.bytes().all(|b| b.is_ascii_digit())
                {
                    return false;
                }
                let version_hyphen = format!("{}-{}", major, minor);
                lower.contains(family) && (lower.contains(version_dot) || lower.contains(&version_hyphen))
            })
            .min_by_key(|r| r.sort_order)
        {
            // 请求名带 -thinking 后缀时，宽松匹配到的行同样要过 thinking
            // 变体门禁——否则第 3 步已经记下的「变体关闭」待定拒绝会被
            // 这里悄悄绕过（宽松匹配不看 -thinking，会把
            // `claude-opus-4-8-thinking` 当成普通请求命中）。
            if lower.ends_with("-thinking") && !row.enabled {
                return Resolution::Rejected(RejectReason::Disabled);
            } else if lower.ends_with("-thinking") && !row.expose_thinking_variant {
                pending_thinking_rejection = true;
            } else {
                return Self::hit(row, row.upstream_id.clone());
            }
        }

        // 第 3/5 步的待定拒绝：后续步骤都没能提供兜底，真正拒绝
        if pending_thinking_rejection {
            return Resolution::Rejected(RejectReason::Unknown);
        }

        // 8. 未收录透传
        if allow_passthrough {
            return Resolution::Passthrough {
                upstream_id: normalized,
                context_window: PASSTHROUGH_CONTEXT_WINDOW,
            };
        }

        Resolution::Rejected(RejectReason::Unknown)
    }

    /// 构造 /v1/models 列表。
    ///
    /// 可见性规则：
    /// - `listed == false`（含全部 prefix 行）→ 不出现
    /// - `enabled == false` → 不出现（人工主动下线，不该再被发现）
    /// - `status == Deprecated` → **仍然出现**（上游消失但不打断在用客户端；
    ///   否则客户端模型列表突然缺项，比报错更难排查）
    ///
    /// `Model.max_tokens` 取 `max_output_tokens`，**不是** `context_window`。
    pub fn exposed_models(&self) -> Vec<Model> {
        let mut rows: Vec<&ModelRow> = self
            .rows
            .iter()
            .filter(|r| r.listed && r.enabled && r.match_kind == MatchKind::Exact)
            .collect();
        rows.sort_by_key(|r| r.sort_order);

        let mut out = Vec::with_capacity(rows.len() * 2);
        for row in rows {
            out.push(Model {
                id: row.exposed_id.clone(),
                object: "model".to_string(),
                created: row.created,
                owned_by: row.owned_by.clone(),
                display_name: row.display_name.clone(),
                model_type: row.model_type.clone(),
                max_tokens: row.max_output_tokens,
            });
            if row.expose_thinking_variant {
                out.push(Model {
                    id: format!("{}-thinking", row.exposed_id),
                    object: "model".to_string(),
                    created: row.created,
                    owned_by: row.owned_by.clone(),
                    display_name: format!("{} (Thinking)", row.display_name),
                    model_type: row.model_type.clone(),
                    max_tokens: row.max_output_tokens,
                });
            }
        }
        out
    }
}

impl ModelRegistry {
    /// 从覆盖层文件构造。任一校验失败即整体拒绝（调用方退回内置默认 + degraded）。
    ///
    /// 校验顺序见 spec §4.5。唯一性校验不可省：重复 exposedId、alias 与
    /// exposedId 撞名都会让解析结果依赖遍历顺序。
    pub fn from_file(file: ModelRegistryFile) -> Result<Self, String> {
        Self::from_file_with_builtin(builtin_rows(), file)
    }

    /// `from_file` 的可注入内置集版本。**存在的唯一理由是可测性**：
    /// 「代码里的内置行定义变了、已有部署必须跟着变」这条回归无法用固定的
    /// `builtin_rows()` 表达，需要注入一份「新版代码」的内置集来断言。
    /// 生产代码只应调用 `from_file` —— 收窄为 `pub(crate)` 把这条约定从注释
    /// 变成编译器保证。
    pub(crate) fn from_file_with_builtin(
        builtin: Vec<ModelRow>,
        file: ModelRegistryFile,
    ) -> Result<Self, String> {
        if file.version != REGISTRY_SCHEMA_VERSION {
            return Err(format!(
                "不支持的 models.json schema 版本: {}（期望 {}）",
                file.version, REGISTRY_SCHEMA_VERSION
            ));
        }

        // 文件内 upstream_id 唯一（必须在叠加前检查：叠加是「同 id 覆盖」，
        // 会把文件内的重复行悄悄合并成一行，导致下面的唯一性检查失效）。
        let mut seen_incoming_upstream: HashSet<&str> = HashSet::new();
        for row in &file.models {
            if !seen_incoming_upstream.insert(row.upstream_id.as_str()) {
                return Err(format!("重复的 upstreamId: {}", row.upstream_id));
            }
        }

        // 覆盖层叠加在内置默认之上（spec §4.6）：
        // 同 upstream_id 用文件中的行替换，其余内置行保留。
        // 这保证「文件里只写一行覆写」不会让其他模型消失。
        let mut rows = builtin;
        for incoming in file.models {
            match rows.iter_mut().find(|r| r.upstream_id == incoming.upstream_id) {
                Some(existing) => *existing = incoming,
                None => rows.push(incoming),
            }
        }

        // N1：同步元数据叠加。放在覆盖层叠加**之后**，因为 syncState.modelMeta 是
        // 同步服务对这三个字段的唯一权威来源；覆盖层行上同名的内联字段只是本分支
        // 开发期写出的老格式残留，有 modelMeta 记录时以 modelMeta 为准。
        //
        // 之所以「叠加元数据」而不是「把行复制进覆盖层再改字段」：上面的叠加是
        // 整行替换（`*existing = incoming`），一旦内置行被写进 models 就冻结成
        // 快照，代码里改内置定义对已有部署失效。见 `SyncMeta` 的文档。
        for row in rows.iter_mut() {
            if let Some(meta) = file.sync_state.model_meta.get(&row.upstream_id) {
                row.missing_sync_rounds = meta.missing_sync_rounds;
                row.status = meta.status;
                row.last_seen_at = meta.last_seen_at.clone();
            }
        }

        // prefix 行强制 listed=false —— 它不是一个真实模型，
        // 若出现在 /v1/models 会多出一个不存在的条目。
        for row in rows.iter_mut() {
            if row.match_kind == MatchKind::Prefix {
                row.listed = false;
                if row.expose_thinking_variant {
                    return Err(format!(
                        "prefix 行不得开启 thinking 变体: {}",
                        row.upstream_id
                    ));
                }
            }
            if row.context_window <= 0 {
                return Err(format!(
                    "contextWindow 必须为正数: {} = {}",
                    row.upstream_id, row.context_window
                ));
            }
            if row.max_output_tokens <= 0 {
                return Err(format!(
                    "maxOutputTokens 必须为正数: {} = {}",
                    row.upstream_id, row.max_output_tokens
                ));
            }
        }

        // upstream_id 唯一
        let mut seen_upstream: HashSet<&str> = HashSet::new();
        for row in &rows {
            if !seen_upstream.insert(row.upstream_id.as_str()) {
                return Err(format!("重复的 upstreamId: {}", row.upstream_id));
            }
        }

        // 全部对外名唯一：exposed_id + 派生 thinking 名 + alias.from
        let mut seen_names: HashSet<String> = HashSet::new();
        for row in &rows {
            if !seen_names.insert(row.exposed_id.to_ascii_lowercase()) {
                return Err(format!("重复的对外模型名: {}", row.exposed_id));
            }
            if row.expose_thinking_variant {
                let name = format!("{}-thinking", row.exposed_id).to_ascii_lowercase();
                if !seen_names.insert(name.clone()) {
                    return Err(format!("重复的对外模型名: {}", name));
                }
            }
        }
        for alias in &file.aliases {
            let from = alias.from.to_ascii_lowercase();
            if !seen_names.insert(from.clone()) {
                return Err(format!("别名与已有对外模型名冲突或重复: {}", alias.from));
            }
            if !rows.iter().any(|r| r.upstream_id == alias.to) {
                return Err(format!("别名指向不存在的 upstreamId: {}", alias.to));
            }
            if file.aliases.iter().any(|a| a.from == alias.to) {
                return Err(format!("别名不得指向另一个别名: {} -> {}", alias.from, alias.to));
            }
        }

        Ok(Self { rows, aliases: file.aliases })
    }
}

use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;

/// 进程当前的注册表实例。**这里只是一个 holder，不含业务逻辑** ——
/// 逻辑本体是 `ModelRegistry`，它可以脱离全局单独构造与单测。
static REGISTRY: LazyLock<RwLock<Arc<ModelRegistry>>> =
    LazyLock::new(|| RwLock::new(Arc::new(ModelRegistry::builtin())));

/// 未收录模型是否放行透传。默认 false（保留「模型名写错」的快速失败信号）。
static ALLOW_PASSTHROUGH: LazyLock<RwLock<bool>> = LazyLock::new(|| RwLock::new(false));

/// 取当前注册表快照。读侧只做 Arc::clone，无锁竞争。
pub fn current_registry() -> Arc<ModelRegistry> {
    REGISTRY.read().clone()
}

/// 热替换注册表。由启动流程与同步任务在落盘成功后调用。
pub fn install_registry(registry: ModelRegistry) {
    *REGISTRY.write() = Arc::new(registry);
}

pub fn set_allow_passthrough(allow: bool) {
    *ALLOW_PASSTHROUGH.write() = allow;
}

pub fn allow_passthrough() -> bool {
    *ALLOW_PASSTHROUGH.read()
}

/// 测试专用串行锁：所有会调用 `install_registry()` 的测试必须先取此锁，
/// 否则并行测试互相覆盖全局状态导致随机失败。用
/// `unwrap_or_else(|e| e.into_inner())` 取锁，避免一个 panic 的测试
/// 毒化锁、连累后续测试全部失败。
#[cfg(test)]
pub(crate) static REGISTRY_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    /// 当前 available_models() 暴露的 23 个模型 id（src/anthropic/handlers.rs:385）。
    /// 内置默认必须逐个覆盖，否则改造会让现有模型消失。
    const CURRENT_EXPOSED_IDS: &[&str] = &[
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "claude-fable-5",
        "claude-fable-5-thinking",
        "claude-sonnet-5",
        "claude-sonnet-5-thinking",
        "claude-opus-4-8",
        "claude-opus-4-8-thinking",
        "claude-sonnet-4-8",
        "claude-sonnet-4-8-thinking",
        "claude-opus-4-7",
        "claude-opus-4-7-thinking",
        "claude-opus-4-6",
        "claude-opus-4-6-thinking",
        "claude-sonnet-4-6",
        "claude-sonnet-4-6-thinking",
        "claude-opus-4-5-20251101",
        "claude-opus-4-5-20251101-thinking",
        "claude-sonnet-4-5-20250929",
        "claude-sonnet-4-5-20250929-thinking",
        "claude-haiku-4-5-20251001",
        "claude-haiku-4-5-20251001-thinking",
    ];

    #[test]
    fn builtin_covers_all_current_exposed_ids() {
        let registry = ModelRegistry::builtin();
        let mut exposed: Vec<String> = Vec::new();
        for row in registry.rows() {
            if !row.listed {
                continue;
            }
            exposed.push(row.exposed_id.clone());
            if row.expose_thinking_variant {
                exposed.push(format!("{}-thinking", row.exposed_id));
            }
        }
        for id in CURRENT_EXPOSED_IDS {
            assert!(exposed.contains(&id.to_string()), "内置默认缺少对外模型: {}", id);
        }
        assert_eq!(exposed.len(), CURRENT_EXPOSED_IDS.len(), "内置默认多出了对外模型: {:?}", exposed);
    }

    #[test]
    fn builtin_has_gpt5_prefix_row_not_listed() {
        let registry = ModelRegistry::builtin();
        let row = registry
            .rows()
            .iter()
            .find(|r| r.match_kind == MatchKind::Prefix)
            .expect("内置默认必须含 gpt-5 prefix 行");
        assert_eq!(row.exposed_id, "gpt-5");
        assert_eq!(row.context_window, 272_000);
        assert!(!row.listed, "prefix 行不能出现在 /v1/models");
    }

    fn mapped(registry: &ModelRegistry, requested: &str) -> (String, i32) {
        match registry.resolve(requested, false) {
            Resolution::Mapped { upstream_id, context_window } => (upstream_id, context_window),
            other => panic!("期望 Mapped，实际 {:?}", other),
        }
    }

    /// 回归基线：改造前 converter.rs 中 map_model / get_context_window_size
    /// 的全部行为，逐条必须保持。
    #[test]
    fn regression_baseline_from_converter() {
        let r = ModelRegistry::builtin();

        // 带日期后缀
        assert_eq!(mapped(&r, "claude-sonnet-4-5-20250929").0, "claude-sonnet-4.5");
        assert_eq!(mapped(&r, "claude-opus-4-5-20251101").0, "claude-opus-4.5");
        // 不带日期后缀（改造前靠 contains("4-5") 命中，必须继续可用）
        assert_eq!(mapped(&r, "claude-opus-4-5").0, "claude-opus-4.5");
        // 日期 + thinking 同时存在（converter.rs:1848 的真实用例）
        assert_eq!(mapped(&r, "claude-sonnet-5-20260101-thinking").0, "claude-sonnet-5");
        // 纯 thinking 后缀
        assert_eq!(mapped(&r, "claude-fable-5-thinking").0, "claude-fable-5");
        // 4.6 / 4.7 / 4.8
        assert_eq!(mapped(&r, "claude-opus-4-6").0, "claude-opus-4.6");
        assert_eq!(mapped(&r, "claude-opus-4-7-thinking").0, "claude-opus-4.7");
        assert_eq!(mapped(&r, "claude-sonnet-4-8").0, "claude-sonnet-4.8");
        // haiku
        assert_eq!(mapped(&r, "claude-haiku-4-5-20251001").0, "claude-haiku-4.5");

        // 窗口
        assert_eq!(mapped(&r, "claude-sonnet-5").1, 1_000_000);
        assert_eq!(mapped(&r, "claude-opus-4-8").1, 1_000_000);
        assert_eq!(mapped(&r, "claude-fable-5").1, 1_000_000);
        assert_eq!(mapped(&r, "claude-opus-4-5-20251101").1, 200_000);
        assert_eq!(mapped(&r, "claude-haiku-4-5-20251001").1, 200_000);
        assert_eq!(mapped(&r, "gpt-5.6-sol").1, 272_000);
    }

    /// legacy claude-3-5-sonnet 不得被误判为 5 代
    /// （改造前 converter.rs:211 刻意规避此冲突）
    #[test]
    fn legacy_three_five_sonnet_is_not_sonnet_5() {
        let r = ModelRegistry::builtin();
        assert!(matches!(
            r.resolve("claude-3-5-sonnet", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
    }

    /// gpt-5* 通配：任意 gpt-5 开头的名字原样透传，窗口 272K
    #[test]
    fn gpt5_prefix_passthrough() {
        let r = ModelRegistry::builtin();
        // 已在列表中的具体型号
        assert_eq!(mapped(&r, "gpt-5.6-sol"), ("gpt-5.6-sol".to_string(), 272_000));
        // 未在列表中的新型号（改造前靠 starts_with("gpt-5") 放行）
        assert_eq!(mapped(&r, "gpt-5.9-nova"), ("gpt-5.9-nova".to_string(), 272_000));
        // 大写请求名要小写化（改造前返回 model_lower）
        assert_eq!(mapped(&r, "GPT-5.9-Nova").0, "gpt-5.9-nova");
        // gpt-4 不放行
        assert!(matches!(
            r.resolve("gpt-4", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
    }

    /// 规范化顺序必须是 thinking → 日期 → 版本段
    #[test]
    fn normalize_strips_thinking_before_date() {
        assert_eq!(normalize_model_name("claude-sonnet-5-20260101-thinking"), "claude-sonnet-5");
        assert_eq!(normalize_model_name("claude-opus-4-5-20251101-thinking"), "claude-opus-4.5");
        assert_eq!(normalize_model_name("CLAUDE-OPUS-4-8"), "claude-opus-4.8");
        // 反例：不得把 legacy 名字改写成 5 代
        assert_eq!(normalize_model_name("claude-3-5-sonnet"), "claude-3-5-sonnet");
    }

    /// expose_thinking_variant = false 的行不得被 xxx-thinking 命中
    #[test]
    fn thinking_variant_disabled_rejects_thinking_request() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-opus-4-8" {
                row.expose_thinking_variant = false;
            }
        }
        assert!(matches!(
            r.resolve("claude-opus-4-8-thinking", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
        // 主模型本身仍可用
        assert_eq!(mapped(&r, "claude-opus-4-8").0, "claude-opus-4.8");
    }

    /// enabled = false → Disabled，与 Unknown 区分
    #[test]
    fn disabled_row_rejects_with_disabled_reason() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-opus-4-8" {
                row.enabled = false;
            }
        }
        assert!(matches!(
            r.resolve("claude-opus-4-8", false),
            Resolution::Rejected(RejectReason::Disabled)
        ));
    }

    #[test]
    fn overlay_merges_onto_builtin_not_replaces() {
        // 覆盖层只写一行，内置默认的其余行必须保留
        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        row.context_window = 800_000;
        row.pinned = vec!["contextWindow".to_string()];

        let registry = ModelRegistry::from_file(file_with(vec![row], vec![])).unwrap();

        // 被覆盖的行取覆盖值
        let opus = registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(opus.context_window, 800_000);
        // 未被覆盖的内置行仍在
        assert!(registry.rows().iter().any(|r| r.upstream_id == "claude-fable-5"));
        assert!(registry.rows().iter().any(|r| r.match_kind == MatchKind::Prefix));
    }

    /// passthrough 开关
    #[test]
    fn unknown_model_passthrough_toggle() {
        let r = ModelRegistry::builtin();
        assert!(matches!(
            r.resolve("claude-opus-9", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
        match r.resolve("claude-opus-9", true) {
            Resolution::Passthrough { upstream_id, context_window } => {
                assert_eq!(upstream_id, "claude-opus-9");
                assert_eq!(context_window, 200_000);
            }
            other => panic!("期望 Passthrough，实际 {:?}", other),
        }
    }

    /// alias 命中；alias 指向 disabled 行 → Disabled
    #[test]
    fn alias_resolution() {
        let mut r = ModelRegistry::builtin();
        r.set_aliases(vec![ModelAlias { from: "opus".to_string(), to: "claude-opus-4.8".to_string() }]);
        assert_eq!(mapped(&r, "opus").0, "claude-opus-4.8");

        for row in r.rows_mut() {
            if row.upstream_id == "claude-opus-4.8" {
                row.enabled = false;
            }
        }
        assert!(matches!(
            r.resolve("opus", false),
            Resolution::Rejected(RejectReason::Disabled)
        ));
    }

    /// 家族通吃：旧 map_model 的 contains("haiku") / contains("fable")
    /// 不看版本号，以及 sonnet 5 代的三种拼法。
    /// 对应 converter.rs 现存测试 test_map_model_haiku 与 test_map_model_sonnet_5。
    #[test]
    fn family_substring_matching_reproduces_legacy_behavior() {
        let r = ModelRegistry::builtin();
        assert_eq!(mapped(&r, "claude-sonnet.5").0, "claude-sonnet-5");
        assert_eq!(mapped(&r, "claude-sonnet5").0, "claude-sonnet-5");
        assert_eq!(mapped(&r, "claude-haiku-4-20250514").0, "claude-haiku-4.5");
        assert_eq!(mapped(&r, "claude-fable-5-preview").0, "claude-fable-5");
    }

    /// 家族关键字不得让 legacy claude-3-5-sonnet 被误判（旧行为是 None）
    #[test]
    fn family_matching_does_not_break_legacy_sonnet_reverse_case() {
        let r = ModelRegistry::builtin();
        assert!(matches!(
            r.resolve("claude-3-5-sonnet", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
        assert!(matches!(
            r.resolve("claude-3-5-sonnet-20241022", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
    }

    /// gpt-5 开头的名字一律原样透传，thinking 后缀也不例外
    /// （旧代码 converter.rs:234 不剥 -thinking）
    #[test]
    fn gpt5_thinking_suffix_still_passes_through() {
        let r = ModelRegistry::builtin();
        assert_eq!(mapped(&r, "gpt-5.6-sol-thinking").0, "gpt-5.6-sol-thinking");
        assert_eq!(mapped(&r, "gpt-5.9-nova-thinking").0, "gpt-5.9-nova-thinking");
    }

    /// 变体关闭时，没有 substring/prefix 兜底的模型仍应被拒
    #[test]
    fn thinking_disabled_still_rejects_when_no_fallback() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-opus-4-8" {
                row.expose_thinking_variant = false;
            }
        }
        assert!(matches!(
            r.resolve("claude-opus-4-8-thinking", false),
            Resolution::Rejected(RejectReason::Unknown)
        ));
    }

    /// /v1/models 必须与改造前逐字段一致（零行为回归的核心断言）
    #[test]
    fn exposed_models_matches_pre_change_output() {
        let models = ModelRegistry::builtin().exposed_models();
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, CURRENT_EXPOSED_IDS, "/v1/models 列表内容或顺序发生变化");

        let sol = models.iter().find(|m| m.id == "gpt-5.6-sol").unwrap();
        assert_eq!(sol.object, "model");
        assert_eq!(sol.created, 1782000000);
        assert_eq!(sol.owned_by, "openai");
        assert_eq!(sol.display_name, "GPT-5.6 Sol");
        assert_eq!(sol.model_type, "chat");
        // max_tokens 是「输出上限」，不是输入窗口
        assert_eq!(sol.max_tokens, 64_000);

        let thinking = models.iter().find(|m| m.id == "claude-opus-4-8-thinking").unwrap();
        assert_eq!(thinking.display_name, "Claude Opus 4.8 (Thinking)");
    }

    /// max_tokens 必须取 max_output_tokens，绝不能取 context_window
    #[test]
    fn max_tokens_is_output_not_input_window() {
        let models = ModelRegistry::builtin().exposed_models();
        let sol = models.iter().find(|m| m.id == "gpt-5.6-sol").unwrap();
        let row = ModelRegistry::builtin()
            .rows()
            .iter()
            .find(|r| r.exposed_id == "gpt-5.6-sol")
            .unwrap()
            .clone();
        assert_eq!(sol.max_tokens, row.max_output_tokens);
        assert_ne!(sol.max_tokens, row.context_window, "把输入窗口错报成输出上限了");
    }

    fn file_with(models: Vec<ModelRow>, aliases: Vec<ModelAlias>) -> ModelRegistryFile {
        ModelRegistryFile {
            version: REGISTRY_SCHEMA_VERSION,
            sync_state: SyncState::default(),
            models,
            aliases,
            credential_support: Default::default(),
        }
    }

    fn sample_row(upstream: &str, exposed: &str) -> ModelRow {
        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        row.upstream_id = upstream.to_string();
        row.exposed_id = exposed.to_string();
        row.origin = ModelOrigin::Manual;
        row
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let mut file = file_with(builtin_rows(), vec![]);
        file.version = 2;
        assert!(ModelRegistry::from_file(file).is_err());
    }

    #[test]
    fn rejects_duplicate_upstream_id() {
        let file = file_with(
            vec![sample_row("claude-x", "claude-x-a"), sample_row("claude-x", "claude-x-b")],
            vec![],
        );
        let err = ModelRegistry::from_file(file).unwrap_err();
        assert!(err.contains("upstreamId"), "错误信息应指出重复的 upstreamId: {}", err);
    }

    #[test]
    fn rejects_duplicate_exposed_name_including_thinking_variant() {
        // a 的 thinking 变体名与 b 的 exposedId 撞名
        let mut a = sample_row("claude-a", "claude-x");
        a.expose_thinking_variant = true;
        let mut b = sample_row("claude-b", "claude-x-thinking");
        b.expose_thinking_variant = false;
        let err = ModelRegistry::from_file(file_with(vec![a, b], vec![])).unwrap_err();
        assert!(err.contains("claude-x-thinking"), "应报出撞名: {}", err);
    }

    #[test]
    fn rejects_alias_conflicts_and_dangling() {
        // dangling
        let err = ModelRegistry::from_file(file_with(
            vec![sample_row("claude-a", "claude-a")],
            vec![ModelAlias { from: "x".into(), to: "claude-missing".into() }],
        ))
        .unwrap_err();
        assert!(err.contains("claude-missing"), "应报出 dangling alias: {}", err);

        // alias.from 与 exposedId 撞名
        let err = ModelRegistry::from_file(file_with(
            vec![sample_row("claude-a", "claude-a")],
            vec![ModelAlias { from: "claude-a".into(), to: "claude-a".into() }],
        ))
        .unwrap_err();
        assert!(err.contains("claude-a"));

        // 重复 alias.from
        let err = ModelRegistry::from_file(file_with(
            vec![sample_row("claude-a", "claude-a")],
            vec![
                ModelAlias { from: "x".into(), to: "claude-a".into() },
                ModelAlias { from: "x".into(), to: "claude-a".into() },
            ],
        ))
        .unwrap_err();
        assert!(err.contains('x'));
    }

    #[test]
    fn rejects_out_of_range_windows() {
        let mut row = sample_row("claude-a", "claude-a");
        row.context_window = 0;
        assert!(ModelRegistry::from_file(file_with(vec![row], vec![])).is_err());

        let mut row = sample_row("claude-b", "claude-b");
        row.max_output_tokens = -1;
        assert!(ModelRegistry::from_file(file_with(vec![row], vec![])).is_err());
    }

    #[test]
    fn rejects_prefix_row_with_thinking_variant() {
        let mut row = sample_row("gpt-6", "gpt-6");
        row.match_kind = MatchKind::Prefix;
        row.expose_thinking_variant = true;
        assert!(ModelRegistry::from_file(file_with(vec![row], vec![])).is_err());
    }

    #[test]
    fn accepts_valid_file_and_forces_prefix_unlisted() {
        let mut row = sample_row("gpt-6", "gpt-6");
        row.match_kind = MatchKind::Prefix;
        row.expose_thinking_variant = false;
        row.listed = true; // 故意写 true，加载时应被强制为 false
        let registry = ModelRegistry::from_file(file_with(vec![row], vec![])).unwrap();
        let added = registry
            .rows()
            .iter()
            .find(|r| r.upstream_id == "gpt-6")
            .expect("覆盖层里新增的 gpt-6 行应存在");
        assert!(!added.listed, "prefix 行的 listed 必须被强制为 false");
    }

    /// listed=false / enabled=false 不出现在列表；deprecated 仍在列表
    #[test]
    fn listing_visibility_rules() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-opus-4-8" {
                row.enabled = false;
            }
            if row.exposed_id == "claude-sonnet-5" {
                row.status = ModelStatus::Deprecated;
            }
        }
        let ids: Vec<String> = r.exposed_models().into_iter().map(|m| m.id).collect();
        assert!(!ids.contains(&"claude-opus-4-8".to_string()), "enabled=false 应从列表移除");
        assert!(!ids.contains(&"claude-opus-4-8-thinking".to_string()));
        assert!(ids.contains(&"claude-sonnet-5".to_string()), "deprecated 应保留在列表");
        assert!(!ids.contains(&"gpt-5".to_string()), "prefix 行不应出现在列表");
    }

    #[test]
    fn install_and_read_global_registry() {
        // 注意：全局状态测试，取串行锁避免与其他 install_registry 测试打架。
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let mut r = ModelRegistry::builtin();
        r.rows_mut().push({
            let mut row = builtin_rows()
                .into_iter()
                .find(|x| x.upstream_id == "claude-opus-4.8")
                .unwrap();
            row.upstream_id = "claude-opus-5".to_string();
            row.exposed_id = "claude-opus-5".to_string();
            row.display_name = "Claude Opus 5".to_string();
            row.sort_order = 55;
            row.origin = ModelOrigin::Synced;
            row
        });
        install_registry(r);
        let current = current_registry();
        assert!(current.rows().iter().any(|x| x.upstream_id == "claude-opus-5"));

        // 复原，避免污染其他测试
        install_registry(ModelRegistry::builtin());
    }

    /// 一行同时 enabled=false 且 expose_thinking_variant=false 时，
    /// xxx-thinking 请求应报 Disabled 而非 Unknown ——
    /// 「配了但禁用」与「没配」是不同的排查方向。
    #[test]
    fn disabled_row_reports_disabled_even_when_thinking_variant_off() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-opus-4-8" {
                row.enabled = false;
                row.expose_thinking_variant = false;
            }
        }
        assert!(matches!(
            r.resolve("claude-opus-4-8-thinking", false),
            Resolution::Rejected(RejectReason::Disabled)
        ));
    }

    /// Bedrock / Vertex 风格的模型 id 必须继续可用 ——
    /// 旧 map_model 的 contains() 作用于整串，天然容忍前后噪音。
    #[test]
    fn family_version_loose_match_tolerates_prefixes_and_suffixes() {
        let r = ModelRegistry::builtin();
        assert_eq!(mapped(&r, "anthropic.claude-opus-4-8"), ("claude-opus-4.8".to_string(), 1_000_000));
        assert_eq!(mapped(&r, "us.anthropic.claude-sonnet-4-6").0, "claude-sonnet-4.6");
        assert_eq!(mapped(&r, "anthropic.claude-sonnet-4-5-20250929-v1:0").0, "claude-sonnet-4.5");
        assert_eq!(mapped(&r, "claude-opus-4-8-latest").0, "claude-opus-4.8");
        assert_eq!(mapped(&r, "claude-opus-4-8@20260101").0, "claude-opus-4.8");
        assert_eq!(mapped(&r, "opus-4.8").0, "claude-opus-4.8");
        assert_eq!(mapped(&r, "sonnet-4.6").0, "claude-sonnet-4.6");
    }

    /// 宽松匹配不得让 legacy claude-3-5-sonnet 被误判（旧行为是 None）
    #[test]
    fn loose_match_still_rejects_legacy_three_five_sonnet() {
        let r = ModelRegistry::builtin();
        for input in ["claude-3-5-sonnet", "claude-3-5-sonnet-20241022", "anthropic.claude-3-5-sonnet"] {
            assert!(
                matches!(r.resolve(input, false), Resolution::Rejected(RejectReason::Unknown)),
                "{} 不应被宽松匹配命中", input
            );
        }
    }

    /// gpt-5* 必须原样透传，日期后缀不得被剥离
    #[test]
    fn gpt5_passthrough_preserves_date_suffix() {
        let r = ModelRegistry::builtin();
        assert_eq!(mapped(&r, "gpt-5.6-sol-20250929"), ("gpt-5.6-sol-20250929".to_string(), 272_000));
        assert_eq!(mapped(&r, "gpt-5.6-sol").0, "gpt-5.6-sol");
        assert_eq!(mapped(&r, "gpt-5.6-sol-thinking").0, "gpt-5.6-sol-thinking");
    }

    /// passthrough 去重集合是进程级全局状态，与 REGISTRY 共用测试串行锁，
    /// 避免与其他改全局状态的测试随机打架。
    #[test]
    fn passthrough_warn_dedup_is_bounded() {
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // 客户端可控的 model 字段意味着无界去重集合可被打爆内存
        for i in 0..200 {
            note_passthrough_model(&format!("unknown-model-{}", i));
        }
        assert!(passthrough_warn_cache_len() <= 64, "去重集合必须有上限");
    }

    #[test]
    fn passthrough_warn_dedups_same_model() {
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = passthrough_warn_cache_len();
        note_passthrough_model("some-unique-model-for-dedup-test");
        let after_first = passthrough_warn_cache_len();
        note_passthrough_model("some-unique-model-for-dedup-test");
        let after_second = passthrough_warn_cache_len();
        assert_eq!(after_first, before + 1, "首次应记入");
        assert_eq!(after_second, after_first, "重复不应再记入");
    }

    /// N1：同步元数据挂在 syncState.modelMeta 下，解析时按 upstreamId 叠加到
    /// 有效行集上——内置行不必被复制进 models 就能带上 status / missingSyncRounds。
    #[test]
    fn sync_meta_overlays_onto_builtin_rows() {
        let mut file = file_with(vec![], vec![]);
        file.sync_state.model_meta.insert(
            "claude-opus-4.8".to_string(),
            SyncMeta {
                missing_sync_rounds: 2,
                status: ModelStatus::Deprecated,
                last_seen_at: Some("2026-07-25T04:00:00Z".to_string()),
            },
        );
        let registry = ModelRegistry::from_file(file).unwrap();
        let row = registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(row.missing_sync_rounds, 2);
        assert_eq!(row.status, ModelStatus::Deprecated);
        assert_eq!(row.last_seen_at.as_deref(), Some("2026-07-25T04:00:00Z"));
        // models 数组里一行都没有，元数据却生效了——这正是解耦的目的。
        assert_eq!(registry.rows().len(), builtin_rows().len());
    }

    /// N1 关键回归：**代码里的内置行定义变了，已有部署必须跟着变。**
    ///
    /// 上半段模拟新版本改了 `builtin_rows()`（窗口/显示名不同），文件里只有同步
    /// 元数据 —— 解析结果必须用新定义。
    /// 下半段是对照组：把内置行整行快照进 `models`（就是被废弃的旧做法），
    /// 新定义立刻被冻结失效。两段合起来说明为什么元数据不能写进 models 数组。
    #[test]
    fn changed_builtin_definition_wins_over_stale_file() {
        let mut new_builtin = builtin_rows();
        for r in new_builtin.iter_mut() {
            if r.upstream_id == "claude-opus-4.8" {
                r.context_window = 333_000;
                r.display_name = "Claude Opus 4.8（新版代码定义）".to_string();
            }
        }

        let mut file = file_with(vec![], vec![]);
        file.sync_state.model_meta.insert(
            "claude-opus-4.8".to_string(),
            SyncMeta { missing_sync_rounds: 1, status: ModelStatus::Active, last_seen_at: None },
        );
        let registry =
            ModelRegistry::from_file_with_builtin(new_builtin.clone(), file).unwrap();
        let row = registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(row.context_window, 333_000, "必须用新的代码定义，而不是旧文件里的值");
        assert_eq!(row.display_name, "Claude Opus 4.8（新版代码定义）");
        assert_eq!(row.missing_sync_rounds, 1, "同步元数据仍要叠加上去");

        // 对照：旧做法把内置行整行写进 models（origin 仍是 builtin），
        // 新定义被那一刻的快照冻结。
        let stale_snapshot = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap(); // context_window = 1_000_000
        let frozen = ModelRegistry::from_file_with_builtin(
            new_builtin,
            file_with(vec![stale_snapshot], vec![]),
        )
        .unwrap();
        let frozen_row =
            frozen.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(
            frozen_row.context_window, 1_000_000,
            "整行快照会冻结定义——这正是同步不得把内置行写进 models 的原因"
        );
    }

    /// N1 老格式兼容：本分支开发期写出的 models.json 里，同步元数据是逐行内联的
    /// （行上带 missingSyncRounds / status）。读取时不得 panic，且内联值仍生效；
    /// 一旦 modelMeta 里有同 upstreamId 的记录，以 modelMeta 为准。
    #[test]
    fn legacy_inline_sync_fields_are_read_and_superseded_by_model_meta() {
        let mut legacy = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        legacy.missing_sync_rounds = 2;
        legacy.status = ModelStatus::Deprecated;
        legacy.last_seen_at = Some("2026-01-01T00:00:00Z".to_string());

        // 只有内联字段、没有 modelMeta → 内联值仍被读出来
        let r = ModelRegistry::from_file(file_with(vec![legacy.clone()], vec![])).unwrap();
        let row = r.rows().iter().find(|x| x.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(row.missing_sync_rounds, 2);
        assert_eq!(row.status, ModelStatus::Deprecated);

        // 两者同时存在 → modelMeta 优先（它是新格式下的唯一权威来源）
        let mut file = file_with(vec![legacy], vec![]);
        file.sync_state.model_meta.insert(
            "claude-opus-4.8".to_string(),
            SyncMeta { missing_sync_rounds: 0, status: ModelStatus::Active, last_seen_at: None },
        );
        let r = ModelRegistry::from_file(file).unwrap();
        let row = r.rows().iter().find(|x| x.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(row.missing_sync_rounds, 0, "modelMeta 应覆盖行内联的老字段");
        assert_eq!(row.status, ModelStatus::Active);
    }

    /// 精确匹配优先级必须高于宽松匹配
    #[test]
    fn exact_match_wins_over_loose_match() {
        let r = ModelRegistry::builtin();
        // claude-opus-4-6 同时能被精确命中和被宽松匹配命中，必须走精确
        assert_eq!(mapped(&r, "claude-opus-4-6").0, "claude-opus-4.6");
        assert_eq!(mapped(&r, "claude-opus-4-5-20251101").0, "claude-opus-4.5");
    }
}


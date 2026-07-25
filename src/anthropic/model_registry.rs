//! 模型注册表：把「有哪些模型 / 映射到哪个上游 id / 输入窗口多大」从编译期
//! 硬编码改为「内置默认 ⊕ models.json 覆盖」。
//!
//! **本模块不含任何 I/O、不含任何时间概念。** 时间语义（deprecated 宽限、
//! lastSeenAt）属于 model_sync；文件读写属于 model_registry_store。
//! 这样模型解析逻辑完全确定性，可直接复用 converter 现有测试作为回归基线。

use serde::{Deserialize, Serialize};

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
    /// 规范化后匹配 upstreamId → match_substrings 家族匹配 → prefix 行 →
    /// 透传 → 拒绝。
    ///
    /// 「thinking 变体被关闭」的拒绝会被延后：第 3/4 步命中但变体关闭时，
    /// 先记下待定拒绝、继续往下走，只有第 5/6 步也未命中才真正拒绝——
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
                if !row.expose_thinking_variant {
                    // 该行关闭了 thinking 变体 → 先记下待定拒绝，继续往下找兜底
                    pending_thinking_rejection = true;
                } else {
                    return Self::hit(row, row.upstream_id.clone());
                }
            }
        }

        // 4. 规范化后匹配 upstream_id
        let normalized = normalize_model_name(&lower);
        if let Some(row) = self
            .rows
            .iter()
            .find(|r| r.match_kind == MatchKind::Exact && r.upstream_id == normalized)
        {
            // 请求名带 thinking 但该行关闭了变体 → 先记下待定拒绝，继续往下找兜底
            if lower.ends_with("-thinking") && !row.expose_thinking_variant {
                pending_thinking_rejection = true;
            } else {
                return Self::hit(row, row.upstream_id.clone());
            }
        }

        // 5. match_substrings 家族匹配：复现旧 map_model 的 contains("haiku") /
        //    contains("fable") / sonnet 5 代三种拼法，不看版本号。
        if let Some(row) = self
            .rows
            .iter()
            .find(|r| r.match_kind == MatchKind::Exact && r.match_substrings.iter().any(|s| lower.contains(s.as_str())))
        {
            return Self::hit(row, row.upstream_id.clone());
        }

        // 6. prefix 行，最长前缀优先；上游 id = 小写请求名原样
        if let Some(row) = self
            .rows
            .iter()
            .filter(|r| r.match_kind == MatchKind::Prefix && lower.starts_with(&r.exposed_id))
            .max_by_key(|r| r.exposed_id.len())
        {
            return Self::hit(row, lower.clone());
        }

        // 第 3/4 步的待定拒绝：第 5/6 步都没能提供兜底，真正拒绝
        if pending_thinking_rejection {
            return Resolution::Rejected(RejectReason::Unknown);
        }

        // 7. 未收录透传
        if allow_passthrough {
            return Resolution::Passthrough {
                upstream_id: normalized,
                context_window: PASSTHROUGH_CONTEXT_WINDOW,
            };
        }

        Resolution::Rejected(RejectReason::Unknown)
    }
}

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
}

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
}

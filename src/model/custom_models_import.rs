//! 旧版 `config.json` `customModels` → 模型注册表的启动时一次性导入。
//!
//! **背景**：上游 PR #46 引入了一套独立的 `customModels` 运行时映射机制
//! （`OnceLock` 注册表，钩在 `map_model` / `get_context_window_size` /
//! `model_supports_native_reasoning` / `available_models` 里）。本项目的模型
//! 注册表（`models.json` + Admin UI，支持热编辑、9 步解析、自动同步、pinned）
//! 是它的功能超集，因此不保留两套映射机制——`customModels` 字段本身在
//! `config.rs` 里保留（配置文件兼容），但只在启动时被消费一次：把注册表里
//! 还没有的条目转换成一条 Manual 行写入 `models.json`，此后完全由注册表接管。
//!
//! **本模块不含任何 I/O**，只负责“给定 customModels 列表 + 当前有效行集，
//! 应该新增哪些行、跳过哪些”这个纯函数判断，方便单元测试幂等性/冲突跳过而不必
//! 起真实文件或 tokio 运行时。真正的落盘调用方在 `main.rs`。

use chrono::{DateTime, Utc};

use crate::anthropic::model_registry::{MatchKind, ModelOrigin, ModelRow, ModelStatus};
use crate::model::config::CustomModel;

/// 一条 `customModels` 条目被跳过的记录，供调用方打日志。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedCustomModel {
    /// 原始 `customModels[].id`（客户端别名）。
    pub id: String,
    /// 原始 `customModels[].backendId`（上游模型 ID）。
    pub backend_id: String,
    /// 跳过原因（人类可读，直接进日志）。
    pub reason: String,
}

/// 一轮导入计划：哪些行要新增，哪些条目被跳过。
#[derive(Debug, Default)]
pub struct ImportPlan {
    pub rows_to_add: Vec<ModelRow>,
    pub skipped: Vec<SkippedCustomModel>,
}

/// 根据 `customModels` 列表与**当前有效行集**（内置 ⊕ models.json 覆盖后的
/// 最终结果）计算导入计划。
///
/// 跳过规则（决策 2）：已存在同 `upstreamId`（= `backendId`）或同 `exposedId`
/// （= `id`，大小写不敏感）的条目一律跳过、不覆盖——注册表是权威，
/// `customModels` 只负责「注册表里没有才补」。
///
/// 幂等性：本函数不做任何写入；只要 `effective_rows` 已经包含上一轮导入产生
/// 的行，同样的 `customs` 输入第二次调用会让 `rows_to_add` 为空——所有条目都
/// 会在 skip 分支命中。调用方（`main.rs`）每次启动都跑这个函数 + 落盘，因此
/// 效果是「只在真正需要时才写 models.json」。
pub fn plan_import(customs: &[CustomModel], effective_rows: &[ModelRow], now: DateTime<Utc>) -> ImportPlan {
    let mut plan = ImportPlan::default();

    // sortOrder 基线取有效行集的最大值，避免与内置行（占 [0,130]）撞号；
    // 同一批新增的行在此基础上递增，保持 customModels 数组内的原始顺序。
    let mut next_sort = effective_rows.iter().map(|r| r.sort_order).max().unwrap_or(0);

    for cm in customs {
        let backend_id = cm.backend_id.trim();
        let exposed_id = cm.id.trim().to_ascii_lowercase();

        let upstream_conflict = effective_rows.iter().any(|r| r.upstream_id == backend_id)
            || plan.rows_to_add.iter().any(|r| r.upstream_id == backend_id);
        let exposed_conflict = effective_rows
            .iter()
            .any(|r| r.exposed_id.to_ascii_lowercase() == exposed_id)
            || plan.rows_to_add.iter().any(|r| r.exposed_id.to_ascii_lowercase() == exposed_id);

        if upstream_conflict {
            plan.skipped.push(SkippedCustomModel {
                id: cm.id.clone(),
                backend_id: cm.backend_id.clone(),
                reason: format!("已存在同名 upstreamId: {}", backend_id),
            });
            continue;
        }
        if exposed_conflict {
            plan.skipped.push(SkippedCustomModel {
                id: cm.id.clone(),
                backend_id: cm.backend_id.clone(),
                reason: format!("已存在同名 exposedId: {}", exposed_id),
            });
            continue;
        }

        next_sort += 10;

        plan.rows_to_add.push(ModelRow {
            upstream_id: backend_id.to_string(),
            match_kind: MatchKind::Exact,
            exposed_id: exposed_id.clone(),
            display_name: cm.display_name.clone().unwrap_or_else(|| cm.id.clone()),
            owned_by: cm.owned_by.clone().unwrap_or_else(|| "custom".to_string()),
            model_type: "chat".to_string(),
            created: now.timestamp(),
            context_window: cm.context_window.unwrap_or(200_000),
            max_output_tokens: cm.max_tokens.unwrap_or(64_000),
            expose_thinking_variant: false,
            enabled: true,
            listed: true,
            status: ModelStatus::Active,
            origin: ModelOrigin::Manual,
            sort_order: next_sort,
            pinned: Vec::new(),
            missing_sync_rounds: 0,
            last_seen_at: None,
            match_substrings: Vec::new(),
            // 存不读：只是把旧配置里的值原样搬进新行，是否据此放行
            // additionalModelRequestFields 是下一个任务的事。
            supports_reasoning: cm.supports_reasoning,
        });
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 26, 0, 0, 0).unwrap()
    }

    fn custom(id: &str, backend_id: &str) -> CustomModel {
        CustomModel {
            id: id.to_string(),
            backend_id: backend_id.to_string(),
            display_name: None,
            context_window: None,
            max_tokens: None,
            supports_reasoning: None,
            owned_by: None,
        }
    }

    fn builtin_row(upstream: &str, exposed: &str) -> ModelRow {
        crate::anthropic::model_registry::builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .map(|mut r| {
                r.upstream_id = upstream.to_string();
                r.exposed_id = exposed.to_string();
                r
            })
            .unwrap()
    }

    #[test]
    fn first_run_imports_all_non_conflicting_entries() {
        let customs = vec![custom("my-opus", "claude-opus-9000")];
        let plan = plan_import(&customs, &[], now());

        assert_eq!(plan.rows_to_add.len(), 1);
        assert!(plan.skipped.is_empty());
        let row = &plan.rows_to_add[0];
        assert_eq!(row.upstream_id, "claude-opus-9000");
        assert_eq!(row.exposed_id, "my-opus");
        assert_eq!(row.origin, ModelOrigin::Manual);
        assert_eq!(row.owned_by, "custom");
        assert_eq!(row.context_window, 200_000);
        assert_eq!(row.max_output_tokens, 64_000);
    }

    /// 幂等性：第二次启动时，effective_rows 已经包含上一轮导入产生的行——
    /// 同样的 customModels 输入这次必须全部落进 skipped，rows_to_add 为空。
    #[test]
    fn second_run_is_idempotent_and_skips_everything() {
        let customs = vec![custom("my-opus", "claude-opus-9000")];
        let first = plan_import(&customs, &[], now());
        assert_eq!(first.rows_to_add.len(), 1);

        // 模拟「已写入 models.json 并重新装载」：有效行集里现在有这一行了。
        let effective_after_first_import = first.rows_to_add.clone();
        let second = plan_import(&customs, &effective_after_first_import, now());

        assert!(second.rows_to_add.is_empty(), "第二次不应产生任何新行");
        assert_eq!(second.skipped.len(), 1);
        assert!(second.skipped[0].reason.contains("upstreamId"));
    }

    #[test]
    fn skips_when_upstream_id_already_present_in_registry() {
        let customs = vec![custom("dup-alias", "claude-opus-4.8")];
        let effective = vec![builtin_row("claude-opus-4.8", "claude-opus-4-8")];
        let plan = plan_import(&customs, &effective, now());

        assert!(plan.rows_to_add.is_empty());
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].id, "dup-alias");
    }

    #[test]
    fn skips_when_exposed_id_already_present_in_registry_case_insensitive() {
        let customs = vec![custom("Claude-Opus-4-8", "claude-opus-custom-backend")];
        let effective = vec![builtin_row("claude-opus-4.8", "claude-opus-4-8")];
        let plan = plan_import(&customs, &effective, now());

        assert!(plan.rows_to_add.is_empty(), "exposedId 冲突不应覆盖注册表已有行");
        assert_eq!(plan.skipped.len(), 1);
        assert!(plan.skipped[0].reason.contains("exposedId"));
    }

    #[test]
    fn does_not_overwrite_conflicting_row_fields() {
        // 注册表里已经有一行占了这个 upstreamId，且被人工改过 displayName；
        // 冲突的 customModels 条目必须被跳过，不能覆盖这行的任何字段。
        let mut effective_row = builtin_row("claude-opus-4.8", "claude-opus-4-8");
        effective_row.display_name = "人工改过的名字".to_string();
        let mut custom_conflicting = custom("whatever", "claude-opus-4.8");
        custom_conflicting.display_name = Some("customModels 里的名字".to_string());

        let plan = plan_import(&[custom_conflicting], &[effective_row.clone()], now());

        assert!(plan.rows_to_add.is_empty());
        assert_eq!(effective_row.display_name, "人工改过的名字", "有效行本身不应被本函数改动");
    }

    #[test]
    fn multiple_new_entries_get_increasing_sort_order_and_preserve_order() {
        let customs = vec![custom("a", "backend-a"), custom("b", "backend-b")];
        let plan = plan_import(&customs, &[], now());

        assert_eq!(plan.rows_to_add.len(), 2);
        assert!(plan.rows_to_add[0].sort_order < plan.rows_to_add[1].sort_order);
    }

    #[test]
    fn supports_reasoning_is_carried_over_but_only_stored() {
        let mut cm = custom("r", "backend-r");
        cm.supports_reasoning = Some(true);
        let plan = plan_import(&[cm], &[], now());

        assert_eq!(plan.rows_to_add[0].supports_reasoning, Some(true));
    }
}

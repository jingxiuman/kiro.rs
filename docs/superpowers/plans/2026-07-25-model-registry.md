# 模型注册表（手动映射 + 上游自动同步）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 kiro.rs 中「有哪些模型 / 映射到哪个上游 id / 输入窗口多大」这三张编译期硬编码表，改造为「编译内置默认 + `models.json` 运行时覆盖 + 可选的上游自动同步」，使上游上线新模型（如 `claude-opus-5`）后无需改代码发版即可使用。

**Architecture:** 新增 `ModelRegistry`（纯数据 + 纯方法，无 I/O 无时间概念）承载解析逻辑，由进程级 `RwLock<Arc<ModelRegistry>>` 持有当前实例；`ModelRegistryStore` 负责 `models.json` 的校验/合并/原子落盘；`ModelSyncService` 经 `ModelListFetcher` trait 拉取上游模型并算 diff。请求入口解析一次模型与窗口，结果随 `ConversionResult` 向下传递，避免热重载导致「用旧表映射、用新表计量」。

**Tech Stack:** Rust 2024 edition、axum 0.8、tokio、serde/serde_json、parking_lot（已有依赖）、chrono（已有依赖）、rusqlite（不使用）；前端 React + TypeScript + Vite + Tailwind + shadcn/ui（`admin-ui`）。

**Spec:** `docs/superpowers/specs/2026-07-25-model-registry-design.md`（v2）。本计划与该 spec 一一对应；spec 的 §13 记录了 v1→v2 的 28 项修订原因，实现时如遇疑问先读 spec。

## 执行期修订（Task 2 评审后追加）

代码评审在 Task 2 抓出两处本计划遗漏的行为回归，spec 已相应修订（见 spec §4.3 `matchSubstrings` 与 §5.3）。**后续任务以下述为准**：

1. **`ModelRow` 多一个字段** `match_substrings: Vec<String>`（`#[serde(default)]`）。内置默认只给三行填值：`claude-fable-5` → `["fable"]`、`claude-haiku-4.5` → `["haiku"]`、`claude-sonnet-5` → `["sonnet-5","sonnet5","sonnet.5"]`；其余全部为空。这是为了复现旧 `map_model` 的「家族通吃」语义 —— 旧代码 `contains("haiku")` / `contains("fable")` 不看版本号就映射，且 sonnet 5 代接受三种拼法。**不得给 sonnet/opus 4.x 行填家族关键字**，否则 `claude-3-5-sonnet` 会被误判（旧行为是 `None`）。
   > 漏掉它会让 `converter.rs:1855`（`claude-sonnet.5`）与 `converter.rs:1873`（`claude-haiku-4-20250514`）两个现存测试在 Task 6 之后必然失败。

2. **`resolve()` 顺序为 8 步**：alias → `exposed_id` 精确 → `{exposed_id}-thinking`(变体开启) → 规范化匹配 `upstream_id` → **`match_substrings` 命中** → prefix 最长前缀 → passthrough → Unknown。

3. **「thinking 变体被关闭」的拒绝延后生效**：第 3/4 步命中但变体关闭时记下待定拒绝并继续往下，只有第 5、6 步都没命中才返回 `Unknown`。`Rejected(Disabled)`（`enabled == false`）不受延后影响，命中即返回。
   > 否则 `gpt-5.6-sol-thinking` 会被 gpt 精确行拦掉，走不到 prefix 透传，而旧代码对任何 `gpt-5` 开头的名字一律原样透传（`converter.rs:234`，不剥 `-thinking`）。

4. **环境事实**：本仓库是 bin-only crate，无 lib target。计划各任务里写的 `cargo test --lib` 一律改用 `cargo test --bin kiro-rs`。全量基线：改造前 523 passed。

## Global Constraints

- **不引入任何新 crate。** 仅使用现有依赖：`parking_lot`、`chrono`、`serde`、`serde_json`、`tokio`、`anyhow`、`thiserror`、`tracing`。
- **零行为回归是硬性验收条件。** 不存在 `models.json` 且 `modelSyncEnabled=false` 时，`/v1/models` 输出与改造前逐字节一致，`cargo test` 全绿。任何一步破坏此条即为该步失败。
- **`modelSyncEnabled` 默认 `false`。** 不存在「启动后自动同步」。
- **错误文案按路径保持各自现有语言**：`src/anthropic/handlers.rs` 路径为中文（`模型不支持: {model}`），`src/anthropic/websearch_loop.rs` 路径为英文（`unsupported model: {model}`）。新增的 Disabled 文案同理：中文 `模型已禁用: {model}`，英文 `model disabled: {model}`。
- **`contextWindow`（输入上下文窗口）与 `maxOutputTokens`（输出上限，即 `Model.max_tokens`）是两个不同的量**，数值可差一个数量级（`gpt-5.6-sol`：272000 vs 64000）。任何一处混用即为 bug。
- **`origin: "builtin"` 的行永不可删**（同步与 API 都不可）。
- **代码注释与文档用中文**，与仓库现有风格一致（`src/anthropic/converter.rs` 等均为中文注释）。
- **每个任务结束后运行 `cargo build` 与 `cargo test`**，两者必须通过才能 commit。前端任务运行 `cd admin-ui && bun run build`。
- **commit message 用中文正文 + 英文 conventional prefix**，与现有历史一致（如 `feat(responses): ...`、`docs: ...`）。

---

## 文件结构

### 新增

| 文件 | 职责 |
|---|---|
| `src/anthropic/model_registry.rs` | `ModelRow`/`ModelAlias`/`ModelRegistryFile` 类型、`BUILTIN_DEFAULTS`、`ModelRegistry`（`resolve` / `exposed_models`）、规范化函数、加载校验。**无 I/O、无时间概念。** |
| `src/anthropic/model_registry_store.rs` | `models.json` 读写：反序列化 + 校验 + 原子落盘 + pinned 逐字段合并 + 乱序丢弃。持有 `tokio::Mutex`。 |
| `src/anthropic/model_sync.rs` | `ModelListFetcher` trait、`ModelSyncService`（选凭据、权威/非权威轮次、diff、deprecated、并集冲突、`credentialSupport` 记录）。 |
| `admin-ui/src/api/models.ts` | 模型表 admin API 客户端。 |
| `admin-ui/src/hooks/use-model-registry.ts` | React Query hooks。 |
| `admin-ui/src/components/model-mapping-dialog.tsx` | 三 tab 全局配置弹窗。 |

### 修改

| 文件 | 改动 |
|---|---|
| `src/anthropic/mod.rs` | 注册三个新模块。 |
| `src/anthropic/converter.rs` | `map_model` / `get_context_window_size` 改为查 registry（签名不变）；`ConversionResult` 增 `context_window`；`ConversionError` 增 `ModelDisabled`；`convert_request_with_mode` 改调 `resolve`。 |
| `src/anthropic/handlers.rs` | `available_models()` 改为查 registry；`:697`、`:1488` 错误 match 增分支；`:1155` 改用传递来的窗口。 |
| `src/anthropic/stream.rs` | `:1525` 改用传递来的窗口。 |
| `src/anthropic/websearch_loop.rs` | `:200` 改用传递来的窗口；`:267` 错误 match 增分支。 |
| `src/model/config.rs` | `Config` 增 4 个字段。 |
| `src/admin/service.rs` | `ModelSyncRuntimeConfig` holder；config 写 mutex；模型表 CRUD 方法。 |
| `src/admin/error.rs` | 增 3 个错误变体。 |
| `src/admin/router.rs` / `handlers.rs` / `types.rs` | 模型表端点。 |
| `src/kiro/token_manager.rs` | `ModelListFetcher` 实现；`credential_matches_request` 增 `credentialSupport` 过滤。 |
| `src/main.rs` | registry 初始化 + 同步调度器（在 admin 分支**之外**）。 |
| `admin-ui/src/components/topbar-tools.tsx` | 「模型映射」按钮。 |
| `admin-ui/src/components/available-models-dialog.tsx` | 收录状态标记 + 「加入映射表」。 |
| `admin-ui/src/types/index.ts` | 新类型。 |
| `config.example.json` / `README.md` | 4 个新配置项文档。 |

---

## Task 1: `ModelRegistry` 核心类型与内置默认

**Files:**
- Create: `src/anthropic/model_registry.rs`
- Modify: `src/anthropic/mod.rs`

**Interfaces:**
- Consumes: `crate::anthropic::types::Model`（`src/anthropic/types.rs:43`）
- Produces:
  ```rust
  pub enum MatchKind { Exact, Prefix }
  pub enum ModelStatus { Active, Deprecated }
  pub enum ModelOrigin { Builtin, Synced, Manual }
  pub struct ModelRow { /* 见 Step 3 */ }
  pub struct ModelAlias { pub from: String, pub to: String }
  pub struct ModelRegistry { /* 私有 */ }
  impl ModelRegistry {
      pub fn builtin() -> Self;
      pub fn rows(&self) -> &[ModelRow];
  }
  pub fn builtin_rows() -> Vec<ModelRow>;
  ```

- [ ] **Step 1: 写失败测试 —— 内置默认必须覆盖当前全部 23 个对外模型**

在 `src/anthropic/model_registry.rs` 末尾创建测试模块：

```rust
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_registry:: 2>&1 | tail -20`
Expected: 编译失败，`cannot find type ModelRegistry` / `unresolved module`。

- [ ] **Step 3: 写类型定义与内置默认**

在 `src/anthropic/model_registry.rs` 顶部（测试模块之前）写入：

```rust
//! 模型注册表：把「有哪些模型 / 映射到哪个上游 id / 输入窗口多大」从编译期
//! 硬编码改为「内置默认 ⊕ models.json 覆盖」。
//!
//! **本模块不含任何 I/O、不含任何时间概念。** 时间语义（deprecated 宽限、
//! lastSeenAt）属于 model_sync；文件读写属于 model_registry_store。
//! 这样模型解析逻辑完全确定性，可直接复用 converter 现有测试作为回归基线。

use serde::{Deserialize, Serialize};

use super::types::Model;

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
```

在 `src/anthropic/mod.rs` 中加入模块声明（与现有 `pub mod converter;` 等同级）：

```rust
pub mod model_registry;
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib model_registry:: 2>&1 | tail -10`
Expected: `test result: ok. 2 passed`

注意 `Model` 目前未被本模块使用会产生 `unused import` 警告 —— Task 2 会用到，此处先删掉 `use super::types::Model;` 这一行，Task 2 再加回。

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_registry.rs src/anthropic/mod.rs
git commit -m "feat(model-registry): 定义注册表类型与内置默认表

把 available_models / map_model / get_context_window_size 三处硬编码
合并为 builtin_rows()，一行一个上游模型，-thinking 由 expose_thinking_variant
派生。gpt-5 通配以 matchKind=prefix + listed=false 表达，仅参与解析、
不出现在 /v1/models。"
```

---

## Task 2: 模型解析（`resolve`）与规范化

**Files:**
- Modify: `src/anthropic/model_registry.rs`

**Interfaces:**
- Consumes: Task 1 的 `ModelRegistry`、`ModelRow`、`MatchKind`
- Produces:
  ```rust
  pub enum RejectReason { Unknown, Disabled }
  pub enum Resolution {
      Mapped { upstream_id: String, context_window: i32 },
      Passthrough { upstream_id: String, context_window: i32 },
      Rejected(RejectReason),
  }
  impl ModelRegistry {
      pub fn resolve(&self, requested: &str, allow_passthrough: bool) -> Resolution;
  }
  pub fn normalize_model_name(requested: &str) -> String;
  ```

- [ ] **Step 1: 写失败测试 —— 回归基线 + 规范化顺序 + prefix + thinking 规则**

追加到 `src/anthropic/model_registry.rs` 的 `mod tests` 内：

```rust
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_registry:: 2>&1 | tail -20`
Expected: 编译失败，`no method named resolve` / `cannot find function normalize_model_name` / `no method named rows_mut`。

- [ ] **Step 3: 实现规范化与 resolve**

在 `src/anthropic/model_registry.rs` 的 `impl ModelRegistry` 之前加入：

```rust
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
```

在 `impl ModelRegistry` 内追加：

```rust
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

    /// 解析请求中的模型名。顺序见 spec §5.3。
    pub fn resolve(&self, requested: &str, allow_passthrough: bool) -> Resolution {
        let lower = requested.trim().to_ascii_lowercase();

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
                    // 该行关闭了 thinking 变体 → 视为不存在这个模型名
                    return Resolution::Rejected(RejectReason::Unknown);
                }
                return Self::hit(row, row.upstream_id.clone());
            }
        }

        // 4. 规范化后匹配 upstream_id
        let normalized = normalize_model_name(&lower);
        if let Some(row) = self
            .rows
            .iter()
            .find(|r| r.match_kind == MatchKind::Exact && r.upstream_id == normalized)
        {
            // 请求名带 thinking 但该行关闭了变体 → 拒绝
            if lower.ends_with("-thinking") && !row.expose_thinking_variant {
                return Resolution::Rejected(RejectReason::Unknown);
            }
            return Self::hit(row, row.upstream_id.clone());
        }

        // 5. prefix 行，最长前缀优先；上游 id = 小写请求名原样
        if let Some(row) = self
            .rows
            .iter()
            .filter(|r| r.match_kind == MatchKind::Prefix && lower.starts_with(&r.exposed_id))
            .max_by_key(|r| r.exposed_id.len())
        {
            return Self::hit(row, lower.clone());
        }

        // 6. 未收录透传
        if allow_passthrough {
            return Resolution::Passthrough {
                upstream_id: normalized,
                context_window: PASSTHROUGH_CONTEXT_WINDOW,
            };
        }

        Resolution::Rejected(RejectReason::Unknown)
    }
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib model_registry:: 2>&1 | tail -15`
Expected: `test result: ok. 9 passed`

如 `normalize_strips_thinking_before_date` 的 `claude-3-5-sonnet` 断言失败，检查 Step 3 中版本段转换的 `rsplitn(3, '-')` 判定 —— `claude-3-5-sonnet` 的末段是 `sonnet`（非纯数字），应当不触发转换。

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_registry.rs
git commit -m "feat(model-registry): 实现模型解析与规范化

解析顺序：alias → exposedId → exposedId-thinking → 规范化后匹配
upstreamId → prefix 行 → 透传 → 拒绝。规范化顺序为 thinking→日期→版本段，
修复 claude-sonnet-5-20260101-thinking 日期残留。
区分 Unknown 与 Disabled 两种拒绝原因。
迁移 converter 现有 map_model / get_context_window_size 全部用例为回归基线。"
```

---

## Task 3: `/v1/models` 列表构造（`exposed_models`）

**Files:**
- Modify: `src/anthropic/model_registry.rs`

**Interfaces:**
- Consumes: Task 1 的 `ModelRow`、`crate::anthropic::types::Model`
- Produces: `impl ModelRegistry { pub fn exposed_models(&self) -> Vec<Model>; }`

- [ ] **Step 1: 写失败测试**

追加到 `mod tests`：

```rust
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_registry:: 2>&1 | tail -15`
Expected: `no method named exposed_models`。

- [ ] **Step 3: 实现**

把 Task 1 Step 4 删掉的 import 加回 `src/anthropic/model_registry.rs` 顶部：

```rust
use super::types::Model;
```

在 `impl ModelRegistry` 内追加：

```rust
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
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib model_registry:: 2>&1 | tail -15`
Expected: `test result: ok. 12 passed`

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_registry.rs
git commit -m "feat(model-registry): 构造 /v1/models 列表

Model.max_tokens 取 maxOutputTokens 而非 contextWindow（二者语义不同，
gpt-5.6-sol 为 64000 vs 272000）。可见性：listed=false 与 enabled=false
不出现，deprecated 仍保留。断言列表内容与顺序与改造前逐字段一致。"
```

---

## Task 4: `models.json` 加载校验

**Files:**
- Modify: `src/anthropic/model_registry.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct ModelRegistryFile {
      pub version: u32,
      pub sync_state: SyncState,
      pub models: Vec<ModelRow>,
      pub aliases: Vec<ModelAlias>,
      pub credential_support: HashMap<String, Vec<String>>,
  }
  pub struct SyncState {
      pub last_sync_at: Option<String>,
      pub last_fetch_started_at: Option<String>,
      pub source: Option<String>,
  }
  pub const REGISTRY_SCHEMA_VERSION: u32 = 1;
  impl ModelRegistry {
      pub fn from_file(file: ModelRegistryFile) -> Result<Self, String>;
  }
  ```

- [ ] **Step 1: 写失败测试**

追加到 `mod tests`：

```rust
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
        assert!(!registry.rows()[0].listed);
    }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_registry:: 2>&1 | tail -20`
Expected: `cannot find type ModelRegistryFile` / `no function from_file`。

- [ ] **Step 3: 实现**

在 `src/anthropic/model_registry.rs` 顶部 import 处加 `use std::collections::{HashMap, HashSet};`，然后在 `ModelRegistry` 定义之后加入：

```rust
/// `models.json` 的 schema 版本。加载时必须精确等于此值。
/// 不做自动迁移：未来版本靠「只增字段」保持前向兼容。
pub const REGISTRY_SCHEMA_VERSION: u32 = 1;

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

impl ModelRegistry {
    /// 从覆盖层文件构造。任一校验失败即整体拒绝（调用方退回内置默认 + degraded）。
    ///
    /// 校验顺序见 spec §4.5。唯一性校验不可省：重复 exposedId、alias 与
    /// exposedId 撞名都会让解析结果依赖遍历顺序。
    pub fn from_file(file: ModelRegistryFile) -> Result<Self, String> {
        if file.version != REGISTRY_SCHEMA_VERSION {
            return Err(format!(
                "不支持的 models.json schema 版本: {}（期望 {}）",
                file.version, REGISTRY_SCHEMA_VERSION
            ));
        }

        let mut rows = file.models;

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
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib model_registry:: 2>&1 | tail -15`
Expected: `test result: ok. 19 passed`

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_registry.rs
git commit -m "feat(model-registry): models.json 加载校验

version 必须精确为 1（不做自动迁移）；校验 upstreamId 唯一、全部对外名
（含派生 thinking 名与 alias.from）唯一、alias 不 dangling 不递归、
窗口为正数、prefix 行不得开 thinking 变体。prefix 行的 listed 强制为 false。
任一失败即整体拒绝，由调用方退回内置默认。"
```

---

## Task 5: `ModelRegistryStore`（读写、pinned 合并、原子落盘、乱序丢弃）

**Files:**
- Create: `src/anthropic/model_registry_store.rs`
- Modify: `src/anthropic/mod.rs`

**Interfaces:**
- Consumes: Task 1/4 的 `ModelRegistry`、`ModelRegistryFile`、`ModelRow`、`REGISTRY_SCHEMA_VERSION`
- Produces:
  ```rust
  pub struct LoadOutcome { pub registry: ModelRegistry, pub file: ModelRegistryFile, pub degraded_reason: Option<String> }
  pub struct ModelRegistryStore { /* 私有 */ }
  impl ModelRegistryStore {
      pub fn new(path: PathBuf) -> Self;
      pub fn load(&self) -> LoadOutcome;
      pub async fn mutate<F>(&self, f: F) -> Result<ModelRegistryFile, String>
          where F: FnOnce(&mut ModelRegistryFile) -> Result<(), String>;
  }
  pub fn merge_synced_row(existing: &mut ModelRow, incoming: &ModelRow);
  ```

- [ ] **Step 1: 写失败测试**

在 `src/anthropic/model_registry_store.rs` 末尾创建：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::model_registry::{builtin_rows, ModelOrigin};

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("kiro-models-test-{}-{}.json", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn row(upstream: &str) -> ModelRow {
        let mut r = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        r.upstream_id = upstream.to_string();
        r.exposed_id = upstream.replace('.', "-");
        r.origin = ModelOrigin::Synced;
        r
    }

    #[test]
    fn load_missing_file_returns_builtin_without_degraded() {
        let store = ModelRegistryStore::new(tmp_path("missing"));
        let out = store.load();
        assert!(out.degraded_reason.is_none(), "文件不存在不是降级状态");
        assert_eq!(out.registry.rows().len(), builtin_rows().len());
    }

    #[test]
    fn load_corrupt_file_returns_builtin_with_degraded() {
        let path = tmp_path("corrupt");
        std::fs::write(&path, b"{ not json").unwrap();
        let out = ModelRegistryStore::new(path).load();
        assert!(out.degraded_reason.is_some(), "损坏文件必须置 degraded");
        assert_eq!(out.registry.rows().len(), builtin_rows().len());
    }

    #[test]
    fn load_invalid_schema_returns_builtin_with_degraded() {
        let path = tmp_path("badversion");
        std::fs::write(&path, br#"{"version":2,"models":[]}"#).unwrap();
        let out = ModelRegistryStore::new(path).load();
        assert!(out.degraded_reason.unwrap().contains("版本"));
    }

    #[tokio::test]
    async fn mutate_writes_atomically_and_reloads() {
        let path = tmp_path("write");
        let store = ModelRegistryStore::new(path.clone());
        store
            .mutate(|f| {
                f.models.push(row("claude-opus-5"));
                Ok(())
            })
            .await
            .unwrap();
        assert!(path.exists());
        let out = store.load();
        assert!(out.degraded_reason.is_none());
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-opus-5"));
        // 不应留下临时文件
        let tmp = path.with_extension("json.tmp");
        assert!(!tmp.exists(), "临时文件未清理");
    }

    #[tokio::test]
    async fn mutate_rejects_invalid_result_and_keeps_old_file() {
        let path = tmp_path("reject");
        let store = ModelRegistryStore::new(path.clone());
        store.mutate(|f| { f.models.push(row("claude-a")); Ok(()) }).await.unwrap();
        let before = std::fs::read(&path).unwrap();

        // 写入重复 upstreamId → 校验失败 → 文件不变
        let err = store
            .mutate(|f| { f.models.push(row("claude-a")); Ok(()) })
            .await
            .unwrap_err();
        assert!(err.contains("重复"));
        assert_eq!(std::fs::read(&path).unwrap(), before, "校验失败时文件不应被改写");
    }

    /// pinned 字段逐字段跳过；未 pinned 字段正常更新
    #[test]
    fn merge_respects_pinned_fields() {
        let mut existing = row("claude-opus-4.8");
        existing.context_window = 800_000;
        existing.display_name = "旧名字".to_string();
        existing.pinned = vec!["contextWindow".to_string()];

        let mut incoming = row("claude-opus-4.8");
        incoming.context_window = 1_000_000;
        incoming.display_name = "Claude Opus 4.8".to_string();

        merge_synced_row(&mut existing, &incoming);

        assert_eq!(existing.context_window, 800_000, "pinned 字段被覆盖了");
        assert_eq!(existing.display_name, "Claude Opus 4.8", "未 pinned 字段应更新");
    }

    /// deprecated 行在上游重新出现时复活
    #[test]
    fn merge_revives_deprecated_row() {
        use crate::anthropic::model_registry::ModelStatus;
        let mut existing = row("claude-opus-4.8");
        existing.status = ModelStatus::Deprecated;
        existing.missing_sync_rounds = 2;
        let incoming = row("claude-opus-4.8");

        merge_synced_row(&mut existing, &incoming);

        assert_eq!(existing.status, ModelStatus::Active);
        assert_eq!(existing.missing_sync_rounds, 0);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_registry_store:: 2>&1 | tail -20`
Expected: 编译失败，`cannot find type ModelRegistryStore`。

- [ ] **Step 3: 实现**

`src/anthropic/model_registry_store.rs` 顶部写入：

```rust
//! `models.json` 的持久化层：加载校验、pinned 逐字段合并、原子落盘。
//!
//! 所有写路径（定时同步、手动同步、UI 逐字段编辑）都经本模块的 Mutex 串行化，
//! 每次写都是 read-modify-write。这正是把模型表从 config.json 中独立出来的
//! 原因——后者只能依赖「读最新再写」的约定（src/admin/service.rs:1236），没有锁。

use std::path::PathBuf;

use tokio::sync::Mutex;

use super::model_registry::{
    ModelRegistry, ModelRegistryFile, ModelRow, ModelStatus, REGISTRY_SCHEMA_VERSION,
};

/// 加载结果。`degraded_reason` 非 None 表示覆盖层不可用、已退回内置默认。
pub struct LoadOutcome {
    pub registry: ModelRegistry,
    pub file: ModelRegistryFile,
    pub degraded_reason: Option<String>,
}

pub struct ModelRegistryStore {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl ModelRegistryStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path, write_lock: Mutex::new(()) }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// 加载覆盖层。任何失败都退回内置默认，绝不 panic、绝不空表。
    pub fn load(&self) -> LoadOutcome {
        let builtin = || LoadOutcome {
            registry: ModelRegistry::builtin(),
            file: ModelRegistryFile::default(),
            degraded_reason: None,
        };

        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // 文件不存在是正常状态（零行为回归路径），不是降级
                return builtin();
            }
            Err(e) => {
                let reason = format!("读取 models.json 失败: {}", e);
                tracing::error!("{}，退回内置默认模型表", reason);
                let mut out = builtin();
                out.degraded_reason = Some(reason);
                return out;
            }
        };

        let file: ModelRegistryFile = match serde_json::from_str(&raw) {
            Ok(f) => f,
            Err(e) => {
                let reason = format!("解析 models.json 失败: {}", e);
                tracing::error!("{}，退回内置默认模型表", reason);
                let mut out = builtin();
                out.degraded_reason = Some(reason);
                return out;
            }
        };

        let file_for_return = file.clone();
        match ModelRegistry::from_file(file) {
            Ok(registry) => LoadOutcome { registry, file: file_for_return, degraded_reason: None },
            Err(reason) => {
                tracing::error!("models.json 校验失败: {}，退回内置默认模型表", reason);
                let mut out = builtin();
                out.degraded_reason = Some(reason);
                out
            }
        }
    }

    /// read-modify-write。闭包中修改文件内容，返回后立即校验并原子落盘。
    /// 校验失败则不写盘，返回 Err。
    pub async fn mutate<F>(&self, f: F) -> Result<ModelRegistryFile, String>
    where
        F: FnOnce(&mut ModelRegistryFile) -> Result<(), String>,
    {
        let _guard = self.write_lock.lock().await;

        let mut file = match std::fs::read_to_string(&self.path) {
            Ok(raw) => serde_json::from_str::<ModelRegistryFile>(&raw)
                .map_err(|e| format!("解析 models.json 失败: {}", e))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ModelRegistryFile::default(),
            Err(e) => return Err(format!("读取 models.json 失败: {}", e)),
        };
        file.version = REGISTRY_SCHEMA_VERSION;

        f(&mut file)?;

        // 落盘前必须先过校验，避免写出一个自己都加载不了的文件
        ModelRegistry::from_file(file.clone())?;

        let json = serde_json::to_string_pretty(&file)
            .map_err(|e| format!("序列化 models.json 失败: {}", e))?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("创建 models.json 所在目录失败: {}", e))?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes())
            .map_err(|e| format!("写入临时文件失败: {}", e))?;
        std::fs::rename(&tmp, &self.path)
            .map_err(|e| format!("原子替换 models.json 失败: {}", e))?;

        Ok(file)
    }
}

/// 把同步得到的 `incoming` 合并进已有行 `existing`，**逐字段跳过 pinned**。
///
/// 字段名用 camelCase，与 JSON 中的 `pinned` 数组一致。
pub fn merge_synced_row(existing: &mut ModelRow, incoming: &ModelRow) {
    let pinned = |name: &str| existing.pinned.iter().any(|p| p == name);

    if !pinned("displayName") {
        existing.display_name = incoming.display_name.clone();
    }
    if !pinned("contextWindow") {
        existing.context_window = incoming.context_window;
    }
    if !pinned("maxOutputTokens") {
        existing.max_output_tokens = incoming.max_output_tokens;
    }
    if !pinned("exposedId") {
        existing.exposed_id = incoming.exposed_id.clone();
    }
    if !pinned("exposeThinkingVariant") {
        existing.expose_thinking_variant = incoming.expose_thinking_variant;
    }

    // 同步元数据不受 pinned 影响
    existing.missing_sync_rounds = 0;
    existing.last_seen_at = incoming.last_seen_at.clone();
    if existing.status == ModelStatus::Deprecated {
        // 上游重新出现 → 复活
        existing.status = ModelStatus::Active;
    }
}
```

在 `src/anthropic/mod.rs` 加入：

```rust
pub mod model_registry_store;
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib model_registry_store:: 2>&1 | tail -15`
Expected: `test result: ok. 7 passed`

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_registry_store.rs src/anthropic/mod.rs
git commit -m "feat(model-registry): models.json 持久化层

load 对「文件不存在」不置 degraded（零行为回归路径），对损坏/校验失败置
degraded 并退回内置默认。mutate 为 Mutex 串行化的 read-modify-write，
落盘前先过校验（避免写出自己都加载不了的文件），再 tmp+rename 原子替换。
merge_synced_row 逐字段跳过 pinned，并在上游重新出现时复活 deprecated 行。"
```

---

## Task 6: 全局 `REGISTRY` + `map_model` / `get_context_window_size` / `available_models` 改为查表

**Files:**
- Modify: `src/anthropic/model_registry.rs`（加全局 holder）
- Modify: `src/anthropic/converter.rs:199-266`
- Modify: `src/anthropic/handlers.rs:385`

**Interfaces:**
- Produces:
  ```rust
  // model_registry.rs
  pub fn current_registry() -> Arc<ModelRegistry>;
  pub fn install_registry(registry: ModelRegistry);
  pub fn set_allow_passthrough(allow: bool);
  pub fn allow_passthrough() -> bool;
  ```

- [ ] **Step 1: 写失败测试 —— 既有 converter 测试必须全绿，且列表来自 registry**

追加到 `src/anthropic/model_registry.rs` 的 `mod tests`：

```rust
    #[test]
    fn install_and_read_global_registry() {
        // 注意：全局状态测试。用一个内置默认之外的行验证 swap 生效。
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
```

在 `src/anthropic/converter.rs` 的 `mod tests` 中追加：

```rust
    /// 改造后 map_model 必须继续通过全部既有用例（查 registry 而非硬编码）
    #[test]
    fn map_model_still_matches_registry() {
        assert_eq!(map_model("claude-opus-4-8"), Some("claude-opus-4.8".to_string()));
        assert_eq!(map_model("gpt-5.9-nova"), Some("gpt-5.9-nova".to_string()));
        assert_eq!(map_model("gpt-4"), None);
        assert_eq!(get_context_window_size("claude-opus-4-8"), 1_000_000);
        assert_eq!(get_context_window_size("gpt-5.6-sol"), 272_000);
        assert_eq!(get_context_window_size("claude-haiku-4-5-20251001"), 200_000);
        assert_eq!(get_context_window_size("完全未知的模型"), 200_000);
    }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib 2>&1 | tail -20`
Expected: `cannot find function install_registry` / `current_registry`。

- [ ] **Step 3: 实现全局 holder 并改写三个函数**

在 `src/anthropic/model_registry.rs` 末尾（`mod tests` 之前）加入：

```rust
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
```

在 `src/anthropic/converter.rs` 中，把 `map_model`（原 199-240 行）与 `get_context_window_size`（原 246-266 行）**整体替换**为：

```rust
/// 模型映射：将 Anthropic 模型名映射到 Kiro 模型 ID。
///
/// 改造后查 `ModelRegistry`（内置默认 ⊕ models.json 覆盖），不再硬编码。
/// 签名保持不变，既有调用点与测试无需改动。
/// 注意：无法表达「命中但被禁用」——需要区分时用
/// `model_registry::current_registry().resolve(...)`。
pub fn map_model(model: &str) -> Option<String> {
    use super::model_registry::{allow_passthrough, current_registry, Resolution};
    match current_registry().resolve(model, allow_passthrough()) {
        Resolution::Mapped { upstream_id, .. } | Resolution::Passthrough { upstream_id, .. } => {
            Some(upstream_id)
        }
        Resolution::Rejected(_) => None,
    }
}

/// 根据模型名称返回输入上下文窗口大小。
///
/// 改造后查 `ModelRegistry`。**这是输入窗口，与 `/v1/models` 的
/// `max_tokens`（输出上限）是两个不同的量。**
///
/// 保留此函数是为了兼容既有测试；请求主链路应使用
/// `ConversionResult.context_window`（单请求内只取一次快照，见 spec §3.3）。
pub fn get_context_window_size(model: &str) -> i32 {
    use super::model_registry::{allow_passthrough, current_registry, Resolution};
    match current_registry().resolve(model, allow_passthrough()) {
        Resolution::Mapped { context_window, .. }
        | Resolution::Passthrough { context_window, .. } => context_window,
        Resolution::Rejected(_) => 200_000,
    }
}
```

在 `src/anthropic/handlers.rs` 中把 `available_models()`（原 385-579 行的整个函数体）替换为：

```rust
/// 可用模型列表。改造后查 `ModelRegistry`，不再硬编码。
fn available_models() -> Vec<Model> {
    crate::anthropic::model_registry::current_registry().exposed_models()
}
```

- [ ] **Step 4: 运行全量测试确认通过**

Run: `cargo test 2>&1 | tail -25`
Expected: 全部通过。特别确认 `converter.rs` 中原有的 `test_map_model_*`、`available_models_include_*` 系列测试全绿 —— 它们是零行为回归的直接证据。

若 `available_models_include_4_8_variants` 之类失败，比对 Task 1 `builtin_rows()` 的 `exposed_id` 与 `sort_order` 是否与原 `available_models()` 逐项一致。

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_registry.rs src/anthropic/converter.rs src/anthropic/handlers.rs
git commit -m "refactor(model-registry): 三个硬编码表改为查注册表

map_model / get_context_window_size / available_models 签名不变，内部改查
进程级 ModelRegistry。全局 holder 只是「当前实例」，逻辑本体 ModelRegistry
可脱离全局单测。既有 converter 测试全绿即零行为回归的直接证据。"
```

---

## Task 7: `ModelDisabled` 错误与窗口随请求传递

**Files:**
- Modify: `src/anthropic/converter.rs`（`ConversionResult`、`ConversionError`、`convert_request_with_mode`）
- Modify: `src/anthropic/handlers.rs:697`、`:1155`、`:1488`
- Modify: `src/anthropic/stream.rs:1525`
- Modify: `src/anthropic/websearch_loop.rs:200`、`:267`

**Interfaces:**
- Consumes: Task 6 的 `current_registry()`、`Resolution`
- Produces:
  ```rust
  // converter.rs
  pub enum ConversionError { UnsupportedModel(String), ModelDisabled(String), EmptyMessages, UnsupportedToolMapping(String) }
  pub struct ConversionResult { /* ...既有字段..., */ pub context_window: i32 }
  ```

- [ ] **Step 1: 写失败测试**

在 `src/anthropic/converter.rs` 的 `mod tests` 中追加：

```rust
    use crate::anthropic::model_registry::{install_registry, ModelRegistry};

    fn minimal_request(model: &str) -> MessagesRequest {
        let mut req: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": model,
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap();
        req.model = model.to_string();
        req
    }

    /// 被禁用的模型必须报 ModelDisabled，而不是 UnsupportedModel
    #[test]
    fn disabled_model_yields_model_disabled_error() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-opus-4-6" {
                row.enabled = false;
            }
        }
        install_registry(r);

        let err = convert_request(&minimal_request("claude-opus-4-6")).unwrap_err();
        assert!(
            matches!(err, ConversionError::ModelDisabled(ref m) if m == "claude-opus-4-6"),
            "期望 ModelDisabled，实际 {:?}",
            err
        );
        assert_eq!(err.to_string(), "模型已禁用: claude-opus-4-6");

        install_registry(ModelRegistry::builtin());
    }

    /// 转换结果必须携带窗口，供响应处理阶段使用（避免热重载导致映射/计量不一致）
    #[test]
    fn conversion_result_carries_context_window() {
        let result = convert_request(&minimal_request("claude-opus-4-8")).unwrap();
        assert_eq!(result.context_window, 1_000_000);

        let result = convert_request(&minimal_request("claude-haiku-4-5-20251001")).unwrap();
        assert_eq!(result.context_window, 200_000);
    }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib converter::tests::disabled_model 2>&1 | tail -15`
Expected: `no variant named ModelDisabled` / `no field context_window`。

- [ ] **Step 3: 实现**

在 `src/anthropic/converter.rs` 中：

1. `ConversionResult`（原 474 行）末尾加字段：

```rust
    /// 本次请求的输入上下文窗口。**在请求入口解析一次并向下传递**，
    /// 响应处理阶段不再回头查全局注册表——否则一次热重载可能导致
    /// 「用旧表映射、用新表计量」（spec §3.3）。
    pub context_window: i32,
```

2. `ConversionError`（原 490 行）加变体与 Display 分支：

```rust
pub enum ConversionError {
    UnsupportedModel(String),
    /// 模型在表中存在但被人工禁用。与 UnsupportedModel 区分：
    /// 「我配了它但不生效」和「我没配它」是不同的排查方向。
    ModelDisabled(String),
    EmptyMessages,
    UnsupportedToolMapping(String),
}
```

```rust
            ConversionError::ModelDisabled(model) => write!(f, "模型已禁用: {}", model),
```

3. `convert_request_with_mode`（原 595 行）开头替换为：

```rust
    // 1. 解析模型：映射 + 窗口一次取齐
    use super::model_registry::{allow_passthrough, current_registry, RejectReason, Resolution};
    let (model_id, context_window) = match current_registry().resolve(&req.model, allow_passthrough())
    {
        Resolution::Mapped { upstream_id, context_window }
        | Resolution::Passthrough { upstream_id, context_window } => (upstream_id, context_window),
        Resolution::Rejected(RejectReason::Disabled) => {
            return Err(ConversionError::ModelDisabled(req.model.clone()));
        }
        Resolution::Rejected(RejectReason::Unknown) => {
            return Err(ConversionError::UnsupportedModel(req.model.clone()));
        }
    };
```

并在函数末尾构造 `ConversionResult` 处补上 `context_window,`。

4. 在 `src/anthropic/handlers.rs:697` 与 `:1488` 的两处 `match &e` 中，`UnsupportedModel` 分支之后各加：

```rust
                ConversionError::ModelDisabled(model) => {
                    ("invalid_request_error", format!("模型已禁用: {}", model))
                }
```

5. 在 `src/anthropic/websearch_loop.rs:267` 的 `match &e` 中加（**英文**，保持该路径既有语言）：

```rust
                ConversionError::ModelDisabled(m) => {
                    ("invalid_request_error", format!("model disabled: {}", m))
                }
```

6. 三处窗口调用点改为使用传递值：

- `src/anthropic/handlers.rs:1155`：把 `let window_size = get_context_window_size(model);` 改为使用从 `conversion_result.context_window` 传入的参数。为此给该函数（`handlers.rs:1037` 附近的 `use` 所属函数）增加一个 `context_window: i32` 参数，调用方传 `conversion_result.context_window`；删除该文件对 `get_context_window_size` 的 import。
- `src/anthropic/stream.rs:1525`：`StreamState`（或该 `self.model` 所属结构体）增加字段 `context_window: i32`，构造处由调用方传入；`let window_size = get_context_window_size(&self.model);` 改为 `let window_size = self.context_window;`；删除该文件的 import。
- `src/anthropic/websearch_loop.rs:200`：`let window = get_context_window_size(model);` 改为 `let window = conversion.context_window;`（该函数内已有 `conversion` 变量，见 `:263`）；如作用域不可达则同样以参数形式传入。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo build 2>&1 | tail -20 && cargo test 2>&1 | tail -25`
Expected: 编译无错误；全部测试通过。

编译器会逐个指出缺少 `context_window` 字段的 `ConversionResult` 构造点与缺少参数的调用点 —— 按提示补齐即可。

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/converter.rs src/anthropic/handlers.rs src/anthropic/stream.rs src/anthropic/websearch_loop.rs
git commit -m "feat(model-registry): 区分 ModelDisabled 并让窗口随请求传递

新增 ConversionError::ModelDisabled，anthropic 路径报中文「模型已禁用」、
web-search 路径报英文「model disabled」（各路径保持既有语言）。
ConversionResult 增 context_window，请求入口解析一次后传递到
handlers/stream/websearch_loop 三处响应处理点，避免热重载导致
「用旧表映射、用新表计量」。"
```

---

## Task 8: 四个运行时开关与 config 写锁

**Files:**
- Modify: `src/model/config.rs`
- Modify: `src/admin/service.rs`
- Modify: `config.example.json`

**Interfaces:**
- Produces:
  ```rust
  // config.rs：Config 新增字段
  pub model_sync_enabled: bool,          // 默认 false
  pub model_sync_time: String,           // 默认 "04:00"
  pub model_sync_probe_credential_id: Option<u64>,
  pub allow_unknown_model_passthrough: bool, // 默认 false
  // service.rs
  pub struct ModelSyncSettings { pub enabled: bool, pub time: String, pub probe_credential_id: Option<u64>, pub allow_passthrough: bool }
  impl AdminService {
      pub fn model_sync_settings(&self) -> ModelSyncSettings;
      pub async fn set_model_sync_settings(&self, req: SetModelSyncSettingsRequest) -> Result<ModelSyncSettings, AdminServiceError>;
  }
  ```

- [ ] **Step 1: 写失败测试**

在 `src/model/config.rs` 的测试模块（若无则新建 `#[cfg(test)] mod tests`）追加：

```rust
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_sync_config_tests 2>&1 | tail -15`
Expected: `no field model_sync_enabled on type Config`。

- [ ] **Step 3: 实现**

在 `src/model/config.rs` 的 `Config` 结构体末尾（与 `usage_log_retention_days` 等同级）加入：

```rust
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
```

在该文件的默认值函数区加入：

```rust
fn default_model_sync_time() -> String {
    "04:00".to_string()
}
```

在 `src/admin/service.rs` 中，仿照 `RuntimeUpdateConfig`（`:113`）加入运行时 holder，并为 `AdminService` 增加一个 config 写 mutex：

```rust
/// 模型同步的运行时配置。不放进不可变的 Config clone
/// （MultiTokenManager 持有的是 clone，token_manager.rs:1003），
/// 否则 PATCH 无法热生效。
#[derive(Debug, Clone)]
pub struct ModelSyncSettings {
    pub enabled: bool,
    pub time: String,
    pub probe_credential_id: Option<u64>,
    pub allow_passthrough: bool,
}

impl ModelSyncSettings {
    pub fn from_config(config: &Config) -> Self {
        Self {
            enabled: config.model_sync_enabled,
            time: config.model_sync_time.clone(),
            probe_credential_id: config.model_sync_probe_credential_id,
            allow_passthrough: config.allow_unknown_model_passthrough,
        }
    }
}
```

`AdminService` 结构体加两个字段：

```rust
    /// 模型同步运行时配置（可热改）
    model_sync: parking_lot::RwLock<ModelSyncSettings>,
    /// config.json 写锁。既有写路径（service.rs:1236）是无保护的
    /// load-modify-save，本锁顺带修掉这一类丢失更新。
    config_write_lock: tokio::sync::Mutex<()>,
```

并加访问方法（`set_` 内部：先取 `config_write_lock`，再 load-modify-save `config.json`，成功后更新 holder，并在 `allow_passthrough` 变化时调用 `crate::anthropic::model_registry::set_allow_passthrough`）：

```rust
    pub fn model_sync_settings(&self) -> ModelSyncSettings {
        self.model_sync.read().clone()
    }

    pub async fn set_model_sync_settings(
        &self,
        req: SetModelSyncSettingsRequest,
    ) -> Result<ModelSyncSettings, AdminServiceError> {
        // 校验时间格式，复用既有解析器（service.rs:209）
        if let Some(time) = req.time.as_deref() {
            parse_auto_apply_time(time)?;
        }

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
        let mut config = Config::load_from(&self.config_path)
            .map_err(|e| AdminServiceError::InternalError(format!("加载配置失败: {}", e)))?;
        config.model_sync_enabled = next.enabled;
        config.model_sync_time = next.time.clone();
        config.model_sync_probe_credential_id = next.probe_credential_id;
        config.allow_unknown_model_passthrough = next.allow_passthrough;
        config
            .save_to(&self.config_path)
            .map_err(|e| AdminServiceError::InternalError(format!("保存配置失败: {}", e)))?;

        *self.model_sync.write() = next.clone();
        crate::anthropic::model_registry::set_allow_passthrough(next.allow_passthrough);
        Ok(next)
    }
```

> 实现时用该文件中已有的 config 加载/保存方式（搜索 `service.rs:1236` 附近的既有写法）替换上面的 `Config::load_from` / `save_to` 占位调用名；若既有代码用的是别的函数名，沿用既有的。

在 `config.example.json` 加入四项：

```json
  "modelSyncEnabled": false,
  "modelSyncTime": "04:00",
  "modelSyncProbeCredentialId": null,
  "allowUnknownModelPassthrough": false
```

在 `src/admin/types.rs` 加入请求体（`probe_credential_id_set` 用于区分「未提供」与「显式置 null」）：

```rust
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetModelSyncSettingsRequest {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub time: Option<String>,
    #[serde(default)]
    pub probe_credential_id: Option<u64>,
    /// 请求体中是否出现了 probeCredentialId 键（区分「不改」与「置空」）
    #[serde(default, rename = "probeCredentialIdSet")]
    pub probe_credential_id_set: bool,
    #[serde(default)]
    pub allow_passthrough: Option<bool>,
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test 2>&1 | tail -20`
Expected: 全部通过，含新增的两个 config 测试。

- [ ] **Step 5: Commit**

```bash
git add src/model/config.rs src/admin/service.rs src/admin/types.rs config.example.json
git commit -m "feat(model-registry): 四个运行时开关与 config 写锁

modelSyncEnabled 与 allowUnknownModelPassthrough 均默认 false，保证零行为
回归。开关放 ModelSyncSettings 运行时 holder（Config clone 不可变，
token_manager.rs:1003），使 PATCH 能热生效。新增 config.json 写 mutex，
顺带修掉既有无保护 load-modify-save（service.rs:1236）的丢失更新。"
```

---

## Task 9: `ModelListFetcher` 与 `ModelSyncService`

**Files:**
- Create: `src/anthropic/model_sync.rs`
- Modify: `src/anthropic/mod.rs`
- Modify: `src/kiro/token_manager.rs`（实现 trait）

**Interfaces:**
- Consumes: Task 5 的 `ModelRegistryStore`、`merge_synced_row`；Task 1/4 的类型
- Produces:
  ```rust
  pub struct UpstreamModel { pub model_id: String, pub model_name: Option<String>, pub max_input_tokens: Option<i64> }
  #[async_trait-free] pub trait ModelListFetcher: Send + Sync {
      fn fetch(&self, credential_id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>>;
      fn candidate_credential_ids(&self) -> Vec<u64>;
      fn is_credential_usable(&self, credential_id: u64) -> bool;
  }
  pub enum RoundKind { Authoritative, Advisory }
  pub struct SyncSummary { pub round: RoundKind, pub added: usize, pub updated: usize, pub deprecated: usize, pub trusted: bool, pub source: String }
  pub struct ModelSyncService { /* 私有 */ }
  impl ModelSyncService {
      pub fn new(store: Arc<ModelRegistryStore>, fetcher: Arc<dyn ModelListFetcher>) -> Self;
      pub async fn sync_once(&self, probe_credential_id: Option<u64>, now: DateTime<Utc>) -> Result<SyncSummary, String>;
  }
  pub const MISSING_ROUNDS_THRESHOLD: u32 = 2;
  pub const SAMPLE_SIZE: usize = 3;
  ```

- [ ] **Step 1: 写失败测试**

在 `src/anthropic/model_sync.rs` 末尾创建：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    struct FakeFetcher {
        /// 凭据 id → 该凭据返回的模型（或错误）
        responses: StdMutex<HashMap<u64, Result<Vec<UpstreamModel>, String>>>,
        usable: Vec<u64>,
    }

    impl FakeFetcher {
        fn new(responses: Vec<(u64, Result<Vec<UpstreamModel>, String>)>) -> Self {
            let usable = responses.iter().map(|(id, _)| *id).collect();
            Self { responses: StdMutex::new(responses.into_iter().collect()), usable }
        }
    }

    impl ModelListFetcher for FakeFetcher {
        fn fetch(&self, id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>> {
            let r = self
                .responses
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .unwrap_or_else(|| Err("no such credential".to_string()));
            Box::pin(async move { r })
        }
        fn candidate_credential_ids(&self) -> Vec<u64> {
            self.usable.clone()
        }
        fn is_credential_usable(&self, id: u64) -> bool {
            self.usable.contains(&id)
        }
    }

    fn upstream(id: &str, window: Option<i64>) -> UpstreamModel {
        UpstreamModel {
            model_id: id.to_string(),
            model_name: Some(id.to_uppercase()),
            max_input_tokens: window,
        }
    }

    fn tmp_store(name: &str) -> Arc<ModelRegistryStore> {
        let mut p = std::env::temp_dir();
        p.push(format!("kiro-sync-test-{}-{}.json", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        Arc::new(ModelRegistryStore::new(p))
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-07-25T04:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    /// 新模型经一轮同步应进入表中（本设计要解决的原始问题）
    #[tokio::test]
    async fn adds_new_upstream_model() {
        let store = tmp_store("add");
        let fetcher = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-opus-5", Some(1_000_000))]),
        )]));
        let svc = ModelSyncService::new(store.clone(), fetcher);

        let summary = svc.sync_once(Some(3), now()).await.unwrap();
        assert!(summary.trusted);
        assert!(matches!(summary.round, RoundKind::Authoritative));
        assert_eq!(summary.added, 1);

        let out = store.load();
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-5").unwrap();
        assert_eq!(row.exposed_id, "claude-opus-5");
        assert_eq!(row.context_window, 1_000_000);
        assert!(row.expose_thinking_variant, "claude-* 应派生 thinking 变体");
    }

    /// gpt 家族的 exposedId 必须原样保留点号
    #[tokio::test]
    async fn gpt_exposed_id_keeps_dots() {
        let store = tmp_store("gpt");
        let fetcher = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("gpt-5.9-nova", Some(272_000))]),
        )]));
        ModelSyncService::new(store.clone(), fetcher).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "gpt-5.9-nova").unwrap();
        assert_eq!(row.exposed_id, "gpt-5.9-nova", "gpt 家族不得把点号转成连字符");
        assert!(!row.expose_thinking_variant);
    }

    /// 全部凭据失败 → 本轮不可信 → 文件字节不变、无 deprecated
    #[tokio::test]
    async fn untrusted_round_does_not_touch_file() {
        let store = tmp_store("untrusted");
        // 先建一行
        let ok = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), ok).sync_once(Some(3), now()).await.unwrap();
        let before = std::fs::read(store.path()).unwrap();

        // 再来一轮全失败
        let bad = Arc::new(FakeFetcher::new(vec![(3, Err("network down".to_string()))]));
        let result = ModelSyncService::new(store.clone(), bad).sync_once(Some(3), now()).await;
        assert!(result.is_err() || !result.unwrap().trusted);
        assert_eq!(std::fs::read(store.path()).unwrap(), before, "不可信轮次不应改动文件");

        let out = store.load();
        assert!(out
            .registry
            .rows()
            .iter()
            .all(|r| r.status == crate::anthropic::model_registry::ModelStatus::Active));
    }

    /// 空列表同样视为不可信
    #[tokio::test]
    async fn empty_upstream_list_is_untrusted() {
        let store = tmp_store("empty");
        let ok = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), ok).sync_once(Some(3), now()).await.unwrap();
        let before = std::fs::read(store.path()).unwrap();

        let empty = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![]))]));
        let r = ModelSyncService::new(store.clone(), empty).sync_once(Some(3), now()).await;
        assert!(r.is_err() || !r.unwrap().trusted);
        assert_eq!(std::fs::read(store.path()).unwrap(), before);
    }

    /// 非权威（采样）轮次不得判定消失
    #[tokio::test]
    async fn advisory_round_never_deprecates() {
        let store = tmp_store("advisory");
        let full = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-a", Some(200_000)), upstream("claude-b", Some(200_000))]),
        )]));
        ModelSyncService::new(store.clone(), full).sync_once(Some(3), now()).await.unwrap();

        // 探针不可用 → 采样轮次；采样到的凭据只看得到 claude-a
        let partial = Arc::new(FakeFetcher::new(vec![(9, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        let svc = ModelSyncService::new(store.clone(), partial);
        // 探针 id=3 已不在 usable 列表中 → 回退采样
        let summary = svc.sync_once(Some(3), now()).await.unwrap();
        assert!(matches!(summary.round, RoundKind::Advisory));
        assert_eq!(summary.deprecated, 0, "非权威轮次不得判消失");

        let out = store.load();
        let b = out.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b.status, crate::anthropic::model_registry::ModelStatus::Active);
        assert_eq!(b.missing_sync_rounds, 0);
    }

    /// 权威轮次连续 2 轮未见 → deprecated，且行不被删除
    #[tokio::test]
    async fn authoritative_rounds_deprecate_after_threshold() {
        use crate::anthropic::model_registry::ModelStatus;
        let store = tmp_store("deprecate");
        let full = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-a", Some(200_000)), upstream("claude-b", Some(200_000))]),
        )]));
        ModelSyncService::new(store.clone(), full).sync_once(Some(3), now()).await.unwrap();

        let shrunk = || Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));

        let s1 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s1.deprecated, 0, "第一轮只累计，不标记");
        let s2 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s2.deprecated, 1);

        let out = store.load();
        let b = out.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b.status, ModelStatus::Deprecated);
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-b"), "永不删行");
    }

    /// 无效 maxInputTokens 回退 200000
    #[tokio::test]
    async fn invalid_max_input_tokens_falls_back() {
        let store = tmp_store("badwindow");
        let f = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![
                upstream("claude-none", None),
                upstream("claude-zero", Some(0)),
                upstream("claude-huge", Some(i64::from(i32::MAX) + 1)),
            ]),
        )]));
        ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        for id in ["claude-none", "claude-zero", "claude-huge"] {
            let row = out.registry.rows().iter().find(|r| r.upstream_id == id).unwrap();
            assert_eq!(row.context_window, 200_000, "{} 应回退到 200000", id);
        }
    }

    /// 并集冲突：窗口取 max，名称按凭据 id 升序取首个非空
    #[tokio::test]
    async fn union_conflict_resolution() {
        let store = tmp_store("union");
        let f = Arc::new(FakeFetcher::new(vec![
            (7, Ok(vec![UpstreamModel { model_id: "claude-x".into(), model_name: Some("从 7 来".into()), max_input_tokens: Some(200_000) }])),
            (2, Ok(vec![UpstreamModel { model_id: "claude-x".into(), model_name: Some("从 2 来".into()), max_input_tokens: Some(1_000_000) }])),
        ]));
        // 探针不可用 → 采样，会同时命中 2 与 7
        let summary = ModelSyncService::new(store.clone(), f).sync_once(None, now()).await.unwrap();
        assert!(matches!(summary.round, RoundKind::Advisory));

        let out = store.load();
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "claude-x").unwrap();
        assert_eq!(row.context_window, 1_000_000, "窗口应取 max");
        assert_eq!(row.display_name, "从 2 来", "名称应按凭据 id 升序取首个非空");
    }

    /// credentialSupport 记录本轮各凭据的可用模型集
    #[tokio::test]
    async fn records_credential_support() {
        let store = tmp_store("credsupport");
        let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        assert_eq!(out.file.credential_support.get("3").unwrap(), &vec!["claude-a".to_string()]);
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib model_sync:: 2>&1 | tail -20`
Expected: `cannot find type ModelSyncService`。

- [ ] **Step 3: 实现**

`src/anthropic/model_sync.rs`：

```rust
//! 上游模型同步。
//!
//! 核心安全规则：**一轮同步只有在「至少一个凭据成功返回非空模型列表」时
//! 才算可信；不可信轮次不写文件、不递增 missingSyncRounds。** 网络抖动、
//! 凭据集体过期、上游 5xx 都会让返回变空；若空列表被当成「上游啥都没了」，
//! 一次抖动就会把全表刷成 deprecated。
//!
//! 第二条规则：**只有探针凭据成功的「权威轮次」能判定消失。** 上游模型集
//! 随订阅等级不同（kiro/model/available_models.rs:6），采样到低等级凭据的
//! 轮次会把高等级独有模型误判为消失。

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use super::model_registry::{
    MatchKind, ModelOrigin, ModelRow, ModelStatus,
};
use super::model_registry_store::{merge_synced_row, ModelRegistryStore};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 权威轮次连续多少轮未见才标 deprecated。
pub const MISSING_ROUNDS_THRESHOLD: u32 = 2;
/// 非权威轮次的采样凭据数。不遍历全部：凭据可达上百（支持批量导入），
/// 每轮遍历即上百次上游请求。
pub const SAMPLE_SIZE: usize = 3;
/// 无效 maxInputTokens 的回退值。
pub const FALLBACK_CONTEXT_WINDOW: i32 = 200_000;

/// 上游返回的单个模型。对应 admin/service.rs:787-804 的 AvailableModelItem。
#[derive(Debug, Clone)]
pub struct UpstreamModel {
    pub model_id: String,
    pub model_name: Option<String>,
    pub max_input_tokens: Option<i64>,
}

/// 拉取上游模型列表。**必须是 trait** ——
/// 现有 get_available_models_for（token_manager.rs:2702）内部直接刷 token 并
/// 发网络请求，无法在单测中替换。
pub trait ModelListFetcher: Send + Sync {
    fn fetch(&self, credential_id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>>;
    /// 可用于同步的启用凭据 id，升序。
    fn candidate_credential_ids(&self) -> Vec<u64>;
    fn is_credential_usable(&self, credential_id: u64) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundKind {
    /// 探针凭据成功。可判定模型消失。
    Authoritative,
    /// 采样并集。只做新增与更新。
    Advisory,
}

#[derive(Debug, Clone)]
pub struct SyncSummary {
    pub round: RoundKind,
    pub added: usize,
    pub updated: usize,
    pub deprecated: usize,
    pub trusted: bool,
    pub source: String,
}

pub struct ModelSyncService {
    store: Arc<ModelRegistryStore>,
    fetcher: Arc<dyn ModelListFetcher>,
}

/// 按 provider 前缀派生对外名。
/// claude-* 点号转连字符；其他（含 gpt-5*）**原样保留** ——
/// handlers.rs:388 刻意暴露带点号的 gpt-5.6-sol。
fn derive_exposed_id(upstream_id: &str) -> String {
    if upstream_id.starts_with("claude-") {
        upstream_id.replace('.', "-")
    } else {
        upstream_id.to_string()
    }
}

fn derive_thinking_variant(upstream_id: &str) -> bool {
    upstream_id.starts_with("claude-")
}

/// maxInputTokens: Option<i64> → i32。无效值回退 200000 并 warn。
fn sanitize_window(upstream_id: &str, raw: Option<i64>) -> i32 {
    match raw {
        Some(v) if v > 0 && v <= i64::from(i32::MAX) => v as i32,
        other => {
            tracing::warn!(
                "上游模型 {} 的 maxInputTokens 无效({:?})，回退 {}",
                upstream_id,
                other,
                FALLBACK_CONTEXT_WINDOW
            );
            FALLBACK_CONTEXT_WINDOW
        }
    }
}

impl ModelSyncService {
    pub fn new(store: Arc<ModelRegistryStore>, fetcher: Arc<dyn ModelListFetcher>) -> Self {
        Self { store, fetcher }
    }

    /// 跑一轮同步。`now` 由调用方注入，便于测试。
    pub async fn sync_once(
        &self,
        probe_credential_id: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<SyncSummary, String> {
        let fetch_started_at = now.to_rfc3339();

        // ---- 选凭据 ----
        let (round, credential_ids) = match probe_credential_id {
            Some(id) if self.fetcher.is_credential_usable(id) => {
                (RoundKind::Authoritative, vec![id])
            }
            _ => {
                let mut ids = self.fetcher.candidate_credential_ids();
                ids.sort_unstable();
                ids.truncate(SAMPLE_SIZE);
                (RoundKind::Advisory, ids)
            }
        };
        if credential_ids.is_empty() {
            return Err("没有可用于同步的凭据".to_string());
        }

        // ---- 拉取 + 并集（按凭据 id 升序，保证冲突解决确定性）----
        let mut union: BTreeMap<String, UpstreamModel> = BTreeMap::new();
        let mut per_credential: HashMap<String, Vec<String>> = HashMap::new();
        let mut any_nonempty = false;

        for id in &credential_ids {
            match self.fetcher.fetch(*id).await {
                Ok(models) => {
                    if !models.is_empty() {
                        any_nonempty = true;
                    }
                    per_credential.insert(
                        id.to_string(),
                        models.iter().map(|m| m.model_id.clone()).collect(),
                    );
                    for m in models {
                        match union.get_mut(&m.model_id) {
                            Some(existing) => {
                                // 窗口取 max
                                let a = existing.max_input_tokens.unwrap_or(0);
                                let b = m.max_input_tokens.unwrap_or(0);
                                if b > a {
                                    existing.max_input_tokens = m.max_input_tokens;
                                }
                                // 名称按凭据 id 升序取首个非空 → 已有非空则不覆盖
                                if existing.model_name.as_deref().unwrap_or("").is_empty() {
                                    existing.model_name = m.model_name.clone();
                                }
                            }
                            None => {
                                union.insert(m.model_id.clone(), m);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("凭据 {} 拉取上游模型失败: {}，本轮跳过该凭据", id, e);
                }
            }
        }

        // ---- 可信度判定 ----
        if !any_nonempty {
            return Err(format!(
                "本轮同步不可信（{} 个凭据均失败或返回空列表），不改动 models.json",
                credential_ids.len()
            ));
        }

        let source = match round {
            RoundKind::Authoritative => format!("probe:{}", credential_ids[0]),
            RoundKind::Advisory => format!(
                "sample:{}",
                credential_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
            ),
        };

        // ---- diff 并写入 ----
        let mut added = 0usize;
        let mut updated = 0usize;
        let mut deprecated = 0usize;
        let seen_at = now.to_rfc3339();

        let file = self
            .store
            .mutate(|file| {
                // 乱序保护：已有更新的结果落盘则丢弃本轮
                if let Some(last) = file.sync_state.last_sync_at.as_deref() {
                    if last > fetch_started_at.as_str() {
                        return Err(format!(
                            "已有更新的同步结果（{}）晚于本轮起始时间（{}），丢弃本轮",
                            last, fetch_started_at
                        ));
                    }
                }

                let mut max_sort = file.models.iter().map(|r| r.sort_order).max().unwrap_or(0);

                for (upstream_id, m) in &union {
                    let incoming = ModelRow {
                        upstream_id: upstream_id.clone(),
                        match_kind: MatchKind::Exact,
                        exposed_id: derive_exposed_id(upstream_id),
                        display_name: m
                            .model_name
                            .clone()
                            .filter(|s| !s.trim().is_empty())
                            .unwrap_or_else(|| upstream_id.clone()),
                        owned_by: if upstream_id.starts_with("claude-") {
                            "anthropic".to_string()
                        } else {
                            "openai".to_string()
                        },
                        model_type: "chat".to_string(),
                        created: now.timestamp(),
                        context_window: sanitize_window(upstream_id, m.max_input_tokens),
                        max_output_tokens: 64_000,
                        expose_thinking_variant: derive_thinking_variant(upstream_id),
                        enabled: true,
                        listed: true,
                        status: ModelStatus::Active,
                        origin: ModelOrigin::Synced,
                        sort_order: 0,
                        pinned: Vec::new(),
                        missing_sync_rounds: 0,
                        last_seen_at: Some(seen_at.clone()),
                    };

                    match file.models.iter_mut().find(|r| &r.upstream_id == upstream_id) {
                        Some(existing) => {
                            merge_synced_row(existing, &incoming);
                            updated += 1;
                        }
                        None => {
                            let mut row = incoming;
                            max_sort += 10;
                            row.sort_order = max_sort;
                            file.models.push(row);
                            added += 1;
                        }
                    }
                }

                // 消失判定：仅权威轮次
                if round == RoundKind::Authoritative {
                    for row in file.models.iter_mut() {
                        if union.contains_key(&row.upstream_id) {
                            continue;
                        }
                        row.missing_sync_rounds += 1;
                        if row.missing_sync_rounds >= MISSING_ROUNDS_THRESHOLD
                            && row.status == ModelStatus::Active
                        {
                            row.status = ModelStatus::Deprecated;
                            deprecated += 1;
                            tracing::warn!(
                                "模型 {} 连续 {} 轮权威同步未出现于上游，标记为 deprecated（保留可用）",
                                row.upstream_id,
                                row.missing_sync_rounds
                            );
                        }
                    }
                }

                for (cred, models) in per_credential.drain() {
                    file.credential_support.insert(cred, models);
                }
                file.sync_state.last_sync_at = Some(now.to_rfc3339());
                file.sync_state.last_fetch_started_at = Some(fetch_started_at.clone());
                file.sync_state.source = Some(source.clone());
                Ok(())
            })
            .await?;

        // 落盘成功后才热替换
        match super::model_registry::ModelRegistry::from_file(file) {
            Ok(registry) => super::model_registry::install_registry(registry),
            Err(e) => {
                tracing::error!("同步后的 models.json 校验失败: {}，保持内存中旧表", e);
            }
        }

        Ok(SyncSummary { round, added, updated, deprecated, trusted: true, source })
    }
}
```

在 `src/anthropic/mod.rs` 加入 `pub mod model_sync;`。

在 `src/kiro/token_manager.rs` 末尾为 `MultiTokenManager` 实现该 trait（复用既有 `get_available_models_for`，`token_manager.rs:2702`）：

```rust
impl crate::anthropic::model_sync::ModelListFetcher for MultiTokenManager {
    fn fetch(
        &self,
        credential_id: u64,
    ) -> crate::anthropic::model_sync::BoxFuture<'_, Result<Vec<crate::anthropic::model_sync::UpstreamModel>, String>>
    {
        Box::pin(async move {
            let resp = self
                .get_available_models_for(credential_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok(resp
                .models
                .into_iter()
                .map(|m| crate::anthropic::model_sync::UpstreamModel {
                    model_id: m.model_id,
                    model_name: m.model_name,
                    max_input_tokens: m.token_limits.and_then(|t| t.max_input_tokens),
                })
                .collect())
        })
    }

    fn candidate_credential_ids(&self) -> Vec<u64> {
        // 启用（未禁用）的凭据 id，升序
        let mut ids: Vec<u64> = self
            .list_credentials()
            .into_iter()
            .filter(|c| !c.disabled)
            .map(|c| c.id)
            .collect();
        ids.sort_unstable();
        ids
    }

    fn is_credential_usable(&self, credential_id: u64) -> bool {
        self.list_credentials()
            .into_iter()
            .any(|c| c.id == credential_id && !c.disabled)
    }
}
```

> `resp.models` / `m.token_limits` 的确切字段名见 `src/kiro/model/available_models.rs:13-45`；`list_credentials()` / `c.disabled` / `c.id` 按 `token_manager.rs` 中既有的凭据列举方式调整（搜索 `fn list_credentials` 或 `credentials.read()`）。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test --lib model_sync:: 2>&1 | tail -20`
Expected: `test result: ok. 9 passed`

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/model_sync.rs src/anthropic/mod.rs src/kiro/token_manager.rs
git commit -m "feat(model-registry): 上游模型同步服务

两条安全规则：不可信轮次（全失败或全空列表）不写文件；只有探针凭据成功的
权威轮次能判定消失（上游模型集随订阅等级不同，采样轮次会误判）。
exposedId 按 provider 派生（claude-* 转连字符，gpt-* 原样保留点号）。
maxInputTokens 无效值回退 200000；并集冲突窗口取 max、名称按凭据 id
升序取首个非空。经 ModelListFetcher trait 拉取，可注入假实现单测。"
```

---

## Task 10: 调度层按 `credentialSupport` 过滤

**Files:**
- Modify: `src/kiro/token_manager.rs:1065`（`credential_matches_request`）

**Interfaces:**
- Consumes: Task 4 的 `credential_support`、Task 6 的 `current_registry()`
- Produces: `pub fn credential_supports_model(credential_id: u64, upstream_id: &str, support: &HashMap<String, Vec<String>>) -> bool`

- [ ] **Step 1: 写失败测试**

在 `src/kiro/token_manager.rs` 的测试模块追加：

```rust
    #[test]
    fn credential_support_filter_rules() {
        use std::collections::HashMap;
        let mut support: HashMap<String, Vec<String>> = HashMap::new();
        support.insert("3".to_string(), vec!["claude-opus-4.8".to_string()]);

        // 有记录且包含 → 放行
        assert!(credential_supports_model(3, "claude-opus-4.8", &support));
        // 有记录但不含 → 拒绝
        assert!(!credential_supports_model(3, "claude-opus-5", &support));
        // 无记录 → 放行（保守，不误杀）
        assert!(credential_supports_model(9, "claude-opus-5", &support));
    }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib credential_support_filter_rules 2>&1 | tail -10`
Expected: `cannot find function credential_supports_model`。

- [ ] **Step 3: 实现**

在 `src/kiro/token_manager.rs` 中 `credential_matches_request` 附近加入：

```rust
/// 该凭据是否已知支持目标上游模型。
///
/// **无记录时放行**（保守，不误杀）——采样模式下大多数凭据没有记录。
/// 残留风险与缓解手段见 spec §6.6 / §12：未记录的凭据仍可能被选中，
/// 导致上游 400 且 provider 不换凭据重试（provider.rs:793）。
pub fn credential_supports_model(
    credential_id: u64,
    upstream_id: &str,
    support: &std::collections::HashMap<String, Vec<String>>,
) -> bool {
    match support.get(&credential_id.to_string()) {
        Some(models) => models.iter().any(|m| m == upstream_id),
        None => true,
    }
}
```

修改 `credential_matches_request`，增加 `credential_id` 与 `credential_support` 两个入参（调用方从 `ModelRegistryStore` 加载的 `file.credential_support` 传入，或由 `MultiTokenManager` 持有一份缓存副本）：

```rust
fn credential_matches_request(
    credentials: &KiroCredentials,
    credential_id: u64,
    model: Option<&str>,
    group: Option<&str>,
    credential_support: &std::collections::HashMap<String, Vec<String>>,
) -> bool {
    let is_opus = model
        .map(|m| m.to_ascii_lowercase().contains("opus"))
        .unwrap_or(false);

    if is_opus && !credentials.supports_opus() {
        return false;
    }

    // 新增：按上游宣告的可用模型过滤（无记录则放行）
    if let Some(m) = model {
        if !credential_supports_model(credential_id, m, credential_support) {
            return false;
        }
    }

    group_matches(&credentials.groups, group)
}
```

> `MultiTokenManager` 增加一个 `credential_support: parking_lot::RwLock<HashMap<String, Vec<String>>>` 字段，启动时由 `ModelRegistryStore::load()` 的 `file.credential_support` 填充，同步完成后更新。调用 `credential_matches_request` 的地方传 `&self.credential_support.read()`。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test 2>&1 | tail -20`
Expected: 全部通过。编译器会指出 `credential_matches_request` 的所有调用点，按提示补参数。

- [ ] **Step 5: Commit**

```bash
git add src/kiro/token_manager.rs
git commit -m "feat(model-registry): 调度层按 credentialSupport 过滤凭据

credential_matches_request 原先只识别 Opus/非 Opus，新同步进来的模型没有
任何按凭据的准入判断，可能选到从未宣告该模型的凭据，而 provider 对 4xx
直接返回不换凭据重试（provider.rs:793）。现增加一层过滤：有记录则要求包含
目标模型，无记录则放行（保守，不误杀）。残留风险见 spec §6.6。"
```

---

## Task 11: Admin API 端点

**Files:**
- Modify: `src/admin/error.rs`、`src/admin/types.rs`、`src/admin/service.rs`、`src/admin/handlers.rs`、`src/admin/router.rs`

**Interfaces:**
- Consumes: Task 5 的 `ModelRegistryStore`、Task 8 的 `ModelSyncSettings`、Task 9 的 `ModelSyncService`
- Produces: 7 组端点（见下表）与 3 个新错误变体

- [ ] **Step 1: 写失败测试**

在 `src/admin/service.rs` 的测试模块追加：

```rust
    /// PATCH 只能改白名单字段；被改字段自动进 pinned
    #[test]
    fn patch_pins_edited_fields_and_rejects_readonly() {
        use crate::admin::types::PatchModelRequest;

        let mut row = crate::anthropic::model_registry::builtin_rows()
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
        assert!(row.pinned.contains(&"contextWindow".to_string()), "被编辑字段应自动 pin");

        // unpin 后该字段回归自动同步
        let req = PatchModelRequest { unpin: vec!["contextWindow".to_string()], ..Default::default() };
        apply_model_patch(&mut row, &req).unwrap();
        assert!(!row.pinned.contains(&"contextWindow".to_string()));
    }

    /// builtin 行不可删
    #[test]
    fn builtin_row_cannot_be_deleted() {
        let rows = crate::anthropic::model_registry::builtin_rows();
        let builtin = rows.iter().find(|r| r.exposed_id == "claude-opus-4-8").unwrap();
        assert!(ensure_deletable(builtin).is_err());
    }
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test --lib patch_pins_edited_fields 2>&1 | tail -10`
Expected: `cannot find function apply_model_patch`。

- [ ] **Step 3: 实现**

`src/admin/error.rs` 的 `AdminServiceError` 加三个变体、Display 分支与 `status_code` 分支：

```rust
    /// 模型表中不存在该 upstreamId
    ModelNotFound(String),
    /// 模型/别名冲突（重复 upstreamId、撞名等）
    ModelConflict(String),
    /// 字段不可写或取值非法
    InvalidModelField(String),
```

```rust
            AdminServiceError::ModelNotFound(id) => write!(f, "模型不存在: {}", id),
            AdminServiceError::ModelConflict(msg) => write!(f, "模型配置冲突: {}", msg),
            AdminServiceError::InvalidModelField(msg) => write!(f, "模型字段无效: {}", msg),
```

```rust
            AdminServiceError::ModelNotFound(_) => StatusCode::NOT_FOUND,
            AdminServiceError::ModelConflict(_) => StatusCode::CONFLICT,
            AdminServiceError::InvalidModelField(_) => StatusCode::BAD_REQUEST,
```

`src/admin/types.rs` 加入：

```rust
/// PATCH /models/{upstreamId} 请求体。只列可写字段。
/// upstreamId / origin / status / missingSyncRounds / lastSeenAt / created 为只读——
/// 尤其 origin 必须只读，否则可把 builtin 改成 manual 绕过删除保护。
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchModelRequest {
    pub exposed_id: Option<String>,
    pub display_name: Option<String>,
    pub context_window: Option<i32>,
    pub max_output_tokens: Option<i32>,
    pub expose_thinking_variant: Option<bool>,
    pub enabled: Option<bool>,
    pub sort_order: Option<i32>,
    pub match_kind: Option<crate::anthropic::model_registry::MatchKind>,
    /// 解除锁定的字段名，使其回归自动同步
    #[serde(default)]
    pub unpin: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRegistryResponse {
    pub models: Vec<crate::anthropic::model_registry::ModelRow>,
    pub aliases: Vec<crate::anthropic::model_registry::ModelAlias>,
    pub sync_state: crate::anthropic::model_registry::SyncState,
    pub settings: ModelSyncSettingsResponse,
    pub degraded: bool,
    pub degraded_reason: Option<String>,
    /// 已记录可用模型的凭据数 / 启用凭据总数，用于 UI 提示覆盖率
    pub credential_support_covered: usize,
    pub credential_total: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSyncSettingsResponse {
    pub enabled: bool,
    pub time: String,
    pub probe_credential_id: Option<u64>,
    pub allow_passthrough: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncSummaryResponse {
    pub round: String,
    pub added: usize,
    pub updated: usize,
    pub deprecated: usize,
    pub trusted: bool,
    pub source: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertAliasRequest {
    pub from: String,
    pub to: String,
}
```

`src/admin/service.rs` 加入两个纯函数（便于单测）与服务方法：

```rust
/// 可写字段 → pinned 中记录的字段名。
const PATCHABLE_PINNED_FIELDS: &[&str] = &[
    "exposedId",
    "displayName",
    "contextWindow",
    "maxOutputTokens",
    "exposeThinkingVariant",
];

/// 应用 PATCH，被编辑的字段自动进 pinned。
pub fn apply_model_patch(
    row: &mut crate::anthropic::model_registry::ModelRow,
    req: &crate::admin::types::PatchModelRequest,
) -> Result<(), AdminServiceError> {
    let mut pin = |row: &mut crate::anthropic::model_registry::ModelRow, field: &str| {
        if PATCHABLE_PINNED_FIELDS.contains(&field) && !row.pinned.iter().any(|p| p == field) {
            row.pinned.push(field.to_string());
        }
    };

    if let Some(v) = req.exposed_id.clone() {
        row.exposed_id = v;
        pin(row, "exposedId");
    }
    if let Some(v) = req.display_name.clone() {
        row.display_name = v;
        pin(row, "displayName");
    }
    if let Some(v) = req.context_window {
        if v <= 0 {
            return Err(AdminServiceError::InvalidModelField(
                "contextWindow 必须为正数".to_string(),
            ));
        }
        row.context_window = v;
        pin(row, "contextWindow");
    }
    if let Some(v) = req.max_output_tokens {
        if v <= 0 {
            return Err(AdminServiceError::InvalidModelField(
                "maxOutputTokens 必须为正数".to_string(),
            ));
        }
        row.max_output_tokens = v;
        pin(row, "maxOutputTokens");
    }
    if let Some(v) = req.expose_thinking_variant {
        row.expose_thinking_variant = v;
        pin(row, "exposeThinkingVariant");
    }
    if let Some(v) = req.enabled {
        row.enabled = v;
    }
    if let Some(v) = req.sort_order {
        row.sort_order = v;
    }
    if let Some(v) = req.match_kind {
        row.match_kind = v;
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
```

`AdminService` 上加方法：`model_registry()`（返回 `ModelRegistryResponse`）、`sync_models()`、`create_model()`、`patch_model()`、`delete_model()`、`upsert_alias()`、`delete_alias()`。每个方法内部经 `store.mutate(...)` 修改，成功后 `install_registry(ModelRegistry::from_file(file)?)`。

`src/admin/handlers.rs` 加对应 handler，`src/admin/router.rs` 注册（与既有 `/credentials/...` 平铺风格一致）：

```rust
        .route("/models", get(get_model_registry).post(create_model))
        .route("/models/sync", post(sync_models))
        .route("/models/{upstream_id}", patch(patch_model).delete(delete_model))
        .route("/models/aliases", post(upsert_alias).delete(delete_alias))
        .route("/models/settings", patch(set_model_sync_settings))
```

> `patch` 需从 `axum::routing::patch` 引入。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo build 2>&1 | tail -20 && cargo test 2>&1 | tail -20`
Expected: 编译通过；全部测试通过。

- [ ] **Step 5: Commit**

```bash
git add src/admin/
git commit -m "feat(model-registry): Admin API 端点

7 组端点：GET /models、POST /models/sync、POST /models、
PATCH|DELETE /models/{upstreamId}、POST|DELETE /models/aliases、
PATCH /models/settings。PATCH 只接受白名单字段，被编辑字段自动进 pinned，
支持 unpin 让字段回归自动同步；origin 只读（否则可绕过 builtin 删除保护）。
新增 ModelNotFound / ModelConflict / InvalidModelField 三个错误变体
（既有 NotFound 只携带数字凭据 id）。"
```

---

## Task 12: 启动接线（registry 初始化 + 同步调度器）

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: Task 5 `ModelRegistryStore`、Task 6 `install_registry`/`set_allow_passthrough`、Task 9 `ModelSyncService`

- [ ] **Step 1: 写失败测试 —— 手工验证清单（本任务为接线，用运行时验证）**

本任务无法用单元测试覆盖启动顺序，改用可复现的手工验证。先写下验证脚本 `/tmp/verify-task12.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail
BASE=http://127.0.0.1:8990

echo "--- 1. models.json 不存在时 /v1/models 必须与改造前一致 ---"
curl -s -H "Authorization: Bearer $API_KEY" "$BASE/v1/models" | jq -r '.data[].id' > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt && echo "OK: 列表一致"

echo "--- 2. 未配 adminApiKey 时同步调度器仍应存在（看日志无 panic 且有调度日志）---"
echo "（人工检查启动日志中是否有 '模型同步调度器已启动'）"
```

- [ ] **Step 2: 采集改造前基线**

在**当前分支的父提交**（改造前）跑一次服务，保存 `/v1/models` 输出作为基线：

Run:
```bash
git stash list >/dev/null; git log --oneline -1
curl -s -H "Authorization: Bearer $API_KEY" http://127.0.0.1:8990/v1/models | jq -r '.data[].id' > /tmp/before.txt
wc -l /tmp/before.txt
```
Expected: 23 行。

> 若无法运行服务，改用 `cargo test --lib model_registry::tests::exposed_models_matches_pre_change_output` 作为等价证据（该测试已断言列表与顺序）。

- [ ] **Step 3: 实现接线**

在 `src/main.rs` 中，**在 admin 分支（`if let Some(admin_key) = &config.admin_api_key`，约 `:270`）之前**加入：

```rust
    // ---- 模型注册表：必须在建 router 之前初始化 ----
    // 注意：放在 admin 分支之外。AdminService 仅在 adminApiKey 非空时创建，
    // 若把同步调度器挂在其内，未配管理密钥的部署将没有自动同步。
    let models_json_path = credentials_dir.join("models.json");
    let model_store = std::sync::Arc::new(
        crate::anthropic::model_registry_store::ModelRegistryStore::new(models_json_path),
    );
    {
        let outcome = model_store.load();
        if let Some(reason) = &outcome.degraded_reason {
            tracing::error!("模型表降级运行（使用内置默认）: {}", reason);
        }
        crate::anthropic::model_registry::install_registry(outcome.registry);
        crate::anthropic::model_registry::set_allow_passthrough(
            config.allow_unknown_model_passthrough,
        );
    }

    // ---- 每日同步调度器（modelSyncEnabled = false 时不启动）----
    if config.model_sync_enabled {
        let store = model_store.clone();
        let fetcher: std::sync::Arc<dyn crate::anthropic::model_sync::ModelListFetcher> =
            token_manager.clone();
        let probe = config.model_sync_probe_credential_id;
        let sync_time = config.model_sync_time.clone();
        tokio::spawn(async move {
            let svc = crate::anthropic::model_sync::ModelSyncService::new(store, fetcher);
            tracing::info!("模型同步调度器已启动，每日 {} 触发", sync_time);
            loop {
                // 睡到下一个 sync_time（本地时区），复用与自动更新一致的语义
                let sleep_secs = seconds_until_local_time(&sync_time).unwrap_or(3600);
                tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
                match svc.sync_once(probe, chrono::Utc::now()).await {
                    Ok(s) => tracing::info!(
                        "模型同步完成: 轮次={:?} 新增={} 更新={} 标记 deprecated={} 来源={}",
                        s.round, s.added, s.updated, s.deprecated, s.source
                    ),
                    Err(e) => tracing::warn!("模型同步跳过: {}", e),
                }
            }
        });
    } else {
        tracing::info!("模型自动同步未启用（modelSyncEnabled=false）");
    }
```

> `credentials_dir` 用 `main.rs` 中既有的凭据目录变量（搜索 `:176` 附近的目录构造）。
> `seconds_until_local_time` 复用 `src/admin/service.rs` 中自动更新调度的同类实现（搜索 `parse_auto_apply_time` 的调用处），若为私有则提取为 `pub(crate)`。
> `token_manager` 需为 `Arc<MultiTokenManager>` 才能 `clone()` 成 `Arc<dyn ModelListFetcher>`；若当前不是，按既有类型调整。

同时把 `model_store` 传入 `AdminService` 构造（Task 11 的端点需要它）。

- [ ] **Step 4: 运行验证**

Run:
```bash
cargo build 2>&1 | tail -5
cargo test 2>&1 | tail -10
```
Expected: 编译通过，测试全绿。

Run（启动服务后）:
```bash
bash /tmp/verify-task12.sh
```
Expected: `OK: 列表一致`；启动日志含 `模型自动同步未启用（modelSyncEnabled=false）`。

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat(model-registry): 启动时初始化注册表并挂载同步调度器

registry 在建 router 之前初始化，models.json 放凭据目录（与既有 registry/
cache 一致）。同步调度器创建于 admin 分支之外——AdminService 仅在
adminApiKey 非空时创建（main.rs:270），挂在其内会让未配管理密钥的部署
没有自动同步。modelSyncEnabled=false 时不启动调度器。"
```

---

## Task 13: 前端 —— 模型映射弹窗与凭据弹窗增强

**Files:**
- Create: `admin-ui/src/api/models.ts`、`admin-ui/src/hooks/use-model-registry.ts`、`admin-ui/src/components/model-mapping-dialog.tsx`
- Modify: `admin-ui/src/types/index.ts`、`admin-ui/src/components/topbar-tools.tsx`、`admin-ui/src/components/available-models-dialog.tsx`

**Interfaces:**
- Consumes: Task 11 的 7 组端点
- Produces: `ModelRow`/`ModelAlias`/`ModelRegistryResponse` TS 类型、`useModelRegistry()`/`useSyncModels()`/`usePatchModel()` hooks

- [ ] **Step 1: 写类型与 API 客户端**

`admin-ui/src/types/index.ts` 追加：

```ts
export type MatchKind = 'exact' | 'prefix'
export type ModelStatus = 'active' | 'deprecated'
export type ModelOrigin = 'builtin' | 'synced' | 'manual'

export interface ModelRow {
  upstreamId: string
  matchKind: MatchKind
  exposedId: string
  displayName: string
  ownedBy: string
  modelType: string
  created: number
  /** 输入上下文窗口 */
  contextWindow: number
  /** 输出上限（/v1/models 的 max_tokens），与 contextWindow 是不同的量 */
  maxOutputTokens: number
  exposeThinkingVariant: boolean
  enabled: boolean
  listed: boolean
  status: ModelStatus
  origin: ModelOrigin
  sortOrder: number
  pinned: string[]
  missingSyncRounds: number
  lastSeenAt: string | null
}

export interface ModelAlias {
  from: string
  to: string
}

export interface ModelSyncSettings {
  enabled: boolean
  time: string
  probeCredentialId: number | null
  allowPassthrough: boolean
}

export interface ModelRegistryResponse {
  models: ModelRow[]
  aliases: ModelAlias[]
  syncState: {
    lastSyncAt: string | null
    lastFetchStartedAt: string | null
    source: string | null
  }
  settings: ModelSyncSettings
  degraded: boolean
  degradedReason: string | null
  credentialSupportCovered: number
  credentialTotal: number
}

export interface SyncSummary {
  round: string
  added: number
  updated: number
  deprecated: number
  trusted: boolean
  source: string
}
```

`admin-ui/src/api/models.ts`（仿 `admin-ui/src/api/credentials.ts` 的 `api` 用法）：

```ts
import { api } from './client'
import type { ModelRegistryResponse, ModelRow, ModelSyncSettings, SyncSummary } from '@/types'

export async function fetchModelRegistry(): Promise<ModelRegistryResponse> {
  const { data } = await api.get<ModelRegistryResponse>('/models')
  return data
}

export async function syncModels(): Promise<SyncSummary> {
  const { data } = await api.post<SyncSummary>('/models/sync')
  return data
}

export async function patchModel(
  upstreamId: string,
  patch: Partial<Pick<ModelRow, 'exposedId' | 'displayName' | 'contextWindow' | 'maxOutputTokens' | 'exposeThinkingVariant' | 'enabled' | 'sortOrder' | 'matchKind'>> & { unpin?: string[] },
): Promise<void> {
  await api.patch(`/models/${encodeURIComponent(upstreamId)}`, patch)
}

export async function deleteModel(upstreamId: string): Promise<void> {
  await api.delete(`/models/${encodeURIComponent(upstreamId)}`)
}

export async function upsertAlias(from: string, to: string): Promise<void> {
  await api.post('/models/aliases', { from, to })
}

export async function deleteAlias(from: string): Promise<void> {
  await api.delete('/models/aliases', { data: { from, to: '' } })
}

export async function setModelSyncSettings(
  patch: Partial<ModelSyncSettings> & { probeCredentialIdSet?: boolean },
): Promise<ModelSyncSettings> {
  const { data } = await api.patch<ModelSyncSettings>('/models/settings', patch)
  return data
}
```

> `api` 的确切 import 路径按 `admin-ui/src/api/credentials.ts` 顶部的写法照抄。

- [ ] **Step 2: 验证类型编译**

Run: `cd admin-ui && bun run build 2>&1 | tail -10`
Expected: 构建成功（此时还没有组件使用这些类型，仅验证类型本身无误）。

- [ ] **Step 3: 写 hooks 与弹窗组件**

`admin-ui/src/hooks/use-model-registry.ts`（仿 `use-credentials.ts` 的 React Query 用法）：

```ts
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import * as apiModels from '@/api/models'

const KEY = ['model-registry'] as const

export function useModelRegistry() {
  return useQuery({ queryKey: KEY, queryFn: apiModels.fetchModelRegistry })
}

export function useSyncModels() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: apiModels.syncModels,
    onSuccess: (s) => {
      if (!s.trusted) {
        toast.warning('本轮同步不可信，已跳过写入')
      } else {
        toast.success(
          `同步完成（${s.round}）：新增 ${s.added}，更新 ${s.updated}，标记 deprecated ${s.deprecated}`,
        )
      }
      qc.invalidateQueries({ queryKey: KEY })
    },
    onError: (e: unknown) => toast.error(String(e)),
  })
}

export function usePatchModel() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ upstreamId, patch }: { upstreamId: string; patch: Parameters<typeof apiModels.patchModel>[1] }) =>
      apiModels.patchModel(upstreamId, patch),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
    onError: (e: unknown) => toast.error(String(e)),
  })
}

export function useModelSyncSettings() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: apiModels.setModelSyncSettings,
    onSuccess: () => qc.invalidateQueries({ queryKey: KEY }),
    onError: (e: unknown) => toast.error(String(e)),
  })
}
```

> `toast` 与 `useQuery` 的确切来源按 `use-credentials.ts` 顶部照抄。

`admin-ui/src/components/model-mapping-dialog.tsx`：三个 tab（模型表 / 别名 / 同步设置）。模型表每行展示：`exposedId`、`upstreamId`、`matchKind`、可编辑的 `contextWindow`（旁边一个 🔒 图标，`pinned.includes('contextWindow')` 时高亮，点击调 `patchModel(upstreamId, {unpin:['contextWindow']})`）、`maxOutputTokens`、thinking 变体开关、启用开关、`status` 徽章、`origin` 徽章。顶部展示 `syncState.lastSyncAt` + `source` + 「立即同步」按钮；`degraded` 时红色横幅显示 `degradedReason`；`credentialSupportCovered < credentialTotal` 时提示「N/M 个凭据尚未记录可用模型，建议配置探针凭据」。`origin === 'builtin'` 的行隐藏删除按钮。

在 `admin-ui/src/components/topbar-tools.tsx` 增加一个按钮，`onClick` 打开该弹窗（沿用该文件中现有按钮的写法与图标风格）。

在 `admin-ui/src/components/available-models-dialog.tsx` 中：调用 `useModelRegistry()`，对每个上游模型行判断 `registry.models.some(r => r.upstreamId === m.modelId)`：已收录显示 `✓ 已在映射表`，未收录显示 `⊕ 未收录` + 一个「加入映射表」按钮（调 `POST /models`，body 用该模型的 `modelId` / `modelName` / `maxInputTokens`）。

- [ ] **Step 4: 验证构建与手工检查**

Run: `cd admin-ui && bun run build 2>&1 | tail -10`
Expected: 构建成功。

手工检查清单（启动服务后打开管理面板）：
1. 凭据管理页顶栏出现「模型映射」按钮，点击弹出三 tab 弹窗
2. 模型表列出 14 行（13 个 exact + 1 个 prefix 行，prefix 行标注 listed=false）
3. 编辑某模型 `contextWindow` 后该字段出现 🔒，点击 🔒 后消失
4. 「立即同步」在未配凭据时报「没有可用于同步的凭据」而非白屏
5. 打开某凭据的「可用模型」弹窗，每行有收录状态标记

- [ ] **Step 5: Commit**

```bash
git add admin-ui/
git commit -m "feat(admin-ui): 模型映射弹窗与凭据可用模型弹窗增强

凭据管理页顶栏新增「模型映射」按钮，打开全局三 tab 弹窗（模型表/别名/
同步设置）。模型表可编辑输入窗口与输出上限，pinned 字段带 🔒 可点击解除。
degraded 时红色横幅；credentialSupport 覆盖率偏低时提示配置探针凭据。
available-models-dialog 每行标注收录状态并提供「加入映射表」。"
```

---

## Task 14: 集成测试与文档

**Files:**
- Modify: `src/anthropic/handlers.rs`（测试模块）
- Modify: `README.md`

**Interfaces:**
- Consumes: 全部前序任务

- [ ] **Step 1: 写集成测试**

在 `src/anthropic/handlers.rs` 的 `mod tests` 追加：

```rust
    use crate::anthropic::model_registry::{install_registry, ModelRegistry, ModelStatus};

    /// deprecated 仍在 /v1/models；enabled=false 从列表移除
    #[test]
    fn models_endpoint_visibility_rules() {
        let mut r = ModelRegistry::builtin();
        for row in r.rows_mut() {
            if row.exposed_id == "claude-sonnet-4-6" {
                row.status = ModelStatus::Deprecated;
            }
            if row.exposed_id == "claude-opus-4-6" {
                row.enabled = false;
            }
        }
        install_registry(r);

        let ids: Vec<String> = available_models().into_iter().map(|m| m.id).collect();
        assert!(ids.contains(&"claude-sonnet-4-6".to_string()), "deprecated 应保留");
        assert!(!ids.contains(&"claude-opus-4-6".to_string()), "disabled 应移除");
        assert!(!ids.contains(&"claude-opus-4-6-thinking".to_string()));

        install_registry(ModelRegistry::builtin());
    }

    /// 三条路由的未知模型错误文案：anthropic 中文、websearch 英文
    #[test]
    fn unknown_model_messages_per_route_unchanged() {
        use crate::anthropic::converter::ConversionError;
        let e = ConversionError::UnsupportedModel("claude-opus-9".to_string());
        assert_eq!(e.to_string(), "模型不支持: claude-opus-9");
        let e = ConversionError::ModelDisabled("claude-opus-9".to_string());
        assert_eq!(e.to_string(), "模型已禁用: claude-opus-9");
    }
```

在 `src/anthropic/websearch_loop.rs` 的测试模块（若无则新建）追加：

```rust
#[cfg(test)]
mod model_error_message_tests {
    use crate::anthropic::converter::ConversionError;

    /// web-search 路径必须保持英文文案（该路径改造前即为英文）
    #[test]
    fn websearch_route_uses_english_messages() {
        // 与 websearch_loop.rs 中 match 分支的 format! 保持一致
        let unknown = format!("unsupported model: {}", "claude-opus-9");
        assert_eq!(unknown, "unsupported model: claude-opus-9");
        let disabled = format!("model disabled: {}", "claude-opus-9");
        assert_eq!(disabled, "model disabled: claude-opus-9");
        // 确认变体存在（编译期保障）
        let _ = ConversionError::ModelDisabled("x".to_string());
    }
}
```

- [ ] **Step 2: 运行测试确认失败或通过**

Run: `cargo test 2>&1 | tail -20`
Expected: 若 Task 7 已正确实现文案，此处直接通过；否则按断言修正 `Display` 实现与 `format!`。

- [ ] **Step 3: 写文档**

在 `README.md` 的配置说明区（`config.json` 字段表附近）加入：

```markdown
### 模型注册表与自动同步

网关的模型表由「编译内置默认」与凭据目录下的 `models.json` 覆盖层合并而成。
`models.json` 不存在时行为与内置默认完全一致。

| 配置项 | 默认 | 说明 |
|---|---|---|
| `modelSyncEnabled` | `false` | 是否启用每日自动同步上游模型 |
| `modelSyncTime` | `"04:00"` | 每日同步时间（本地 24 小时制） |
| `modelSyncProbeCredentialId` | `null` | 探针凭据 id。设置且可用时为「权威轮次」，可判定模型消失；否则回退为采样 3 个凭据的「非权威轮次」，只做新增与更新 |
| `allowUnknownModelPassthrough` | `false` | 未收录模型是否原样透传给上游（窗口按 200K 估算） |

管理面板：凭据管理页顶栏 →「模型映射」。可手动映射对外模型名到上游 id、
覆写输入上下文窗口与输出上限、增删别名。**被人工编辑过的字段会被锁定
（🔒），自动同步不会覆盖它**；点击 🔒 可解除锁定使其回归自动同步。

上游不再返回某模型时，该模型会被标记为 `deprecated` 但**保留且仍可用**
（不打断在用客户端），也仍出现在 `/v1/models`；需要真正下线时手动关闭
其「启用」开关。
```

- [ ] **Step 4: 全量验证**

Run:
```bash
cargo build 2>&1 | tail -5
cargo test 2>&1 | tail -15
cd admin-ui && bun run build 2>&1 | tail -5
```
Expected: 三者全部成功。

- [ ] **Step 5: Commit**

```bash
git add src/anthropic/handlers.rs src/anthropic/websearch_loop.rs README.md
git commit -m "test(model-registry): 集成测试与文档

断言 /v1/models 的可见性规则（deprecated 保留、disabled 移除）与三条路由
各自的错误文案（anthropic 中文、websearch 英文）。README 补四个配置项与
pinned/deprecated 的语义说明。"
```

---

## 验收清单（对应 spec §11）

实现完成后逐条验证：

- [ ] 1. 无 `models.json` 且 `modelSyncEnabled=false` 时 `/v1/models` 与改造前逐字节一致；`cargo test` 全绿
- [ ] 2. `gpt-5.6-sol` 与任意 `gpt-5*` 行为不变（对外 id 保留点号、原样透传、窗口 272K）
- [ ] 3. `claude-sonnet-5-20260101-thinking` 等既有测试输入解析结果不变
- [ ] 4. 开启 `modelSyncEnabled` 后上游新增模型经一轮同步即可用
- [ ] 5. 手动覆写 `contextWindow` 后同步该值不变；`unpin` 后恢复被覆盖
- [ ] 6. 上游返回空列表 / 全凭据失败 → `models.json` 字节未变、无 deprecated
- [ ] 7. 非权威（采样）轮次未见某模型 → **不**标 deprecated
- [ ] 8. 权威轮次连续 2 轮未见 → `deprecated`，仍可解析、仍在列表、UI 标黄
- [ ] 9. `enabled=false` → 从列表移除，请求报「模型已禁用」（websearch 路径 `model disabled`）
- [ ] 10. `/v1/models` 中 `max_tokens == maxOutputTokens` 且 `!= get_context_window_size()`
- [ ] 11. passthrough 关 → 400；开 → 发往上游并打一条 warn
- [ ] 12. 为 `claude-opus-5` 建别名或等一轮同步后该模型可用

---

## 自查记录

**Spec 覆盖检查：** spec 各节 → 任务映射

| spec 节 | 任务 |
|---|---|
| §3.1 组件职责 | Task 1/5/9 |
| §3.2 接线与改动面 | Task 6/7 |
| §3.3 快照一致性 | Task 7 |
| §4.1 运行时开关 | Task 8 |
| §4.2/4.3 数据模型 | Task 1/4 |
| §4.4 exposedId 派生 | Task 9 |
| §4.5 加载校验与版本 | Task 4 |
| §4.6 内置默认 | Task 1 |
| §5.1/5.2/5.3 解析 | Task 2 |
| §6.1 触发 | Task 12 |
| §6.2 权威/非权威轮次 | Task 9 |
| §6.3 diff | Task 9 |
| §6.4 数值与并集冲突 | Task 9 |
| §6.5 落盘/乱序/并发 | Task 5/9 |
| §6.6 调度配套 | Task 10 |
| §6.7 deprecated 语义 | Task 3/9 |
| §7.1 错误区分 | Task 7 |
| §7.2 passthrough warn / degraded | Task 5/7 |
| §8 Admin API | Task 11 |
| §9 UI | Task 13 |
| §10 测试策略 | Task 1-5/9/14 |
| §11 验收标准 | 验收清单 |
| §12 已知限制 | Task 10 注释 + README |

**发现并已修补的问题：**

1. **§7.2 的「warn 去重容量上限 64 + 分钟节流」在任务分解中缺失。** 已在下方补为 Task 7 的追加步骤。
2. **`ModelRegistry::from_file` 会丢弃内置默认。** Task 4 的实现只用文件中的行构造 registry，意味着 `models.json` 一旦存在就**完全替代**内置默认，而 spec §4.6 说的是「覆盖层叠加其上」。已在下方补为 Task 4 的追加步骤。

### Task 4 追加步骤：覆盖层必须叠加在内置默认之上

- [ ] **Step 6: 写失败测试**

```rust
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
```

- [ ] **Step 7: 运行确认失败**

Run: `cargo test --lib overlay_merges_onto_builtin 2>&1 | tail -10`
Expected: FAIL —— `claude-fable-5` 不在结果中。

- [ ] **Step 8: 修改 `from_file` 先铺内置默认再叠加**

把 `from_file` 中 `let mut rows = file.models;` 替换为：

```rust
        // 覆盖层叠加在内置默认之上（spec §4.6）：
        // 同 upstream_id 用文件中的行替换，其余内置行保留。
        // 这保证「文件里只写一行覆写」不会让其他模型消失。
        let mut rows = builtin_rows();
        for incoming in file.models {
            match rows.iter_mut().find(|r| r.upstream_id == incoming.upstream_id) {
                Some(existing) => *existing = incoming,
                None => rows.push(incoming),
            }
        }
```

- [ ] **Step 9: 运行确认通过**

Run: `cargo test --lib model_registry:: 2>&1 | tail -10`
Expected: 全部通过。注意 `rejects_duplicate_upstream_id` 等测试的 `sample_row` 用的是 `claude-x` 这类非内置 id，不受影响。

- [ ] **Step 10: Commit**

```bash
git add src/anthropic/model_registry.rs
git commit -m "fix(model-registry): 覆盖层叠加在内置默认之上而非替换

from_file 原先只用文件中的行构造 registry，导致 models.json 一旦存在就
完全替代内置默认——只写一行覆写会让其余 22 个模型消失。改为先铺
builtin_rows() 再按 upstreamId 叠加。"
```

### Task 7 追加步骤：passthrough warn 去重需有界

- [ ] **Step 6: 写失败测试**

在 `src/anthropic/model_registry.rs` 的 `mod tests` 追加：

```rust
    #[test]
    fn passthrough_warn_dedup_is_bounded() {
        // 客户端可控的 model 字段（handlers.rs:623）意味着无界去重集合可被打爆内存
        for i in 0..200 {
            note_passthrough_model(&format!("unknown-model-{}", i));
        }
        assert!(passthrough_warn_cache_len() <= 64, "去重集合必须有上限");
    }
```

- [ ] **Step 7: 运行确认失败**

Run: `cargo test --lib passthrough_warn_dedup 2>&1 | tail -10`
Expected: `cannot find function note_passthrough_model`。

- [ ] **Step 8: 实现有界去重**

在 `src/anthropic/model_registry.rs` 中加入：

```rust
/// passthrough 命中时的 warn 去重集合容量上限。
/// `MessagesRequest.model` 由客户端控制（handlers.rs:623），
/// 无界集合可被打爆内存。
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
```

在 `converter.rs` 的 `convert_request_with_mode` 中，`Resolution::Passthrough` 分支里调用 `super::model_registry::note_passthrough_model(&req.model);`（需要把 Task 7 Step 3 中合并的 `Mapped | Passthrough` 分支拆开）。

- [ ] **Step 9: 运行确认通过**

Run: `cargo test 2>&1 | tail -15`
Expected: 全部通过。

- [ ] **Step 10: Commit**

```bash
git add src/anthropic/model_registry.rs src/anthropic/converter.rs
git commit -m "feat(model-registry): passthrough warn 去重集合设上限

MessagesRequest.model 由客户端控制（handlers.rs:623），无界去重集合可被
打爆内存。容量上限 64，满则清空重来（粗粒度节流）。"
```

**类型一致性检查：** 已核对以下跨任务引用的名称一致
- `ModelRow` 字段名在 Task 1 定义、Task 5/9/11 使用，均为 snake_case Rust 字段 + camelCase JSON
- `pinned` 中的字段名字符串统一为 camelCase（`contextWindow` / `displayName` / `maxOutputTokens` / `exposedId` / `exposeThinkingVariant`），Task 5 `merge_synced_row` 与 Task 11 `PATCHABLE_PINNED_FIELDS` 一致
- `Resolution` / `RejectReason` 在 Task 2 定义，Task 6/7 使用
- `install_registry` / `current_registry` 在 Task 6 定义，Task 9/12/14 使用
- `ModelRegistryStore::mutate` 签名在 Task 5 定义，Task 9/11 使用
- `ModelListFetcher::fetch` 返回 `BoxFuture<'_, Result<Vec<UpstreamModel>, String>>`，Task 9 定义并在 `token_manager.rs` 实现

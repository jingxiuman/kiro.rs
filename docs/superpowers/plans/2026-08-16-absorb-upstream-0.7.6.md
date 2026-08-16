# 吸收上游 v0.7.6 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把上游 ZyphrZero/kiro.rs v0.7.6 中我们确实缺失的三项能力（GPT-5.6 原生 reasoning effort、OpenAI 协议侧会话亲和、opus-5 内置 1M 行）落到本 fork，并补一条"未知上游事件名"告警，为后续判断是否接入 `metadataEvent.tokenUsage` 取证。

**Architecture:** 五处独立改动 + 一次部署取证。effort 走 `additionalModelRequestFields.reasoning.effort`（GPT 家族）与 `output_config.effort`（Claude 家族）二选一；下发失败时由 provider 层统一「剥字段重试一次」兜底，保证该字段永远只是优化项、不会把请求打死。会话亲和把 OpenAI 侧的 `prompt_cache_key`/亲和头规范成我们已有的 `metadata.user_id` 通道，复用既有的 conversationId 推导；同时在缓存计量的隔离种子里给这条来源加 `key_id` 命名空间，避免客户端可控字符串跨 Key 串味。

**Tech Stack:** Rust 2024 / axum / serde / uuid / tracing；测试用 `cargo test`。

## Global Constraints

- 本 fork 与上游已分叉到 `v0.7.2` 基线（`git merge-base HEAD v0.7.6` = `8f074e5`），**不做 merge**，逐项手工对齐。
- 不新增配置项、不新增依赖、不做数据迁移（与上游 0.7.6 的兼容承诺一致）。
- 不改动 `stream.rs` / `websearch_loop.rs` 的用量聚合逻辑——那部分是上游围绕自身流式架构的重写，我们的 thinking 闩锁/块顺序/占位符行为已分叉且经生产验证，本轮不动。
- 已确认**无需**吸收的两项：`cache_metering` 的 JSON `user_id` 解析（`src/anthropic/metadata.rs:15-21` 已实现并有测试）、生产 opus-5 的 1M 窗口（`data/models.json` 已有 `origin=manual` 行）。
- 所有新增注释用中文，与周边风格一致；wire 字段名一律 snake_case（`output_config` / `reasoning`），外层 key 保持 camelCase。

## 与上游的两处刻意偏离

1. **GPT 家族判定用前缀而非三个硬编码名**。上游写死 `gpt-5.6-sol|terra|luna`；我们的模型注册表本来就用 `gpt-5` 前缀伪行做家族通配（`src/anthropic/model_registry.rs:181-203`，`MatchKind::Prefix`），硬编码三个名字会和这套设计打架，且上游每出一个新 gpt 型号就要改代码。改用 `starts_with("gpt-")`，安全性由 Task 2 的 400 兜底承担。
2. **OpenAI 侧 `metadata.user_id` 用 `openai_client__session_<uuid>` 而非上游的裸 `session_<uuid>`**。我们的 `extract_session_id` 用 `split_once("_session_")` 解析（`src/anthropic/metadata.rs:23`），裸 `session_<uuid>` 前面没有下划线，会解析失败——照抄上游格式在我们这儿等于什么都没做。带前缀的形式既能被现有解析器识别，又给 Task 4 提供了「这条 session 来自 OpenAI 协议」的判据。

## File Structure

| 文件 | 职责 | 本轮改动 |
|---|---|---|
| `src/kiro/model/requests/kiro.rs` | Kiro 上游请求 wire 类型 | 新增 `KiroReasoningConfig`，`AdditionalModelRequestFields` 加 `reasoning` 字段 |
| `src/anthropic/converter.rs` | Anthropic→Kiro 请求转换、模型能力判定 | 新增 GPT 家族判定与 `EffortTier::None`，`build_additional_model_request_fields` 按家族二选一 |
| `src/kiro/provider.rs` | 上游调用与多凭据重试 | 400 命中 effort 字段被拒时剥字段重试一次 |
| `src/kiro/endpoint/mod.rs` | 上游错误报文分类 | 新增 `default_is_effort_field_rejected` |
| `src/anthropic/metadata.rs` | session 标识解析 | 新增 OpenAI 会话 user_id 的构造/识别常量与函数 |
| `src/anthropic/openai.rs` | `/v1/chat/completions` 入口 | 接收 `prompt_cache_key` 与亲和头，产出 `metadata` |
| `src/anthropic/responses.rs` | `/v1/responses` 入口 | 同上 |
| `src/anthropic/cache_metering.rs` | 模拟缓存计量 | `isolation_seed` 对 OpenAI 来源 session 加 `key_id` 命名空间 |
| `src/kiro/model/events/base.rs` | 上游事件解析 | 未知事件名 warn-once |
| `src/anthropic/model_registry.rs` | 模型注册表内置默认 | 补 `claude-opus-5` 1M 行 |

---

### Task 1: GPT 家族的原生 reasoning.effort 映射

**背景：** 当前 `model_supports_native_reasoning_builtin`（`converter.rs:288`）只认 claude 系，gpt-5.6-* 一律返回 `false` → `build_additional_model_request_fields` 提前 return None → OpenAI 客户端发的 `reasoning_effort` 被**静默丢弃**，一个字段都不下发。

**Files:**
- Modify: `src/kiro/model/requests/kiro.rs:57-80`（`AdditionalModelRequestFields` / 新增 `KiroReasoningConfig`）
- Modify: `src/anthropic/converter.rs:288-298`（builtin 判定）、`395-421`（`EffortTier`）、`448-483`（`build_additional_model_request_fields`）
- Test: 同文件内 `mod tests`

**Interfaces:**
- Produces: `pub struct KiroReasoningConfig { pub effort: String }`（`crate::kiro::model::requests::kiro`）
- Produces: `AdditionalModelRequestFields { output_config: Option<KiroOutputConfig>, reasoning: Option<KiroReasoningConfig> }` —— 两个字段互斥，同一请求只填其一
- Produces: `fn model_uses_gpt_reasoning_effort(model_id: &str) -> bool`（`converter.rs` 私有）
- Consumes: 无

- [ ] **Step 1: 写 wire 格式的失败测试**

在 `src/kiro/model/requests/kiro.rs` 的 `mod tests` 末尾追加：

```rust
    #[test]
    fn test_gpt_reasoning_effort_wire_format() {
        let fields = AdditionalModelRequestFields {
            output_config: None,
            reasoning: Some(KiroReasoningConfig {
                effort: "xhigh".to_string(),
            }),
        };
        let v = serde_json::to_value(&fields).unwrap();
        assert_eq!(v["reasoning"]["effort"], "xhigh");
        assert!(
            v.get("output_config").is_none(),
            "GPT 路径不得同时下发 output_config，got {v}"
        );
    }
```

同时把已有的 `test_output_config_wire_format`（约 `kiro.rs:118`）里的结构体字面量补上新字段并加反向断言：

```rust
        let fields = AdditionalModelRequestFields {
            output_config: Some(KiroOutputConfig {
                effort: "max".to_string(),
            }),
            reasoning: None,
        };
        let v = serde_json::to_value(&fields).unwrap();
        assert_eq!(v["output_config"]["effort"], "max");
        assert!(v.get("reasoning").is_none());
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test kiro::model::requests::kiro -- --nocapture`
Expected: 编译失败 —— `struct AdditionalModelRequestFields has no field named reasoning` / `cannot find struct KiroReasoningConfig`

- [ ] **Step 3: 加 wire 类型**

`src/kiro/model/requests/kiro.rs`，把 `AdditionalModelRequestFields` 改为：

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdditionalModelRequestFields {
    /// Claude 家族的 effort 开关（`output_config.effort`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_config: Option<KiroOutputConfig>,
    /// GPT 家族的 effort 开关（`reasoning.effort`）。
    ///
    /// 两个字段互斥：同一请求按模型家族只填其一，填错家族会被上游以
    /// `Invalid additionalModelRequestFields` 400 掉（provider 层有剥字段兜底）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<KiroReasoningConfig>,
}
```

并在 `KiroOutputConfig` 定义之后追加：

```rust
/// GPT 家族（Kiro 侧 Mantle 后端）接受的 effort 控制字段。
///
/// 与 `KiroOutputConfig` 同为 snake_case 内层键，不继承外层的 camelCase。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KiroReasoningConfig {
    pub effort: String,
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test kiro::model::requests::kiro`
Expected: PASS（`test_output_config_wire_format`、`test_gpt_reasoning_effort_wire_format` 均通过）

- [ ] **Step 5: 写 converter 侧的失败测试**

在 `src/anthropic/converter.rs` 的 `mod tests` 中追加。注意 `build_additional_model_request_fields` 需要 `MessagesRequest`，沿用该文件已有的构造辅助（若 `mod tests` 里已有 `fn req_with_effort` 之类辅助则复用；没有就用下面这个本地辅助）：

```rust
    fn req_with_effort(model: &str, effort: &str) -> MessagesRequest {
        MessagesRequest {
            model: model.to_string(),
            max_tokens: 64,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::Value::String("hi".to_string()),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: Some(crate::anthropic::types::OutputConfig {
                effort: effort.to_string(),
            }),
            metadata: None,
        }
    }

    /// GPT 家族走 reasoning.effort，Claude 家族走 output_config.effort，互斥。
    #[test]
    fn gpt_family_uses_reasoning_effort_field() {
        let f = build_additional_model_request_fields(&req_with_effort("gpt-5.6-sol", "high"), "gpt-5.6-sol")
            .expect("GPT 显式 effort 必须下发");
        assert_eq!(f.reasoning.as_ref().unwrap().effort, "high");
        assert!(f.output_config.is_none());

        let f = build_additional_model_request_fields(
            &req_with_effort("claude-opus-4.7", "high"),
            "claude-opus-4.7",
        )
        .expect("Claude 显式 effort 必须下发");
        assert_eq!(f.output_config.as_ref().unwrap().effort, "high");
        assert!(f.reasoning.is_none());
    }

    /// 前缀判定覆盖未来型号，且不误伤 claude。
    #[test]
    fn gpt_family_detection_is_prefix_based() {
        assert!(model_uses_gpt_reasoning_effort("gpt-5.6-terra"));
        assert!(model_uses_gpt_reasoning_effort("gpt-5.9-nova"));
        assert!(model_uses_gpt_reasoning_effort("GPT-5.6-Luna"));
        assert!(!model_uses_gpt_reasoning_effort("claude-opus-5"));
    }

    /// `none` 档只对 GPT 有意义；Claude 侧降级为 high，避免上游 400。
    #[test]
    fn none_effort_only_survives_on_gpt_family() {
        let f = build_additional_model_request_fields(&req_with_effort("gpt-5.6-sol", "none"), "gpt-5.6-sol")
            .unwrap();
        assert_eq!(f.reasoning.as_ref().unwrap().effort, "none");

        let f = build_additional_model_request_fields(
            &req_with_effort("claude-opus-4.7", "none"),
            "claude-opus-4.7",
        )
        .unwrap();
        assert_eq!(f.output_config.as_ref().unwrap().effort, "high");
    }

    /// 没显式要 reasoning 的 GPT 请求维持现状：一个字段都不下发。
    #[test]
    fn gpt_without_explicit_effort_sends_nothing() {
        let mut req = req_with_effort("gpt-5.6-sol", "high");
        req.output_config = None;
        assert!(build_additional_model_request_fields(&req, "gpt-5.6-sol").is_none());
    }
```

- [ ] **Step 6: 运行测试确认失败**

Run: `cargo test anthropic::converter -- gpt_family none_effort`
Expected: 编译失败 `cannot find function model_uses_gpt_reasoning_effort`

- [ ] **Step 7: 实现 converter 侧改动**

7a. `src/anthropic/converter.rs` 顶部 import 补 `KiroReasoningConfig`：

```rust
use crate::kiro::model::requests::kiro::{
    AdditionalModelRequestFields, KiroOutputConfig, KiroReasoningConfig,
};
```

7b. 在 `model_supports_native_reasoning`（`converter.rs:271`）之前插入：

```rust
/// 该模型是否用 GPT 家族的 `reasoning.effort` 而非 Claude 的 `output_config.effort`。
///
/// 用前缀而非硬编码型号名：模型注册表本就用 `gpt-5` 前缀伪行做家族通配
/// （见 `model_registry::builtin_rows`），逐个列名字会和那套设计打架，且每出
/// 一个新型号都要改代码。判错的代价由 provider 层的「剥字段重试一次」兜底
/// （见 `KiroProvider::call_api_with_retry` 的 400 分支）。
fn model_uses_gpt_reasoning_effort(model_id: &str) -> bool {
    model_id.to_ascii_lowercase().starts_with("gpt-")
}
```

7c. `model_supports_native_reasoning_builtin`（`converter.rs:288`）函数体首行插入：

```rust
    if model_uses_gpt_reasoning_effort(model_id) {
        return true;
    }
```

7d. `EffortTier`（`converter.rs:395`）加 `None` 档：枚举加 `None,` 变体（放在 `Low` 之前），`parse` 加 `"none" => Some(Self::None),`，`as_str` 加 `Self::None => "none",`。

7e. `normalize_effort_for_model`（`converter.rs:407` 起）里的 `normalized` 计算改为：

```rust
    // `none` 只有 GPT 家族接受；Claude 侧收到会 400，降级到 high。
    let normalized = if requested == EffortTier::None && !model_uses_gpt_reasoning_effort(model_id) {
        EffortTier::High
    } else if requested == EffortTier::XHigh && !model_supports_xhigh_effort(model_id) {
        EffortTier::High
    } else {
        requested
    };
```

7f. `build_additional_model_request_fields`（`converter.rs:480`）的返回改为：

```rust
    let effort = select_native_reasoning_effort(req, model_id);
    if model_uses_gpt_reasoning_effort(model_id) {
        Some(AdditionalModelRequestFields {
            output_config: None,
            reasoning: Some(KiroReasoningConfig { effort }),
        })
    } else {
        Some(AdditionalModelRequestFields {
            output_config: Some(KiroOutputConfig { effort }),
            reasoning: None,
        })
    }
```

- [ ] **Step 8: 运行全量 lib 测试**

Run: `cargo test`
Expected: 全绿。若 `model_supports_native_reasoning_allows_confirmed_and_5_family`（`converter.rs:2179`）等既有测试因新增字段编译失败，补 `reasoning: None` 字面量即可，**不要**改断言语义。

- [ ] **Step 9: 提交**

```bash
git add src/kiro/model/requests/kiro.rs src/anthropic/converter.rs
git commit -m "feat(effort): GPT 家族走 reasoning.effort 原生 wire 字段

对齐上游 v0.7.6 #64。此前 gpt-5.6-* 在 model_supports_native_reasoning
返回 false，客户端的 reasoning_effort 被静默丢弃。家族判定改用 gpt- 前缀
（我们的注册表本就以前缀伪行通配），并新增 none 档（仅 GPT 接受）。"
```

---

### Task 2: effort 字段被拒时剥字段重试一次

**背景：** Task 1 的 wire 格式来自上游贡献者 PR，我们没有实测证据。effort 是纯优化项——不下发只是丢失档位、不影响正确性——所以正确的失败语义是「退化到现状」，而不是把请求打 400。爆炸半径限于显式带 effort/thinking 的请求。

**Files:**
- Modify: `src/kiro/endpoint/mod.rs`（新增分类函数，紧邻 `default_is_client_validation_error`，约 `:187`）
- Modify: `src/kiro/provider.rs:570-580`（循环前建可变 body）、`:703`（发送点）、`:813-820`（400 分支）
- Test: 两文件各自的 `mod tests`

**Interfaces:**
- Consumes: Task 1 产出的 `additionalModelRequestFields.reasoning` wire 形态
- Produces: `pub fn default_is_effort_field_rejected(body: &str) -> bool`（`crate::kiro::endpoint`）
- Produces: `fn strip_additional_model_request_fields(body: &str) -> Option<String>`（`provider.rs` 私有关联函数 `KiroProvider::strip_additional_model_request_fields`）

- [ ] **Step 1: 写分类函数的失败测试**

`src/kiro/endpoint/mod.rs` 的 `mod tests` 追加：

```rust
    #[test]
    fn test_effort_field_rejection_detected() {
        assert!(default_is_effort_field_rejected(
            r#"{"__type":"ValidationException","message":"Invalid additionalModelRequestFields"}"#
        ));
        assert!(default_is_effort_field_rejected(
            r#"{"message":"additionalModelRequestFields is not supported for this model"}"#
        ));
        // 与 effort 无关的 400 不得命中
        assert!(!default_is_effort_field_rejected(
            r#"{"__type":"ValidationException","message":"Improperly formed request."}"#
        ));
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test kiro::endpoint -- effort_field_rejection`
Expected: 编译失败 `cannot find function default_is_effort_field_rejected`

- [ ] **Step 3: 实现分类函数**

`src/kiro/endpoint/mod.rs`，在 `default_is_client_validation_error` 之后追加：

```rust
/// 上游是否因 `additionalModelRequestFields` 本身而拒绝请求。
///
/// 该字段是纯优化项（effort 档位），命中时调用方应剥掉它重试一次，而不是把
/// 请求打死。判据用字段名裸子串即可——这是我们自己发出的 key 名，只会出现在
/// 针对它的错误报文里，不会与正常内容冲突。
pub fn default_is_effort_field_rejected(body: &str) -> bool {
    body.contains("additionalModelRequestFields")
}
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test kiro::endpoint -- effort_field_rejection`
Expected: PASS

- [ ] **Step 5: 写剥字段函数的失败测试**

`src/kiro/provider.rs` 的 `mod tests` 追加：

```rust
    #[test]
    fn strip_additional_model_request_fields_removes_only_that_key() {
        let body = r#"{"conversationState":{"conversationId":"c1"},"additionalModelRequestFields":{"reasoning":{"effort":"high"}}}"#;
        let stripped = KiroProvider::strip_additional_model_request_fields(body)
            .expect("含该字段时必须返回剥离后的报文");
        let v: serde_json::Value = serde_json::from_str(&stripped).unwrap();
        assert!(v.get("additionalModelRequestFields").is_none());
        assert_eq!(v["conversationState"]["conversationId"], "c1");
    }

    #[test]
    fn strip_additional_model_request_fields_returns_none_when_absent() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        assert!(KiroProvider::strip_additional_model_request_fields(body).is_none());
        // 非 JSON 报文不得 panic
        assert!(KiroProvider::strip_additional_model_request_fields("not json").is_none());
    }
```

- [ ] **Step 6: 运行确认失败**

Run: `cargo test kiro::provider -- strip_additional_model_request_fields`
Expected: 编译失败 `no function or associated item named strip_additional_model_request_fields`

- [ ] **Step 7: 实现剥字段 + 接线**

7a. 在 `impl KiroProvider` 内（紧邻 `extract_model_from_request`，`provider.rs:1083`）加：

```rust
    /// 从已序列化的请求体里剥掉 `additionalModelRequestFields`。
    ///
    /// 返回 `None` 表示报文里本来就没有该字段（或不是合法 JSON），调用方据此
    /// 判断「剥了也没用，别重试」。
    fn strip_additional_model_request_fields(request_body: &str) -> Option<String> {
        let mut json: serde_json::Value = serde_json::from_str(request_body).ok()?;
        let obj = json.as_object_mut()?;
        obj.remove("additionalModelRequestFields")?;
        serde_json::to_string(&json).ok()
    }
```

7b. `call_api_with_retry`（`provider.rs:570`）在 `let model = Self::extract_model_from_request(request_body);`（`:585`）之后加：

```rust
        // effort 字段被上游拒绝时会就地剥掉并重试；用 Cow 避免正常路径的拷贝。
        let mut effective_body: std::borrow::Cow<'_, str> = std::borrow::Cow::Borrowed(request_body);
        let mut effort_field_stripped = false;
```

7c. 发送点（`provider.rs:703`）`let body = endpoint.transform_api_body(request_body, &rctx);` 改为：

```rust
            let body = endpoint.transform_api_body(effective_body.as_ref(), &rctx);
```

7d. 把 400 分支（`provider.rs:813-820`）整体替换为：

```rust
            if status.as_u16() == 400 {
                // effort 字段本身被拒：剥掉重试一次。该字段只是档位优化，
                // 失败语义应当是「退化到不带 effort 的现状」，而不是打死请求。
                if !effort_field_stripped
                    && endpoint.is_effort_field_rejected(&body)
                    && let Some(retry_body) =
                        Self::strip_additional_model_request_fields(effective_body.as_ref())
                {
                    tracing::warn!(
                        model = ?model,
                        "上游拒绝 additionalModelRequestFields，剥掉该字段重试一次: {}",
                        body
                    );
                    effective_body = std::borrow::Cow::Owned(retry_body);
                    effort_field_stripped = true;
                    Self::emit_attempt(
                        sink, attempt, ctx.id, endpoint_name, Some(400),
                        outcome::BAD_REQUEST, Some(&body), attempt_start, proxy_url.as_deref(),
                    );
                    continue;
                }
                Self::emit_attempt(
                    sink, attempt, ctx.id, endpoint_name, Some(400),
                    outcome::BAD_REQUEST, Some(&body), attempt_start, proxy_url.as_deref(),
                );
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }
```

7e. `KiroEndpoint` trait（`src/kiro/endpoint/mod.rs`）加默认方法，与既有的 `is_client_validation_error` 并列：

```rust
    /// 上游是否因 `additionalModelRequestFields` 本身而 400。
    fn is_effort_field_rejected(&self, body: &str) -> bool {
        default_is_effort_field_rejected(body)
    }
```

- [ ] **Step 8: 运行确认通过**

Run: `cargo test`
Expected: 全绿。若 `let ... && let ...` 链式 let 在本 crate 的 edition 下不可用，拆成嵌套 `if`。

- [ ] **Step 9: 提交**

```bash
git add src/kiro/provider.rs src/kiro/endpoint/mod.rs
git commit -m "feat(provider): effort 字段被上游拒绝时剥字段重试一次

additionalModelRequestFields 是纯优化项，失败语义应为退化到不带 effort，
而不是把请求打 400。为 Task 1 的 GPT reasoning.effort wire 格式兜底。"
```

---

### Task 3: OpenAI 协议侧的会话亲和

**背景：** `converter.rs:623-629` 早就会从 `metadata.user_id` 提取 session UUID 当 conversationId，但 OpenAI 两个入口写死 `metadata: None`（`openai.rs:245`、`responses.rs:344`），导致 gpt/Codex 客户端每次请求都是随机 conversationId，上游 prompt cache 永远打不中。

**Files:**
- Modify: `src/anthropic/metadata.rs`（新增构造/识别函数）
- Modify: `src/anthropic/openai.rs:39-56`（请求体加字段）、`:61-65`（handler 收 headers）、`:138`（转换函数签名）、`:245`（metadata 出口）
- Modify: `src/anthropic/responses.rs:110-129`（请求体加字段）、`:140-144`（handler 收 headers）、`:216-218`（转换函数签名）、`:344`（metadata 出口）
- Test: `src/anthropic/metadata.rs`、`src/anthropic/openai.rs` 的 `mod tests`

**Interfaces:**
- Consumes: 既有 `extract_session_id(user_id: &str) -> Option<String>`（`metadata.rs:9`）
- Produces: `pub(crate) const OPENAI_SESSION_USER_ID_PREFIX: &str = "openai_client__session_";`
- Produces: `pub(crate) fn openai_session_user_id(uuid: &Uuid) -> String`
- Produces: `pub(crate) fn is_openai_client_session(user_id: &str) -> bool` —— Task 4 消费
- Produces: `pub(super) fn resolve_session_metadata(prompt_cache_key: Option<&str>, headers: &HeaderMap) -> Option<Metadata>`（`openai.rs`，`responses.rs` 复用）

- [ ] **Step 1: 写 metadata.rs 的失败测试**

`src/anthropic/metadata.rs` 的 `mod tests` 追加：

```rust
    #[test]
    fn openai_session_user_id_roundtrips_through_extract() {
        let uuid = uuid::Uuid::parse_str(SESSION_ID).unwrap();
        let user_id = super::openai_session_user_id(&uuid);
        assert_eq!(user_id, format!("openai_client__session_{SESSION_ID}"));
        // 必须能被既有解析器识别，否则 conversationId 推导拿不到它
        assert_eq!(extract_session_id(&user_id), Some(SESSION_ID.to_string()));
        assert!(super::is_openai_client_session(&user_id));
        // Claude Code 的两种形态都不得被误判为 OpenAI 来源
        assert!(!super::is_openai_client_session(&format!(
            "user_xxx_account__session_{SESSION_ID}"
        )));
        assert!(!super::is_openai_client_session(&format!(
            r#"{{"session_id":"{SESSION_ID}"}}"#
        )));
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test anthropic::metadata`
Expected: 编译失败 `cannot find function openai_session_user_id`

- [ ] **Step 3: 实现 metadata 辅助**

`src/anthropic/metadata.rs` 在 `extract_session_id` 之后追加：

```rust
/// OpenAI 协议侧会话标识的 `metadata.user_id` 前缀。
///
/// 不用上游的裸 `session_<uuid>`：本文件的解析走 `split_once("_session_")`，
/// 裸形式前面没有下划线会解析失败。带前缀既能被既有解析器识别，又给缓存计量
/// 的隔离种子提供了「这条 session 来自客户端可控字段」的判据
/// （见 `cache_metering::isolation_seed`）。
pub(crate) const OPENAI_SESSION_USER_ID_PREFIX: &str = "openai_client__session_";

/// 把 OpenAI 侧解析出的会话 UUID 包装成 `metadata.user_id`。
pub(crate) fn openai_session_user_id(session: &Uuid) -> String {
    format!("{OPENAI_SESSION_USER_ID_PREFIX}{}", session.hyphenated())
}

/// 该 `user_id` 是否由 OpenAI 协议入口构造。
pub(crate) fn is_openai_client_session(user_id: &str) -> bool {
    user_id.trim().starts_with(OPENAI_SESSION_USER_ID_PREFIX)
}
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test anthropic::metadata`
Expected: PASS

- [ ] **Step 5: 写 openai.rs 的失败测试**

`src/anthropic/openai.rs` 的 `mod tests` 追加：

```rust
    const UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
    const UUID_B: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

    #[test]
    fn session_metadata_prefers_prompt_cache_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-session-affinity", UUID_B.parse().unwrap());
        let md = resolve_session_metadata(Some(UUID_A), &headers).unwrap();
        assert_eq!(
            md.user_id.as_deref(),
            Some(format!("openai_client__session_{UUID_A}").as_str())
        );
    }

    #[test]
    fn session_metadata_falls_through_header_chain() {
        let mut headers = HeaderMap::new();
        headers.insert("x-client-request-id", UUID_B.parse().unwrap());
        let md = resolve_session_metadata(None, &headers).unwrap();
        assert_eq!(
            md.user_id.as_deref(),
            Some(format!("openai_client__session_{UUID_B}").as_str())
        );
    }

    #[test]
    fn session_metadata_accepts_session_prefixed_value() {
        let headers = HeaderMap::new();
        let md = resolve_session_metadata(Some(&format!("session_{UUID_A}")), &headers).unwrap();
        assert_eq!(
            md.user_id.as_deref(),
            Some(format!("openai_client__session_{UUID_A}").as_str())
        );
    }

    /// 非 UUID 值维持无状态：不构造 metadata，conversationId 仍随机。
    #[test]
    fn session_metadata_ignores_non_uuid_values() {
        let headers = HeaderMap::new();
        assert!(resolve_session_metadata(Some("my-app-v1"), &headers).is_none());
        assert!(resolve_session_metadata(None, &headers).is_none());
    }
```

- [ ] **Step 6: 运行确认失败**

Run: `cargo test anthropic::openai -- session_metadata`
Expected: 编译失败 `cannot find function resolve_session_metadata`

- [ ] **Step 7: 实现 openai.rs 改动**

7a. import 补 `HeaderMap` 与 `Metadata`：

```rust
use axum::http::{HeaderMap, StatusCode, header};
use super::types::{
    DEFAULT_MAX_TOKENS, Message, MessagesRequest, Metadata, OutputConfig, SystemMessage, Tool,
};
```

7b. `ChatCompletionRequest`（`openai.rs:39`）末尾加字段：

```rust
    /// OpenAI 官方的会话缓存键。用于把同一会话钉到同一 conversationId，
    /// 让上游 prompt cache 能命中。
    #[serde(default)]
    pub prompt_cache_key: Option<String>,
```

7c. 在 `// ==== Handler ====` 之前加：

```rust
/// 从 OpenAI 请求体或会话亲和请求头中提取并规范化会话 UUID。
///
/// 取值顺序：`prompt_cache_key` → `x-session-affinity` → `x-client-request-id`
/// → `session_id`。值可带 `session_` 前缀。解析不出 UUID 时返回 `None`，
/// 保持无状态语义（conversationId 随机，与改动前一致）。
pub(super) fn resolve_session_metadata(
    prompt_cache_key: Option<&str>,
    headers: &HeaderMap,
) -> Option<Metadata> {
    let candidates = [
        prompt_cache_key,
        headers.get("x-session-affinity").and_then(|v| v.to_str().ok()),
        headers.get("x-client-request-id").and_then(|v| v.to_str().ok()),
        headers.get("session_id").and_then(|v| v.to_str().ok()),
    ];

    candidates.into_iter().flatten().find_map(|candidate| {
        let raw = candidate.trim().strip_prefix("session_").unwrap_or(candidate.trim());
        let uuid = Uuid::parse_str(raw).ok()?;
        Some(Metadata {
            user_id: Some(super::metadata::openai_session_user_id(&uuid)),
        })
    })
}
```

7d. `post_chat_completions`（`openai.rs:61`）签名加 `headers: HeaderMap,`（放在 `Json(req)` **之前**——axum 要求 body extractor 最后），函数体开头加：

```rust
    let metadata = resolve_session_metadata(req.prompt_cache_key.as_deref(), &headers);
```

并把调用改为 `openai_to_anthropic(req, metadata)`。

7e. `openai_to_anthropic`（`openai.rs:138`）签名改为：

```rust
fn openai_to_anthropic(
    req: ChatCompletionRequest,
    metadata: Option<Metadata>,
) -> Result<MessagesRequest, String> {
```

出口（`openai.rs:245`）`metadata: None,` 改为 `metadata,`。

- [ ] **Step 8: 运行确认通过**

Run: `cargo test anthropic::openai`
Expected: PASS。既有测试若因 `openai_to_anthropic` 签名变化编译失败，补第二个实参 `None`。

- [ ] **Step 9: 同样接线 responses.rs**

9a. import 补 `HeaderMap` 与 `Metadata`（与 7a 同形）。

9b. `ResponsesRequest`（`responses.rs:110`）末尾加：

```rust
    /// OpenAI 官方的会话缓存键（codex CLI 会发）。见 `openai::resolve_session_metadata`。
    #[serde(default)]
    pub prompt_cache_key: Option<String>,
```

9c. `post_responses`（`responses.rs:140`）签名加 `headers: HeaderMap,`（`Json(req)` 之前），函数体开头加：

```rust
    let metadata = super::openai::resolve_session_metadata(req.prompt_cache_key.as_deref(), &headers);
```

调用改为 `responses_to_anthropic(req, metadata)`。

9d. `responses_to_anthropic`（`responses.rs:216`）加第二参 `metadata: Option<Metadata>`，出口（`responses.rs:344`）`metadata: None,` 改为 `metadata,`。

9e. 追加测试到 `responses.rs` 的 `mod tests`：

```rust
    /// codex 的 prompt_cache_key 必须一路带到 MessagesRequest.metadata。
    #[test]
    fn responses_carries_prompt_cache_key_into_metadata() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "gpt-5.6-sol",
            "input": "hi",
            "prompt_cache_key": uuid
        }))
        .unwrap();
        let md = super::super::openai::resolve_session_metadata(
            req.prompt_cache_key.as_deref(),
            &HeaderMap::new(),
        );
        let (anthropic, _) = responses_to_anthropic(req, md).unwrap();
        assert_eq!(
            anthropic.metadata.and_then(|m| m.user_id).as_deref(),
            Some(format!("openai_client__session_{uuid}").as_str())
        );
    }
```

- [ ] **Step 10: 运行全量测试并确认路由仍编译**

Run: `cargo test && cargo build`
Expected: 全绿。若 axum 因 extractor 顺序报 trait 不满足，把 `headers: HeaderMap` 移到 `Json(...)` 正前方。

- [ ] **Step 11: 提交**

```bash
git add src/anthropic/metadata.rs src/anthropic/openai.rs src/anthropic/responses.rs
git commit -m "feat(openai): 会话亲和标识透传到 conversationId

对齐上游 v0.7.6 #64。converter 早就会从 metadata.user_id 推导 conversationId，
但 OpenAI 两个入口写死 metadata: None，gpt/codex 客户端每次都是随机会话，
上游 prompt cache 永远打不中。user_id 用 openai_client__session_ 前缀，
既能被既有 extract_session_id 识别，也供计量隔离区分来源。"
```

---

### Task 4: OpenAI 来源的 session 在计量隔离种子里加 key_id 命名空间

**背景：** Task 3 让客户端可控字符串流进了 `isolation_seed`（`cache_metering.rs:568`）。`prompt_cache_key` 是文档化的公开字段，值可能是人手写的固定串；两个不同客户端 Key 撞上同一个 UUID 就会共享模拟缓存命名空间，产生跨用户虚假 `cache_read`——正是该函数注释里「主 Key 无 session 时不模拟缓存」要防的那类事。Claude Code 的 session 是自己生成的随机 UUID，碰撞概率不是一个量级，维持原样。

**Files:**
- Modify: `src/anthropic/cache_metering.rs:568-576`（`isolation_seed` 的第 1 级）
- Test: 同文件 `mod tests`

**Interfaces:**
- Consumes: Task 3 的 `is_openai_client_session(user_id: &str) -> bool`
- Produces: 无（内部行为变更）

- [ ] **Step 1: 写失败测试**

`src/anthropic/cache_metering.rs` 的 `mod tests` 追加：

```rust
    /// OpenAI 来源的 session 必须按 key 命名空间隔离：
    /// 同一个 prompt_cache_key 落到不同客户端 Key 不得互相命中。
    #[test]
    fn openai_session_seed_is_namespaced_by_key() {
        let session = "550e8400-e29b-41d4-a716-446655440000";
        let mk = |user_id: &str| {
            let mut req = base_request();
            req.metadata = Some(super::super::types::Metadata {
                user_id: Some(user_id.to_string()),
            });
            req
        };
        let openai = format!("openai_client__session_{session}");
        let a = isolation_seed(&mk(&openai), 7);
        let b = isolation_seed(&mk(&openai), 9);
        assert!(a.is_some());
        assert_ne!(a, b, "不同客户端 Key 不得共享 OpenAI 来源的种子");

        // Claude Code 来源维持原样：跨 Key 共享同一会话
        let cc = format!("user_xxx_account__session_{session}");
        assert_eq!(isolation_seed(&mk(&cc), 7), isolation_seed(&mk(&cc), 9));
    }
```

`base_request()` 用该文件 `mod tests` 里已有的请求构造辅助；若没有同名辅助，就照该文件既有测试（如 `cache_metering.rs:1378` 附近）的 `MessagesRequest` 字面量写法内联构造一个最小请求。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test anthropic::cache_metering -- openai_session_seed`
Expected: FAIL —— `assert_ne!` 触发（当前两者都是 `sess:<uuid>`）

- [ ] **Step 3: 实现**

`src/anthropic/cache_metering.rs` 的 `isolation_seed` 第一段改为：

```rust
    if let Some(user_id) = req.metadata.as_ref().and_then(|m| m.user_id.as_deref())
        && let Some(session) = extract_session_id(user_id)
    {
        // OpenAI 协议侧的 session 来自客户端可控的 prompt_cache_key / 亲和头，
        // 值可能是人手写的固定串（如 "my-app-v1" 风格的 UUID 复用），跨 Key
        // 碰撞是现实风险 → 按 key 命名空间隔离。Claude Code 的 session 是客户端
        // 自生成的随机 UUID，维持跨 Key 共享，不改既有命中率。
        if super::metadata::is_openai_client_session(user_id) {
            return Some(format!("sess:{key_id}:{session}"));
        }
        return Some(format!("sess:{session}"));
    }
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test anthropic::cache_metering`
Expected: PASS，且该文件既有的会话隔离测试（`cache_metering.rs:1360` 起）不受影响。

- [ ] **Step 5: 提交**

```bash
git add src/anthropic/cache_metering.rs
git commit -m "fix(metering): OpenAI 来源的 session 种子按 key 命名空间隔离

prompt_cache_key 是客户端可控的公开字段，值可能是手写固定串；不加命名空间
时两个客户端 Key 撞同一个值就会共享模拟缓存，产生跨用户虚假 cache_read。
Claude Code 的随机 session UUID 维持跨 Key 共享，命中率不变。"
```

---

### Task 5: 未知上游事件名 warn-once

**背景：** `base.rs:34` 的 `_ => Self::Unknown` 把事件名当场丢掉，上游发了 `metadataEvent` 我们也看不见。这是本轮取证 `metadataEvent.tokenUsage` 是否存在的手段，也是以后上游改协议时的第一道信号。

**Files:**
- Modify: `src/kiro/model/events/base.rs:120-140`（`from_frame` 的 Unknown 分支）
- Test: 同文件 `mod tests`

**Interfaces:**
- Produces: 无公开 API 变更（`Event::Unknown {}` 形状不变，避免波及 `stream.rs` 的匹配臂）

- [ ] **Step 1: 写失败测试**

`src/kiro/model/events/base.rs` 的 `mod tests` 追加：

```rust
    /// 未知事件仍解析为 Unknown（形状不变），但事件名会被记录一次。
    #[test]
    fn unknown_event_name_is_recorded_once() {
        super::reset_seen_unknown_events_for_test();
        assert!(super::note_unknown_event("metadataEvent"), "首次出现必须上报");
        assert!(!super::note_unknown_event("metadataEvent"), "同名不得重复上报");
        assert!(super::note_unknown_event("someOtherEvent"), "新名字仍要上报");
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test kiro::model::events::base -- unknown_event_name`
Expected: 编译失败 `cannot find function note_unknown_event`

- [ ] **Step 3: 实现**

`src/kiro/model/events/base.rs` 文件内加：

```rust
/// 已上报过的未知事件名。只在 Unknown 分支访问，不在热路径上。
static SEEN_UNKNOWN_EVENTS: std::sync::OnceLock<parking_lot::Mutex<std::collections::HashSet<String>>> =
    std::sync::OnceLock::new();

/// 登记一个未知事件名，返回 `true` 表示本进程内首次见到（调用方据此只 warn 一次）。
fn note_unknown_event(name: &str) -> bool {
    SEEN_UNKNOWN_EVENTS
        .get_or_init(Default::default)
        .lock()
        .insert(name.to_string())
}

#[cfg(test)]
fn reset_seen_unknown_events_for_test() {
    SEEN_UNKNOWN_EVENTS.get_or_init(Default::default).lock().clear();
}
```

并把 `from_frame` 的 Unknown 分支（`base.rs:137`）改为：

```rust
            EventType::Unknown => {
                // 上游新增事件不会自己冒头——这里是唯一能看见它的地方。
                // 按事件名去重，避免流式高频刷屏。
                if let Some(name) = frame.event_type()
                    && note_unknown_event(name)
                {
                    tracing::warn!(event_type = %name, "上游返回了未识别的事件类型（本进程首次）");
                }
                Ok(Self::Unknown {})
            }
```

注意：`frame` 在该 match 之前若已被移动进其他分支，需把 `frame.event_type()` 的取值提前到 match 之前存成 `let event_name = frame.event_type().map(str::to_string);`，再在分支里用它。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test kiro::model::events`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add src/kiro/model/events/base.rs
git commit -m "feat(events): 未知上游事件名 warn 一次

此前 _ => Unknown 把事件名当场丢掉，上游改协议我们零感知。用于取证
metadataEvent.tokenUsage 是否存在，也是后续协议变更的第一道信号。"
```

---

### Task 6: 内置注册表补 claude-opus-5 的 1M 行

**背景：** 生产 `data/models.json` 已有 `claude-opus-5 ctx=1000000 origin=manual`，但 `builtin_rows()`（`model_registry.rs:207-219`）缺这一行——全新部署或 models.json 重建时会落到 passthrough 的 200_000，导致 `ContextUsage` 上报缩小 5 倍（上游 0.7.6 修的正是这个）。

**Files:**
- Modify: `src/anthropic/model_registry.rs:207-219`（`builtin_rows` 的 vec 与 `claude` 闭包的 `match_substrings`）
- Test: 同文件 `mod tests`

**Interfaces:** 无

- [ ] **Step 1: 写失败测试**

`src/anthropic/model_registry.rs` 的 `mod tests` 追加：

```rust
    /// opus-5 必须在内置默认里就是 1M：漏配会让 ContextUsage 上报缩小 5 倍，
    /// 客户端进度条与自动压缩阈值全部失准（上游 v0.7.6 修复项）。
    #[test]
    fn builtin_rows_include_opus_5_with_1m_window() {
        let row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-5")
            .expect("内置默认必须含 claude-opus-5");
        assert_eq!(row.context_window, 1_000_000);
        // opus-4.5 不得被顺手改成 1M
        let old = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.5")
            .unwrap();
        assert_eq!(old.context_window, 200_000);
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test anthropic::model_registry -- builtin_rows_include_opus_5`
Expected: FAIL —— `内置默认必须含 claude-opus-5`

- [ ] **Step 3: 实现**

3a. `src/anthropic/model_registry.rs` 的 `vec![...]`，在 `claude("claude-fable-5", ...)` 之前插入（`sort_order` 取 35，落在 gpt 的 30 与 fable-5 的 40 之间，不改动既有行的排序值）：

```rust
        claude("claude-opus-5", "claude-opus-5", "Claude Opus 5", 1781481600, 1_000_000, 35),
```

3b. `claude` 闭包的 `match_substrings`（`model_registry.rs:171-178`）加一臂，与 `claude-sonnet-5` 同形：

```rust
            "claude-opus-5" => {
                vec!["opus-5".to_string(), "opus5".to_string(), "opus.5".to_string()]
            }
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test anthropic::model_registry`
Expected: PASS。特别确认既有的映射回归测试（`model_registry.rs:1282` 起「改造前基线」那组）仍全绿——若 `opus-5` 子串把 `claude-opus-4-5` 抢走了，说明子串匹配优先级有问题，此时删掉 3b 只保留 3a。

- [ ] **Step 5: 提交**

```bash
git add src/anthropic/model_registry.rs
git commit -m "fix(registry): 内置默认补 claude-opus-5 的 1M 上下文行

对齐上游 v0.7.6 #61。生产 models.json 已有手工行，但内置默认缺失，
全新部署会落到 passthrough 200k，使 ContextUsage 上报缩小 5 倍。"
```

---

### Task 7: 构建、部署与生产取证

**Files:** 无代码改动；产出 `reports/2026-08-16-upstream-0.7.6/verification.md`

**Interfaces:**
- Consumes: Task 1–6 的全部改动

- [ ] **Step 1: 全量校验**

Run:
```bash
cargo test 2>&1 | tail -20
cargo clippy --all-targets -- -D warnings 2>&1 | tail -20
```
Expected: 测试全绿；clippy 无 warning。

- [ ] **Step 2: 版本号与 CHANGELOG**

`Cargo.toml` 的 `version` 升至 `0.9.13`；`CHANGELOG.md` 顶部加一节，列出本轮四项（GPT effort / 会话亲和 / 计量命名空间 / opus-5 内置行 / 未知事件告警）与「吸收上游 v0.7.6」的出处。提交：

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "chore: 版本号升至 0.9.13（吸收上游 v0.7.6）"
```

- [ ] **Step 3: 部署**

按容器重建核对集逐项确认（端口 / 挂载 / 命令 / 网络 / restart 五项），在 `/workspace/podman_project/kiro` 下构建并重建容器。**必须**确认容器加入 unified 网络、端口仍为 19095。部署后从消费方网络内验证，而不是从宿主机。

- [ ] **Step 4: 取证 A —— metadataEvent 是否存在**

发一次经过我们代理的真实请求（任意 claude 模型，走 `/v1/messages`），然后查容器日志：

```bash
podman logs --since 10m <容器名> 2>&1 | grep "未识别的事件类型"
```

记录结论到 `reports/2026-08-16-upstream-0.7.6/verification.md`：
- 若出现 `event_type=metadataEvent` → 上游确实在发精确用量，**下一轮**单独立项评估是否接入（会牵动 cache_metering 的口径，不在本计划内）。
- 若未出现 → 记「本次上游未观察到 metadataEvent」，并写明观察窗口与请求数——**不要**写成「上游不发」，样本量不足以支撑全称否定。

- [ ] **Step 5: 取证 B —— GPT effort 实际生效**

对 `/v1/chat/completions` 发两次同 prompt 请求，分别带 `"reasoning_effort": "low"` 与 `"reasoning_effort": "xhigh"`，模型用 `gpt-5.6-sol`。判据：
- 两次都必须是 2xx（若 400 且日志出现「上游拒绝 additionalModelRequestFields，剥掉该字段重试一次」→ 说明 Task 1 的 wire 猜测错了，兜底生效、请求未死；据此回滚 Task 1 的家族判定并记录）。
- 两次的响应时长/输出长度应有可见差异（上游 effort 阶梯的既有观察）。若无差异，只能记「未观察到差异」，不得断言「生效」。

- [ ] **Step 6: 取证 C —— OpenAI 会话亲和命中**

对 `/v1/chat/completions` 用**同一个** `prompt_cache_key`（合法 UUID）连发两次带长 system 的请求，检查第二次的 `usage.prompt_tokens_details.cached_tokens`（或管理面板 trace 里的 cache_read）是否非零；再换一个 UUID 发一次，确认它不继承前一个会话的缓存。数字对不上时先回答「差的去哪了」，不要跳过。

- [ ] **Step 7: 记录并提交验证报告**

```bash
git add reports/2026-08-16-upstream-0.7.6/verification.md
git commit -m "docs: v0.7.6 吸收项的生产验证记录"
```

# 模型视图按凭据组对齐 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `/v1/models` 与请求时模型校验按调用 key 所在凭据组的可用模型并集收窄;模型同步的 credentialSupport 拉取覆盖全部可用凭据。

**Architecture:** 注册表(model_registry)保持全局纯净不动。token_manager 新增纯函数计算"组支持集"(并集,含无记录凭据则不设限);handlers 在 `/v1/models` 和各请求入口消费它;model_sync 在既有轮次之后补一轮"仅 credentialSupport"的全凭据拉取。

**Tech Stack:** Rust / axum / tokio;测试用 `cargo test`(内置单测,无外部依赖)。

**Spec:** `docs/superpowers/specs/2026-07-31-group-model-view-design.md`

## Global Constraints

- 仓库:`/workspace/podman_project/kiro/kiro.rs`,分支 `feat/ops-module`。
- "无 credentialSupport 记录 = 未知 = 放行/不设限",全链路一致(spec §设计-1/5)。
- 并集语义:组内任一凭据支持即展示/放行(spec 已确认,不做交集)。
- `auto` 恒展示、恒放行。
- 校验用 resolve 后的 **upstream id**,不用请求原名。
- 组不支持 → HTTP 404 + error type `not_found_error`,消息 `model not supported for this key group: <requested>`。
- 注册表 `models`/`aliases` 的权威/采样/消失判定逻辑一律不改。
- 每个任务完成即 `cargo test` 全绿再 commit;提交信息中文,风格同仓库近期提交。

---

### Task 1: token_manager 组支持集查询

**Files:**
- Modify: `src/kiro/token_manager.rs`(在 `credential_supports_model` 附近加纯函数;`impl MultiTokenManager` 加包装方法,放 `credential_support()` 访问器旁,约 1316 行)
- Modify: `src/kiro/provider.rs`(加 `token_manager()` 访问器,`token_manager` 字段目前私有,约 104 行)

**Interfaces:**
- Produces:
  - `pub fn group_supported_models_from(creds: &[(u64, Vec<String>, bool)], group: Option<&str>, support: &HashMap<String, Vec<String>>) -> Option<HashSet<String>>`(纯函数;creds 元组为 `(凭据id, groups, disabled)`)
  - `impl MultiTokenManager { pub fn group_supported_models(&self, group: Option<&str>) -> Option<HashSet<String>> }`
  - `impl KiroProvider { pub fn token_manager(&self) -> &Arc<MultiTokenManager> }`

- [ ] **Step 1: 写失败测试**(`token_manager.rs` 既有 `#[cfg(test)]` 模块内)

```rust
#[test]
fn group_supported_models_union_of_group_credentials() {
    use std::collections::HashMap;
    let support: HashMap<String, Vec<String>> = HashMap::from([
        ("1".into(), vec!["auto".into(), "claude-opus-5".into(), "claude-opus-4.8".into()]),
        ("2".into(), vec!["auto".into(), "claude-opus-5".into()]),
    ]);
    let creds = vec![
        (1u64, vec!["own".to_string()], false),
        (2u64, vec!["own".to_string()], false),
        (3u64, vec!["other".to_string()], false),
    ];
    // 组内并集:1 号有 opus-4.8,2 号没有 → 并集含 opus-4.8
    let set = group_supported_models_from(&creds, Some("own"), &support)
        .expect("全部有记录,应返回 Some");
    assert!(set.contains("claude-opus-4.8"));
    assert!(set.contains("claude-opus-5"));
}

#[test]
fn group_supported_models_unknown_credential_means_unrestricted() {
    use std::collections::HashMap;
    // 凭据 2 无记录 → 整组不设限(None)
    let support: HashMap<String, Vec<String>> =
        HashMap::from([("1".into(), vec!["claude-opus-5".into()])]);
    let creds = vec![
        (1u64, vec!["own".to_string()], false),
        (2u64, vec!["own".to_string()], false),
    ];
    assert!(group_supported_models_from(&creds, Some("own"), &support).is_none());
}

#[test]
fn group_supported_models_skips_disabled_and_empty_group_is_none() {
    use std::collections::HashMap;
    let support: HashMap<String, Vec<String>> = HashMap::from([
        ("1".into(), vec!["claude-opus-5".into()]),
        ("2".into(), vec!["glm-5".into()]),
    ]);
    // 2 号禁用:不计入并集,也不因它触发"无记录放行"以外的语义
    let creds = vec![
        (1u64, vec!["own".to_string()], false),
        (2u64, vec!["own".to_string()], true),
    ];
    let set = group_supported_models_from(&creds, Some("own"), &support).unwrap();
    assert!(set.contains("claude-opus-5"));
    assert!(!set.contains("glm-5"));
    // 组内无凭据 → None(不设限,由既有"无可用凭据"路径兜底报错)
    assert!(group_supported_models_from(&creds, Some("ghost"), &support).is_none());
    // group=None → 按全部未禁用凭据计算
    let all = group_supported_models_from(&creds, None, &support).unwrap();
    assert!(all.contains("claude-opus-5"));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test group_supported_models -- --nocapture`
Expected: 编译错误 `cannot find function group_supported_models_from`

- [ ] **Step 3: 实现纯函数 + 方法 + 访问器**

`token_manager.rs`(紧邻 `credential_supports_model` 之后):

```rust
/// 组内凭据可用上游模型 id 的并集。
///
/// 返回 `None` 表示"不设限",两种情形:
/// - 组内存在无 `credential_support` 记录的未禁用凭据(未知=放行,
///   与 `credential_supports_model` 的保守语义一致);
/// - 组内没有任何未禁用凭据(交由既有"无可用凭据"错误路径,不在本层报错)。
///
/// `creds` 元组:(凭据 id, groups, disabled)。禁用凭据不计入。
pub fn group_supported_models_from(
    creds: &[(u64, Vec<String>, bool)],
    group: Option<&str>,
    support: &std::collections::HashMap<String, Vec<String>>,
) -> Option<std::collections::HashSet<String>> {
    let mut union = std::collections::HashSet::new();
    let mut any = false;
    for (id, groups, disabled) in creds {
        if *disabled || !group_matches(groups, group) {
            continue;
        }
        any = true;
        match support.get(&id.to_string()) {
            Some(models) => union.extend(models.iter().cloned()),
            None => return None,
        }
    }
    if any { Some(union) } else { None }
}
```

`impl MultiTokenManager`(放在 `credential_support()` 访问器旁):

```rust
/// 见 [`group_supported_models_from`]。entries 快照 + credential_support 缓存。
pub fn group_supported_models(
    &self,
    group: Option<&str>,
) -> Option<std::collections::HashSet<String>> {
    let creds: Vec<(u64, Vec<String>, bool)> = self
        .entries
        .lock()
        .iter()
        .map(|e| (e.id, e.credentials.groups.clone(), e.disabled))
        .collect();
    let support = self.credential_support.read();
    group_supported_models_from(&creds, group, &support)
}
```

`provider.rs`(`impl KiroProvider` 内,靠近其它访问器):

```rust
/// 暴露 token_manager,供 handlers 查询组支持集等只读信息。
pub fn token_manager(&self) -> &Arc<MultiTokenManager> {
    &self.token_manager
}
```

注意:`entries` 元素字段名以实际代码为准(探查 `struct` 定义,预期为
`e.id` / `e.credentials.groups` / `e.disabled`;若不同,按实际字段改)。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test group_supported_models`
Expected: 3 passed

- [ ] **Step 5: 提交**

```bash
git add src/kiro/token_manager.rs src/kiro/provider.rs
git commit -m "feat(token_manager): 组支持集查询(并集,无记录不设限)"
```

---

### Task 2: GET /v1/models 按组收窄

**Files:**
- Modify: `src/anthropic/handlers.rs`(`get_models` 约 651 行;`available_models` 约 619 行)
- 路由不动:`/v1/models` 已在 `auth_middleware` 之后(router.rs:82),`KeyContext` 经 request extension 传入(与 `post_messages` 同款取法,探查其签名照抄)。

**Interfaces:**
- Consumes: `MultiTokenManager::group_supported_models`(Task 1)、`current_registry().resolve(...)`。
- Produces: `fn filter_models_by_group(models: Vec<Model>, allowed: &std::collections::HashSet<String>) -> Vec<Model>`(测试用纯函数)。

- [ ] **Step 1: 写失败测试**(handlers.rs 测试模块,仿 `models_endpoint_visibility_follows_installed_registry` 的注册表装载写法)

```rust
#[test]
fn models_list_narrowed_by_group_support_set() {
    // exposed id 经 resolve 得 upstream id,按 upstream id ∈ allowed 过滤;
    // "auto" 恒保留。
    let registry = crate::anthropic::model_registry::current_registry();
    let models = registry.exposed_models();
    assert!(!models.is_empty(), "内置注册表不应为空");
    let mut allowed = std::collections::HashSet::new();
    allowed.insert("claude-opus-5".to_string());
    let filtered = filter_models_by_group(models.clone(), &allowed);
    for m in &filtered {
        if m.id == "auto" { continue; }
        let upstream = match registry.resolve(&m.id, false) {
            crate::anthropic::model_registry::Resolution::Mapped { upstream_id, .. }
            | crate::anthropic::model_registry::Resolution::Passthrough { upstream_id, .. } => upstream_id,
            _ => panic!("exposed 模型必可解析: {}", m.id),
        };
        assert_eq!(upstream, "claude-opus-5", "过滤后只应剩 allowed 内的模型: {}", m.id);
    }
    assert!(filtered.len() < models.len(), "应有收窄效果");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test models_list_narrowed_by_group_support_set`
Expected: 编译错误 `cannot find function filter_models_by_group`

- [ ] **Step 3: 实现过滤 + 接入 handler**

handlers.rs(`available_models` 旁):

```rust
/// 按组支持集过滤 /v1/models 输出。
/// `allowed` 为 upstream id 集合;exposed id 先经注册表 resolve 再比对。
/// `auto` 恒保留;解析失败的行保留(保守,不因视图层吞行)。
fn filter_models_by_group(
    models: Vec<Model>,
    allowed: &std::collections::HashSet<String>,
) -> Vec<Model> {
    use crate::anthropic::model_registry::{current_registry, Resolution};
    let registry = current_registry();
    models
        .into_iter()
        .filter(|m| {
            if m.id == "auto" {
                return true;
            }
            match registry.resolve(&m.id, false) {
                Resolution::Mapped { upstream_id, .. }
                | Resolution::Passthrough { upstream_id, .. } => {
                    upstream_id == "auto" || allowed.contains(&upstream_id)
                }
                _ => true,
            }
        })
        .collect()
}
```

`get_models` 改造(签名对齐 `post_messages` 取 KeyContext 的既有方式,通常为
`Extension(key_ctx): Extension<KeyContext>`;`State(state): State<AppState>`):

```rust
pub async fn get_models(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
) -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");
    let mut models = available_models();
    if let Some(provider) = &state.kiro_provider
        && let Some(allowed) = provider
            .token_manager()
            .group_supported_models(key_ctx.group.as_deref())
    {
        models = filter_models_by_group(models, &allowed);
    }
    Json(ModelsResponse { object: "list".to_string(), data: models })
}
```

零回归底线:`group_supported_models` 返回 `None`(无 credentialSupport 数据、
或组含未知凭据)时不过滤——既有端点级测试
(`models_endpoint_visibility_follows_installed_registry` 等)必须原样通过,
若它们直接调用 `get_models` 需按新签名补 Extension/State 构造。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cargo test models_`
Expected: 新旧测试全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/anthropic/handlers.rs
git commit -m "feat(models): /v1/models 按调用 key 凭据组并集收窄"
```

---

### Task 3: 请求时组支持校验(404 not_found_error)

**Files:**
- Modify: `src/anthropic/handlers.rs`(`post_messages`、`post_messages_cc`、`count_tokens`,在 `override_thinking_from_model_name` / 转换之前统一插入)
- Modify: `src/anthropic/openai.rs`、`src/anthropic/responses.rs`(各自的模型入口,同一 helper)

**Interfaces:**
- Consumes: Task 1 的 `group_supported_models`。
- Produces:
  - `pub(crate) fn group_model_check(state: &AppState, group: Option<&str>, requested_model: &str) -> Result<(), (StatusCode, Json<ErrorResponse>)>`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn group_model_check_rejects_with_404_not_found_error() {
    // 纯逻辑层测试:allowed 集合里没有该模型的 upstream id → 404 not_found_error
    let mut allowed = std::collections::HashSet::new();
    allowed.insert("glm-5".to_string());
    let err = group_model_check_against(&allowed, "claude-opus-5")
        .expect_err("组不支持应拒绝");
    assert_eq!(err.0, StatusCode::NOT_FOUND);
    assert_eq!(err.1.0.error.r#type, "not_found_error");
    assert_eq!(
        err.1.0.error.message,
        "model not supported for this key group: claude-opus-5"
    );
}

#[test]
fn group_model_check_allows_auto_alias_and_unresolvable() {
    let allowed: std::collections::HashSet<String> =
        [("glm-5".to_string())].into_iter().collect();
    // auto 恒放行
    assert!(group_model_check_against(&allowed, "auto").is_ok());
    // 注册表不认识的名字:放行,交由既有 conversion_error_response 路径报 400,
    // 两类错误不混同
    assert!(group_model_check_against(&allowed, "no-such-model-xyz").is_ok());
}
```

(注:`ErrorResponse` 内部结构按实际定义取字段;若无 `r#type` 直接字段,
用序列化后 JSON 断言,参考既有 404/400 报文测试写法,如
`upstream_overload_maps_to_529_overloaded_error`。)

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test group_model_check`
Expected: 编译错误 `cannot find function group_model_check_against`

- [ ] **Step 3: 实现 helper 并接入各入口**

handlers.rs(`conversion_error_response` 旁):

```rust
/// 组支持校验的纯判定层:requested 先 resolve 成 upstream id 再比对。
/// 放行条件:auto / 解析失败(交给下游既有 400 路径)/ allowed 命中。
pub(crate) fn group_model_check_against(
    allowed: &std::collections::HashSet<String>,
    requested_model: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    use crate::anthropic::model_registry::{allow_passthrough, current_registry, Resolution};
    let upstream = match current_registry().resolve(requested_model, allow_passthrough()) {
        Resolution::Mapped { upstream_id, .. }
        | Resolution::Passthrough { upstream_id, .. } => upstream_id,
        _ => return Ok(()),
    };
    if upstream == "auto" || allowed.contains(&upstream) {
        return Ok(());
    }
    Err((
        StatusCode::NOT_FOUND,
        Json(ErrorResponse::new(
            "not_found_error",
            format!("model not supported for this key group: {}", requested_model),
        )),
    ))
}

/// 入口封装:取组支持集(None=不设限)后调用判定层。
pub(crate) fn group_model_check(
    state: &AppState,
    group: Option<&str>,
    requested_model: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let Some(provider) = &state.kiro_provider else { return Ok(()) };
    match provider.token_manager().group_supported_models(group) {
        Some(allowed) => group_model_check_against(&allowed, requested_model),
        None => Ok(()),
    }
}
```

各入口接入(以 `post_messages` 为例,在 websearch 分流之前、拿到
`payload.model` 后):

```rust
if let Err(resp) = group_model_check(&state, key_ctx.group.as_deref(), &payload.model) {
    hook.record(0, 0, 0, (0, 0), 0.0, "error");
    return resp.into_response();
}
```

`post_messages_cc`、`count_tokens`、`openai.rs` chat_completions、
`responses.rs` 同点接入(各文件里模型名字段可能为 `payload.model` /
`req.model`,以实际为准;openai/responses 若 handler 无 `state`/`key_ctx`
参数,按其既有签名补,均在 auth_middleware 之后,extension 可取)。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cargo test`
Expected: 全部 PASS(重点看 handlers/openai/responses 既有报文测试无回归)

- [ ] **Step 5: 提交**

```bash
git add src/anthropic/handlers.rs src/anthropic/openai.rs src/anthropic/responses.rs
git commit -m "feat(handlers): 组不支持的模型请求提前拦截为 404 not_found_error"
```

---

### Task 4: 模型同步 credentialSupport 全凭据覆盖

**Files:**
- Modify: `src/anthropic/model_sync.rs`(`sync_once_with`,在主轮次 fetch 之后、落盘之前补一轮;约 304 行 `let FetchOutcome {...}` 处)

**Interfaces:**
- Consumes: 既有 `self.fetcher.candidate_credential_ids()`、`self.fetch_from(&ids)`。
- Produces: 无新公开接口;行为变化——每轮同步后 `file.credential_support` 覆盖全部可用凭据。

- [ ] **Step 1: 写失败测试**(model_sync.rs 测试模块,仿既有 `MockFetcher`/`calls` 记录写法,如 N4 测试)

```rust
#[tokio::test]
async fn credential_support_covers_all_usable_credentials() {
    // 探针为 1,candidate 为 1..=5:权威轮只拉 1,
    // 补充轮应把 2..=5 也各拉一次,per_credential 覆盖 5 张。
    // MockFetcher 让每张凭据返回不同模型集,断言:
    // 1) fetch 调用序列包含 1,2,3,4,5 各一次(1 不重复拉);
    // 2) 落盘后的 credential_support 有 5 个键;
    // 3) 凭据 3 拉取失败时:其余 4 张照常写入,3 的旧记录保留不清空。
}
```

(具体构造照抄既有测试的 MockFetcher 与临时 store 写法;此测试文件里已有
可复用的 fixture,先读 `model_sync.rs` 测试模块再落笔,断言点以上述三条为准。)

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test credential_support_covers_all_usable`
Expected: FAIL(补充轮不存在,per_credential 只有探针 1 张)

- [ ] **Step 3: 实现补充轮**

`sync_once_with` 中,主轮次 `outcome` 解构之后(约 304 行):

```rust
let FetchOutcome { union, mut per_credential, any_nonempty, .. } = outcome;

// ---- credentialSupport 补充轮:覆盖主轮次之外的全部可用凭据 ----
// 只补 per_credential(凭据可用模型集),不参与 union/权威消失判定——
// 注册表轮次语义不变(spec §设计-4)。失败凭据不写入,落盘时旧记录自然保留
// (下方 insert 按键覆盖)。每日一轮、串行拉取,凭据上百也可接受。
{
    let mut extra_ids = self.fetcher.candidate_credential_ids();
    extra_ids.retain(|id| !credential_ids.contains(id));
    extra_ids.sort_unstable();
    if !extra_ids.is_empty() {
        let extra = self.fetch_from(&extra_ids).await;
        for (cred, models) in extra.per_credential {
            per_credential.insert(cred, models);
        }
    }
}
```

注意:`fetch_from` 现有实现对失败凭据不会往 `per_credential` 写条目
(读 183-233 行确认;若会写空列表,需在合并时跳过失败项)。禁用凭据由
`candidate_credential_ids()` 天然排除(它只返回可用凭据,与既有采样一致)。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cargo test --lib model_sync`
Expected: 新旧测试全部 PASS(尤其 N4/I3 等轮次语义测试不回归)

- [ ] **Step 5: 提交**

```bash
git add src/anthropic/model_sync.rs
git commit -m "feat(model_sync): credentialSupport 补充轮覆盖全部可用凭据"
```

---

### Task 5: 全量验证与收尾

**Files:** 无新改动(验证任务)。

- [ ] **Step 1: 全量测试 + clippy + release 构建**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo build --release
```
Expected: 全绿、无警告、构建成功。

- [ ] **Step 2: 手工冒烟(可选,需容器环境)**

构建镜像后以两个不同 group 的 key 调 `GET /v1/models`,确认列表差异;
用组内不支持的模型调 `POST /v1/messages`,确认 404 `not_found_error` 报文。

- [ ] **Step 3: 收尾提交(如有 CHANGELOG 惯例则更新)**

```bash
git log --oneline -5   # 确认本计划 4 个 feat 提交齐全
```

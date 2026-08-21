# 凭据↔代理批量重绑 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `POST /api/admin/credentials/proxy/batch`（全有或全无的凭据↔代理批量重绑）与管理面板「批量绑代理」弹窗。

**Architecture:** 三层各加一小块：`token_manager` 加单锁内批量更新+单次落盘的 `update_credentials_batch`；`service` 加整体校验（凭据存在、代理存在且 enabled 且非 autoDisabled、无重复 credentialId）后调用它的 `assign_proxies_batch`；handler/router 照抄 `assign_proxies_round_robin` 形态。UI 照 `batch-edit-credential-dialog.tsx` 的既有模式新建弹窗组件。

**Tech Stack:** Rust (axum/serde/parking_lot) + React (shadcn Dialog / tanstack-query / sonner toast)。

Spec：`docs/superpowers/specs/2026-08-21-proxy-batch-rebind-design.md`（已用户确认）。

## Global Constraints

- 本 crate **无 lib target**：测试命令一律 `cargo test` / `cargo test <模块路径>`，`--lib` 会报错。
- 不新增配置项、不新增依赖、不做数据迁移。注释一律中文。
- 语义逐字来自 spec：**全有或全无**（任一条非法整批拒绝，返回**全部**失败条目）；`proxyId: null` = 解绑回落全局代理；**未出现在请求里的凭据不动**；同一 `credentialId` 重复出现 = 校验失败；`autoDisabled` 的代理不可选。
- 全量落盘**一次**（不是每凭据一次）。
- clippy 基线本就不干净（约百条既有告警集中在 token_manager.rs 等），验收标准是**所碰文件不新增告警**，不是全局零告警。
- 全量 `cargo test` 基线：938 passed / 1 failed，唯一允许的失败是既有基线项 `http_client::tests::reqwest_timeout_error_is_tagged_before_the_chain`（沙箱对 192.0.2.1 返 502 而非超时）。出现其他失败 = 真回归。
- UI 量级按 4 代理 / 8 凭据设计：不分页、不搜索。

## 已核实的现状（实现者不要重查，直接用）

- `MultiTokenManager::update_credential`（token_manager.rs:3498）**每次调用都 `persist_credentials()`**——这就是为什么需要新的批量函数而不是循环调它。批量函数的锁内写法参照同文件 `reassign_proxy_url`（:3537 附近）。
- `ProxyPoolManager::get_url`（proxy_pool.rs:347）**只检查 `enabled`，不检查 `auto_disabled`**——所以批量校验不能用它，要用 `list() -> Vec<ProxyEntry>`（:224）自建 map。`ProxyEntry` 含 `id/url/enabled/auto_disabled/health/latency_ms`。
- `AssignProxyRequest`（types.rs:816）：`proxy_id: Option<u64>`，null=清除。新类型的 derive/serde 属性照抄它（用 `grep -B4 'struct AssignProxyRequest' src/admin/types.rs` 看齐 camelCase rename）。
- service 测试构造范式（service.rs:4968 附近 `invalid_grant_...` 测试）：
  `MultiTokenManager::new(Config::default(), vec![KiroCredentials::default()...], None, None, false)` + `AdminService::new(manager, Vec::<String>::new(), Arc::new(ProxyPoolManager::new(None, TlsBackend::Rustls)), Arc::new(BalanceCache::new(None)))`。`ProxyPoolManager::new(None, ..)` = 不落盘；加代理用 `pool.add(url, None)`。
- `CredentialUpdate.proxy_url: Option<Option<String>>`：`Some(None)`=清除、`Some(Some(url))`=设置。
- UI：弹窗组件范式 = `admin-ui/src/components/batch-edit-credential-dialog.tsx`；API 封装在 `admin-ui/src/api/credentials.ts`（`assignProxyToCredential` 在 :377，`getProxyPool` 在 :344）；批量按钮挂在 `dashboard.tsx` 工具栏（搜「批量验活」定位同类按钮）。
- 前端构建：`cd admin-ui && bun run build`（产物 `admin-ui/dist` 被 rust-embed 打进二进制）。

---

### Task 1: `token_manager::update_credentials_batch`

**Files:**
- Modify: `src/kiro/token_manager.rs`（`update_credential` 之后新增函数；`mod tests` 加测试）

**Interfaces:**
- Produces: `pub fn update_credentials_batch(&self, updates: Vec<(u64, CredentialUpdate)>) -> Result<(), BatchUpdateError>`
- Produces: `pub enum BatchUpdateError { MissingCredentials(Vec<u64>), Persist(anyhow::Error) }`（与函数同文件定义，`pub` 供 service 使用）
- Consumes: 既有 `CredentialUpdate`、`persist_credentials`

- [ ] **Step 1: 写失败测试**

在 `src/kiro/token_manager.rs` 的 `mod tests` 末尾追加（id 按现有测试惯例从 1 递增；若断言失败提示 id 不符，参照同文件现有 `update_credential` 相关测试的取 id 方式调整，不要改产品代码迁就测试）：

```rust
    #[tokio::test]
    async fn update_credentials_batch_is_all_or_nothing() {
        let config = Config::default();
        let mut c1 = KiroCredentials::default();
        c1.refresh_token = Some("a".repeat(150));
        let mut c2 = KiroCredentials::default();
        c2.refresh_token = Some("b".repeat(150));
        let manager = MultiTokenManager::new(config, vec![c1, c2], None, None, false).unwrap();

        // 混入一个不存在的 id：整批拒绝，已存在的凭据不得被改动
        let err = manager
            .update_credentials_batch(vec![
                (1, CredentialUpdate {
                    proxy_url: Some(Some("http://p1:8080".to_string())),
                    ..CredentialUpdate::default()
                }),
                (999, CredentialUpdate {
                    proxy_url: Some(None),
                    ..CredentialUpdate::default()
                }),
            ])
            .unwrap_err();
        match err {
            BatchUpdateError::MissingCredentials(ids) => assert_eq!(ids, vec![999]),
            other => panic!("预期 MissingCredentials，实际 {other:?}"),
        }
        // 凭据 1 未被部分应用
        let creds = manager.export_credentials();
        assert!(creds.iter().all(|c| c.proxy_url.as_deref() != Some("http://p1:8080")));

        // 全合法：两条一次生效（设置 + 清除）
        manager
            .update_credentials_batch(vec![
                (1, CredentialUpdate {
                    proxy_url: Some(Some("http://p1:8080".to_string())),
                    ..CredentialUpdate::default()
                }),
                (2, CredentialUpdate {
                    proxy_url: Some(None),
                    ..CredentialUpdate::default()
                }),
            ])
            .unwrap();
        let creds = manager.export_credentials();
        assert!(creds.iter().any(|c| c.proxy_url.as_deref() == Some("http://p1:8080")));
    }
```

注意：`export_credentials` 若不存在，用同文件测试里现成的凭据读取途径（grep `proxy_url` 在 `mod tests` 里的既有断言写法）替换这两处读取断言，断言语义不变。

- [ ] **Step 2: 跑测试确认编译失败**

Run: `cargo test kiro::token_manager::tests::update_credentials_batch -- --nocapture`
Expected: 编译错误 `cannot find ... update_credentials_batch` / `BatchUpdateError`

- [ ] **Step 3: 实现**

在 `update_credential` 之后追加：

```rust
/// [`MultiTokenManager::update_credentials_batch`] 的失败形态。
#[derive(Debug)]
pub enum BatchUpdateError {
    /// 请求里有不存在的凭据 id（全部列出）。整批未应用。
    MissingCredentials(Vec<u64>),
    /// 内存态已更新但落盘失败——与单条 `update_credential` 在该场景下的语义一致。
    Persist(anyhow::Error),
}

impl MultiTokenManager {
    /// 单锁内批量更新凭据，落盘一次。全有或全无：任一 id 不存在则整批不应用。
    ///
    /// 与循环调 `update_credential` 的区别：后者每条各自加锁、各自落盘，
    /// 中途失败会留下半应用状态；批量重绑（admin 代理批量分配）要求原子语义。
    pub fn update_credentials_batch(
        &self,
        updates: Vec<(u64, CredentialUpdate)>,
    ) -> Result<(), BatchUpdateError> {
        {
            let mut entries = self.entries.lock();
            // 先整体校验存在性，报全部缺失 id 而不是第一个
            let missing: Vec<u64> = updates
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| !entries.iter().any(|e| e.id == *id))
                .collect();
            if !missing.is_empty() {
                return Err(BatchUpdateError::MissingCredentials(missing));
            }
            for (id, update) in updates {
                let entry = entries
                    .iter_mut()
                    .find(|e| e.id == id)
                    .expect("已通过存在性校验");
                if let Some(v) = update.proxy_url {
                    entry.credentials.proxy_url = v.filter(|s| !s.is_empty());
                }
                // 本函数当前只服务代理批量重绑，其余字段刻意不支持：
                // 收窄语义好过悄悄接受但行为与单条接口不一致的字段。
            }
        }
        self.persist_credentials().map_err(BatchUpdateError::Persist)?;
        Ok(())
    }
}
```

若 `impl MultiTokenManager` 块已存在则把函数放进现有块，不要新开重复 impl。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test kiro::token_manager::tests::update_credentials_batch`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/kiro/token_manager.rs
git commit -m "feat(token-manager): 单锁批量更新凭据代理，落盘一次

循环调 update_credential 会每条各自落盘且中途失败留半应用状态；
批量重绑要求全有或全无 + 单次持久化。"
```

---

### Task 2: `service::assign_proxies_batch` + 请求/失败类型

**Files:**
- Modify: `src/admin/types.rs`（`AssignProxyRequest` 附近新增三个类型）
- Modify: `src/admin/service.rs`（`assign_proxy_to_credential` 之后新增函数；`mod tests`（:4951）加测试）

**Interfaces:**
- Consumes: Task 1 的 `update_credentials_batch` / `BatchUpdateError`
- Produces: `pub struct AssignProxyBatchRequest { pub assignments: Vec<AssignmentEntry> }`
- Produces: `pub struct AssignmentEntry { pub credential_id: u64, pub proxy_id: Option<u64> }`（serde camelCase → JSON 为 `credentialId`/`proxyId`）
- Produces: `pub struct BatchAssignFailure { pub credential_id: u64, pub reason: String }`（Serialize，camelCase）
- Produces: `pub fn assign_proxies_batch(&self, req: AssignProxyBatchRequest) -> Result<usize, BatchAssignError>`；`pub enum BatchAssignError { Validation(Vec<BatchAssignFailure>), Internal(String) }`

- [ ] **Step 1: types.rs 加类型**

在 `AssignProxyRequest` 之后追加（derive/serde 属性照抄它，确保 camelCase）：

```rust
/// 批量重绑请求（`POST /credentials/proxy/batch`）
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignProxyBatchRequest {
    pub assignments: Vec<AssignmentEntry>,
}

/// 单条映射：proxy_id 为 null 表示解绑（回落全局代理）
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentEntry {
    pub credential_id: u64,
    #[serde(default)]
    pub proxy_id: Option<u64>,
}

/// 批量重绑校验失败的单条明细
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchAssignFailure {
    pub credential_id: u64,
    pub reason: String,
}
```

（若该文件的既有结构体没写 `rename_all` 而是逐字段 rename，跟随文件现状，别引入两种风格。）

- [ ] **Step 2: 写失败测试**

`src/admin/service.rs` 的 `mod tests` 追加：

```rust
    #[tokio::test]
    async fn assign_proxies_batch_validates_all_before_applying() {
        let mut c1 = KiroCredentials::default();
        c1.refresh_token = Some("a".repeat(150));
        let mut c2 = KiroCredentials::default();
        c2.refresh_token = Some("b".repeat(150));
        let manager = Arc::new(
            MultiTokenManager::new(Config::default(), vec![c1, c2], None, None, false).unwrap(),
        );
        let pool = Arc::new(ProxyPoolManager::new(None, crate::model::config::TlsBackend::Rustls));
        let p1 = pool.add("http://127.0.0.1:1080".to_string(), None).unwrap();
        let p2 = pool.add("http://127.0.0.1:1081".to_string(), None).unwrap();
        pool.set_enabled(p2.id, false).unwrap();
        let service = AdminService::new(
            manager.clone(),
            Vec::<String>::new(),
            pool,
            Arc::new(BalanceCache::new(None)),
        );

        // 三类非法混在一批：全部报出、整批不应用
        let err = service
            .assign_proxies_batch(AssignProxyBatchRequest {
                assignments: vec![
                    AssignmentEntry { credential_id: 1, proxy_id: Some(p1.id) }, // 本条合法
                    AssignmentEntry { credential_id: 999, proxy_id: Some(p1.id) }, // 凭据不存在
                    AssignmentEntry { credential_id: 2, proxy_id: Some(p2.id) },   // 代理已禁用
                    AssignmentEntry { credential_id: 1, proxy_id: None },          // 重复 credentialId
                ],
            })
            .unwrap_err();
        match err {
            BatchAssignError::Validation(fails) => {
                let ids: Vec<u64> = fails.iter().map(|f| f.credential_id).collect();
                assert!(ids.contains(&999), "缺失凭据必须报出: {fails:?}");
                assert!(ids.contains(&2), "禁用代理必须报出: {fails:?}");
                assert!(
                    fails.iter().filter(|f| f.credential_id == 1).count() >= 1,
                    "重复 credentialId 必须报出: {fails:?}"
                );
            }
            other => panic!("预期 Validation，实际 {other:?}"),
        }

        // 全合法：设置 + 解绑各一条
        let n = service
            .assign_proxies_batch(AssignProxyBatchRequest {
                assignments: vec![
                    AssignmentEntry { credential_id: 1, proxy_id: Some(p1.id) },
                    AssignmentEntry { credential_id: 2, proxy_id: None },
                ],
            })
            .unwrap();
        assert_eq!(n, 2);
    }
```

- [ ] **Step 3: 跑测试确认编译失败**

Run: `cargo test admin::service::tests::assign_proxies_batch -- --nocapture`
Expected: 编译错误 `cannot find ... assign_proxies_batch`

- [ ] **Step 4: 实现 service 函数**

`src/admin/service.rs`，紧跟 `assign_proxy_to_credential` 之后：

```rust
/// [`AdminService::assign_proxies_batch`] 的失败形态
#[derive(Debug)]
pub enum BatchAssignError {
    /// 校验失败：整批未应用，携带全部非法条目
    Validation(Vec<BatchAssignFailure>),
    /// 应用阶段内部错误（如落盘失败）
    Internal(String),
}

impl AdminService {
    /// 批量重绑凭据↔代理。全有或全无：先整体校验（凭据存在、代理存在且
    /// enabled 且非 autoDisabled、无重复 credentialId），任一非法整批拒绝并
    /// 返回**全部**失败条目；全部合法则单锁应用、落盘一次。
    ///
    /// 校验不用 `get_url`——它不检查 `auto_disabled`，而绑一个已被健康检查
    /// 踢掉的代理没有意义。
    pub fn assign_proxies_batch(
        &self,
        req: AssignProxyBatchRequest,
    ) -> Result<usize, BatchAssignError> {
        let mut failures: Vec<BatchAssignFailure> = Vec::new();

        // 重复 credentialId：歧义输入不猜测意图
        let mut seen = std::collections::HashSet::new();
        for a in &req.assignments {
            if !seen.insert(a.credential_id) {
                failures.push(BatchAssignFailure {
                    credential_id: a.credential_id,
                    reason: "同一凭据在请求里出现多次".to_string(),
                });
            }
        }

        // 代理校验：存在、enabled、非 autoDisabled
        let pool: std::collections::HashMap<u64, crate::admin::proxy_pool::ProxyEntry> =
            self.proxy_pool.list().into_iter().map(|e| (e.id, e)).collect();
        let mut updates: Vec<(u64, CredentialUpdate)> = Vec::new();
        for a in &req.assignments {
            let proxy_url = match a.proxy_id {
                None => None,
                Some(pid) => match pool.get(&pid) {
                    None => {
                        failures.push(BatchAssignFailure {
                            credential_id: a.credential_id,
                            reason: format!("代理 #{pid} 不存在"),
                        });
                        continue;
                    }
                    Some(p) if !p.enabled || p.auto_disabled => {
                        failures.push(BatchAssignFailure {
                            credential_id: a.credential_id,
                            reason: format!("代理 #{pid} 已禁用或被健康检查自动禁用"),
                        });
                        continue;
                    }
                    Some(p) => Some(p.url.clone()),
                },
            };
            updates.push((
                a.credential_id,
                CredentialUpdate { proxy_url: Some(proxy_url), ..CredentialUpdate::default() },
            ));
        }

        if !failures.is_empty() {
            return Err(BatchAssignError::Validation(failures));
        }

        let n = updates.len();
        self.token_manager.update_credentials_batch(updates).map_err(|e| match e {
            crate::kiro::token_manager::BatchUpdateError::MissingCredentials(ids) => {
                BatchAssignError::Validation(
                    ids.into_iter()
                        .map(|id| BatchAssignFailure {
                            credential_id: id,
                            reason: "凭据不存在".to_string(),
                        })
                        .collect(),
                )
            }
            crate::kiro::token_manager::BatchUpdateError::Persist(err) => {
                BatchAssignError::Internal(format!("凭据落盘失败: {err}"))
            }
        })?;
        Ok(n)
    }
}
```

路径引用（`crate::admin::proxy_pool::ProxyEntry` 等）按文件顶部既有 use 风格改为 import；若已存在同名 impl 块就并入。注意凭据存在性校验刻意留在 token_manager 锁内做（避免 TOCTOU），service 只负责代理与重复校验——两层失败都归入 `Validation`。

- [ ] **Step 5: 跑测试确认通过**

Run: `cargo test admin::service::tests::assign_proxies_batch`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/admin/types.rs src/admin/service.rs
git commit -m "feat(admin): 凭据↔代理批量重绑 service 层（全有或全无）

先整体校验（凭据存在/代理可用/无重复），报全部失败条目；
全合法才经 update_credentials_batch 单锁应用、落盘一次。
凭据存在性留在 token_manager 锁内校验，避免 TOCTOU。"
```

---

### Task 3: handler + 路由

**Files:**
- Modify: `src/admin/handlers.rs`（`assign_proxies_round_robin`（:496）之后）
- Modify: `src/admin/router.rs`（`/credentials/{id}/proxy` 那行（:101）附近加路由；顶部 use 列表补 handler 名）

**Interfaces:**
- Consumes: Task 2 的 `assign_proxies_batch` / `AssignProxyBatchRequest` / `BatchAssignError` / `BatchAssignFailure`
- Produces: 路由 `POST /credentials/proxy/batch`；400 响应体 `{"error": "...", "failures": [{"credentialId": .., "reason": ".."}]}`

- [ ] **Step 1: 写 handler + 响应类型**

`src/admin/handlers.rs`，`assign_proxies_round_robin` 之后追加（`Serialize`、`json!` 等 import 按文件现状补）：

```rust
/// POST /api/admin/credentials/proxy/batch
/// 批量重绑凭据↔代理。全有或全无：校验失败返回 400 + 全部失败条目。
pub async fn assign_proxies_batch(
    State(state): State<AdminState>,
    Json(payload): Json<AssignProxyBatchRequest>,
) -> impl IntoResponse {
    match state.service.assign_proxies_batch(payload) {
        Ok(n) => Json(SuccessResponse::new(format!("已更新 {n} 张凭据的代理绑定"))).into_response(),
        Err(BatchAssignError::Validation(failures)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "批量重绑校验失败，未应用任何变更",
                "failures": failures,
            })),
        )
            .into_response(),
        Err(BatchAssignError::Internal(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response(),
    }
}
```

- [ ] **Step 2: 挂路由**

`src/admin/router.rs`：use 列表加 `assign_proxies_batch`（handler 名与 service 方法重名但分属不同模块，若 use 冲突则 `assign_proxies_batch as assign_proxies_batch_handler` 并同步路由处）；在 `.route("/credentials/{id}/proxy", ...)` 之后加：

```rust
        .route("/credentials/proxy/batch", post(assign_proxies_batch))
```

注意路由顺序：axum 的 `{id}` 段不会吞掉字面量 `proxy`，`/credentials/proxy/batch` 与 `/credentials/{id}/proxy` 不冲突（`{id}` 是单段参数、后缀不同）；若编译或路由测试提示冲突，把 batch 路由放在参数路由之前注册。

- [ ] **Step 3: 写 400 响应形状测试**

`src/admin/handlers.rs` 的 `mod tests`（若无则新建）追加——不起 HTTP 服务，直接断言 failures 序列化形状（防止 camelCase rename 遗漏）：

```rust
    #[test]
    fn batch_assign_failure_serializes_camel_case() {
        let f = BatchAssignFailure { credential_id: 7, reason: "凭据不存在".to_string() };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["credentialId"], 7, "必须是 camelCase，前端按 credentialId 读: {v}");
        assert_eq!(v["reason"], "凭据不存在");
    }
```

- [ ] **Step 4: 跑测试 + 编译**

Run: `cargo test admin:: && cargo build`
Expected: 全绿、编译通过。

- [ ] **Step 5: Commit**

```bash
git add src/admin/handlers.rs src/admin/router.rs
git commit -m "feat(admin): POST /credentials/proxy/batch 路由与 400 全量失败明细"
```

---

### Task 4: 管理面板「批量绑代理」弹窗

**Files:**
- Modify: `admin-ui/src/api/credentials.ts`（`assignProxyToCredential`（:377）附近加 API 封装与类型）
- Create: `admin-ui/src/components/batch-assign-proxy-dialog.tsx`
- Modify: `admin-ui/src/components/dashboard.tsx`（工具栏加按钮 + 挂弹窗，搜「批量验活」定位同类按钮位置）

**Interfaces:**
- Consumes: Task 3 的 `POST /credentials/proxy/batch`；既有 `getProxyPool()`（credentials.ts:344）、`getCredentials()`
- Produces: `batchAssignProxy(assignments: BatchAssignEntry[]): Promise<SuccessResponse>`；`interface BatchAssignEntry { credentialId: number; proxyId: number | null }`

- [ ] **Step 1: API 封装**

`admin-ui/src/api/credentials.ts`，`assignProxyToCredential` 之后：

```typescript
export interface BatchAssignEntry {
  credentialId: number
  /** null = 解绑，回落全局代理 */
  proxyId: number | null
}

export interface BatchAssignFailure {
  credentialId: number
  reason: string
}

/** 批量重绑凭据↔代理。400 时后端返回 { error, failures }，由调用方从
 *  AxiosError.response.data 中取 failures 逐行展示。 */
export async function batchAssignProxy(
  assignments: BatchAssignEntry[],
): Promise<SuccessResponse> {
  const { data } = await api.post<SuccessResponse>('/credentials/proxy/batch', { assignments })
  return data
}
```

- [ ] **Step 2: 弹窗组件**

新建 `admin-ui/src/components/batch-assign-proxy-dialog.tsx`。结构照 `batch-edit-credential-dialog.tsx`（同样的 Dialog/Button/toast/queryClient 用法），要点：

- Props：`{ open, onOpenChange, credentials: CredentialStatusItem[] }`（全部凭据，不筛选）。
- 打开时 `getProxyPool()` 拉代理池；下拉选项 = `enabled && !autoDisabled` 的代理，显示 `#${id} ${url}（${health}, ${latencyMs ?? '-'}ms）`；首项固定「跟随全局代理」（值 null）。
- 每行：凭据 email（空则 `#id`）+ `<select>`（用项目里现成的 Select 组件，参照 batch-edit 弹窗内的选择器写法）；初值 = 该凭据当前 `proxyUrl` 反查代理池 id（查不到 URL 匹配则显示「自定义: <url>」且该行只读——面板不破坏手工配置的池外代理）。
- 提交体**只含被改动的行**（当前选值 ≠ 初值）；无改动时提交按钮 disabled。
- 成功：`toast.success(resp.message)` + `queryClient.invalidateQueries` 凭据查询 key（key 抄 batch-edit 弹窗）+ 关弹窗。
- 400：从 `err.response?.data?.failures` 取明细，映射到对应行内联红字展示 reason；无 failures 字段则 `toast.error` 整体错误文案。**不关弹窗、不清空已选**。

- [ ] **Step 3: 挂按钮**

`dashboard.tsx`：在「批量验活」按钮同一工具栏区域加「批量绑代理」按钮，点击置 `open=true`；弹窗组件与其它批量弹窗并列挂载，传入当前凭据列表。

- [ ] **Step 4: 构建验证**

Run: `cd admin-ui && bun run build`
Expected: 构建成功无 TS 错误。

- [ ] **Step 5: Commit**

```bash
git add admin-ui/src/api/credentials.ts admin-ui/src/components/batch-assign-proxy-dialog.tsx admin-ui/src/components/dashboard.tsx
git commit -m "feat(admin-ui): 批量绑代理弹窗（仅提交改动行，400 行内展示失败原因）"
```

---

### Task 5: 全量验证 + 版本收尾

**Files:**
- Modify: `Cargo.toml`（version → `0.9.14`）、`Cargo.lock`（构建自动更新）、`CHANGELOG.md`

**Interfaces:** Consumes 全部前置任务。

- [ ] **Step 1: 全量测试**

Run: `cargo test 2>&1 | tail -5`
Expected: 比基线多出的新增测试全绿；失败仍只有 `http_client::tests::reqwest_timeout_error_is_tagged_before_the_chain` 一条。

- [ ] **Step 2: clippy 无新增**

Run: `cargo clippy --all-targets --message-format=short 2>&1 | grep -cE "^src/(kiro/token_manager|admin/(service|handlers|router|types))\.rs"`
与改动前基线比较不得变多（token_manager.rs 基线本就约 57 条，只看增量）。

- [ ] **Step 3: 版本号与 CHANGELOG**

`Cargo.toml` version 改 `0.9.14`；`CHANGELOG.md` 顶部新增 `## [0.9.14]` 一节：凭据↔代理批量重绑（API `POST /credentials/proxy/batch` 全有或全无 + 面板弹窗），风格照抄 0.9.13 条目。跑一次 `cargo build` 让 Cargo.lock 跟上。

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "chore: 版本号升至 0.9.14（凭据↔代理批量重绑）"
```

- [ ] **Step 5: 部署与手工验收（需用户在场，不自动执行）**

构建镜像 `localhost/kiro-rs:0.9.14` → 按容器重建核对集（端口 127.0.0.1:19095:8990 / 挂载 data:/app/config / 命令 / unified 网络 / restart unless-stopped）替换容器 → 面板手工验收：改 2 条提交 → 刷新后绑定正确 → `data/credentials.json` 落盘值正确 → 故意提交一条选了池外凭据的（无法直接构造就跳过，以单测为准）。

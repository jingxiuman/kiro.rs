# 吸收上游 v0.7.3 —— 设计

日期：2026-07-27
上游：`ZyphrZero/kiro.rs` v0.7.2..v0.7.3（3 commit / +2194 行）
本地：fork 0.8.3，v0.7.2 是本地祖先，v0.7.3 **不是**

## 1. 背景与判定

上游 0.7.3 的主题是「动态模型发现 + 模型感知路由 + 开放透传」。本地在 v0.7.2 之上
并行造了 model registry（`model_registry.rs` / `model_sync.rs` / `models.json` 持久化 +
人工覆盖层），解决的是同一个问题。所以本次是**逐条比对后的选择性吸收**，不是合并。

| 上游改动 | 本地现状 | 判定 |
|---|---|---|
| `/v1/models` 改上游动态目录 | `model_sync` 定时同步 + 注册表 | 已覆盖（本地更重：持久化 + 人工覆盖） |
| 逐凭据缓存 TTL + singleflight + 预热 | 同步服务 + 持久化 | 已覆盖（架构不同，目标相同） |
| 模型感知路由 | `credential_support` + `credential_supports_model()`，无记录放行 | 已覆盖，语义一致 |
| 未知模型 ID 开放透传 | `ALLOW_PASSTHROUGH` + 家族/版本宽松匹配 | 已覆盖 |
| 不再发布合成 `-thinking` 别名 | 本地**有意**发布 thinking 变体行（有测试钉死） | 冲突，不吸收 |
| `max_tokens` 缺省默认 + ≤0 校验 | 无 | **吸收（改动 1）** |
| balanced 模式 `currentId` / `isCurrent` 语义 | `service.rs:611` 无条件比较 | **吸收（改动 2）** |
| `POST /models/test` 真实请求验证 | 无 | **吸收（改动 3）** |
| 只读查询不扰动调度指针 | 本地无「按池选凭据做只读查询」的路径 | 不适用 |

**不吸收上游的 `selectionMode` 字段与 `GET /api/admin/models`**：本地该路径已被注册表占用，
语义不同；再加一套并行口径只会制造第二个真相源。

## 2. 改动 1 —— `max_tokens` 缺省默认 + ≤0 校验

- `src/anthropic/types.rs`：新增 `pub const DEFAULT_MAX_TOKENS: i32 = 32_000`；
  `MessagesRequest.max_tokens`（types.rs:119 结构体内）加 `#[serde(default = "default_max_tokens")]`。
- **收敛既有重复常量**：`openai.rs:36` 与 `responses.rs:58` 各有一份 `DEFAULT_MAX_TOKENS = 32000`，
  改为引用 `types.rs` 的单一来源，不新增第三份。
- `src/anthropic/handlers.rs`：`post_messages` 与 `post_messages_cc` 两个入口加
  `validate_max_tokens(payload.max_tokens)`，≤0 返回 `invalid_request_error`
  （消息：`max_tokens must be greater than 0`）。

测试：缺省 → 32000；显式 4096 保留；0 / -1 → 400。

## 3. 改动 2 —— balanced 模式状态语义（真 bug）

`src/admin/service.rs:611` 现在无条件 `is_current: entry.id == snapshot.current_id`。
balanced 模式下 `current_id` 只是内部调度指针，管理端却渲染成「当前活跃账号」——显示假信息。

- 建列表前算一次 `exposed_current_id`：`token_manager.get_load_balancing_mode() == "balanced"`
  时为 `0`，否则取 `snapshot.current_id`。
- `is_current` 与响应体 `current_id` 都改用它。
- `src/admin/types.rs` 两处 doc 注释同步为「优先级模式下的当前优先凭据；均衡模式固定为 0 / false」。

测试：balanced 下 `current_id == 0` 且全部 `is_current == false`；priority 下第一条为 true。

## 4. 改动 3 —— `POST /api/admin/models/test`

落在本地已有的 `/models` 族下（`/models/sync`、`/models/aliases`、`/models/settings` 旁）。

请求：`{ modelId, credentialId? }`
响应：`{ modelId, resolvedModelId, thinking, credentialId, latencyMs, responseText, creditUsage?, creditUnit? }`

流程：

1. `registry.resolve(modelId, allow_passthrough)`。`Mapped` / `Passthrough` 取其 `upstream_id`；
   `Rejected` 不发请求，直接返回 `ModelNotFound` / `InvalidModelField`。
   —— 测的是「客户端发这个模型名时本代理实际会发生什么」，含别名映射、禁用判定、thinking 变体、透传开关。
2. 构造最小 `KiroRequest`：`"Reply with exactly: OK"`，`agent_task_type=vibe`、
   `chat_trigger_type=MANUAL`、`origin=AI_EDITOR`（同上游）。
3. 发送，90s 超时：
   - 无 `credentialId` → `provider.call_api(&body, None, None)`，走正常账号池（priority/balanced）。
   - 有 `credentialId` → 新增 `provider.call_api_pinned(id, &body)`：私有的
     `call_api_with_retry` 加一个 `pinned: Option<u64>` 参数，为 `Some` 时改用新增的
     `token_manager.acquire_context_pinned(id)`（薄封装既有的 `prepare_request_token`，
     它已处理 API Key 凭据与按需刷新），且**不跨凭据故障转移**（重试预算降为单凭据额度）。
     现有两个调用点传 `None`，行为不变。
4. `EventStreamDecoder` 解帧：累计 `AssistantResponse` 文本与 `Metering` 计费；
   `Error` / `Exception` 事件转 `UpstreamError`；文本过 `strip_tool_use_xml_leaks`。
5. 这是**真实请求**，照常记 `report_success` / `report_failure`，不做只读豁免（与上游一致）。

## 5. 验证

- `cargo test` 通过（`--no-run` 只证明编译，不算通过）。
- 三项各自的新增单测见上。
- `/models/test` 的真实上游调用不进自动化测试，需部署后手工验证一次。

## 6. 风险

改动 3 对 `call_api_with_retry` 的参数穿透落在全项目最热的路径上。改动本身是
「多一个默认 `None` 的参数 + 一处分支」，现有调用点行为不变，但需要回归测试覆盖。

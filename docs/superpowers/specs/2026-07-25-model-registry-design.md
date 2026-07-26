# 模型注册表：手动映射 + 上游自动同步

- 日期：2026-07-25
- 版本：v2（v1 经 Codex 静态审查后重写，修订记录见 §13）
- 状态：待实现
- 影响范围：`src/anthropic/{converter,handlers,stream,websearch_loop}.rs`、`src/kiro/token_manager.rs`、`src/admin/*`、`src/main.rs`、`admin-ui`
- 新增文件：`src/anthropic/model_registry.rs`、`src/anthropic/model_sync.rs`、`models.json`（运行时数据）

## 1. 背景与问题

网关把「有哪些模型、模型叫什么、窗口多大」写死在编译期常量里：

| 决定什么 | 位置 | 现状 |
|---|---|---|
| 对外暴露哪些模型（`/v1/models`） | `src/anthropic/handlers.rs:385` `available_models()` | 硬编码 `Vec<Model>` |
| 请求能否进入、映射成哪个上游 id | `src/anthropic/converter.rs:199` `map_model()` | 硬编码 `contains` 分支 |
| 输入上下文窗口 | `src/anthropic/converter.rs:246` `get_context_window_size()` | 硬编码 1M / 272K / 200K |

上游 `ListAvailableModels` 已被调用且已返回 `maxInputTokens`：

```
src/kiro/token_manager.rs:627  拉取上游
  → src/admin/service.rs:787-804  转为 AvailableModelItem{modelId, modelName, description, maxInputTokens}
    → GET /credentials/{id}/models  (src/admin/handlers.rs:152)
      → admin-ui/src/components/available-models-dialog.tsx  只读展示
```

**根因：上游实际提供什么、和网关认什么，是两套互不同步的知识。** 直接后果是上游上线 `claude-opus-5` 后网关仍返回 `400 模型不支持`，必须改代码发版。

## 2. 目标与非目标

### 目标

1. 三张硬编码表变为「编译内置默认 + 运行时可覆盖」。
2. 手动映射：任意对外模型名 → 任意上游模型 id。
3. 手动覆写输入窗口与输出上限，且覆写不被自动同步冲掉。
4. **可选开启**从上游自动同步模型列表与窗口，无需改代码发版。
5. **零行为回归**：不开启同步、不存在 `models.json` 时，行为与当前版本等价。

> 目标 4 与目标 5 的相容性靠 `modelSyncEnabled` **默认 `false`** 保证。v1 曾让每日同步默认开启并在启动后 30s 无条件跑一次，与目标 5 直接矛盾。

### 非目标

1. **不做按凭据/按分组的差异化映射表。** 映射发生在选凭据之前（`handlers.rs:694` 先 `convert_request_with_mode()`，之后才由 provider 层注入 `profile_arn`），改为 per-credential 需重构请求主链路时序。
2. 不做模型能力矩阵（vision / tool use / effort 支持）。`model_supports_native_reasoning()` / `model_supports_xhigh_effort()` 保持不变。
3. 不引入新 crate（`parking_lot`、`chrono`、`rusqlite` 均已在依赖内）。
4. 不做 `models.json` 的自动 schema 迁移（见 §4.5）。

### 关于参考项目

需求提出时参考了 [Wei-Shaw/sub2api](https://github.com/Wei-Shaw/sub2api)。经查证其 README 记载的是多账号管理（OAuth / API Key）、Composite Groups 路由、API Key 分发，**未记载模型映射与上游模型自动同步**。本设计仅在「凭据管理的面板形态」上与其同构，映射与同步机制为自有设计。

## 3. 架构

```
┌─ 写入侧（默认关闭）───────────────────────────────────┐
│  ModelSyncService                                      │
│   ├─ modelSyncEnabled=true 时：每日 modelSyncTime      │
│   ├─ 任何时候：POST /models/sync 手动触发              │
│   ├─ 探针凭据（权威源，可判 deprecated）                │
│   │    └─ 不可用 → 采样 3 个（仅新增/更新，不判消失）   │
│   └─ 经 ModelListFetcher trait 拉取（可注入假实现）     │
└──────────────────────┬─────────────────────────────────┘
                       │ 原子落盘成功后 swap
┌─ 读取侧 ─────────────▼─────────────────────────────────┐
│  ModelRegistry (src/anthropic/model_registry.rs)        │
│   BUILTIN_DEFAULTS ⊕ models.json 覆盖层（加载时校验）    │
│   ├─ resolve(requested) -> Resolution                  │
│   └─ exposed_models() -> Vec<Model>                    │
└──────────────────────┬─────────────────────────────────┘
                       │ 请求入口解析一次，结果随请求传递
        ConversionResult{ upstream_id, context_window }
```

### 3.1 组件职责

| 组件 | 职责 | 依赖 | 测试方式 |
|---|---|---|---|
| `ModelRegistry` | 纯数据 + 纯方法：给定模型名返回上游 id / 窗口 / 是否启用 | 无 | 直接构造后调 `resolve`，不碰全局、不碰文件 |
| `ModelRegistryStore` | `models.json` 读 / 写 / 合并（保 pinned）/ 校验 / 原子落盘 | 文件系统 | 临时目录真实读写 |
| `ModelListFetcher`（trait） | `fetch(credential_id) -> Vec<AvailableModelItem>` | — | 测试注入假实现 |
| `ModelSyncService` | 选凭据、拉取、算 diff、判 deprecated、调 Store | `dyn ModelListFetcher`、`Store` | 注入假 fetcher，断言 diff 与 deprecated 判定 |
| `ModelSyncRuntimeConfig` | 三个开关的运行时 holder（可热改） | — | 直接读写 |
| 全局 `REGISTRY` | 「进程当前实例」holder，无业务逻辑 | `parking_lot::RwLock<Arc<_>>` | 不单测 |

**硬约束一：`ModelRegistry` 内不含 I/O、不含时间概念。** 时间语义（deprecated 宽限、`lastSeenAt`）只属于 `ModelSyncService`，使模型解析完全确定性，可直接复用现有 `map_model` 测试用例作为回归基线。

**硬约束二：拉取上游必须经 `ModelListFetcher` trait。** 现有 `get_available_models_for`（`token_manager.rs:2702`）内部直接刷 token 并发网络请求，无法在单测中替换；`ModelSyncService` 只依赖 trait，`MultiTokenManager` 提供实现。

### 3.2 接线方式与改动面

选定「进程级 registry」，但**改动面不是「零改动」**，准确清单如下：

| 位置 | 改动 | 原因 |
|---|---|---|
| `converter.rs:595`（`convert_request_with_mode`） | 改调 `registry().resolve()`；新增 `ConversionError::ModelDisabled(String)`；`ConversionResult` 增加 `context_window: i32` 字段 | `Option<String>` 无法表达「命中但被禁用」；窗口需随请求传递（见下） |
| `handlers.rs:697`、`handlers.rs:1488` 错误 match | 各加 `ModelDisabled` 分支 | 新错误变体 |
| `websearch_loop.rs:267` 错误 match | 加 `ModelDisabled` 分支 | 同上 |
| `handlers.rs:1155`、`stream.rs:1525`、`websearch_loop.rs:200` | 改用随请求传递的 `context_window`，不再调用 `get_context_window_size()` | 快照一致性，见 §3.3 |
| `map_model()` / `get_context_window_size()` | 签名不变，内部查 registry。保留为兼容入口 | 现有测试大量直接调用 |
| `token_manager.rs:1065` `credential_matches_request` | 增加基于 `credentialSupport` 的过滤 | 见 §6.6 |
| `main.rs` | 在 admin 分支**之外**创建 registry 与同步调度器 | `AdminService` 仅在 `adminApiKey` 非空时创建（`main.rs:270`），否则自动同步不存在 |

已评估并否决的替代方案：

- **依赖注入 `convert_request_with_mode(req, mode, &registry)`**：无全局状态，但需改 `AppState` + `converter.rs` 中大量既有测试签名，且 `convert_request` 便捷入口失去意义。**遗留问题**：`create_router_with_provider`（`router.rs:27`）是对外嵌入扩展点，不接受配置；本设计要求调用方在建 router 前完成 `REGISTRY` 初始化，需在该函数文档注释中写明（未初始化时退回内置默认，不 panic）。
- **在 handler 内查表再传给 converter**：把「模型解析」切成两半散在两层，`websearch_loop` 需各自维护查表逻辑。

### 3.3 快照一致性

`get_context_window_size()` 有 **3 个 converter 之外的调用点**（v1 错误地断言只有 converter 与测试）：

```
src/anthropic/handlers.rs:1155      非流式响应处理
src/anthropic/stream.rs:1525        流式事件处理
src/anthropic/websearch_loop.rs:200 web-search 循环
```

这三处都在**上游响应处理阶段**执行，而模型映射在请求入口（`handlers.rs:694`）执行。若两者各自 `Arc::clone` 全局 registry，一次热重载可能导致「用旧表映射、用新表计量」。

**规则：窗口在请求入口随模型一起解析一次，放入 `ConversionResult.context_window`，向下传递到流处理。** 单请求内只取一次快照。

## 4. 数据模型

### 4.1 运行时开关（`ModelSyncRuntimeConfig`）

不放进不可变的 `Config` clone（`MultiTokenManager` 持有的是 clone，`token_manager.rs:1003`），沿用既有 `RuntimeUpdateConfig`（`admin/service.rs:122`）的可变 holder 模式，落盘仍写 `config.json`：

```json
{
  "modelSyncEnabled": false,
  "modelSyncTime": "04:00",
  "modelSyncProbeCredentialId": null,
  "allowUnknownModelPassthrough": false
}
```

- `modelSyncEnabled` 缺省 `false` —— 保证零行为回归。
- `modelSyncTime` 缺省 `"04:00"`；校验与时区语义复用既有实现（`admin/service.rs:209` 校验 hh:mm 范围，`:923` 使用本地时区）。
- `config.json` 写入经**新增的 config 写 mutex** 串行化 —— **仅覆盖本设计新增的 `set_model_sync_settings` 路径**。
  > 实现期修正：原文曾写「本设计顺带修掉这一类丢失更新」，这是写过头了。既有的 6 处 `update_config_file` 调用点仍是无保护的 load-modify-save；把它们纳入同一把锁需要将若干同步方法改成 async 并连带修改 `admin/handlers.rs`，范围远超本设计。该问题是改造前既有行为、非本次引入的回归，列为已知限制（§12）。

### 4.2 模型表（`models.json`）

**位置：凭据目录**（与既有 registry / cache 一致，`main.rs:176`、`README.md:668`），不放 config 目录 —— config 与凭据路径可独立指定（`main.rs:35`）。

```json
{
  "version": 1,
  "syncState": {
    "lastSyncAt": "2026-07-25T04:00:00Z",
    "lastFetchStartedAt": "2026-07-25T03:59:58Z",
    "source": "probe:3"
  },
  "models": [
    {
      "upstreamId": "claude-opus-4.8",
      "matchKind": "exact",
      "exposedId": "claude-opus-4-8",
      "displayName": "Claude Opus 4.8",
      "ownedBy": "anthropic",
      "modelType": "chat",
      "created": 1782000000,
      "contextWindow": 1000000,
      "maxOutputTokens": 128000,
      "exposeThinkingVariant": true,
      "enabled": true,
      "status": "active",
      "origin": "synced",
      "sortOrder": 30,
      "pinned": ["contextWindow"],
      "missingSyncRounds": 0,
      "lastSeenAt": "2026-07-25T04:00:00Z"
    }
  ],
  "aliases": [{ "from": "opus", "to": "claude-opus-4.8" }],
  "credentialSupport": { "3": ["claude-opus-4.8", "claude-sonnet-5"] }
}
```

### 4.3 字段语义

| 字段 | 说明 |
|---|---|
| `upstreamId` | 上游 `modelId`，行主键 |
| `matchKind` | `exact`（默认）\| `prefix`。见 §5.3 |
| `exposedId` | 对外名。派生规则见 §4.4 |
| `contextWindow` | **输入**上下文窗口，供 `get_context_window_size()` |
| `maxOutputTokens` | **输出**上限，供 `/v1/models` 的 `Model.max_tokens` |
| `ownedBy` / `modelType` / `created` / `sortOrder` | 构造 `Model` 所需字段（`anthropic/types.rs:43` 要求 `object`/`created`/`owned_by`/`display_name`/`type`/`max_tokens`）；`sortOrder` 保持列表顺序稳定，替代当前硬编码 `Vec` 的隐式顺序 |
| `exposeThinkingVariant` | 是否额外暴露 `{exposedId}-thinking`，映射到同一 `upstreamId` |
| `enabled` | `false` → 拒绝请求 **且从 `/v1/models` 移除** |
| `listed` | 是否出现在 `/v1/models`。缺省 `true`；`matchKind == "prefix"` 的行**强制 `false`**（仅用于解析，不是一个真实模型）|
| `matchSubstrings` | 额外的子串匹配关键字，缺省空。**用于复现旧 `map_model` 的「家族通吃」语义**：旧代码 `contains("haiku")` / `contains("fable")` 不看版本号就映射，`contains("sonnet.5")` / `contains("sonnet5")` 也命中 5 代。内置默认只给三行填值——`claude-fable-5`: `["fable"]`、`claude-haiku-4.5`: `["haiku"]`、`claude-sonnet-5`: `["sonnet-5","sonnet5","sonnet.5"]`。**不得给 sonnet/opus 4.x 行填家族关键字**：旧代码对它们要求版本匹配，填了会让 `claude-3-5-sonnet` 被误判（旧行为是 `None`）|
| `status` | `active` \| `deprecated`（上游消失但保留，仍可用、仍在列表） |
| `origin` | `builtin` \| `synced` \| `manual` |
| `pinned` | 已人工编辑、同步时逐字段跳过的字段名 |
| `missingSyncRounds` | 连续多少轮**权威**同步未见于上游 |
| `credentialSupport` | `凭据 id → 该凭据可用 upstreamId 列表`，同步时顺带记录（零额外请求），供 §6.6 调度过滤 |

> **`contextWindow` 与 `maxOutputTokens` 必须是两个字段。** 二者语义不同且数值差一个数量级：`gpt-5.6-sol` 的 `Model.max_tokens` 是 `64000`（输出），而 `get_context_window_size()` 返回 `272000`（输入）。v1 用单一 `contextWindow` 同时喂两处，会把 `/v1/models` 的输出上限错报成输入窗口。

设计取舍：

- **一行一个上游模型，而非一行一个对外名。** `-thinking` 由 `exposeThinkingVariant` 派生，因其与主模型共享 `upstreamId` 与窗口；拆两行会导致「改窗口需记得改两处」。
- **`pinned` 是字段级。** 人工写了窗口、上游改了 `displayName`，两者各自生效。
- **`aliases` 独立于 `models`。** `models` 会被同步覆盖，`aliases` 永远只属于人工。
- **`enabled=false` 从列表移除、`deprecated` 保留在列表。** 前者是人工主动下线（不该再被发现），后者是上游消失但不应打断在用客户端。

### 4.4 `exposedId` 派生规则（按 provider，非全局）

| 上游前缀 | 派生 | 依据 |
|---|---|---|
| `claude-` | 点号转连字符：`claude-opus-4.8` → `claude-opus-4-8` | 现有对外名均为连字符风格 |
| 其他（含 `gpt-5*`） | **原样保留** | `handlers.rs:388` 刻意暴露带点号的 `gpt-5.6-sol` |

`exposeThinkingVariant` 对新增行同样按前缀判定：`claude-*` 为 `true`，其他为 `false`。

> v1 的「一律点号转连字符」会把 `gpt-5.6-sol` 错误暴露成 `gpt-5-6-sol`，破坏现有客户端。

### 4.5 加载校验与版本策略

加载 `models.json` 时按序校验，**任一项失败即整体拒绝该文件**、退回内置默认并置 `degraded = true`（不 panic、不空表）：

1. `version` 缺失、`!= 1` → 拒绝（不做自动迁移；未来版本靠只增字段保持前向兼容）
2. `upstreamId` 唯一
3. 全部对外名唯一 —— 包含 `exposedId`、派生的 `{exposedId}-thinking`、以及全部 `alias.from`
4. `alias.to` 必须命中某个 `upstreamId`（dangling → 拒绝）；`alias.to` 不得指向另一个 alias（不支持递归）
5. `contextWindow` / `maxOutputTokens` 为 `1..=i32::MAX`
6. `matchKind == "prefix"` 的行不得设 `exposeThinkingVariant = true`

> 唯一性校验不可省：重复 `exposedId`、alias 与 `exposedId` 撞名都会让解析结果依赖遍历顺序。既有 `groups.rs:66` 用键控插入天然免疫，本表是数组，必须显式校验。

### 4.6 内置默认

`available_models()` / `map_model()` / `get_context_window_size()` 现有内容降级为 `BUILTIN_DEFAULTS`（`origin: "builtin"`），`models.json` 作为覆盖层叠加其上。

- 文件不存在 → 行为与当前版本等价（**零行为回归**）
- 文件损坏或校验失败 → 退回内置默认 + `degraded = true`
- `origin: "builtin"` 的行永不被同步删除，也不可经 API 删除

### 4.7 覆盖层的应用粒度（决策：按 pinned 字段级）

`models.json` 的覆盖层在**存储上**仍是整行，但**加载时不得整行替换**。

问题：`from_file` 原先的叠加是 `*existing = incoming`，于是用户只 PATCH 一个字段（如 `contextWindow`），整行就被冻结成编辑那一刻的快照 —— 后续版本在代码里修改该模型的 `displayName` / `maxOutputTokens` 对该部署静默失效。由于 `modelSyncEnabled` 默认 `false`，**默认配置下没有任何东西会刷新覆盖层，整行冻结是常态而非边界情况**。

这与 §4.6 的 N1（同步元数据污染覆盖层）是同一类错误的两条路径：N1 走同步写入，本条走手动编辑。

**决策：`pinned` 即「用户要保护哪些字段」的权威记录 —— 同步服务已经尊重它，加载器也必须尊重它。**

优先级：**用户 pinned > 同步写入 > 代码内置默认**。

> **陷阱（实现前必读）**：规则若简单写成「只应用 pinned 字段」，会把自动同步整个打死。`merge_synced_row` 写入的 5 个字段（`displayName` / `contextWindow` / `maxOutputTokens` / `exposedId` / `exposeThinkingVariant`）在用户没手动改过时**不在 `pinned` 里**，按「只应用 pinned」会被内置默认盖回去，上游数据全部丢失。

**必须满足的行为**（每条都要有可执行断言，具体规则由实现者设计）：

| # | 场景 | 期望 |
|---|---|---|
| B1 | 用户 PATCH 内置模型的 `contextWindow`，之后新版代码改了该模型的 `displayName` | `contextWindow` 用用户的值，`displayName` **跟随新代码** |
| B2 | 同步开启、上游返回该模型、用户未 pin `contextWindow` | 上游的 `contextWindow` **仍然生效**，不被内置默认盖回去 |
| B3 | 用户已 pin `contextWindow`，同步开启且上游给了不同值 | **用户的值胜出**（既有行为，不得回归） |
| B4 | 纯 `Manual` 行，`upstreamId` 不在 `builtinRows()` 里 | 所有字段照常生效（无回退基准） |
| B5 | `Synced` 行，`upstreamId` 不在 `builtinRows()` 里 | 所有字段照常生效 |

实现者选定的规则必须能用一句话说清，并写进代码注释。

## 5. 模型解析

### 5.1 返回类型

```rust
enum Resolution {
    Mapped { upstream_id: String, context_window: i32 },
    Passthrough { upstream_id: String, context_window: i32 },  // 200_000
    Rejected(RejectReason),                                    // Unknown | Disabled
}
```

### 5.2 规范化规则

当前 `map_model` 用模糊 `contains` 匹配，能容忍 `claude-opus-4-5-20251101`、`claude-sonnet-5-20260101-thinking`（真实测试见 `converter.rs:1848`）。改为查表后必须保住该容忍度。**按序**执行：

1. 转小写
2. **先剥 `-thinking` 后缀**
3. **再剥日期后缀** `-\d{8}$`
4. 版本段连字符转点号：`-(\d+)-(\d+)$` → `-$1.$2`

> 顺序不可颠倒。v1 先剥日期再剥 thinking，对 `claude-sonnet-5-20260101-thinking` 而言 `-\d{8}$` 因尾部是 `-thinking` 而不匹配，日期永久残留 → 该真实用例必然失败。

**反例约束**（必须有测试）：`claude-3-5-sonnet` 不得被规范化为命中 `claude-sonnet-5`。现有代码已刻意规避（`converter.rs:211` 注释「精确匹配 5 代，避免命中 legacy claude-3-5-sonnet」）。

### 5.3 解析顺序

1. `aliases.from` 精确命中 → 取其 `to` 指向的行
2. `exposedId` 精确命中
3. `{exposedId}-thinking` 命中 **且该行 `exposeThinkingVariant == true`**
4. 规范化后与 **`upstreamId`** 精确命中
   > 规范化的产物形态恰好就是上游 id 的形态（点号版本段），所以这一步对 `upstreamId` 匹配、而非 `exposedId`。这是必需的：`claude-opus-4-5`（不带日期后缀）今天能用（`map_model` 走 `contains("4-5")`），但表里的 `exposedId` 是 `claude-opus-4-5-20251101`；只有规范化到 `claude-opus-4.5` 再匹配 `upstreamId` 才能保住它。
5. `matchSubstrings` 命中（请求名小写后包含其中任一子串）→ 取该行
6. `matchKind == "prefix"` 的行按 `exposedId` 做前缀匹配，最长前缀优先；`upstream_id` = **请求名原样**（规范化前、仅小写）
7. `allowUnknownModelPassthrough == true` → `Passthrough`（规范化后名字，窗口 200K）
8. 否则 `Rejected(Unknown)`

**「thinking 变体被关闭」的拒绝必须延后到第 6 步之后再生效。** 即：第 3/4 步发现请求名带 `-thinking` 而命中行 `exposeThinkingVariant == false` 时，**记下这个待定拒绝但继续往下走**；只有第 5、6 步都没命中，才返回 `Rejected(Unknown)`。
> 否则 `gpt-5.6-sol-thinking` 会在第 4 步被 gpt 精确行的 `exposeThinkingVariant: false` 拦掉，走不到第 6 步的 prefix 透传——而旧代码对任何 `gpt-5` 开头的名字一律原样透传（`converter.rs:234` 的 `starts_with("gpt-5") => Some(model_lower)`，不做 `-thinking` 剥离）。`claude-opus-4-8-thinking` 在变体关闭时仍然正确返回 `Unknown`，因为它既无 `matchSubstrings` 也无 prefix 行可命中。

补充规则：

- 命中行 `enabled == false` → `Rejected(Disabled)`（含经 alias 命中）
- **请求名带 `-thinking`、但命中行 `exposeThinkingVariant == false` → `Rejected(Unknown)`**
  > v1 缺此规则：第 2 步条件性识别 `-thinking`，但规范化又无条件剥掉它，导致关掉 thinking 变体的模型仍能被 `xxx-thinking` 命中。
- **`prefix` 行是 `gpt-5*` 通配的落点。** 内置默认含一行 `{ upstreamId: "gpt-5", matchKind: "prefix", exposedId: "gpt-5", contextWindow: 272000, listed: false }`，复现 `converter.rs:234` 现有的 `starts_with("gpt-5")` 原样透传语义（返回值为**小写后的请求名原样**，与现有 `Some(model_lower)` 一致）。
  该行 `listed: false`，否则 `/v1/models` 会多出一个并不存在的 `gpt-5` 条目——而三个真实的 `gpt-5.6-sol` / `-terra` / `-luna` 仍以 `matchKind: "exact"` 行存在于列表中（`handlers.rs:388-412`）。
  > v1 只有精确/规范化匹配 + 默认关闭的 passthrough，**无法复现现有 `gpt-5*` 通配**，是行为回归。

## 6. 同步流程

### 6.1 触发

- `modelSyncEnabled == true` → 每日 `modelSyncTime`（本地时区）
- 任何时候 → `POST /models/sync` 手动
- **无「启动后 30s 自动跑一次」**（v1 有，与零回归矛盾）

调度器创建于 `main.rs` 的 admin 分支**之外** —— `AdminService` 仅在 `adminApiKey` 非空时创建（`main.rs:270`），若挂在其内，未配管理密钥的部署将没有自动同步。手动触发端点仍在 admin 路由下。

### 6.2 凭据来源与「权威轮次」

```
探针凭据 (modelSyncProbeCredentialId)
  ├─ 存在、未禁用、token 可刷新 → 权威轮次（authoritative）
  └─ 否则 → 采样 3 个启用凭据取并集 → 非权威轮次（advisory）
       └─ 全部失败 → 本轮放弃，不写文件
```

采样而非遍历全部：项目支持批量导入（`batch-import-dialog`、`kam-import-dialog`），凭据可达上百，每轮遍历即上百次上游请求。

**只有权威轮次能判定消失。** 非权威轮次只做新增与更新，**不递增 `missingSyncRounds`、不标 deprecated**。

> 这是对 v1 的关键收紧。上游契约明确说明模型集随订阅等级不同（`kiro/model/available_models.rs:6`）；v1 的「一个凭据返回非空即整轮可信」会让采到低等级凭据的轮次把高等级独有模型误判为消失。

### 6.3 Diff 规则

| 情形 | 动作 |
|---|---|
| 上游有、表内无 | 新增行：`origin=synced`、`enabled=true`、`status=active`、`matchKind=exact`、`exposedId` 按 §4.4 派生、`contextWindow` = `maxInputTokens` 经 §6.4 校验后取值、`maxOutputTokens` = 同族内置默认或 64000、`sortOrder` = 当前最大值 + 10 |
| 两侧都有 | 逐字段更新**非 `pinned`** 字段；`missingSyncRounds = 0`；`lastSeenAt = now`；若原 `status = deprecated` 则**复活为 `active`** |
| 表内有、上游无（**仅权威轮次**） | `missingSyncRounds += 1`；达阈值 `M = 2` → `status = deprecated`。**永不删行** |

> **「表内有」指的是有效行集 = 内置默认 ∪ 覆盖层，不是 `file.models` 单独。** 实现期修正：原文只写「表内有」，实现者合理地读成了覆盖层文件里的行，后果是**最该被标 deprecated 的老内置模型永远标不上** —— 它们不在 `file.models` 里，消失判定遍历不到。同一根因还导致首轮同步把全部内置模型计为「新增」，让 `/models/sync` 的 diff 摘要失真。
> 正确做法：diff 基线取 `ModelRegistry::from_file(file.clone())?.rows()` 的有效行集。新行的 `sortOrder` 基线同理要取内置 ∪ 覆盖层的最大值，否则会与内置行撞号。
>
> **同步元数据不得写进 `models` 数组。** 二次修正：本节原先写「对不在 `file.models` 里的内置行，按需在覆盖层补写一行以承载 `missingSyncRounds` / `status`」—— 这个处方是错的。`from_file` 的叠加语义是 `*existing = incoming`（**整行替换**，非逐字段合并），所以一旦内置行被补写进 `file.models`，它就成了那一刻的**完整冻结快照**：后续版本在代码里修改该行的 `contextWindow` / `displayName` 对已有部署**完全失效**，`models.json` 也从「稀疏的人工覆盖」退化成「内置表的全量副本」。实测首个权威轮次即写入 14 行 `origin=Builtin` 快照。
> 正确做法：`missingSyncRounds` / `status` / `lastSeenAt` 是**纯同步元数据**，应存在 `syncState` 下的 `modelMeta: { <upstreamId>: { missingSyncRounds, status, lastSeenAt } }`，与 `models` 数组解耦。解析时由 `ModelRegistry` 把元数据叠加到有效行集上，而不是把行复制进覆盖层。
>
> **`matchKind == "prefix"` 的行必须排除在消失判定之外。** 上游的 `modelId` 永远不可能等于 `gpt-5`（它是家族通配符，不是真实模型），参与判定会导致**每次部署都确定性地误标 Deprecated**。
>
> **消失判定需要一道「单轮标记比例」护栏。** 规则 1 防的是「网络抖动把全表刷成 deprecated」，但探针配置错误（例如把低订阅等级的凭据设为探针）会从另一条路径造成同样后果 —— 实测探针只返回 1 个模型时，第 2 个权威轮次把 14/14 内置行全部标记。当单轮缺失行数超过有效行集的 50% 时，应判定为「探针可能不具代表性」，**跳过本轮消失判定并打 error 级日志**，而不是照单执行。
>
> **比例必须只按 `status == Active` 的行计算。** 三次修正：上一版没有限定口径，实测这会让护栏变成一个**只增不减的棘轮** —— 已判定为 `Deprecated` 的行永不删除，却同时留在分子和分母里，于是每正常退役一批模型，比例就单调上升一格，越过 50% 后**永久锁死消失判定**（实测：上游一次性只保留 5/13 个模型，连跑 10 个权威轮，计数器一次都没递增）。已经不在上游的行本来就不该参与「探针是否具代表性」的判断。计数循环仍可遍历完整缺失集（对已 Deprecated 的行继续累加无害），只有护栏判据换成 Active 口径。
>
> **返回空列表的凭据不得写入 `credentialSupport` 记录。** 空列表已被规则「不可信轮次」判为不可信信号，若仍持久化成一条空记录，按 §6.6 的过滤语义（有记录则要求包含目标模型）就等于断言「该凭据不支持任何模型」，一次 token 抖动会把凭据永久踢出轮换。实现上应 `if models.is_empty() { continue; }` 后再记录。
>
> **乱序保护必须解析时间戳后比较，不得用 RFC3339 字符串字典序。** 时区偏移会双向破坏单调性：负偏移导致漏挡（旧观测覆盖新观测），正偏移导致误挡（同步在偏移时长内全部停摆）。`models.json` 是人可编辑的配置文件，写成 `...Z` 或本地偏移即触发。解析失败时按「无记录」放行。

### 6.4 数值与并集冲突

- `maxInputTokens` 上游为 `Option<i64>`（`kiro/model/available_models.rs:45`），目标 `i32`。转换规则：`None` 或 `<= 0` 或 `> i32::MAX` → 回退 `200000` 并打 `warn`。
- 采样并集中同一 `modelId` 出现多次且值不同：`contextWindow` 取 **max**；`displayName` / `description` 取**凭据 id 升序的首个非空值**（保证确定性）。

### 6.5 落盘、乱序与并发

- **落盘**：写临时文件 → `rename` 原子替换 → 成功后才 swap `REGISTRY`。写失败则不 swap，内存保持旧值。
- **乱序保护**：每轮记录 `fetchStartedAt`；写入前若 `syncState.lastSyncAt > 本轮 fetchStartedAt`，说明有更新的结果已落盘，**丢弃本轮**。手动与定时同步并发完成时避免旧观测覆盖新观测。
- **并发写**：`Store` 内一个 `tokio::Mutex` 串行化所有写路径（定时同步、手动同步、UI 逐字段编辑），每次写均为 read-modify-write。

### 6.6 调度层配套改动（必需）

`credential_matches_request`（`token_manager.rs:1065`）目前只识别 Opus / 非 Opus：

```rust
let is_opus = model.map(|m| m.to_ascii_lowercase().contains("opus")).unwrap_or(false);
if is_opus && !credentials.supports_opus() { return false; }
```

一个新同步进来的非 Opus 模型没有任何按凭据的准入判断，可能选到从未宣告该模型的凭据；而 provider 对非 429/5xx 的 4xx **直接返回、不换凭据重试**（`provider.rs:793`），客户端直接吃 400。

**改动**：`credential_matches_request` 增加一层过滤 —— 若 `credentialSupport` 中存在该凭据的记录，则要求记录包含目标 `upstreamId`；**无记录的凭据视为「未知」并放行**（保守，不误杀）。现有 `supports_opus()` 特判保留。

**残留风险（明确接受）**：采样模式下大多数凭据无 `credentialSupport` 记录 → 放行 → 仍可能选到不支持的凭据。缓解手段：把探针凭据设为最高订阅等级（探针轮次会记录其模型集），以及在 UI 上提示「未记录可用模型的凭据数」。彻底根治需要让 provider 对「模型不被该凭据支持」类 4xx 换凭据重试，属于 provider 重试语义变更，不在本设计范围。

### 6.7 deprecated 的确切语义

- 仍可解析、仍可用 —— 不打断正在使用它的客户端
- 仍出现在 `/v1/models` —— 否则客户端模型列表突然缺项，比报错更难排查
- admin UI 显著标黄；人工可置 `enabled = false` 真正下线（此时从列表移除）

## 7. 错误处理

### 7.1 拒绝原因区分，且**各路径保持各自现有语言**

| 路径 | Unknown（文案不变） | Disabled（新增） |
|---|---|---|
| `handlers.rs:697` / `:1488` | `模型不支持: {model}` | `模型已禁用: {model}` |
| `websearch_loop.rs:267` | `unsupported model: {model}` | `model disabled: {model}` |

> v1 声称「统一保持中文文案不变」，但 web-search 路径本来就是英文（`websearch_loop.rs:267`）。统一语言会改变该路径的既有文案。

「我配了它但不生效」与「我没配它」是不同的排查方向，共用一个报错等于丢弃该信息。

### 7.2 其他

- **Passthrough 命中**：打 `warn`（「该模型未在表中，走透传，窗口按 200K 估算」）。去重集合**容量上限 64**，超出后按分钟节流 —— `MessagesRequest.model` 由客户端控制（`handlers.rs:623`），无界去重集合可被打爆内存。
- **`models.json` 损坏或校验失败**：`error` 日志 + 内置默认继续启动；`GET /models` 响应中以 `degraded: true` + `degradedReason` 暴露，UI 顶部红色横幅。
- **同步任务异常**：捕获后 `warn`，不影响服务，不改动 `models.json`。

## 8. Admin API

路由风格与现有一致（`admin/router.rs` 为平铺路由）。

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/models` | 全表 + `aliases` + `syncState` + `settings` + `degraded` + `credentialSupport` 覆盖率 |
| `POST` | `/models/sync` | 手动同步，返回 diff 摘要（新增 N / 更新 N / 标记 deprecated N / 轮次类型 authoritative\|advisory） |
| `POST` | `/models` | 手动新增一行（`origin = manual`） |
| `PATCH` | `/models/{upstreamId}` | 编辑字段；被编辑字段自动进 `pinned`；支持 `{"unpin": ["contextWindow"]}` |
| `DELETE` | `/models/{upstreamId}` | 仅允许 `origin != builtin` |
| `POST` / `DELETE` | `/models/aliases` | 别名增删 |
| `PATCH` | `/models/settings` | 四个开关（写 `ModelSyncRuntimeConfig` + `config.json`） |

**PATCH 可写字段白名单**：`exposedId`、`displayName`、`contextWindow`、`maxOutputTokens`、`exposeThinkingVariant`、`enabled`、`sortOrder`、`matchKind`。
**只读字段**：`upstreamId`、`origin`、`status`、`missingSyncRounds`、`lastSeenAt`、`created`。
> `origin` 必须只读，否则可把 `builtin` 改成 `manual` 绕过删除保护。

`unpin` 不可省略：缺少它则「手动改过一次窗口」等于永久放弃该字段的自动同步，长期会使整表锁死为陈旧手写值。

**新增 Admin 错误变体**（现有 `AdminError::NotFound` 只携带数字凭据 id、`InvalidCredential` 是凭据专用，`admin/error.rs:15,35`）：`ModelNotFound(String)`、`ModelConflict(String)`、`InvalidModelField(String)`。

## 9. UI

作用域全局，入口在凭据管理页 —— 两者不矛盾。

1. **凭据管理页顶栏**（`topbar-tools.tsx` 已有工具区）新增「模型映射」按钮 → 打开全局 `model-mapping-dialog.tsx`。
2. **增强 `available-models-dialog.tsx`**（现为按凭据只读展示）：每行标注「✓ 已在映射表 / ⊕ 未收录」，未收录行提供「加入映射表」快捷按钮。查看某凭据可用模型时顺手补齐缺项，是最自然的发现路径。

`model-mapping-dialog` 三个 tab：

- **模型表**：对外名、上游 id、匹配方式（exact/prefix）、输入窗口（可编辑，带 🔒 pin，可点击解除）、输出上限、thinking 变体开关、启用开关、状态徽章（active/deprecated）、来源徽章（builtin/synced/manual）
- **别名**：`from` → `to` 增删，`to` 下拉限定为已存在的 `upstreamId`
- **同步设置**：`modelSyncEnabled`、`modelSyncTime`、探针凭据选择、passthrough 开关

顶部：`lastSyncAt` + 轮次类型 + 「立即同步」按钮；`degraded` 时红色横幅；`credentialSupport` 覆盖率偏低时提示配置探针凭据（关联 §6.6 残留风险）。

## 10. 测试策略

| # | 层 | 内容 |
|---|---|---|
| 1 | `ModelRegistry::resolve` **回归基线** | 迁移 `converter.rs` 现有 `map_model` / `get_context_window_size` 的**全部**用例：`claude-sonnet-4-5-20250929`、`claude-sonnet-5-20260101-thinking`（`converter.rs:1848`）、`claude-fable-5-thinking`、`sonnet-5` 与 legacy `claude-3-5-sonnet` 优先级、**`gpt-5*` 通配透传**、各模型 1M/272K/200K 窗口 |
| 2 | 规范化规则 | thinking→日期→版本段的**顺序**；`claude-3-5-sonnet` 反例；`xxx-thinking` 在 `exposeThinkingVariant=false` 行上被拒 |
| 3 | `prefix` 行 | `gpt-5.6-sol` / `gpt-5.9-xyz` 命中同一 prefix 行且 `upstream_id` 原样；最长前缀优先 |
| 4 | 加载校验 | 版本缺失/为 2、重复 `upstreamId`、`exposedId` 与 alias 撞名、dangling alias、alias 指向 alias、窗口越界 → 均 `degraded=true` + 退回内置默认 |
| 5 | `Store` | 临时目录真实读写；pinned 不被覆盖；`unpin` 后恢复可覆盖；`rename` 原子替换；乱序丢弃（`lastSyncAt > fetchStartedAt`） |
| 6 | `SyncService` diff | 注入假 `ModelListFetcher`；新增/更新/消失；`M=2` 才 deprecated；deprecated 复活；**非权威轮次不判消失**；**全凭据失败 → 文件字节未变**；并集冲突取 max / 确定性 displayName |
| 7 | `/v1/models` 构造 | `Model` 全字段正确，`max_tokens` 取 `maxOutputTokens` 而非 `contextWindow`；`sortOrder` 顺序稳定；deprecated 在列表内、`enabled=false` 不在 |
| 8 | 调度过滤 | 有 `credentialSupport` 记录且不含目标模型 → 该凭据被跳过；无记录 → 放行；`supports_opus()` 特判仍生效 |
| 9 | 集成 | 未知模型 400 文案**分路径**逐字不变（anthropic 中文 / websearch 英文）；passthrough 开关 on/off；覆盖 `/v1/messages`、`/v1/chat/completions`（`openai.rs:85`）、`/v1/responses`（`responses.rs:162`）三条路径的错误渲染 |
| 10 | 全量回归 | `cargo test` 全绿 |

第 1 项与第 6 项的两条加粗断言是本设计的安全绳：前者保证改造不破坏现有可用能力，后者保证一次网络抖动或一次低等级采样不会把全表判死。

## 11. 验收标准

1. 不存在 `models.json` 且 `modelSyncEnabled=false` 时，`/v1/models` 输出与改造前逐字节一致；`cargo test` 全绿。
2. `gpt-5.6-sol` 与任意 `gpt-5*` 模型行为与改造前一致（对外 id 保留点号、原样透传、窗口 272K）。
3. `claude-sonnet-5-20260101-thinking` 等既有测试输入解析结果不变。
4. 开启 `modelSyncEnabled` 后，上游新增模型经一轮同步即在 `/v1/models` 出现并可请求，无需改代码或重启。
5. 手动覆写 `contextWindow` 后同步，该值不变而其他字段正常更新；`unpin` 后恢复被同步覆盖。
6. 模拟上游返回空列表 / 全凭据失败 → `models.json` 字节未变、无模型被标 deprecated。
7. 非权威（采样）轮次未见某模型 → **不**标 deprecated。
8. 权威轮次连续 2 轮未见 → `status = deprecated`，仍可解析、仍在 `/v1/models`，UI 标黄。
9. `enabled = false` → 从 `/v1/models` 移除，请求返回 `模型已禁用`（web-search 路径为 `model disabled`）。
10. `/v1/models` 中某模型的 `max_tokens` 等于其 `maxOutputTokens`，且与 `get_context_window_size()` 返回值不相等（验证两个量未被混淆）。
11. 请求未在表中的模型：passthrough 关 → 400；开 → 发往上游并打一条 warn。
12. 为 `claude-opus-5` 建别名或等一轮同步后该模型可用（本设计要解决的原始问题）。

## 12. 已知限制

1. **调度层残留风险**：采样模式下大多数凭据无 `credentialSupport` 记录，可能选到不支持目标模型的凭据，导致 400 且不换凭据重试。缓解见 §6.6。
2. **不支持 schema 自动迁移**：`version != 1` 直接拒绝并退回内置默认。
3. **`create_router_with_provider` 需调用方先初始化 registry**：未初始化时退回内置默认（不 panic），需在文档注释中写明。
4. **`config.json` 与 `models.json` 是两个文件**：settings 与模型表分处两地，需分别备份。

## 13. v1 → v2 修订记录

经 Codex 静态审查（对照 commit `767cd5c`）并逐条核验代码后修订。核验方式为直接读取被引用的源码位置，6/6 抽检为真。

### 阻断级

| # | v1 问题 | v2 处理 |
|---|---|---|
| 1 | 每日同步默认开启 + 启动后 30s 无条件同步，与「零行为回归」矛盾 | 新增 `modelSyncEnabled` 默认 `false`；删除启动自动同步（§2、§6.1） |
| 2 | 纯精确/规范化匹配无法复现现有 `gpt-5*` 通配（`converter.rs:234`） | 新增 `matchKind: "prefix"` 行类型，内置 `gpt-5` prefix 行（§5.3） |
| 3 | 把按凭据的模型准入推给调度器，但 `credential_matches_request` 只认 Opus/非 Opus（`token_manager.rs:1065`），且 provider 对 4xx 不换凭据重试（`provider.rs:793`） | 新增 `credentialSupport` 缓存 + 调度过滤；明确残留风险与缓解（§6.6、§12） |

### 主要

| # | v1 问题 | v2 处理 |
|---|---|---|
| 4 | 规范化先剥日期再剥 thinking，`claude-sonnet-5-20260101-thinking`（真实测试 `converter.rs:1848`）日期永久残留 | 顺序改为 thinking → 日期 → 版本段（§5.2） |
| 5 | 规范化无条件剥 `-thinking`，绕过 `exposeThinkingVariant=false` | 新增显式拒绝规则（§5.3） |
| 6 | 「一个凭据返回非空即整轮可信」会因订阅差异误判 deprecated（`available_models.rs:6`） | 引入权威/非权威轮次；只有探针轮次能判消失（§6.2） |
| 7 | 一律「点号转连字符」会把 `gpt-5.6-sol` 破坏为 `gpt-5-6-sol`（`handlers.rs:388`） | 派生规则按 provider 前缀区分（§4.4） |
| 8 | 表行缺 `Model` 必需字段，且用单一 `contextWindow` 同时喂输入窗口与 `Model.max_tokens`（二者语义不同、数值差一个量级） | 拆为 `contextWindow` / `maxOutputTokens`，补 `ownedBy`/`modelType`/`created`/`sortOrder`（§4.2、§4.3） |
| 9 | 错误断言 `get_context_window_size` 只被 converter 与测试调用 | 修正为另有 3 处（`handlers.rs:1155`、`stream.rs:1525`、`websearch_loop.rs:200`），并因此引入请求级快照一致性规则（§3.3） |
| 10 | 声称「统一保持中文文案」，但 web-search 路径本为英文（`websearch_loop.rs:267`） | 改为各路径各自保持原语言（§7.1） |
| 11 | 三个开关塞进不可变 `Config` clone（`token_manager.rs:1003`），PATCH 无法热生效 | 引入 `ModelSyncRuntimeConfig` 运行时 holder（§4.1） |
| 12 | 自动同步调度器落在仅 `adminApiKey` 非空才创建的 `AdminService` 分支内（`main.rs:270`） | 调度器移出 admin 分支（§6.1） |
| 13 | `SyncService` 可测性被高估：依赖具体 `TokenManager`，`get_available_models_for` 直接发网络请求（`token_manager.rs:2702`） | 引入 `ModelListFetcher` trait（§3.1） |
| 14 | alias 语义未定义（指向什么、递归、dangling、指向 disabled） | `to` 必须是 `upstreamId`、禁递归、dangling 拒绝加载、指向 disabled → `Rejected(Disabled)`（§4.5、§5.3） |
| 15 | `enabled=false` 的列表可见性未定义 | 明确从 `/v1/models` 移除，与 deprecated 保留形成对比（§4.3、§6.7） |
| 16 | 无 schema 版本策略 | `version != 1` 拒绝加载 + degraded（§4.5） |
| 17 | `maxInputTokens: Option<i64>` → `i32` 无校验（`available_models.rs:45`） | 无效值回退 200000 + warn（§6.4） |
| 18 | 无唯一性校验，解析结果依赖遍历顺序 | 加载时校验 upstreamId / 全部对外名 / alias.from 唯一（§4.5） |
| 19 | 采样并集对同一模型不同值无冲突策略 | 窗口取 max、名称按凭据 id 升序取首个非空（§6.4） |
| 20 | 同步乱序完成可用旧观测覆盖新观测 | `fetchStartedAt` 比对丢弃（§6.5） |
| 21 | PATCH 可写字段未限定，可改 `origin` 绕过 builtin 删除保护 | 白名单 + 只读字段清单（§8） |
| 22 | Admin 错误类型不适配（`error.rs:15,35` 为凭据专用） | 新增三个模型相关变体（§8） |
| 23 | `config.json` 写入沿用无保护 load-modify-save（`service.rs:1236`） | 新增 config 写 mutex（§4.1） |

### 次要

| # | v1 问题 | v2 处理 |
|---|---|---|
| 24 | `models.json` 放 config 目录，与既有运行时数据归属不一致（`main.rs:35`、`:176`） | 改放凭据目录（§4.2） |
| 25 | `modelSyncTime` 无校验与时区语义 | 复用既有实现（`service.rs:209`、`:923`）（§4.1） |
| 26 | warn 去重集合无界，`model` 由客户端控制（`handlers.rs:623`） | 容量上限 64 + 分钟节流（§7.2） |
| 27 | 测试遗漏 `/v1/chat/completions`、`/v1/responses` 路径 | 补入集成测试（§10 第 9 项） |
| 28 | `create_router_with_provider`（`router.rs:27`）无 registry 初始化契约 | 写明「未初始化退回内置默认」并列入已知限制（§3.2、§12） |

# 模型注册表：手动映射 + 上游自动同步

- 日期：2026-07-25
- 状态：待实现
- 影响范围：`src/anthropic/converter.rs`、`src/anthropic/handlers.rs`、`src/admin/*`、`admin-ui`
- 新增文件：`src/anthropic/model_registry.rs`、`models.json`（运行时配置）

## 1. 背景与问题

网关当前把「有哪些模型、模型叫什么、上下文窗口多大」写死在三处编译期常量里：

| 决定什么 | 位置 | 现状 |
|---|---|---|
| 对外暴露哪些模型（`/v1/models`） | `src/anthropic/handlers.rs:385` `available_models()` | 硬编码 `Vec<Model>` |
| 请求能否进入、映射成哪个上游 id | `src/anthropic/converter.rs:199` `map_model()` | 硬编码 `contains` 分支 |
| 上下文窗口大小 | `src/anthropic/converter.rs:246` `get_context_window_size()` | 硬编码 1M / 272K / 200K |

同时，上游 `ListAvailableModels` 已经被调用并且**已经返回了 `maxInputTokens`**：

```
src/kiro/token_manager.rs:627  拉取上游
  → src/admin/service.rs:787-804  转为 AvailableModelItem{modelId, modelName, description, maxInputTokens}
    → GET /credentials/{id}/models  (src/admin/handlers.rs:152)
      → admin-ui/src/components/available-models-dialog.tsx  只读展示
```

**根因：上游实际提供什么，和网关认什么，是两套互不同步的知识。** 直接后果是上游上线新模型（例如 `claude-opus-5`）后网关仍返回 `400 模型不支持`，必须改代码并发版才能使用。

## 2. 目标与非目标

### 目标

1. 三张硬编码表变为「编译内置默认 + 运行时可覆盖」。
2. 支持手动映射：任意对外模型名 → 任意上游模型 id。
3. 支持手动覆写上下文窗口，且覆写不被自动同步冲掉。
4. 支持从上游自动同步模型列表与窗口大小，无需改代码或发版。
5. **零行为回归**：不配置任何东西时，行为与当前版本逐字节等价。

### 非目标

1. **不做按凭据/按分组的差异化映射。** 映射发生在选凭据之前（`handlers.rs:694` 先 `convert_request_with_mode()`，之后才由 provider 层注入 `profile_arn`），改为 per-credential 需要重构请求主链路时序，风险与收益不成比例。不同订阅等级凭据可用模型不同的问题，属于**调度层**职责（调度时跳过不支持该模型的凭据），不在本设计内。
2. 不做模型能力矩阵（是否支持 vision / tool use / effort 等）。现有 `model_supports_native_reasoning()` / `model_supports_xhigh_effort()` 保持不变。
3. 不引入新的 crate 依赖（`parking_lot` 已在依赖内）。

### 关于参考项目的说明

需求提出时参考了 [Wei-Shaw/sub2api](https://github.com/Wei-Shaw/sub2api)。经查证，该项目 README 记载的能力为多账号管理（OAuth / API Key）、Composite Groups 路由、API Key 分发，**未记载模型映射与上游模型自动同步**。因此本设计仅在「凭据/账号管理的面板形态」上与其同构，映射与同步机制为本项目自有设计。

## 3. 架构

```
┌─ 写入侧 ──────────────────────────────────────────────┐
│  ModelSyncService                                      │
│   ├─ 启动后延迟 30s 一次 / 每日 modelSyncTime / 手动    │
│   ├─ 选探针凭据 → 不可用则回退采样 3 个取并集           │
│   └─ read-modify-write models.json（逐字段保留 pinned） │
└──────────────────────┬─────────────────────────────────┘
                       │ 落盘成功后 swap
┌─ 读取侧 ─────────────▼─────────────────────────────────┐
│  ModelRegistry (src/anthropic/model_registry.rs)        │
│   BUILTIN_DEFAULTS (编译内置) ⊕ models.json 覆盖层      │
│   ├─ resolve(requested) -> Resolution                  │
│   └─ exposed_models() -> Vec<Model>                    │
└──────────────────────┬─────────────────────────────────┘
                       │ 三个纯函数内部查它，签名不变
      map_model / get_context_window_size / available_models
                       │
                现有调用点零改动
```

### 3.1 组件职责

| 组件 | 职责 | 依赖 | 测试方式 |
|---|---|---|---|
| `ModelRegistry` | 纯数据 + 纯方法：给定模型名返回上游 id / 窗口 / 是否启用 | 无（构造时注入 defaults + overlay） | 直接构造后调 `resolve`，不碰全局、不碰文件 |
| `ModelRegistryStore` | `models.json` 的读 / 写 / 合并（保 pinned）/ 原子落盘 | 文件系统 | 临时目录真实读写 |
| `ModelSyncService` | 选凭据、拉上游、算 diff、判 deprecated、调 Store | `TokenManager`、`Store` | 注入假模型列表，断言 diff 与 deprecated 判定 |
| 全局 `REGISTRY` | 「进程当前实例」的 holder，无业务逻辑 | `parking_lot::RwLock<Arc<_>>` | 不单测 |

**硬约束：`ModelRegistry` 内不含任何 I/O、不含任何时间概念。** 时间语义（deprecated 宽限、`lastSeenAt`）只属于 `ModelSyncService`。这使模型解析这段最热的逻辑完全确定性，可直接复用现有 `map_model` 测试用例作为回归基线。

### 3.2 接线方式

选定「进程级 registry + 纯函数内部查表」：

```rust
static REGISTRY: LazyLock<RwLock<Arc<ModelRegistry>>>;   // parking_lot::RwLock

pub fn map_model(model: &str) -> Option<String> {        // 签名不变
    match registry().resolve(model) {
        Resolution::Mapped { upstream_id, .. } | Resolution::Passthrough { upstream_id } => Some(upstream_id),
        Resolution::Rejected(_) => None,
    }
}
```

热重载为 `*REGISTRY.write() = Arc::new(new)`；读侧 `Arc::clone` 取快照，无锁竞争。

**改动面的准确边界**（不是「零改动」，有一处必须改）：

`map_model` 返回 `Option<String>`，无法携带「命中了但被禁用」这一信息——`None` 只会退化成 `模型不支持`，与 7.1 节要求的 `模型已禁用` 冲突。因此：

| 位置 | 改动 |
|---|---|
| `converter.rs:595`（`convert_request_with_mode` 内） | 改为直接调 `registry().resolve()`，据 `Resolution` 分派；新增 `ConversionError::ModelDisabled(String)` |
| `handlers.rs:697` 与 `handlers.rs:1488` 的错误 match | 各加一个 `ModelDisabled` 分支（`Unknown` 分支文案不动） |
| `websearch_loop.rs:263` 的错误处理 | 同上，加一个分支 |
| `map_model()` / `get_context_window_size()` | 签名不变，内部改为查 registry。二者此后仅被 `converter` 内部与测试调用（已确认无其他调用方） |

即：**模型解析逻辑集中在 registry 一处，外层只增加一个错误分支**。这与被否决的「方案 3」不同——后者要求三处调用点各自实现查表，本方案三处只是各自渲染一个新错误。

已评估并否决的替代方案：

- **依赖注入**（`convert_request_with_mode(req, mode, &registry)`）：无全局状态，但需改 3 个调用点 + `AppState` + `converter.rs` 中大量既有测试的签名，且 `convert_request` 便捷入口失去意义。
- **在 handler 内先查表再传给 converter**：converter 更纯，但把「模型解析」这一内聚概念切成两半散在两层，`websearch_loop` 需各自维护查表逻辑，三处调用点各自可能漏查——把「一个全局量」换成「三处重复」。

## 4. 数据模型

### 4.1 配置开关（`config.json`）

人工 / admin API 拥有，**同步任务永不写入**。形态沿用既有 `updateAutoApplyTime`。

```json
{
  "modelSyncTime": "04:00",
  "modelSyncProbeCredentialId": 3,
  "allowUnknownModelPassthrough": false
}
```

三者均可缺省：`modelSyncTime` 缺省 `"04:00"`，`modelSyncProbeCredentialId` 缺省 `null`（直接走采样），`allowUnknownModelPassthrough` 缺省 `false`。

### 4.2 模型表（`models.json`，与 `config.json` 同目录）

同步任务与人工共同写入，统一经 `Store` 的 read-modify-write。

```json
{
  "version": 1,
  "syncState": { "lastSyncAt": "2026-07-25T04:00:00Z", "source": "probe:3" },
  "models": [
    {
      "upstreamId": "claude-opus-4.8",
      "exposedId": "claude-opus-4-8",
      "displayName": "Claude Opus 4.8",
      "contextWindow": 1000000,
      "exposeThinkingVariant": true,
      "enabled": true,
      "status": "active",
      "origin": "synced",
      "pinned": ["contextWindow"],
      "missingSyncRounds": 0,
      "lastSeenAt": "2026-07-25T04:00:00Z"
    }
  ],
  "aliases": [
    { "from": "opus", "to": "claude-opus-4.8" }
  ]
}
```

字段语义：

| 字段 | 说明 |
|---|---|
| `upstreamId` | 上游 `modelId`，行主键。上游使用点号（`claude-opus-4.8`） |
| `exposedId` | 对外 Anthropic 风格名，使用连字符（`claude-opus-4-8`） |
| `contextWindow` | 上下文窗口。同步时取 `maxInputTokens`，缺失则 200000 |
| `exposeThinkingVariant` | 是否额外暴露 `{exposedId}-thinking`，映射到同一 `upstreamId` |
| `enabled` | `false` 时解析被拒（区别于「不存在」，见 6.1） |
| `status` | `active` \| `deprecated` |
| `origin` | `builtin` \| `synced` \| `manual` |
| `pinned` | 已被人工编辑、同步时逐字段跳过的字段名列表 |
| `missingSyncRounds` | 连续多少轮可信同步未在上游出现 |

设计取舍说明：

- **一行一个上游模型，而非一行一个对外名。** `-thinking` 变体由 `exposeThinkingVariant` 派生，因其与主模型共享 `upstreamId` 与 `contextWindow`；拆成两行会导致「改窗口需记得改两处」，是配置漂移的来源。
- **`pinned` 是字段级而非行级。** 人工写了窗口、上游改了 `displayName`，两者应各自生效。
- **`aliases` 独立于 `models`。** 生命周期不同：`models` 会被同步覆盖，`aliases` 永远只属于人工。

### 4.3 内置默认

`available_models()` / `map_model()` / `get_context_window_size()` 现有内容降级为 `BUILTIN_DEFAULTS`（`origin: "builtin"`），`models.json` 作为覆盖层叠加其上。

- `models.json` 不存在 → 行为与当前版本等价（**零行为回归**）。
- `models.json` 损坏 → 退回内置默认并置 `degraded = true`（见 6.3），不 panic、不空表。
- `origin: "builtin"` 的行永不被同步删除。

## 5. 模型解析

### 5.1 解析顺序

```rust
enum Resolution {
    Mapped { upstream_id: String, context_window: i32 },
    Passthrough { upstream_id: String },   // 窗口固定 200_000
    Rejected(RejectReason),                // Unknown | Disabled
}
```

1. `aliases` 精确命中
2. `exposedId` 精确命中（含 `{exposedId}-thinking`，当 `exposeThinkingVariant = true`）
3. **规范化后**精确命中
4. `allowUnknownModelPassthrough = true` → `Passthrough`（规范化后的名字原样发上游，窗口 200K）
5. 否则 `Rejected(Unknown)`

命中第 2 / 3 步但 `enabled = false` → `Rejected(Disabled)`。

### 5.2 规范化规则

当前 `map_model` 使用模糊 `contains` 匹配，能容忍 `claude-opus-4-5-20251101`、`claude-sonnet-4-6-thinking` 等写法。改为查表后**必须保住该容忍度**，否则构成行为回归。规范化按序执行：

1. 转小写
2. 剥日期后缀：尾部 `-\d{8}`
3. 剥 `-thinking` 后缀
4. 版本段连字符转点号：`-(\d+)-(\d+)$` → `-$1.$2`

**反例约束**（必须有测试覆盖）：`claude-3-5-sonnet` 不得被规范化为命中 `claude-sonnet-5`。当前代码已刻意规避此冲突（`converter.rs:211` 注释「精确匹配 5 代，避免命中 legacy claude-3-5-sonnet」），新实现须保持。

## 6. 同步流程

### 6.1 触发与凭据选择

触发：启动后延迟约 30s 一次（避开启动风暴）+ 每日 `modelSyncTime` + `POST /models/sync` 手动。

```
探针凭据 (modelSyncProbeCredentialId)
  ├─ 存在、未禁用、token 可刷新 → 使用
  └─ 否则 → 从启用凭据中采样 3 个，取模型并集
       └─ 全部失败 → 本轮放弃（见 6.3）
```

采样而非遍历全部，是因为项目支持批量导入（`batch-import-dialog`、`kam-import-dialog`），凭据可达上百，每轮遍历即上百次上游请求。

### 6.2 Diff 规则

| 情形 | 动作 |
|---|---|
| 上游有、表内无 | 新增行：`origin = synced`、`enabled = true`、`status = active`、`exposedId` = `upstreamId` 点号转连字符、`contextWindow` = `maxInputTokens ?? 200000`、`exposeThinkingVariant` = `upstreamId.starts_with("claude-")` |
| 两侧都有 | 逐字段更新**非 `pinned`** 字段；`missingSyncRounds = 0`；`lastSeenAt = now`；若原 `status = deprecated` 则**复活为 `active`** |
| 表内有、上游无 | `missingSyncRounds += 1`；达阈值 `M = 2` → `status = deprecated`。**永不删行** |

`exposeThinkingVariant` 对新增行按前缀判定：`claude-*` 派生 thinking 变体，`gpt-5*` 家族不派生。

### 6.3 失败语义

> **一轮同步仅在「至少一个凭据成功返回非空模型列表」时才算可信。不可信轮次不递增 `missingSyncRounds`、不写文件。**

这是本设计唯一可能造成真实损害的路径，必须在设计层面堵死：网络抖动、凭据集体过期、上游 5xx 都会使返回变为空列表；若空列表被当作「上游已无任何模型」，一次抖动即可将全表刷成 deprecated。

同理，采样中**部分凭据失败**时：只使用成功者的并集，失败凭据不参与 deprecated 判定。

### 6.4 deprecated 的确切语义

- 仍可解析、仍可用 —— 不打断正在使用它的客户端。
- 仍出现在 `/v1/models` —— 否则客户端模型列表会突然缺项，比报错更难排查。
- admin UI 显著标黄；人工可手动置 `enabled = false` 真正下线。

### 6.5 落盘与并发

落盘：写临时文件 → `rename` 原子替换 → 成功后才 swap `REGISTRY`。写失败则不 swap，内存保持旧值。

并发：`Store` 内部一个 `tokio::Mutex` 串行化**所有**写路径（定时同步、手动同步、UI 逐字段编辑），每次写均为 read-modify-write。这正是将模型表从 `config.json` 中独立出来的原因——后者只能依赖「读最新再写」的约定（`admin/handlers.rs:818`），没有锁。

## 7. 错误处理

### 7.1 拒绝原因区分

| 原因 | HTTP | 文案 |
|---|---|---|
| `Unknown` | 400 | `模型不支持: {model}` —— **保持现有文案逐字不变**，客户端可能在做字符串匹配（`handlers.rs:698`） |
| `Disabled` | 400 | `模型已禁用: {model}` —— 新增 |

「我配了它但不生效」与「我没配它」是两个不同的排查方向，共用一个报错等于丢弃该信息。

### 7.2 其他

- **Passthrough 命中**：按模型名去重打一条 `warn`（「该模型未在表中，走透传，窗口按 200K 估算」）。该日志用于发现「客户端在用尚未同步到的模型」。
- **`models.json` 损坏**：启动时打 `error` 日志 + 用内置默认继续启动；该状态在 `GET /models` 响应中以 `degraded: true` 暴露，UI 顶部红色横幅提示。
- **同步任务异常**：捕获后打 `warn`，不影响服务运行，不改动 `models.json`。

## 8. Admin API

路由风格与现有一致（`src/admin/router.rs` 为平铺路由）。

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/models` | 全表 + `aliases` + `syncState` + `settings` + `degraded` |
| `POST` | `/models/sync` | 手动同步，返回 diff 摘要（新增 N / 更新 N / 标记 deprecated N / 本轮是否可信） |
| `POST` | `/models` | 手动新增一行（`origin = manual`） |
| `PATCH` | `/models/{upstreamId}` | 编辑字段；被编辑字段自动进 `pinned`。支持 `{"unpin": ["contextWindow"]}` 解除锁定 |
| `DELETE` | `/models/{upstreamId}` | 仅允许删除 `origin != builtin` 的行 |
| `POST` / `DELETE` | `/models/aliases` | 别名增删 |
| `PATCH` | `/models/settings` | `modelSyncTime` / 探针凭据 / passthrough 开关（写 `config.json`） |

`unpin` 不可省略：缺少它则「手动改过一次窗口」等于永久放弃该字段的自动同步，长期使用会使整表锁死为陈旧手写值。

## 9. UI

作用域是全局，入口在凭据管理页——两者不矛盾。

1. **凭据管理页顶栏**（`admin-ui/src/components/topbar-tools.tsx` 已有工具区）新增「模型映射」按钮 → 打开全局 `model-mapping-dialog.tsx`。
2. **增强 `available-models-dialog.tsx`**（现为按凭据只读展示上游模型）：每行标注「✓ 已在映射表 / ⊕ 未收录」，未收录行提供「加入映射表」快捷按钮。这是最自然的发现路径——查看某凭据可用模型时顺手补齐缺项。

`model-mapping-dialog` 三个 tab：

- **模型表**：列为对外名、上游 id、窗口（可编辑，带 🔒 pin 图标，可点击解除）、thinking 变体开关、启用开关、状态徽章（active / deprecated）、来源徽章（builtin / synced / manual）。
- **别名**：`from` → `to` 增删。
- **同步设置**：`modelSyncTime`、探针凭据选择、passthrough 开关。

顶部显示 `lastSyncAt` + 数据来源 + 「立即同步」按钮；`degraded = true` 时挂红色横幅。

## 10. 测试策略

| # | 层 | 内容 |
|---|---|---|
| 1 | `ModelRegistry::resolve` **回归基线** | 迁移 `converter.rs` 现有 `map_model` / `get_context_window_size` 的**全部**用例并逐条验证：`claude-sonnet-4-5-20250929`、`claude-fable-5-thinking`、`sonnet-5` 与 legacy `claude-3-5-sonnet` 的优先级、`gpt-5*` 透传、各模型 1M / 272K / 200K 窗口 |
| 2 | 规范化规则 | 点↔连字符、日期后缀剥离、`-thinking` 剥离，含反例（`claude-3-5-sonnet` 不得命中 `claude-sonnet-5`） |
| 3 | `Store` | 临时目录真实读写；**pinned 字段不被同步覆盖**；`unpin` 后恢复可被覆盖；损坏文件 → 回退内置默认且 `degraded = true`；`rename` 原子替换 |
| 4 | `SyncService` diff | 新增 / 更新 / 消失三情形；`missingSyncRounds` 达 `M = 2` 才 deprecated；deprecated 复活；**不可信轮次（空列表 / 全凭据失败）断言文件字节未变且无任何 deprecated** |
| 5 | 集成 | `/v1/models` 反映表内容（含 deprecated 仍在列表）；未知模型 400 **文案逐字不变**；passthrough 开关 on / off 行为差异 |
| 6 | 全量回归 | `cargo test` 全绿 |

第 1 项与第 4 项末条是本设计的两根安全绳：前者保证改造不破坏现有可用能力，后者保证一次网络抖动不会把全表判死。

## 11. 验收标准

1. 不存在 `models.json` 时，`/v1/models` 输出与改造前逐字节一致；`cargo test` 全绿。
2. 上游新增模型后，一轮同步即可在 `/v1/models` 出现并可正常请求，无需改代码或重启。
3. 手动覆写某模型 `contextWindow` 后，执行同步，该值不变而其他字段正常更新。
4. 模拟上游返回空列表，执行同步，`models.json` 字节未变、无模型被标 deprecated。
5. 上游连续 2 轮可信同步不再返回某模型 → 该模型 `status = deprecated`，仍可解析、仍在 `/v1/models`，UI 标黄。
6. 请求未在表中的模型：passthrough 关 → `400 模型不支持: {model}`；passthrough 开 → 请求发往上游并打一条 warn。
7. 手动为 `claude-opus-5` 建别名或等一轮同步后，该模型可用（本设计要解决的原始问题）。

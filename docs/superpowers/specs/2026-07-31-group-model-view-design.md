# 模型视图按凭据组对齐(group model view)设计

日期:2026-07-31
状态:待评审

## 背景与问题

不同 Kiro 账号(凭据)从上游拉到的可用模型集不同(例:凭据 1 有 19 个模型含
claude-opus-4.8,凭据 2 只有 11 个)。当前 `GET /v1/models` 与请求时的模型解析
只使用全局注册表(`models.json` 的 `models`/`aliases`),呈现的是全凭据并集视角,
与调用方 key 实际所在凭据组的能力不对齐:客户端会选到本组没有的模型,请求被
路由后吃到上游 400 `INVALID_MODEL_ID`(trace 中已观测到)。

已有基础设施:

- `models.json.credentialSupport`:凭据 id → 上游可用模型 id 列表,按凭据维度,
  由模型同步写入(当前为**采样**拉取,非全量)。
- `TokenManager.credential_support` 缓存 + `credential_matches_request`:
  选凭据时按此过滤,语义为"无记录 = 未知,放行"。
- `key_ctx.group`:client key → 凭据组,在 handlers 全程可得。

## 目标

1. `GET /v1/models` 按调用 key 所在组收窄:只展示该组内**至少一张凭据**支持的
   模型(并集语义,已确认;不做交集)。
2. 请求时校验:解析成功后,若该组无任何凭据支持解析出的上游模型,直接返回
   Anthropic 语义的 404 `not_found_error`,不再路由上游吃 400。
3. 模型同步的 credentialSupport 拉取从采样改为**全凭据覆盖**,尽量消灭"无记录"。

非目标(YAGNI):

- 不把注册表(`model_registry`)改为按凭据分表。上游模型 id 在账号间同名,
  差异只是"有无",注册表保持全局、纯确定性、无凭据概念的现有设计。
- 不改变注册表权威探针/采样轮次对 `models`/`aliases` 本身的维护逻辑。
- 不改变凭据选择(`credential_matches_request`)的现有过滤与放行语义。

## 设计

### 1. 组支持集查询(token_manager)

新增:

```rust
/// 返回组内凭据可用上游模型 id 的并集。
/// 组内存在"无 credentialSupport 记录"的凭据时返回 None(未知=不设限,
/// 与 credential_supports_model 的保守放行语义一致)。
/// group 为 None 时按"全部凭据"计算。
pub fn group_supported_models(&self, group: Option<&str>) -> Option<HashSet<String>>
```

- 数据源:现有 `credential_support` 缓存(RwLock<HashMap<String, Vec<String>>>)。
- 只统计未禁用凭据;组内没有任何凭据时返回 None(交给现有"无可用凭据"错误路径,
  不在本层报错)。

### 2. `GET /v1/models` 按组过滤(handlers)

- 现状:`current_registry().exposed_models()` 全局输出。
- 改为:取 `key_ctx.group` → `group_supported_models(group)`;为 `Some(set)` 时,
  仅保留"注册表行的 upstream id ∈ set"的模型;`None` 时输出不变。
- 并集语义:组内任一凭据支持即展示(已确认)。`auto` 等虚拟条目恒展示。
- 响应因 key 而异:该端点无缓存头,不新增缓存;报文派生逻辑
  (逐字段来自注册表行)不变,只是行集合收窄。

### 3. 请求时校验(handlers,messages / count_tokens / openai 兼容层共用)

- 位置:`registry.resolve()` 成功之后、构造上游请求之前。
- 判据:解析出的 **upstream id**(不是请求原名,避免 alias 误判)是否
  ∈ `group_supported_models(key_ctx.group)`;`None`(存在未知凭据)恒放行;
  `auto` 恒放行。
- 不通过 → HTTP 404,body 为 Anthropic `not_found_error`:
  `model not supported for this key group: <requested>`。
- 解析失败(注册表就不认识)维持现状(400 invalid_request_error /
  `unsupported model`),两类错误不混同。

### 4. 模型同步全凭据覆盖(model_sync)

- 现状:credentialSupport 仅由权威探针 + 采样轮次的少数凭据写入。
- 改为:每轮同步对**全部可用凭据**各拉一次可用模型列表,写入
  `file.credential_support`。注册表 `models`/`aliases` 的权威/采样判定逻辑不动。
- 约束:
  - 并发上限沿用现有同步的串行/限速形态,凭据多时逐张拉取即可(每日一次,
    延迟不敏感);
  - 单凭据拉取失败:保留其旧记录不清空,打 warn;不影响其他凭据与注册表轮次;
  - 禁用凭据跳过。

### 5. 错误处理与边界

| 场景 | 行为 |
| --- | --- |
| 组内有无记录凭据 | 整组视为不设限(列表全量、校验放行) |
| 组名不存在/组内无凭据 | 本层返回 None,由既有"无可用凭据"路径报错 |
| key 无 group(全局 key) | 按全部凭据的并集计算 |
| alias 请求(如 claude-opus-5 别名) | 先 resolve 再按 upstream id 校验 |
| `auto` | 恒展示、恒放行 |
| 同步从未跑过(credential_support 空) | 所有凭据无记录 → 不设限,行为与现状一致 |

### 6. 测试

- token_manager:并集计算;含无记录凭据 → None;禁用凭据不计入;None group=全量。
- handlers:`/v1/models` 按组收窄的端点级测试(仿既有
  `models_endpoint_visibility_follows_installed_registry` 的写法);
  组不支持 → 404 报文逐字段断言;alias 解析后按 upstream id 校验;
  `auto` 放行;无记录组回退全量(零回归底线:无 credentialSupport 时
  报文与现状逐字节一致)。
- model_sync:全凭据拉取;单凭据失败保留旧记录;禁用凭据跳过。

## 实施影响

- 触点:`token_manager.rs`(新查询)、`handlers.rs`(列表过滤+请求校验)、
  `model_sync.rs`(拉取范围)、`openai.rs`/`responses.rs` 若各自独立走
  resolve 则同点接入。
- 不改配置格式、不改 models.json schema(credentialSupport 已存在)。
- 部署:合入后随镜像发布;对无分组用户(单组全凭据)行为仅在凭据能力
  确有差异时可见。

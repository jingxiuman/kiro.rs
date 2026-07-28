# kiro-rs

**该项目基于 [hank9999/kiro.rs](https://github.com/hank9999/kiro.rs) 进行的二次开发**

`kiro-rs` 是一个用 Rust 编写的 Anthropic Messages API 与 OpenAI Chat Completions / Responses API 兼容代理。它把 `/v1/messages`、`/v1/chat/completions`、`/v1/responses` 等请求转换为 Kiro / Amazon Q 后端请求，并提供一个可选的 Web Admin 面板来管理凭据、客户端 Key、用量、代理池、请求日志和在线更新。

项目当前的核心目标是：让 Claude Code、Codex CLI、Anthropic / OpenAI SDK 或其它兼容客户端，通过统一的本地 / 自托管服务访问 Kiro 账号能力，同时在服务端集中处理多凭据、token 刷新、故障转移、用量统计和可观测性。

## 🔎 快速引导

- [声明](#notice)
- [功能](#features)
- [快速开始](#quick-start)
- [调用 API](#api-usage)
- [API 路由](#api-routes)
- [配置](#configuration)
- [凭据](#credentials)
- [模型](#models)
- [Thinking、工具与 WebSearch](#thinking-tools-websearch)
- [图片处理](#images)
- [用量、缓存与日志](#usage-cache-logs)
- [Admin UI](#admin-ui)
- [代理和 Region](#proxy-region)
- [负载均衡与故障转移](#load-balancing-failover)
- [在线更新和发布](#updates-release)
- [开发](#development)
- [目录结构](#project-structure)
- [License](#license)
- [社区支持](#community)
- [致谢](#acknowledgements)

<a id="notice"></a>
## 📚 声明

本项目仅供研究和自用。使用本项目产生的任何后果由使用者自行承担。本项目与 AWS、Kiro、Amazon Q、Anthropic、Claude 等官方实体无关，不代表任何官方立场。

<a id="features"></a>
## ✨ 功能

- **Anthropic Messages API 兼容**：`/v1/messages`、`/v1/models`、`/v1/messages/count_tokens`。
- **OpenAI API 兼容**：`/v1/chat/completions` 和 `/v1/responses`，支持非流式响应与合成 SSE，可供 OpenAI SDK 和新版 Codex CLI 使用。
- **Claude Code 兼容端点**：`/cc/v1/messages`、`/cc/v1/messages/count_tokens`。
- **GPT-5.6 模型族**：`gpt-5.6-sol`、`gpt-5.6-terra`、`gpt-5.6-luna`。
- 流式和非流式响应：支持 Anthropic SSE 与 OpenAI SSE 事件格式。
- **多凭据管理**：OAuth、Builder ID、Social、Enterprise / IdC、企业 SSO（Microsoft Entra ID / Azure AD）、Kiro API Key。
- 自动 token 刷新：支持刷新后回写 `credentials.json`。
- **多凭据调度**：`priority` 固定优先级和 `balanced` 均衡分配。
- **故障转移**：凭据失败、额度用尽、账号级 429 风控冷却、token 失效强制刷新。
- **profileArn 策略**：流式端点按账号类型注入真实 ARN 或 Builder ID 占位 ARN；用量类 / 头部类调用跳过占位 ARN。
- **端点抽象**：按凭据选择 `ide` 或 `cli` endpoint。
- **工具调用**：支持 `tool_use` / `tool_result` 配对、工具名缩短与反向映射。
- **Thinking / Reasoning 兼容**：支持 `thinking.type=enabled` / `adaptive`、Claude Code 默认 thinking 请求、Kiro 原生 `reasoningContentEvent` 到 Anthropic thinking / signature / redacted thinking 事件的转换。
- **WebSearch**：支持纯 `web_search` 请求和混合工具场景下的本地 agentic web_search loop。
- **图像处理**：入站图片按环境变量自动缩放 / 重编码，降低 AWS Q 单字段大小限制导致的 400 风险。
- **Prompt cache 计量**：模拟 Anthropic cache_control 的 `cache_creation` / `cache_read` token 统计。
- **用量统计**：按客户端 Key、模型、凭据、日期聚合 input/output/cache token 和 credits。
- **请求链路追踪**：SQLite `traces.db`，记录成功 / 失败请求、尝试链路和错误类型。
- 客户端 Key 分发：Admin 面板生成 `sk-...` Key，支持独立启停、轮换、分组和统计；鉴权不强制 Key 前缀。
- **Admin UI**：概览、凭据管理、客户端 Key、请求日志四个主视图。
- 代理能力：全局代理、凭据级代理、代理池、健康检查、轮询分配。
- **在线更新**：从 GitHub Release / Docker Hub 拉取新版本，支持镜像定时自动更新与手动回退。
- **多平台发布**：GitHub Release 构建 Windows、Linux、macOS 和 Docker Hub 多架构镜像。

<a id="quick-start"></a>
## 🚀 快速开始

### Docker

推荐生产部署使用 Docker。仓库提供的 `docker-compose.yml` 默认使用 Docker Hub 镜像：

```yaml
image: ${KIRO_RS_IMAGE:-zyphrzero/kiro-rs:latest}
ports:
  - "8990:8990"
volumes:
  - ./data/:/app/config/
```

部署：

```bash
mkdir -p /opt/kiro-rs/data
cd /opt/kiro-rs
curl -O https://raw.githubusercontent.com/ZyphrZero/kiro.rs/master/docker-compose.yml
docker compose up -d
```

首次启动时，程序会在挂载目录中自动生成：

```text
data/
├── config.json
└── credentials.json
```

`config.json` 会包含随机生成的 `apiKey` 和 `adminApiKey`。查看日志：

```bash
docker compose logs --tail=200 kiro-rs
```

也可以直接打开 `data/config.json` 查看：

```json
{
  "host": "0.0.0.0",
  "port": 8990,
  "apiKey": "sk-kiro-rs-...",
  "adminApiKey": "sk-admin-...",
  "region": "us-east-1",
  "tlsBackend": "rustls",
  "defaultEndpoint": "ide"
}
```

访问：

- API: `http://<host>:8990/v1/messages`
- Admin UI: `http://<host>:8990/admin`

指定镜像版本：

```bash
KIRO_RS_IMAGE=zyphrzero/kiro-rs:0.7.2 docker compose up -d
```

### 下载二进制

正式版本会在 [GitHub Release](https://github.com/ZyphrZero/kiro.rs/releases/latest) 中发布以下平台产物：

- Windows x64
- Linux x64 / arm64
- Linux musl x64 / arm64
- macOS x64 / arm64

下载后把二进制放到工作目录，首次启动会自动生成 `config.json` 和 `credentials.json`。

```bash
./kiro-rs
```

Windows:

```powershell
.\kiro-rs.exe
```

指定配置文件：

```bash
./kiro-rs --config /path/to/config.json --credentials /path/to/credentials.json
```

### 从源码构建

前端 Admin UI 会通过 `rust-embed` 嵌入到最终二进制。构建后端前先构建前端：

```bash
cd admin-ui
bun install
bun run build
cd ..
cargo build --release --locked
```

测试：

```bash
cargo test
```

<a id="api-usage"></a>
## 调用 API

`/v1` 路由支持 `x-api-key` 和 `Authorization: Bearer` 两种鉴权方式。Key 可以是 `config.json` 中用户自定义的 `apiKey`，也可以是 Admin 面板生成的 `sk-...` 客户端 Key。鉴权只比较完整 Key，不限制自定义 `apiKey` 的前缀或格式。

### Anthropic Messages

```bash
curl http://127.0.0.1:8990/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: sk-kiro-rs-..." \
  -d '{
    "model": "claude-sonnet-4-5-20250929",
    "max_tokens": 1024,
    "stream": true,
    "messages": [
      { "role": "user", "content": "Hello" }
    ]
  }'
```

Claude Code 兼容端点：

```bash
curl http://127.0.0.1:8990/cc/v1/messages \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-kiro-rs-..." \
  -d '{
    "model": "claude-sonnet-4-8",
    "max_tokens": 1024,
    "stream": true,
    "messages": [
      { "role": "user", "content": "Hello from Claude Code style endpoint" }
    ]
  }'
```

列出模型：

```bash
curl http://127.0.0.1:8990/v1/models \
  -H "Authorization: Bearer sk-kiro-rs-..."
```

估算 token：

```bash
curl http://127.0.0.1:8990/v1/messages/count_tokens \
  -H "Content-Type: application/json" \
  -H "x-api-key: sk-kiro-rs-..." \
  -d '{
    "model": "claude-sonnet-4-5-20250929",
    "messages": [
      { "role": "user", "content": "Count this." }
    ]
  }'
```

### OpenAI Chat Completions

`POST /v1/chat/completions` 接受 OpenAI 消息、函数工具、`tool_choice`、`reasoning_effort`、`max_tokens` / `max_completion_tokens`：

```bash
curl http://127.0.0.1:8990/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-kiro-rs-..." \
  -d '{
    "model": "gpt-5.6-sol",
    "reasoning_effort": "high",
    "stream": false,
    "messages": [
      { "role": "user", "content": "Hello from an OpenAI client" }
    ]
  }'
```

### OpenAI Responses / Codex CLI

`POST /v1/responses` 接受字符串或 input item 数组形式的 `input`，并支持 `instructions`、`reasoning.effort`、`max_output_tokens` 和非流式 / SSE 响应：

```bash
curl http://127.0.0.1:8990/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-kiro-rs-..." \
  -d '{
    "model": "gpt-5.6-sol",
    "instructions": "Answer concisely.",
    "input": "What can you do?",
    "reasoning": { "effort": "high" },
    "stream": false
  }'
```

新版 Codex CLI 使用 Responses API。可在 `~/.codex/config.toml` 中添加：

```toml
model = "gpt-5.6-sol"
model_provider = "kiro-rs"

[model_providers.kiro-rs]
name = "kiro-rs"
base_url = "http://127.0.0.1:8990/v1"
env_key = "KIRO_RS_API_KEY"
wire_api = "responses"
```

启动 Codex 前设置与 `config.apiKey` 或客户端 Key 相同的环境变量：

```bash
export KIRO_RS_API_KEY='sk-kiro-rs-...'
codex
```

两个 OpenAI 端点都会复用现有的模型映射、凭据故障转移和用量计量链路。当前实现会先取得完整的内部非流式响应，再为 `stream: true` 合成 SSE，因此不是逐 token 的上游实时流。Responses 端点不会把 Codex 的 `exec`、`shell`、`apply_patch` 等本地执行工具声明转发给 Kiro；时效性查询由服务端的 Kiro MCP WebSearch 处理。

<a id="api-routes"></a>
## API 路由

### Anthropic 兼容

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/v1/models` | 返回本服务声明支持的模型列表 |
| `POST` | `/v1/messages` | Anthropic Messages API 兼容入口 |
| `POST` | `/v1/messages/count_tokens` | Anthropic count_tokens 兼容入口 |
| `POST` | `/cc/v1/messages` | Claude Code 兼容入口，流式事件顺序针对 Claude Code 调整 |
| `POST` | `/cc/v1/messages/count_tokens` | Claude Code 兼容 count_tokens |

### OpenAI 兼容

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions 兼容入口，支持消息、函数工具和 reasoning effort |
| `POST` | `/v1/responses` | OpenAI Responses 兼容入口，适用于新版 Codex CLI |

### Admin

启用 `adminApiKey` 后会挂载：

| 路径 | 说明 |
|---|---|
| `/admin` | 嵌入式 Web 管理界面 |
| `/api/admin/credentials` | 凭据列表、新增、编辑、删除 |
| `/api/admin/credentials/{id}/balance` | 查询单个凭据订阅 / 用量 |
| `/api/admin/credentials/{id}/models` | 查询该凭据上游实际可用模型 |
| `/api/admin/models*` | 模型注册表：查询、增删改、别名、手动同步、同步设置 |
| `/api/admin/client-keys` | 客户端 Key 管理 |
| `/api/admin/stats/*` | 用量统计 |
| `/api/admin/traces` | 请求链路追踪查询 |
| `/api/admin/proxy-pool` | 代理池 |
| `/api/admin/config/*` | 运行时配置 |
| `/api/admin/auth/*` | Social / IdC 登录流程 |
| `/api/admin/system/update/*` | 在线更新、回退、版本检查 |

Admin API 鉴权同样支持：

- `x-api-key: <adminApiKey>`
- `Authorization: Bearer <adminApiKey>`

<a id="configuration"></a>
## ⚙️ 配置

默认配置文件名是 `config.json`。首次启动如果文件不存在，会自动生成最小配置。

### 最小配置

```json
{
  "host": "0.0.0.0",
  "port": 8990,
  "apiKey": "sk-kiro-rs-change-me",
  "adminApiKey": "sk-admin-change-me",
  "region": "us-east-1",
  "tlsBackend": "rustls",
  "defaultEndpoint": "ide"
}
```

### 常用字段

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `host` | `127.0.0.1` | 监听地址。自动生成配置时为 `0.0.0.0` |
| `port` | `8080` | 监听端口。自动生成配置时为 `8990` |
| `apiKey` | 无 | 配置的 `id=0` 系统 Key；可使用任意非空自定义值，不限制前缀。`/v1` 和 `/cc/v1` 也可使用 Admin 面板创建的客户端 Key |
| `adminApiKey` | 无 | 设置后启用 `/admin` 和 `/api/admin` |
| `region` | `us-east-1` | 全局默认 Region |
| `authRegion` | 无 | token 刷新用 Region，未配置时回退 `region` |
| `apiRegion` | 无 | Kiro API 请求用 Region，未配置时回退 `region` |
| `defaultEndpoint` | `ide` | 凭据未指定 endpoint 时使用的端点 |
| `tlsBackend` | `rustls` | `rustls` 或 `native-tls` |
| `proxyUrl` | 无 | 全局代理，支持 `http://`、`https://`、`socks5://` |
| `proxyUsername` / `proxyPassword` | 无 | 全局代理认证 |
| `requireProxy` | `false` | 强制走代理：无可用代理时拒绝出网而非降级直连（见「代理优先级」） |
| `loadBalancingMode` | `priority` | `priority` 或 `balanced` |
| `accountThrottleFailover` | `true` | 账号级 429 suspicious activity 时是否冷却并切换凭据 |
| `accountThrottleCooldownSecs` | `1800` | 账号级风控冷却秒数 |
| `extractThinking` | `true` | 非流式响应是否把旧 `<thinking>` 文本提取成 thinking block |
| `traceEnabled` | `true` | 是否写入 `traces.db` |
| `traceRetentionDays` | `7` | trace 保留天数 |
| `usageLogRetentionDays` | `31` | `usage_log.*.jsonl` 保留天数 |
| `countTokensApiUrl` | 无 | 外部 count_tokens API 地址 |
| `countTokensApiKey` | 无 | 外部 count_tokens API Key |
| `countTokensAuthType` | `x-api-key` | `x-api-key` 或 `bearer` |
| `githubToken` | 无 | 在线更新访问 GitHub API 时使用，降低 rate limit 风险 |
| `updateAutoApply` | `false` | 是否每天自动检查并应用新版本 |
| `updateAutoApplyTime` | `03:00` | 自动更新时间，本地时区 `HH:MM` |
| `modelSyncEnabled` | `false` | 是否每日自动同步上游模型表，详见[模型注册表与自动同步](#model-registry) |
| `modelSyncTime` | `04:00` | 模型同步时间，本地时区 `HH:MM` |
| `modelSyncProbeCredentialId` | 无 | 模型同步探针凭据 ID，未配置时同步降级为采样、不判定模型消失 |
| `allowUnknownModelPassthrough` | `false` | 未收录模型是否原样透传给上游 |

非空的 `config.apiKey` 每次启动都会同步为不可删除、可轮换的系统 Key `id=0`。手动修改配置后，旧系统 Key 立即失效，新值自动启用；已有名称、描述、分组和累计统计会保留。Admin 面板新建或轮换的客户端 Key 统一以 `sk-` 开头，但请求鉴权不会对任何已存储 Key 强制检查前缀。`adminApiKey` 独立用于 Admin UI / Admin API 登录，不参与 `/v1` 业务流量鉴权。

<a id="credentials"></a>
## 🔐 凭据

默认凭据文件名是 `credentials.json`。推荐通过 Admin UI 添加、登录和重登凭据；直接编辑文件时建议使用数组格式。

```json
[
  {
    "id": 1,
    "refreshToken": "xxx",
    "expiresAt": "2026-12-31T00:00:00Z",
    "authMethod": "idc",
    "provider": "BuilderId",
    "clientId": "xxx",
    "clientSecret": "xxx",
    "priority": 0
  }
]
```

### 支持的凭据类型

#### Builder ID / IdC

```json
{
  "refreshToken": "xxx",
  "expiresAt": "2026-12-31T00:00:00Z",
  "authMethod": "idc",
  "provider": "BuilderId",
  "clientId": "xxx",
  "clientSecret": "xxx"
}
```

#### Enterprise IAM Identity Center

```json
{
  "refreshToken": "xxx",
  "expiresAt": "2026-12-31T00:00:00Z",
  "authMethod": "idc",
  "provider": "Enterprise",
  "startUrl": "https://example.awsapps.com/start",
  "region": "us-east-1",
  "clientId": "xxx",
  "clientSecret": "xxx"
}
```

Enterprise / IdC 账号在流式调用前会按需调用 `ListAvailableProfiles` 解析真实 `profileArn`，成功后写回凭据。纯 Builder ID/free 账号没有 Enterprise profile 时，会回退到官方 IDE 使用的 Builder ID 占位 ARN，以避免流式端点缺少 `profileArn` 返回 400。

#### Social 登录

```json
{
  "refreshToken": "xxx",
  "expiresAt": "2026-12-31T00:00:00Z",
  "authMethod": "social",
  "provider": "Github"
}
```

`provider` 可为 `Github` 或 `Google`。Social 登录会使用固定 Social profile ARN。

#### 企业 SSO（Microsoft Entra ID / Azure AD）

```json
{
  "refreshToken": "xxx",
  "accessToken": "xxx",
  "expiresAt": "2026-12-31T00:00:00Z",
  "authMethod": "external_idp",
  "provider": "AzureAD",
  "clientId": "11111111-2222-3333-4444-555555555555",
  "tokenEndpoint": "https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token",
  "issuerUrl": "https://login.microsoftonline.com/<tenant>/v2.0",
  "scopes": "openid profile offline_access <resource-scope>"
}
```

适用于 Microsoft 365 / Entra ID / Azure AD 企业租户账号（既不是 AWS Builder ID 也不是 IAM Identity Center）。Token 刷新走 IdP 的 OAuth2 `refresh_token` grant（公共客户端，表单编码，无 `clientSecret`）：`clientId` 与 `tokenEndpoint` 必填，`scopes` 需含 `offline_access` 才能拿到 refresh token。数据面与 Profile 请求会自动携带 `TokenType: EXTERNAL_IDP` 头，真实 `profileArn` 由 `ListAvailableProfiles` 懒解析回填。

`authMethod` 除 `external_idp` 外也接受 `azuread` / `azure` / `entra` / `microsoft` / `m365` 等别名（统一归一化）；未写 `authMethod` 但带 `tokenEndpoint` 时会自动判定为 `external_idp`。出于防 SSRF / refresh token 外泄考虑，`tokenEndpoint`（及 `issuerUrl`）必须为 `https` 且 host 命中允许列表（`*.microsoftonline.com` / `.us` / `.cn`），否则拒绝导入。

> 核心逻辑参考 [Quorinex/Kiro-Go#131](https://github.com/Quorinex/Kiro-Go/pull/131)。本版仅支持以 JSON 导入（不含浏览器门户登录），按需手动获取 Azure 凭据后导入即可。

#### Kiro API Key

```json
{
  "kiroApiKey": "ksk_xxx",
  "authMethod": "api_key",
  "endpoint": "cli"
}
```

也可以通过环境变量临时注入最高优先级 API Key 凭据：

```bash
KIRO_API_KEY=ksk_xxx ./kiro-rs
```

### 凭据字段

| 字段 | 说明 |
|---|---|
| `id` | 凭据 ID，Admin 管理时自动分配 |
| `refreshToken` / `accessToken` | OAuth token |
| `expiresAt` | RFC3339 过期时间 |
| `authMethod` | `idc`、`social`、`external_idp`、`api_key`。旧值 `builder-id`、`iam` 会规范化为 `idc`；`azuread` / `entra` 等别名归一化为 `external_idp` |
| `provider` | `BuilderId`、`Enterprise`、`Github`、`Google`、`IAM_SSO`、`AzureAD` 等 |
| `clientId` / `clientSecret` | IdC 刷新 token 所需 OIDC client；`external_idp` 仅需 `clientId`（公共客户端，无 `clientSecret`） |
| `startUrl` | Enterprise IAM Identity Center Start URL |
| `tokenEndpoint` / `issuerUrl` / `scopes` | 企业 SSO（Entra ID / Azure AD）专用：IdP 刷新端点 / OIDC issuer（备注）/ 已授权 scope（需含 `offline_access`） |
| `profileArn` | 真实 profile ARN 或已知固定 ARN；通常由程序维护 |
| `priority` | 数字越小优先级越高 |
| `region` | 凭据级 Region，兼容旧配置 |
| `authRegion` | 凭据级 token 刷新 Region |
| `apiRegion` | 凭据级 API 请求 Region |
| `machineId` | 凭据级 machine id，未填时自动派生 |
| `email` / `subscriptionTitle` | Admin 查询后回填的展示信息 |
| `proxyUrl` | 凭据级代理；填 `direct` 表示绕过全局代理 |
| `proxyUsername` / `proxyPassword` | 凭据级代理认证 |
| `disabled` | 是否禁用 |
| `kiroApiKey` | `ksk_*` Kiro API Key |
| `endpoint` | `ide` 或 `cli`，未填使用 `config.defaultEndpoint` |

<a id="models"></a>
## 模型

`GET /v1/models` 返回本服务声明支持的模型 ID。真实可用性仍取决于上游账号订阅；Admin 的“凭据模型”会查询该凭据的上游真实可用模型列表。

下面的列表、映射规则和窗口取值是**编译内置默认**。没有 `models.json` 时它就是最终结果；有 `models.json` 时会在其上叠加人工覆盖与自动同步的结果，见[模型注册表与自动同步](#model-registry)。

内置默认列表包含：

- `gpt-5.6-sol`
- `gpt-5.6-terra`
- `gpt-5.6-luna`
- `claude-fable-5` / `claude-fable-5-thinking`
- `claude-sonnet-5` / `claude-sonnet-5-thinking`
- `claude-opus-4-8` / `claude-opus-4-8-thinking`
- `claude-sonnet-4-8` / `claude-sonnet-4-8-thinking`
- `claude-opus-4-7` / `claude-opus-4-7-thinking`
- `claude-opus-4-6` / `claude-opus-4-6-thinking`
- `claude-sonnet-4-6` / `claude-sonnet-4-6-thinking`
- `claude-opus-4-5-20251101` / `claude-opus-4-5-20251101-thinking`
- `claude-sonnet-4-5-20250929` / `claude-sonnet-4-5-20250929-thinking`
- `claude-haiku-4-5-20251001` / `claude-haiku-4-5-20251001-thinking`

模型映射按关键词归一化到 Kiro 内部模型 ID：

| 请求模型关键词 | 上游模型 |
|---|---|
| 以 `gpt-5` 开头 | 原样透传，例如 `gpt-5.6-sol` |
| `fable`（任意） | `claude-fable-5` |
| `sonnet` + `5`（`sonnet-5` / `sonnet5` / `sonnet.5`） | `claude-sonnet-5` |
| `sonnet` + `4-8` / `4.8` | `claude-sonnet-4.8` |
| `sonnet` + `4-6` / `4.6` | `claude-sonnet-4.6` |
| `sonnet` + `4-5` / `4.5` | `claude-sonnet-4.5` |
| `opus` + `4-8` / `4.8` | `claude-opus-4.8` |
| `opus` + `4-7` / `4.7` | `claude-opus-4.7` |
| `opus` + `4-6` / `4.6` | `claude-opus-4.6` |
| `opus` + `4-5` / `4.5` | `claude-opus-4.5` |
| 任意 `haiku` | `claude-haiku-4.5` |

没有命中上述规则、且不在下文「自定义模型」表中的模型会作为不支持模型处理。

上下文窗口估算：

- `gpt-5.*`：`272_000`（GPT-5.6 静态模型声明最大输出为 `64_000`）
- `claude-sonnet-4.6`、`claude-sonnet-4.8`、`claude-sonnet-5`、`claude-opus-4.6`、`claude-opus-4.7`、`claude-opus-4.8`、`claude-fable-5`：`1_000_000`
- 其它模型：`200_000`

<a id="model-registry"></a>
### 模型注册表与自动同步

模型表由「编译内置默认」与覆盖层文件 `models.json` 合并而成。

`models.json` 放在**凭据目录**（`credentials.json` 所在目录，Docker 部署即 `./data/`），不是 `config.json` 所在目录，也不是 `config.json` 的一部分——模型表会被自动同步反复重写，独立成文件才能用自己的写锁串行化，不必和运行时配置抢同一份 load-modify-save。

**文件不存在时行为与改造前完全一致**：直接使用上面的内置默认列表、映射规则和窗口取值，不算降级，不打错误日志。文件损坏、schema 版本不符或校验不过时，整体拒绝该文件、退回内置默认，并打一条 error 日志（降级运行，服务照常起）。

#### 配置项

四个开关都在 `config.json` 里，也可在 Admin UI 修改（热生效，不必重启）：

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `modelSyncEnabled` | `false` | 是否启用每日自动同步上游模型列表。关闭时模型表只由内置默认与人工编辑决定 |
| `modelSyncTime` | `04:00` | 每日同步时刻，本地时区 `HH:MM` |
| `modelSyncProbeCredentialId` | 无 | 探针凭据 ID。见下方「探针凭据」 |
| `allowUnknownModelPassthrough` | `false` | 未收录模型是否原样透传给上游。关闭时返回 400；开启时按 `200_000` 估算窗口发往上游，并打一条 warn |

Admin API：`GET/POST /api/admin/models`、`PATCH/DELETE /api/admin/models/{upstreamId}`、`POST /api/admin/models/sync`（手动触发一轮）、`POST/DELETE /api/admin/models/aliases`、`PATCH /api/admin/models/settings`。Admin UI 入口在凭据管理页顶栏的「模型映射」。

#### 锁定字段（`pinned`）

被锁字段的用户值胜过自动同步，也胜过代码内置默认；未锁字段跟随更新。**通过 Admin API / UI PATCH 一个字段，会自动把它锁上**，不需要单独操作。

只有下面 5 个字段会被同步覆盖，因而也只有它们值得锁：

| 字段 | 含义 |
|---|---|
| `exposedId` | 对外模型名 |
| `displayName` | 展示名 |
| `contextWindow` | **输入**上下文窗口 |
| `maxOutputTokens` | **输出**上限，即 `/v1/models` 里的 `max_tokens` |
| `exposeThinkingVariant` | 是否派生 `-thinking` 变体 |

其余字段（`enabled`、`listed`、`sortOrder`、别名等）本来就只属于人工，同步不碰，无所谓锁不锁。解锁某字段后，它会在下一轮同步时回归上游值。

#### 探针凭据

不同凭据的订阅等级不同，能看到的模型也不同。因此「上游是否还返回某模型」这个判断只有在一个**固定的、可信的**凭据上做才有意义：

- **配了 `modelSyncProbeCredentialId` 且该凭据可用** → 本轮是**权威轮次**，可以判定模型消失。
- **没配，或探针凭据本轮拉取失败** → 降级为**采样轮次**：抽若干凭据取并集，只做新增与更新，**绝不判定任何模型消失**。

**探针应当使用订阅等级最高的凭据。** 用一个低等级凭据当探针，它看不到的高等级模型会被持续判为「上游已下线」，最终标成 deprecated。下面的护栏能兜住整表误删这种最坏情况，但兜不住个别模型被误标。

#### 安全护栏

模型表被误删整表的代价很高（客户端模型列表突然清空），所以消失判定上叠了四道：

1. **全部凭据拉取失败的轮次直接拒绝**，不写文件——「一个模型都没返回」在语义上无法与「上游全下线」区分，一律按故障处理。
2. **采样轮次绝不判定消失**，只做新增与更新。
3. **单轮中若「仍在服役的模型」缺失比例超过 50%**，判定这个探针不具代表性（多半是订阅等级不够或凭据出了问题），**跳过本轮的消失判定**并打一条 error 日志。
4. **需连续 2 个权威轮次未见**才把模型标为 `deprecated`。中间任意一轮重新见到即计数归零、状态复活。

`deprecated` 只是一个标记：该模型**仍然出现在 `/v1/models`、仍然可以正常请求**，UI 里标黄。模型不会凭空从列表里消失。真要下线，手动关掉它的「启用」开关——这时它才从 `/v1/models` 移除，请求返回「模型已禁用」（web-search 路径为英文 `model disabled`），与「模型不支持」区分开，便于排查是配置问题还是拼写问题。

#### 已知限制

以下都是当前确实存在、且有意选择不修的边界：

1. **同步开启时，PATCH 过某个内置模型之后、到下一轮同步之前**，该行 4 个未锁的同步字段取的是「编辑那一刻」的值，不会跟随新版代码里的内置定义变化。下一轮同步会把它们刷新回来。彻底消除需要逐字段记录来源，会改变 `models.json` 的序列化格式，代价大于收益。
2. **`syncState.modelMeta` 没有清理机制。** 曾经出现过、后来被上游彻底移除、且从未进入覆盖层的模型，其元数据条目会永久滞留在文件里。上界是「历史上见过的所有模型」，不会无限增长，但也不会缩小。
3. **护栏 3 的 error 日志按同步周期重复打印，无去重。** 探针配错是一个持续状态，不是一次性事件，因此每轮都会刷一条。看到重复日志请去改探针凭据，而不是当噪音忽略。
4. **`config.json` 既有的 6 处写入点仍是无保护的 load-modify-save**，并发写有丢失更新的可能。这是本次改造之前就存在的行为，不是新引入的；本次只把新增的 `PATCH /models/settings` 路径纳入了锁保护。`models.json` 自身的所有写路径都经写锁串行化。
5. **「同步成功后刷新 `credentialSupport`」缺端到端验证。** 测试环境没有可用的上游凭据，`sync_once` 在选凭据阶段就会失败。启动时的灌入路径已实测，同步写入侧有单测，但这条链路整体未跑通过。

### 自定义模型（`customModels`，已迁移至模型注册表）

`config.json` 里的 `customModels` 数组是**旧版**自定义模型映射机制，字段含义（`id`/`backendId`/`displayName`/`contextWindow`/`maxTokens`/`supportsReasoning`/`ownedBy`）与以前一致，但**不再由它直接驱动路由**——现在由上面的[模型注册表](#model-registry)统一管理。

启动时，若 `customModels` 非空，程序会把其中**注册表里还不存在**（按 `backendId` 对应的 `upstreamId` 或 `id` 对应的 `exposedId` 判断）的条目自动导入为 `models.json` 里的一条人工（Manual）行；已存在同名条目的则跳过、不覆盖注册表里的编辑（注册表以 Admin UI / API 的修改为准）。这个导入每次启动都会执行，但只在首次真正写入 —— 已导入过的条目第二次会被跳过，`models.json` 不再变化。

**建议**：新部署直接用 Admin UI（凭据管理页顶栏「模型映射」）或 `POST /api/admin/models` 添加自定义模型，不必再写 `customModels`。这个配置项只是为了让老配置文件平滑过渡，后续版本可能移除。

<a id="thinking-tools-websearch"></a>
## Thinking、工具与 WebSearch

### Thinking

客户端可以显式发送 Anthropic `thinking` 字段，也可以直接使用带 `-thinking` 后缀的模型名。Claude Code 当前也可能在普通模型名下默认发送 `thinking.type=enabled`；服务端会按请求体实际 thinking 配置处理，不依赖模型名是否带后缀。

普通 thinking：

```json
{
  "model": "claude-sonnet-4-8-thinking",
  "max_tokens": 4096,
  "thinking": {
    "type": "enabled",
    "budget_tokens": 20000
  },
  "messages": [
    { "role": "user", "content": "推理一下这个问题" }
  ]
}
```

`budget_tokens` 会限制在 `24576` 以内。

模型名带 `-thinking` 后缀时会自动覆写 thinking 配置：

- Opus 4.6：`thinking.type=adaptive`，并默认设置 `output_config.effort=high`。
- 其它 thinking 模型：`thinking.type=enabled`，`budget_tokens=20000`。

Adaptive thinking：

```json
{
  "model": "claude-opus-4-6-thinking",
  "max_tokens": 4096,
  "thinking": {
    "type": "adaptive"
  },
  "output_config": {
    "effort": "high"
  },
  "messages": [
    { "role": "user", "content": "给出完整分析" }
  ]
}
```

`additionalModelRequestFields.output_config` 是 Kiro 上游的窄兼容字段。当前只会在已知可接受该字段的 Opus 4.6 adaptive thinking 路径上传递；Sonnet 4.5 / 4.8、Opus 4.6 非 adaptive thinking 等路径会跳过该字段，避免上游返回 `additionalModelRequestFields is not supported for this model`。`effort` 会先归一化大小写和空格；已知 4.5 / 4.6 系列不接受 `xhigh`，会降级为最接近的 `high`；Opus 4.7 / 4.8、Fable 5、Mythos 5 会保留 `xhigh`；其它未知模型的已知 effort 值也会保持原样，避免维护一张容易过期的模型白名单；未知 effort 值会回退到 `high`。

Kiro 上游可能返回原生 `reasoningContentEvent`。`kiro-rs` 会把它转换为 Anthropic 兼容内容：

- `text` → 流式 `thinking_delta`，非流式 `thinking` block。
- `signature` → 流式 `signature_delta`，非流式 `thinking.signature`。
- `redactedContent` → `redacted_thinking` block。
- 如果当前请求没有启用 thinking，明文 reasoning 会降级为普通 text；签名和 redacted 内容不会输出。

非流式响应优先使用原生 reasoning 事件；只有没有原生 reasoning 时，才回退到旧的 `<thinking>...</thinking>` 文本提取路径。

### Tool Use

服务端会把 Anthropic tools 转成 Kiro 工具定义，并处理以下兼容逻辑：

- 长工具名会被缩短，并在响应流中恢复原始名称。
- 孤立的 `tool_use` / `tool_result` 会被过滤或修复，避免上游因消息配对错误返回不可恢复错误。
- tool_result 中的图片会提升到 Kiro 顶层图片字段，并走同一套图片缩放逻辑。

### WebSearch

支持 Anthropic web_search tool：

```json
{
  "model": "claude-sonnet-4-8",
  "max_tokens": 2048,
  "stream": true,
  "tools": [
    {
      "type": "web_search_20250305",
      "name": "web_search",
      "max_uses": 5
    }
  ],
  "messages": [
    { "role": "user", "content": "搜索今天的相关信息" }
  ]
}
```

纯 web_search 请求会直接走上游 MCP 搜索接口。混合工具场景下，如果上游返回只包含 `web_search` 的工具调用，`kiro-rs` 会内部调用同一套 MCP 搜索接口，把结果作为 tool_result 喂回上游，直到上游停止搜索或达到轮数限制；其它工具调用会原样返回给客户端。

<a id="images"></a>
## 图片处理

入站图片会在本地 CPU 上按需压缩，默认策略：

- 长边上限：`1568px`
- base64 字段大小上限：`400000`
- JPEG 质量：`85`
- PNG / JPEG / WebP 大图会重编码为 JPEG
- GIF 保留原格式，避免破坏动画
- 解码失败时保留原图并记录 warning，不会让整个请求失败

环境变量：

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `KIRO_RS_IMAGE_RESIZE` | `1` | `0`、`false`、`no`、`off` 可关闭 |
| `KIRO_RS_IMAGE_MAX_LONG_SIDE` | `1568` | 长边像素上限 |
| `KIRO_RS_IMAGE_MAX_BYTES` | `400000` | base64 字段大小阈值 |
| `KIRO_RS_IMAGE_JPEG_QUALITY` | `85` | JPEG 输出质量 |

<a id="usage-cache-logs"></a>
## 用量、缓存与日志

运行数据默认落在 `credentials.json` 所在目录。Docker 部署时就是 `./data/`。

```text
data/
├── config.json
├── credentials.json
├── client_api_keys.json
├── kiro_stats.json
├── kiro_balance_cache.json
├── proxy_pool.json
├── cache_metering.json
├── models.json
├── traces.db
└── usage_log.YYYY-MM-DD.jsonl
```

说明：

- `client_api_keys.json`：系统 Key 和 Admin 生成的 `sk-...` 客户端 Key，明文存储，用于鉴权。
- `kiro_stats.json`：凭据成功 / 失败 / 额度 / 冷却等统计。
- `kiro_balance_cache.json`：凭据订阅、额度、邮箱等缓存。
- `proxy_pool.json`：代理池与健康状态。
- `cache_metering.json`：prompt cache 计量缓存，定期落盘。
- `models.json`：模型注册表覆盖层，见[模型注册表与自动同步](#model-registry)。文件不存在是正常状态。
- `traces.db`：SQLite 请求链路追踪数据库，WAL 模式。
- `usage_log.*.jsonl`：按日滚动请求用量日志。

`CacheMeter` 会基于 `cache_control` 和会话信息模拟 Anthropic prompt cache 口径，输出互斥的：

- `input_tokens`
- `cache_creation_input_tokens`
- `cache_read_input_tokens`
- `output_tokens`

<a id="admin-ui"></a>
## 🖥️ Admin UI

启用 `adminApiKey` 后访问 `/admin`。当前页面：

- 概览：整体请求量、token、模型分布、凭据贡献。
- 凭据管理：添加、登录、重登、删除、禁用、优先级、余额、模型列表、超额开关、代理绑定。
- 客户端 Key：创建、编辑、禁用、轮换、删除、分组绑定和重置统计；系统 Key 不可删除但可轮换。
- 请求日志：查询 `traces.db`，查看失败原因、状态码、凭据尝试链路和 token 用量。

Admin 还提供：

- Social 登录和 IdC / Enterprise 登录流程。
- 全局代理设置和代理池健康检查。
- 负载均衡模式配置。
- 账号级风控故障转移配置。
- trace / usage log 保留策略。
- 在线更新、自动更新和回退。

<a id="proxy-region"></a>
## 代理和 Region

### Region 优先级

Token 刷新：

```text
credential.authRegion -> credential.region -> config.authRegion -> config.region
```

API 请求：

```text
credential.apiRegion -> config.apiRegion -> config.region
```

部分 REST / 管理类上游接口只在 `us-east-1` 和 `eu-central-1` 提供服务，代码会按账号区域选择候选端点并在必要时回退。

### 代理优先级

```text
credential.proxyUrl -> config.proxyUrl -> direct
```

凭据级 `proxyUrl` 填 `direct` 表示即使配置了全局代理也直连。

#### 强制走代理（`requireProxy`）

默认情况下，链路末端是直连：凭据未配代理则回退全局代理，全局也没有就直连出网。代理池
自动禁用后若找不到可换绑的代理，受影响凭据同样会落到这条回退路径。这在需要隐藏真实
出口 IP 的部署里是个隐患——代理挂掉不会中断服务，而是**静默换成裸连**。

`requireProxy: true` 关掉这条回退：`effective_proxy` 为 `None` 的出网一律被拒绝并
记录原因，包括显式配置的 `"proxyUrl": "direct"`。覆盖所有出网路径（API 调用、token
刷新、余额与模型查询、登录、版本探测），检查点在 `build_client` 这一唯一出口。

默认关闭，开启前请确认每个凭据都有可用代理，否则服务会拒绝全部请求。

支持：

- `http://host:port`
- `https://host:port`
- `socks5://host:port`

如果 `rustls` 环境下代理或证书行为异常，可以在 `config.json` 中切到：

```json
{
  "tlsBackend": "native-tls"
}
```

<a id="load-balancing-failover"></a>
## 负载均衡与故障转移

`loadBalancingMode` 支持：

- `priority`：优先使用 priority 数字最小的可用凭据。
- `balanced`：在可用凭据之间均衡分配。

故障处理：

- 单凭据连续 API 失败会增加失败计数，达到阈值后跳过。
- 402 / quota exhausted 会禁用该凭据并切换。
- 401 / 403 中识别到 bearer token 失效时，会对该凭据强制刷新一次 token 后重试。
- 429 + suspicious activity 可触发账号级冷却并切换凭据。
- 400 客户端请求错误不会切换凭据。
- 网关超时和部分不可恢复错误会快速失败，避免一次请求内无限放大重试。

<a id="updates-release"></a>
## 在线更新和发布

发布 tag `vX.Y.Z` 会触发 Release workflow：

- 校验 `Cargo.toml` 版本和 tag 一致。
- 构建 Admin UI。
- 构建多平台二进制。
- 构建并推送 Docker Hub 多架构镜像。
- 创建 GitHub Release。

当前稳定版：[v0.7.2](https://github.com/ZyphrZero/kiro.rs/releases/tag/v0.7.2)。

Docker 镜像：

- `zyphrzero/kiro-rs:<version>`
- `zyphrzero/kiro-rs:latest`
- `zyphrzero/kiro-rs:beta`（master beta 构建）

容器内在线更新会下载对应平台二进制并替换当前可执行文件；替换后进程退出，由 Docker `restart: unless-stopped` 拉起新进程。回退依赖本地 `<exe>.backup`。

<a id="development"></a>
## 开发

常用命令：

```bash
# 后端测试
cargo test

# 前端构建
cd admin-ui && bun run build

# 后端 release 构建
cargo build --release --locked

# 开启 debug 日志
RUST_LOG=debug ./target/release/kiro-rs
```

发布前建议：

```bash
cargo test
cd admin-ui && bun run build
git diff --check
```

<a id="project-structure"></a>
## 目录结构

```text
.
├── src/
│   ├── anthropic/      # Anthropic API 兼容层
│   ├── kiro/           # Kiro / Amazon Q 上游、token、endpoint、event-stream
│   ├── admin/          # Admin API、用量、trace、代理池、在线更新
│   ├── admin_ui/       # 嵌入式 Admin UI 静态资源路由
│   ├── model/          # CLI 参数和 config.json 模型
│   ├── common/         # 通用鉴权工具
│   ├── image_resize.rs # 图片缩放与 token 估算
│   ├── token.rs        # count_tokens 估算和远程 count_tokens 调用
│   └── main.rs         # 入口
├── admin-ui/           # React Admin UI
├── .github/workflows/  # build、docker、release workflows
├── docker-compose.yml
├── Cargo.toml
└── CHANGELOG.md
```

<a id="license"></a>
## License

见 [LICENSE](LICENSE)。

<a id="community"></a>
## 💬 社区支持

欢迎到 [linux.do](https://linux.do/) 交流、分享和反馈。

<a id="acknowledgements"></a>
## 🙏 致谢

本项目的实现离不开社区项目和反馈的帮助：

- [hank9999/kiro.rs](https://github.com/hank9999/kiro.rs)
- [kiro2api](https://github.com/caidaoli/kiro2api)
- [proxycast](https://github.com/aiclientproxy/proxycast)
- [Kiro-account-manager](https://github.com/chaogei/Kiro-account-manager)

感谢所有 issue、PR、测试和部署反馈的贡献者。

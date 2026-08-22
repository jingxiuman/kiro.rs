// 凭据状态响应
export interface CredentialsStatusResponse {
  total: number
  available: number
  currentId: number
  credentials: CredentialStatusItem[]
}

// 单个凭据状态
export interface CredentialStatusItem {
  id: number
  priority: number
  disabled: boolean
  failureCount: number
  /** 累计失败次数（所有失败类型，只增不减，仅手动重置归零） */
  totalFailureCount: number
  isCurrent: boolean
  expiresAt: string | null
  authMethod: string | null
  provider?: string | null
  hasProfileArn: boolean
  email?: string
  refreshTokenHash?: string
  apiKeyHash?: string
  maskedApiKey?: string
  successCount: number
  lastUsedAt: string | null
  hasProxy: boolean
  proxyUrl?: string
  refreshFailureCount: number
  disabledReason?: string
  /** 账号级风控冷却剩余秒数（>0 表示冷却中） */
  throttledRemainingSecs?: number
  /** 402 配额耗尽被动冷冻到期时间（Unix 秒）；缺省/已过期 = 未冷冻，到期自然恢复 */
  frozenUntil?: number
  endpoint: string
  /** 账号所属分组（可属于多个分组） */
  groups?: string[]
  /** 账号来源渠道（纯备注） */
  sourceChannel?: string
  /** 后端缓存的最近一次余额（5 分钟内） */
  balance?: BalanceResponse
  /** 余额缓存的更新时间（Unix 秒） */
  balanceUpdatedAt?: number
}

// 余额响应
export interface BalanceResponse {
  id: number
  subscriptionTitle: string | null
  currentUsage: number
  usageLimit: number
  remaining: number
  usagePercentage: number
  nextResetAt: number | null
  /** 用户是否当前开启了超额 */
  overageEnabled?: boolean
  /** 账号订阅是否可以开启超额 */
  overageCapable?: boolean
  /** 上游 overageCapability 原始字符串，用于排查"未知"状态 */
  overageCapabilityRaw?: string
}

// 某凭据当前可用的模型列表响应
export interface AvailableModelsResponse {
  id: number
  models: AvailableModelItem[]
}

// 单个可用模型
export interface AvailableModelItem {
  modelId: string
  modelName?: string
  description?: string
  maxInputTokens?: number
}

// 成功响应
export interface SuccessResponse {
  success: boolean
  message: string
}

// 错误响应
export interface AdminErrorResponse {
  error: {
    type: string
    message: string
  }
}

// 请求类型
export interface SetDisabledRequest {
  disabled: boolean
}

export interface SetPriorityRequest {
  priority: number
}

// 添加凭据请求
export interface AddCredentialRequest {
  refreshToken?: string
  accessToken?: string
  profileArn?: string
  expiresAt?: string
  authMethod?: 'social' | 'idc' | 'api_key' | 'external_idp'
  provider?: string
  clientId?: string
  clientSecret?: string
  startUrl?: string
  /** 企业 SSO (external_idp) 的 OAuth2 Token 端点（external_idp 必填） */
  tokenEndpoint?: string
  /** 企业 SSO 的 OIDC Issuer URL（可选） */
  issuerUrl?: string
  /** 企业 SSO 授予的 scopes（空格分隔，可选） */
  scopes?: string
  priority?: number
  authRegion?: string
  apiRegion?: string
  machineId?: string
  proxyUrl?: string
  proxyUsername?: string
  proxyPassword?: string
  kiroApiKey?: string
  endpoint?: string
  email?: string
  groups?: string[]
  sourceChannel?: string
}

// 添加凭据响应
export interface AddCredentialResponse {
  success: boolean
  message: string
  credentialId: number
  email?: string
}

// 更新凭据请求（字段为 undefined 表示不修改，空字符串表示清除）
export interface UpdateCredentialRequest {
  email?: string
  proxyUrl?: string
  proxyUsername?: string
  proxyPassword?: string
  /** 账号所属分组（undefined 表示不修改，数组表示整体替换） */
  groups?: string[]
  /** 账号来源渠道（undefined 表示不修改，空串表示清除） */
  sourceChannel?: string
}

// 更新 refreshToken 请求
export interface UpdateRefreshTokenRequest {
  refreshToken: string
  accessToken?: string
  expiresAt?: string
}

// 代理健康状态
export type ProxyHealth = 'unknown' | 'healthy' | 'unhealthy'

// 代理池条目
export interface ProxyPoolEntry {
  id: number
  url: string
  label?: string
  enabled: boolean
  credentialCount: number
  health: ProxyHealth
  latencyMs?: number
  lastCheckedAt?: string
  consecutiveFailures: number
  autoDisabled: boolean
  /** 请求级连续失败计数（真实上游流量反馈；达阈值自动禁用） */
  requestFailures: number
  /** 最近一次请求级失败原因 */
  lastRequestError?: string
}

// 代理池列表响应
export interface ProxyPoolResponse {
  total: number
  proxies: ProxyPoolEntry[]
}

// 添加代理请求
export interface AddProxyRequest {
  url: string
  label?: string
}

// 批量添加代理请求
export interface BatchAddProxyRequest {
  urls: string[]
}

// 分配代理给凭据请求
export interface AssignProxyRequest {
  proxyId?: number | null
}

// 批量添加代理响应
export interface BatchAddProxyResponse {
  added: number
  errors: number
  proxies: ProxyPoolEntry[]
  errorMessages: string[]
}

// 单个代理健康检查响应
export interface ProxyCheckResponse {
  id: number
  health: ProxyHealth
  latencyMs?: number
  lastCheckedAt?: string
  enabled: boolean
  autoDisabled: boolean
}

// 全量健康检查响应
export interface ProxyCheckAllResponse {
  healthy: number
  unhealthy: number
  autoDisabled: number
}

// 轮询批量分配请求
export interface AssignRoundRobinRequest {
  credentialIds?: number[] | null
}

// 轮询批量分配响应
export interface AssignRoundRobinResponse {
  assigned: number
  proxyCount: number
}

// 全局代理配置
export interface GlobalProxyResponse {
  proxyUrl: string | null
}

export interface SetGlobalProxyRequest {
  proxyUrl: string | null
}

// 在线更新配置
export interface UpdateConfigResponse {
  /** 上一次更新前正在运行的版本号（带 v 前缀）；存在时可调用回退接口 */
  previousVersion?: string
  /** 上一次成功完成在线更新的时间（RFC3339） */
  lastAppliedAt?: string
  /** 是否已配置 GitHub Token（仅返回布尔，不回明文） */
  githubTokenSet: boolean
  /** 是否开启无人值守自动更新 */
  autoApply: boolean
  /** 自动更新触发时间（本地时区，HH:MM 24 小时制） */
  autoApplyTime: string
}

export interface SetUpdateConfigRequest {
  /** GitHub Personal Access Token；空字符串表示清除 */
  githubToken?: string
  autoApply?: boolean
  autoApplyTime?: string
}

/** GitHub API 限流状态（含 token 验证结果） */
export interface GitHubRateLimitInfo {
  /** 提供的 token 是否有效（无 token 时为 false 但仍能查到匿名限额） */
  valid: boolean
  /** 是否带 token 调用（false = 匿名查询） */
  authenticated: boolean
  /** 限流上限（匿名 60，认证 5000） */
  limit: number
  /** 剩余可用次数 */
  remaining: number
  /** 已用次数 */
  used: number
  /** 限流窗口重置时间（Unix 秒） */
  reset: number
  /** token 对应的用户名（可能为空） */
  login?: string
  /** 失败时的提示信息 */
  warning?: string
}

export interface ImageUpdateResponse {
  success: boolean
  message: string
  output?: string
  applied: boolean
  needRestart: boolean
}

export interface UpdateCheckInfo {
  currentVersion: string
  latestVersion: string
  hasUpdate: boolean
  buildType: string
  releaseName?: string
  releaseNotes?: string
  releaseUrl?: string
  publishedAt?: string
  checkedAt: string
  cached: boolean
  warning?: string
}

// 登录API密钥修改（adminApiKey —— 管理面板登录密钥）
export interface UpdateAdminKeyRequest {
  newKey: string
}

// IdC 设备授权登录
export interface StartIdcLoginRequest {
  region: string
  startUrl?: string
  priority?: number
  email?: string
  proxyUrl?: string
}

export interface StartIdcLoginResponse {
  sessionId: string
  userCode: string
  verificationUri: string
  verificationUriComplete?: string
  expiresAt: string
  pollInterval: number
}

export type PollIdcLoginResponse =
  | { status: 'pending' }
  | { status: 'success'; credentialId: number }
  | { status: 'expired' }

// Social 登录（Portal PKCE OAuth）
export interface StartSocialLoginRequest {
  priority?: number
  email?: string
  proxyUrl?: string
  authEndpoint?: string
}

/** 远程访问时手动完成 Social 登录：从浏览器地址栏粘贴的回调 URL 中提取参数 */
export interface CompleteSocialLoginRequest {
  code: string
  state: string
  loginOption?: string
  path?: string
}

export interface StartSocialLoginResponse {
  sessionId: string
  portalUrl: string
  expiresAt: string
}

export type PollSocialLoginResponse = PollIdcLoginResponse

// ============ 客户端 API Key 分发 ============

export interface ClientKeyItem {
  id: number
  /** 脱敏后的 Key（仅展示） */
  maskedKey: string
  name: string
  description?: string
  disabled: boolean
  createdAt: string
  lastUsedAt?: string
  totalCalls: number
  totalInputTokens: number
  totalOutputTokens: number
  totalCacheCreationTokens: number
  totalCacheReadTokens: number
  /** 绑定的账号分组（未绑定时为 undefined） */
  group?: string
  /** 是否系统密钥（由 config.json apiKey 同步，不可删除、可轮换） */
  isSystem: boolean
}

export interface ClientKeysResponse {
  total: number
  keys: ClientKeyItem[]
}

export interface CreateClientKeyRequest {
  name: string
  description?: string
  group?: string
}

/** 创建响应：明文 Key 仅在此处返回一次 */
export interface CreateClientKeyResponse {
  id: number
  key: string
  name: string
  createdAt: string
}

export interface UpdateClientKeyRequest {
  name?: string
  description?: string
  group?: string
}

// ============ 用量统计 ============

export type StatsRange = '24h' | '7d' | '30d'
export type StatsGranularity = 'hour' | 'day'

export interface StatsTimeFilter {
  range?: StatsRange
  startDate?: string
  endDate?: string
  granularity: StatsGranularity
}

export interface StatsFilter {
  /** 不传 = 全部；其它值 = 客户端 Key id */
  keyId?: number
  /** 按账号分组筛选（仅影响 timeseries / by-credential，by-model 不支持） */
  group?: string
}

export interface OverviewStats {
  todayCalls: number
  todayInputTokens: number
  todayOutputTokens: number
  todayErrors: number
  todayCredits: number
  weekCalls: number
  weekInputTokens: number
  weekOutputTokens: number
  weekCredits: number
  activeClientKeys: number
  activeCredentials: number
}

export interface TimeSeriesPoint {
  ts: string
  inputTokens: number
  outputTokens: number
  cacheCreationTokens: number
  cacheReadTokens: number
  calls: number
  errors: number
  credits: number
}

export interface ModelDistribution {
  model: string
  calls: number
  inputTokens: number
  outputTokens: number
  cacheCreationTokens: number
  cacheReadTokens: number
}

/** 各账号积分时序里的一条账号系列（图例项） */
export interface CreditSeriesMeta {
  credentialId: number
  email: string | null
  /** 窗口内该账号积分合计，决定图例排序与 Top N 入选 */
  totalCredits: number
}

/** 单个时间桶内各账号的积分；key = credentialId 的字符串形式，稀疏（无消耗不出现） */
export interface CreditPoint {
  ts: string
  credits: Record<string, number>
}

/** GET /stats/credits-by-credential 响应 */
export interface CreditsByCredential {
  series: CreditSeriesMeta[]
  points: CreditPoint[]
  /** 窗口内有消耗的账号总数；> series.length 表示被 Top N 截断 */
  totalCredentials: number
}

export interface CredentialDistribution {
  credentialId: number
  email?: string
  calls: number
  inputTokens: number
  outputTokens: number
  cacheCreationTokens: number
  cacheReadTokens: number
  errors: number
}

// ============ 请求链路追踪 ============

/** 单次上游尝试 */
export interface TraceAttempt {
  attempt: number
  credentialId: number
  email?: string | null
  endpoint: string
  /** 上游 HTTP 状态码；null = 网络层失败 */
  httpStatus: number | null
  /** success / quota_exhausted / account_throttled / auth_failed / transient / network_error / bad_request / unknown */
  outcome: string
  /** 上游错误体片段（已截断） */
  errorSnippet: string | null
  durationMs: number
  /** 本跳相对请求起点的偏移；null/undefined = 未知（该列存在前的历史行），此时只能顺序堆叠 */
  startedMs?: number | null
  /** 本跳出口：'direct' = 直连；null/undefined = 未知（该列存在前的历史行）；其余为代理 URL */
  proxyUrl?: string | null
}

/** 响应处理的一段；流式与非流式段名不同 */
export interface TracePhase {
  seq: number
  /** 流式：first_token | streaming | finish；非流式：first_token | body_read | decode | assemble */
  phase: string
  startedMs: number
  durationMs: number
  /** 复用 outcome 常量；client_disconnected = 客户端断开，非故障 */
  outcome: string
  /** 该段结束时已下发字节数 */
  bytes?: number | null
  detail?: string | null
}

/** 段 × 出口 的窗口失败率 */
export interface PhaseBaselineRow {
  phase: string
  /** 'direct' = 直连；'' = 未知 */
  proxyUrl: string
  total: number
  failed: number
}

/** 流形态摘要里的单个内容块 */
export interface StreamShapeBlock {
  /** 块类型：thinking / text / tool_use / redacted_thinking */
  t: string
  /** 块出现时刻（相对请求开始 ms） */
  ms: number
  /** 内容字节数 */
  b: number
}

/** 一个外部请求的完整链路 */
export interface TraceRecord {
  traceId: string
  ts: string
  keyId: number
  /** masterApiKey = 历史 master 调用；clientKey = 客户端 Key；internal = 系统内部任务 */
  keySource: 'masterApiKey' | 'clientKey' | 'internal'
  /** inference / balance_refresh / token_refresh */
  operation: string
  /** 发起请求的客户端 Key 名称（master 表示主 apiKey；管理员业务 Key 可为 null） */
  keyName?: string | null
  model: string
  isStream: boolean
  /** success / error / interrupted */
  finalStatus: string
  finalCredentialId: number
  finalEmail?: string | null
  errorType: string | null
  errorMessage: string | null
  totalAttempts: number
  durationMs: number
  /** 流式中断时已发送字节数 */
  interruptedAfterBytes: number | null
  /** 输入 token */
  inputTokens?: number
  /** 输出 token */
  outputTokens?: number
  /** 缓存创建 token */
  cacheCreationTokens?: number
  /** 缓存读取 token */
  cacheReadTokens?: number
  /** 总 token = input + output + cache_creation + cache_read */
  totalTokens?: number
  /** 费用（credits） */
  credits?: number
  /** 首 Token 延迟（毫秒，仅流式有值） */
  firstTokenMs?: number | null
  /** 首个可渲染帧时刻（相对请求开始 ms）。与 firstTokenMs 差值大 = 假活流（流在推进但客户端无可渲染内容） */
  firstRenderMs?: number | null
  /** 流形态摘要：每个内容块的类型 / 出现时刻 / 内容字节；无形态数据为 null */
  streamShape?: StreamShapeBlock[] | null
  /** Claude Code 会话 id（metadata.user_id 的 _session_<uuid>）；同一 Key 上区分会话/子代理 */
  sessionId?: string | null
  attempts: TraceAttempt[]
  /** 流生命周期分段；非流式为空数组 */
  phases?: TracePhase[]
}

/** 链路查询参数 */
export interface TraceQuery {
  operation?: string
  status?: string
  errorType?: string
  credentialId?: number
  /** 按发起请求的客户端 Key 筛选（0 = master apiKey） */
  keyId?: number
  /** 该凭据在某一跳失败过（即便 trace 最终成功）——用于凭据失败详情 */
  failedAttemptCredentialId?: number
  model?: string
  /** 按账号分组名筛选（只返回 final_credential_id 属于该分组的 trace） */
  group?: string
  onlyFailed?: boolean
  limit?: number
  offset?: number
}

/** 分页响应 */
export interface TracePage {
  records: TraceRecord[]
  total: number
}

/** 单凭据失败分类计数（鉴权 / 账号风控 / 流中断 / 其他） */
export interface FailureStats {
  auth: number
  throttle: number
  other: number
  /** 流中断 + 上游截断（stream_interrupted / upstream_truncated） */
  interrupted: number
}

/** credentialId(字符串) → 失败分类计数 */
export type FailureStatsMap = Record<string, FailureStats>

// ============ 运维（Ops）============

/** 窗口内错误类型分布 */
export interface OpsErrorTypeCount {
  errorType: string
  count: number
}

/** 运维概览 */
export interface OpsOverview {
  windowHours: number
  total: number
  success: number
  error: number
  interrupted: number
  byErrorType: OpsErrorTypeCount[]
  avgDurationMs: number
  avgFirstTokenMs?: number | null
  /** 中断类耗时分位；null = 窗口内无中断样本 */
  interruptedDuration?: DurationPercentiles | null
}

/** 耗时分位。多簇集中（如 ~240s 与 ~720s）指向链路上存在多个固定超时 */
export interface DurationPercentiles {
  p50: number
  p95: number
  p99: number
  /** 样本数。n 小时 p99 等于最大值本身，面板须一并显示以免被当成有效分位 */
  n: number
}

/** 交叉表维度 */
export type CrosstabDimension = 'credential' | 'proxy' | 'model' | 'endpoint'

/** 交叉表里的一个维度桶 */
export interface CrosstabBucket {
  /** credential 为 id 字符串；proxy 为 URL（'direct' = 直连，空串 = 未知）；model 为模型名 */
  key: string
  /** 仅 credential 维度有 */
  email?: string | null
  count: number
  /** 该桶窗口内的全部请求数（成功+失败），即流量基线 */
  traffic: number
  /**
   * 超额倍数 =（本桶错误份额）÷（本桶流量份额）。
   *
   * ≈1 表示错误份额与流量份额相称（不是问题，只是承载得多）；
   * 明显 >1 才是「这个对象错得不成比例」。null = 该桶无流量，算不出。
   */
  lift?: number | null
}

/** 一个 error_type 在某维度上的分布 */
export interface CrosstabRow {
  errorType: string
  total: number
  buckets: CrosstabBucket[]
  /** 最大桶占比。单看会被流量分布带偏，必须与桶上的 lift 一起读 */
  concentration: number
  distinctKeys: number
}

export interface OpsErrorCrosstab {
  dimension: CrosstabDimension
  rows: CrosstabRow[]
}

/** 错误指纹：归一化后的一类错误 */
export interface ErrorFingerprint {
  /** 归一化模板，可变部分已被 <status> / <id> / N 替换 */
  fingerprint: string
  /** 从消息里提取出的 HTTP 状态码；null = 该消息不含状态码 */
  httpStatus?: number | null
  count: number
  /**
   * 原始未归一化消息（1~2 条）。
   * 必须展示：这是发现「两个不同根因被误并成一个指纹」的唯一途径。
   */
  samples: string[]
  firstSeen: string
  lastSeen: string
  /** 映射到该指纹的 error_type。长度 > 1 说明分类与消息内容有歧义 */
  errorTypes: string[]
}

export interface OpsErrorFingerprints {
  windowHours: number
  rows: ErrorFingerprint[]
}

/** 重试阶梯的一级 */
export interface RetryLadderStep {
  attempt: number
  /** 到达这一跳的请求数，**累积量**：跑了 4 跳的也算到达过第 1 跳 */
  reached: number
  /** 在这一跳成功的数量 */
  rescued: number
  /** 该跳独有贡献 ÷ 到达该跳数。塌到很低说明这一跳在白等 */
  marginalYield: number
  /** 进入该跳前的 backoff 中位数；null = 无可用样本（见 backoffCoverage） */
  backoffP50Ms?: number | null
}

export interface RetryEffectiveness {
  windowHours: number
  steps: RetryLadderStep[]
  /** 首跳失败但最终成功的总数 */
  totalRescued: number
  /** 跑完全部重试仍失败的数量 */
  totalExhausted: number
  /** 边际收益塌掉的跳数；null = 未塌 */
  yieldCollapseAt?: number | null
  /**
   * backoff 计算覆盖率 = 两端 started_ms 都非空的相邻跳对 ÷ 全部相邻跳对。
   * 0 表示 backoff 完全无数据 —— 必须展示，否则 null 的 p50 会被当成 0ms。
   */
  backoffCoverage: number
}

/** 按小时趋势点 */
export interface OpsTrendPoint {
  bucketEpoch: number
  total: number
  success: number
  error: number
  interrupted: number
  /**
   * 该桶内 error_type → 计数，稀疏（无错误的桶整个字段缺失）。
   *
   * **口径警告：各值之和 ≠ error 字段。** error 是 total-success-interrupted
   * （即 final_status='error'），而本字段按 traces.error_type 分组，该列同时
   * 存在于 error 与 interrupted 两种状态上（stream_interrupted 与
   * client_disconnected 属后者）。所以之和 ≈ error + interrupted。
   * 拿两者对账必然对不上，这不是 bug。
   */
  byErrorType?: Record<string, number>
}

/** 按凭据统计行 */
export interface OpsCredentialRow {
  credentialId: number
  email?: string | null
  total: number
  success: number
  error: number
  interrupted: number
  authFailed: number
  accountThrottled: number
  networkError: number
  otherFailed: number
}

/** 按代理统计行（proxyUrl 为空串 = 直连） */
export interface OpsProxyRow {
  proxyUrl: string
  attempts: number
  success: number
  networkError: number
  otherFailed: number
  interrupted: number
}

/** 按代理统计响应（附代理池当前状态） */
export interface OpsProxiesResponse {
  stats: OpsProxyRow[]
  pool: ProxyPoolResponse
}

/** 自动处置事件 */
export interface OpsEvent {
  id: number
  ts: string
  /** proxy_auto_disable / proxy_reassign / proxy_probe_disable */
  category: string
  severity: string
  subject: string
  message: string
}

// ============ 账号分组（独立实体）============

export interface GroupItem {
  name: string
  description?: string
  createdAt: string
  /** 引用计数：有多少个凭据带这个分组 */
  credentialCount: number
  /** 引用计数：有多少把客户端 Key 绑定这个分组 */
  clientKeyCount: number
}

export interface GroupsResponse {
  total: number
  groups: GroupItem[]
}

export interface CreateGroupRequest {
  name: string
  description?: string
}

export interface UpdateGroupRequest {
  /** 新名字；不传或与原名一致则不改名 */
  newName?: string
  /** 新备注；空字符串清除；undefined 保留原值 */
  description?: string
}

// ============ 模型注册表 ============
// 与 src/admin/types.rs 的 ModelRegistryResponse / src/anthropic/model_registry.rs 的
// ModelRow 一一对应（后端 serde rename_all = "camelCase"）。

/** 匹配方式：精确匹配，或前缀通配（如 `gpt-5*` 透传行） */
export type MatchKind = 'exact' | 'prefix'

/** 行状态：上游仍在返回 / 上游已不再返回但保留可用 */
export type ModelStatus = 'active' | 'deprecated'

/** 行来源：代码内置（不可删）/ 自动同步产生 / 人工新增 */
export type ModelOrigin = 'builtin' | 'synced' | 'manual'

/**
 * 会被自动同步覆盖、因而支持「锁定」的字段。
 *
 * 与后端 `PATCHABLE_PINNED_FIELDS` 保持一致。`enabled` / `sortOrder` /
 * `matchKind` 是本地策略，同步不碰，因此不在此列、也不该显示锁标记。
 */
export const LOCKABLE_MODEL_FIELDS = [
  'exposedId',
  'displayName',
  'contextWindow',
  'maxOutputTokens',
  'exposeThinkingVariant',
] as const

export type LockableModelField = (typeof LOCKABLE_MODEL_FIELDS)[number]

/** 模型表一行 = 一个上游模型 */
export interface ModelRow {
  /** 上游 modelId，行主键 */
  upstreamId: string
  matchKind: MatchKind
  /** 对外暴露的模型名 */
  exposedId: string
  displayName: string
  ownedBy: string
  modelType: string
  created: number
  /** **输入**上下文窗口 */
  contextWindow: number
  /** **输出**上限（/v1/models 的 max_tokens），与 contextWindow 是不同的量 */
  maxOutputTokens: number
  exposeThinkingVariant: boolean
  enabled: boolean
  /** 是否出现在 /v1/models 列表中。prefix 行强制 false */
  listed: boolean
  status: ModelStatus
  origin: ModelOrigin
  sortOrder: number
  /** 已锁定（人工编辑过、同步时逐字段跳过）的字段名 */
  pinned: string[]
  missingSyncRounds: number
  lastSeenAt: string | null
  /** 额外的子串匹配关键字（只读，后端内置） */
  matchSubstrings?: string[]
  /**
   * 是否支持原生 reasoning / `output_config`。三态：`true`/`false` 是显式声明；
   * 字段缺失（`undefined`）表示未设置，运行时回落到内置硬编码判断。与
   * `enabled`/`sortOrder`/`matchKind` 同组，不受自动同步管辖，不显示锁图标。
   */
  supportsReasoning?: boolean
  /**
   * 这一行能否被删除，只读，响应期计算。与后端 `DELETE /models/{upstreamId}`
   * 的判据同源，不与 `origin` 等价：PATCH 一个内置模型会在覆盖层落一条
   * `origin = 'manual'` 的行，但它本质仍是内置模型、仍不可删。删除按钮的
   * 显隐必须看这个字段，不能看 `origin === 'builtin'`。
   */
  deletable: boolean
}

/** 手动别名：from → to（to 必须是某个存在的 upstreamId） */
export interface ModelAlias {
  from: string
  to: string
}

export interface ModelSyncSettings {
  enabled: boolean
  /** 每日自动同步时间，HH:MM */
  time: string
  probeCredentialId: number | null
  allowPassthrough: boolean
}

export interface ModelSyncState {
  lastSyncAt: string | null
  lastFetchStartedAt: string | null
  source: string | null
  /**
   * upstreamId → 同步元数据。**注意**：这些值后端已经叠加到 `models` 的行上，
   * UI 直接读行上的 status / missingSyncRounds / lastSeenAt 即可，不要读这里。
   */
  modelMeta?: Record<
    string,
    { missingSyncRounds: number; status: ModelStatus; lastSeenAt: string | null }
  >
}

/** `GET /models` 以及所有写端点的统一响应体 */
export interface ModelRegistryResponse {
  /** 有效行集（内置 ∪ 覆盖层叠加后的结果），已按 sortOrder 升序排好 */
  models: ModelRow[]
  aliases: ModelAlias[]
  syncState: ModelSyncState
  settings: ModelSyncSettings
  /** models.json 读取/校验失败，当前跑在纯内置表上 */
  degraded: boolean
  degradedReason: string | null
  /** 已记录可用模型的凭据数 */
  credentialSupportCovered: number
  /** 启用中的凭据总数 */
  credentialTotal: number
  /**
   * 最近一轮同步是否因「单轮标记比例护栏」暂停了消失判定（spec §6.3 第四版）。
   * 从 syncState.source 解码而来，服务重启、定时同步（不经 admin 层）之后
   * 仍能看见——不暴露这个字段，系统会长期处于「同步天天成功、消失判定其实
   * 已停机」而无人知晓的状态。
   */
  disappearanceCheckSkipped: boolean
  /** 触发护栏判据的实际缺失比例（0.0~1.0）。未跳过时为 0 */
  missingRatio: number
}

/** `POST /models/sync` 的 diff 摘要 */
export interface SyncSummary {
  /** authoritative（权威轮，可写入）/ advisory（参考轮，不写入） */
  round: string
  added: number
  updated: number
  deprecated: number
  trusted: boolean
  source: string
  /** 本轮是否因护栏暂停了消失判定（新增/更新照常进行） */
  disappearanceCheckSkipped: boolean
  missingRatio: number
}

/** `PATCH /models/{upstreamId}` 请求体。只列可写字段 */
export interface PatchModelRequest {
  exposedId?: string
  displayName?: string
  contextWindow?: number
  maxOutputTokens?: number
  exposeThinkingVariant?: boolean
  enabled?: boolean
  sortOrder?: number
  matchKind?: MatchKind
  /**
   * 是否支持原生 reasoning / `output_config`。要清回「跟随内置默认」（即把
   * 后端字段变回 `None`）需要传 `supportsReasoning: undefined` 且
   * `supportsReasoningSet: true`——只传 `undefined` 的 `supportsReasoning`
   * 而不带 `Set` 标记，后端视为「本次不改这个字段」。
   */
  supportsReasoning?: boolean
  supportsReasoningSet?: boolean
  /** 解除锁定的字段名，使其回归自动同步 */
  unpin?: string[]
}

/** `POST /models` 请求体。只有 upstreamId 必填 */
export interface CreateModelRequest {
  upstreamId: string
  exposedId?: string
  displayName?: string
  contextWindow?: number
  maxOutputTokens?: number
  exposeThinkingVariant?: boolean
  enabled?: boolean
  sortOrder?: number
  matchKind?: MatchKind
}

/** `PATCH /models/settings` 请求体 */
export interface SetModelSyncSettingsRequest {
  enabled?: boolean
  time?: string
  probeCredentialId?: number | null
  /** 请求体中是否出现了 probeCredentialId 键（区分「不改」与「置空」） */
  probeCredentialIdSet?: boolean
  allowPassthrough?: boolean
}

/** 余额历史中的一个时间点（后端按小时桶取均值） */
export interface BalancePoint {
  ts: string
  credentialId: number
  currentUsage: number
  remaining: number
  usagePercentage: number
  usageLimit: number
}

/** 某账号的消耗速率与耗尽预测 */
export interface BalanceBurnRate {
  credentialId: number
  email: string | null
  /** 窗口内每小时平均消耗额度 */
  perHour: number
  remaining: number
  /** 按当前速率预计还能撑多少小时；null = 没在消耗或样本不足 */
  hoursToExhaust: number | null
  /** 估算基于多少个快照点，太少时应标注样本不足 */
  samplePoints: number
}

export interface BalanceHistory {
  windowHours: number
  series: BalanceBurnRate[]
  points: BalancePoint[]
}

# 余额被动化 + 402 冷冻自然恢复 设计

日期：2026-08-22
状态：已与用户对齐（方向由用户裁决：完全被动，不做流量门控折中；402 用冷冻期
自然恢复，参考 sub2api 实现模式）

## 背景

生产实测（2026-08-12..20 trace）：`balance_refresh` 操作 16603 次，是真实推理
请求（8289）的两倍——300s 周期全天候刷新的开销大于业务本身。但直接砍掉周期
刷新有一颗已知地雷：**QuotaExceeded 自愈目前只有余额刷新一条路**
（`token_manager.rs:2623`，git 历史里 0.9.11 曾因门控刷新导致 402 凭据永远
回不了池，专门修复并有测试 `refresh_all_balances_runs_regardless_of_mode`
钉死）。所以被动化必须先把自愈机制从「探测发现恢复」改成「冷冻到期自然恢复」，
两件事是一个整体，不能只做一半。

参考 sub2api（研究报告 `reports/2026-08-22-sub2api-absorb/r4-cooldown-passive.md`）：
- 冷却 = 时间戳，选号时惰性比较，全仓库无扫描解冻后台任务
- 有上游真实 reset 时间用真实的，没有用固定短兜底；不做指数退避
- 覆盖式重冻 + 池内稀释防 402 风暴，不需要 single-flight
- 最深的坑：可自愈状态误用永久禁用（OAuth 401 曾锁死账号）——
  **「可自愈进冷冻、不可自愈才禁用」是本设计的分界线**

## A 部分：402 冷冻自然恢复

### 状态模型

`token_manager` 的凭据条目新增 `frozen_until: Option<i64>`（epoch 秒，随
credentials.json 持久化，serde 缺省 None 兼容旧文件）：

- `report_quota_exhausted(id)`（`token_manager.rs:2575`）改为写 `frozen_until`，
  **不再置 `disabled=true`**：
  - 到期时间优先取该凭据余额快照的 `next_reset_at`（`BalanceCache` /
    `kiro_balance_cache.json`，402 报文本身只有 reason 不带时间）**+ 300s 安全
    余量**（上游重置生效可能滞后）；
  - `next_reset_at` 缺失或已过期 → 兜底 `now + FREEZE_FALLBACK_SECS`
    （常量 3600，注释写明「到期乐观回池，仍超限被下一个真实 402 再冻，
    每小时一次试探成本可忽略」）。
- 再次 402 → **覆盖式重冻**（重新按上述规则计算；此时余额快照可能已有下月
  reset 时间）。不叠加、不指数。
- `disabled` / `DisabledReason::QuotaExceeded` 不再由 402 路径写入；
  `DisabledReason` 枚举保留 `QuotaExceeded` 变体用于旧数据反序列化。

### 解冻：纯惰性

- 候选过滤处（`acquire_context` 的选号路径与 `dispatch.rs::pick` 的候选集
  构建）把「可调度」判定从 `!disabled` 扩为 `!disabled && !frozen(now)`，
  `frozen(now) = frozen_until.is_some_and(|t| now < t)`。
- 到期后**不需要任何状态写回**即可参与调度；首个成功请求顺手清掉
  `frozen_until = None`（落盘一次），避免时间戳残留造成面板误读。
- 无后台解冻任务、无试探性放量：多请求同时打到刚解冻的凭据，最坏是各收
  一次 402 然后覆盖式重冻——与 sub2api 同判断，池内多凭据稀释即可。

### 存量迁移与自愈路径替换

- 加载 credentials.json 时：`disabled && disabled_reason == QuotaExceeded` 的
  条目一次性转换为 `disabled=false, frozen_until=按上述规则计算`（迁移属于
  「可自愈状态回归冷冻语义」，不动其他禁用原因）。
- `token_manager.rs:2623` 的「余额刷新发现恢复则解禁」逻辑保留但降级为
  兜底（被动刷新仍可能触发它）；正路是冷冻到期。
- `refresh_all_balances` 里「QuotaExceeded 也要探测」的特例（service.rs:973）
  随周期刷新一起失去存在意义，删除；对应测试
  `refresh_all_balances_runs_regardless_of_mode` 的**保护目标已被冷冻机制
  取代**，改写为断言冷冻到期后凭据无需刷新即可回池（语义继承，不是删除保护）。

### 面板

- 凭据卡片新增「冷冻中（至 HH:MM）」状态显示，与「已禁用」视觉区分
  （冷冻=可自愈=暖色计时；禁用=需人工=现状红色）。
- 快照/状态 API（`snapshot()` 序列化处，注意 `token_manager.rs:4888` 的
  字面量契约测试先例）新增 `frozenUntil` 字段，缺省不序列化保持兼容。
- 三态循环按钮语义不变（启用/禁用仍是人工操作）；冷冻不可被按钮切换，
  但「立即解冻」作为卡片上的独立小动作提供（写 `frozen_until=None`），
  给人工干预留出口。

## B 部分：余额完全被动

- **删除** `start_balance_refresher`（service.rs:1032）及 main.rs:548 的启动
  调用；`refresh_all_balances` 函数保留（面板手动刷新与被动触发复用）。
- weighted 选号路径（`dispatch.rs::pick` 的调用侧）：发现余额快照
  `now - cached_at > BALANCE_CACHE_TTL_SECS`（沿用 300）时，**异步**触发
  刷新（tokio::spawn，不阻塞本次请求；本次照常用旧快照 + 消耗回写）：
  - single-flight：进程内 `AtomicBool`（或等价）保证同时只有一轮被动刷新；
  - 每凭据最小间隔 `PASSIVE_REFRESH_MIN_INTERVAL_SECS = 60` 防抖（刷新循环
    内部按 cached_at 跳过 60s 内刷过的凭据）；
  - 只刷「参与本次候选集的凭据 + 冷冻中但 `next_reset_at` 缺失的凭据」
    （后者是为了尽快拿到真实 reset 时间修正兜底冷冻，属低频）。
- priority 模式：选号不触发刷新（该模式不消费余额权重）。
- 面板手动刷新按钮、单凭据查余额 API 行为不变。
- 启动时不做首轮刷新：weighted 对「全员余额未知」已有降级路径
  （`all_unknown_degrades_to_least_consumed` 测试在案）。

### 已接受的代价（用户确认）

- ops 面板余额历史曲线变稀疏：仅有流量时段有点位。
- 空闲转活跃的第一批请求用旧余额加权，靠消耗回写补偿，精度损失有限。

## 不做

- 指数退避冷冻、解冻试探性放量、per-凭据冷冻时长配置项（常量 + 注释，
  R3 原则：无当前明确收益不加配置层）。
- 流量门控周期刷新（用户已裁决直接走完全被动）。
- CN 类主动轮询通道（kiro 上游状态可被动感知，无此需求）。

## 测试要点

- 冷冻：402 → frozen_until 写入（有/无 next_reset_at 两分支）；到期前不可
  调度、到期后无需任何刷新即可被选中；再 402 覆盖式重冻；成功请求清冻结；
  存量 disabled+QuotaExceeded 迁移。
- 被动刷新：TTL 内选号不触发；过期触发且 single-flight 不重入；60s 防抖；
  priority 模式零触发。
- 契约：snapshot 序列化 `frozenUntil` 字段名字面量测试（先例 :4888）。
- 改写 `refresh_all_balances_runs_regardless_of_mode` 为冷冻语义的等价保护。

## 验收（部署后）

- `operation='balance_refresh'` 的日增量从 ~2300 降一个数量级以上；
- 人为把一张凭据标成冷冻已过期，验证无刷新条件下它自然回池；
- 面板冷冻态显示与「立即解冻」动作可用。

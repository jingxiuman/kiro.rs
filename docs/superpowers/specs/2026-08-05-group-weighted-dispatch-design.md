# 组内会话分流：有效剩余额度调度 + 会话粘滞（`loadBalancingMode: weighted`）

日期：2026-08-05
状态：已过 codex 评审并据评审重写（第二版）

## 1. 问题

组内多个账号无法分流，流量长期集中在单个账号上。

现状实测（`data/kiro_stats.json` + `data/kiro_balance_cache.json`，2026-08-05）。老竹组 7 个号，priority 1~7：

| id | priority | success_count | 余额已用 | last_used |
|---|---|---|---|---|
| 2 | 1 | 7323 | 26.25% | 08-05 07:14 |
| 12 | 2 | 0 | 0% | 08-03 |
| 10 | 3 | 0 | 0% | 从未 |
| 8 | 4 | 12 | 0.48% | 07-27 |
| 4 | 5 | 362 | 0.16% | 07-27 |
| 5 | 6 | 0 | 0.56% | 从未 |
| 11 | 7 | 0 | 0% | 从未 |

7 个号中 3 个从未被使用。

### 根因

生效配置为 `loadBalancingMode: "priority"`（`data/config.json:24`，亦为默认值）：

1. `acquire_context_excluding`（`src/kiro/token_manager.rs:1809`）优先复用全局 `current_id`，只要它通过硬过滤就直接返回，不进入排序。
2. 未命中时 `select_next_credential_excluding`（`:1731`）取 `min_by_key(priority)`，恒定指向 priority 最小的号。

换号只由**故障**触发（402 quota / 401·403 连续失败 3 次 / 429 suspicious activity 冷却），从不由**余额**触发。

**余额数据完全不参与选路。** `BalanceSnapshot`（`src/admin/balance_store.rs:20`）只写入 DuckDB 与 admin 面板，选路代码从未读取。

### 现有 `balanced` 模式为何不能直接用

`balanced` 取 `min_by_key((success_count, priority))`。`success_count` 持久化在 `kiro_stats.json`，当前 id2=7323 而其余多为 0，直接切换会导致零计数账号被连续打满 7000+ 次的冷启动踩踏。且 `success_count` 不按组隔离，跨组账号排名会被其他组流量污染。

更根本的是「次数」不等于「额度」：id4 成功 362 次消耗 0.16%，id2 成功 7323 次消耗 26.25%，单次成本相差近一个量级。

## 2. 目标与非目标

### 目标

- 组内各账号的**额度消耗**趋于同步（计量单位为 credits，见 §4.3）。
- 同一会话的多轮请求尽量落在同一账号，保住上游 prompt cache。
- 默认行为零变化，可热切换、可热回退。

### 非目标

- 不修改 `MAX_TOTAL_RETRIES = 4`（`src/kiro/provider.rs:33`）。但必须补测试，见 §7。
- 不改动 `priority` 与 `balanced` 两个既有模式的任何行为。

## 3. 决策记录

| 编号 | 决策 | 备选与否决理由 |
|---|---|---|
| D1 | 均衡口径按**剩余额度（credits）** | 按请求次数均衡与实际额度消耗不成正比（实测差近一个量级） |
| D2 | **启用会话粘滞** | 不粘滞会打散上游 prompt cache，与既有 cache metering 工作直接冲突 |
| D3 | 落地为**第三种 mode** `weighted` | 改造现有 `balanced` 会改变已有语义且无法 A/B 对比 |
| D4' | 粘滞 key **仅在解析出 UUID session 时启用**，其余请求 `sticky_key = None` 走纯调度 | 原 D4 拟复用 `cache_metering::isolation_seed`，**已推翻**：该函数只在 `key_id == 0` 时走 `cc` 级降级，普通 client key 直接返回 `key:<key_id>`（`src/anthropic/cache_metering.rs:577`），会导致同一 client key 下所有会话共享一条粘滞记录并被永久钉死在一个账号——正是本设计要修的病 |
| D5' | 调度算法为 **argmax(有效剩余)**，有效剩余 = 缓存余额 − 本代次本地已消耗 credits | 原 D5 为 SWRR，**已推翻**：SWRR 只在粘滞 miss 时运行，粘滞命中不动权重，因此只能控制「新会话分配数」而控制不了 token/额度消耗；长会话可长期严重倾斜。且 SWRR 的 `current_weight` 在候选集变化时会冻结、恢复后携带旧信用或旧债务 |
| D6' | 余额三态：fresh / 有限期 stale / unavailable；unavailable 取组内已知中位数；跨 `next_reset_at` 的快照直接失效 | 原 D6 的「过期照用」无限期，与启动加载会丢弃超 TTL 条目（`src/admin/service.rs:2719`）语义不一致 |
| D7 | 粘滞空闲过期 30 分钟，容量 10000 条，不持久化 | 重启后上游 cache 状态不可知，恢复粘滞是假精确 |
| D8 | 新逻辑抽出独立模块 `src/kiro/dispatch.rs` | `token_manager.rs` 已 5000+ 行；独立模块可脱离 tokio 与凭据构造做纯单测 |
| D9 | **临时排除不触发会话迁移** | 真实 `excluded` 主要是并发门禁 queue full/timeout（`src/kiro/provider.rs:585`、`:649`）与 RPM 竞争，一次短暂拥塞不应永久毁掉整个会话的 prompt cache |
| D10 | 余额 refresher **仅在 `weighted` 生效时启动** | 无条件启动会让默认（`priority`）部署凭空多出周期性上游余额请求，违反「默认行为零变化」 |

### D5' 的推导

三个候选都被验证过：

- `argmax(remaining)` 单独用不行。`BALANCE_CACHE_TTL_SECS = 300`（`src/admin/service.rs:43`），两次刷新之间 `remaining` 是冻结常量，argmax 恒定指向同一账号——与当前病状相同。
- SWRR 不行，理由见 D5' 表格列。
- **`argmax(remaining − 本地已消耗)` 可行**，且同时解掉上面两条：
  - 缓存冻结期内本地消耗持续增长，argmax 自然轮转；
  - 粘滞命中同样回写消耗，长会话会压低其所属账号的有效剩余，新会话自然不再流向该账号；
  - 状态语义是「自上次余额快照以来消耗了多少」，与候选集无关，账号被冷却期间不产生冻结信用，恢复后无补偿突发；
  - 可复算：能直接回答「为什么选了 A」——A 的有效剩余组内最高。

关键前提（已验证）：`UsageRecordHook::record`（`src/anthropic/handlers.rs:70`）的 `credits: f64` 参数与余额的 `remaining` / `usageLimit` 同量纲（均为 credits，`usageLimit` 为 10000）。因此 `remaining − Σcredits` 是**精确**校正，而非按 token 数做的代理估算。

## 4. 架构

### 4.1 模式枚举化（必须先做）

当前 `weighted` 无法工作，有三处硬编码只认 `priority` / `balanced`：

| 位置 | 现状 | 必须改为 |
|---|---|---|
| `src/kiro/token_manager.rs:1829` | `is_balanced = mode == "balanced"` 才跳过 `current_id` 快路径 | `balanced` 与 `weighted` 同属动态选号模式，均跳过 |
| `src/kiro/token_manager.rs:3730` | `if mode != "priority" && mode != "balanced" { 拒绝 }` | 接受 `weighted` |
| `src/admin/service.rs:2417` | 同上 | 接受 `weighted` |
| `src/admin/service.rs:657` | 只对 `balanced` 隐藏「当前账号」展示 | `weighted` 同样隐藏 |

引入 `enum LoadBalancingMode { Priority, Balanced, Weighted }` 并集中解析，避免继续用裸字符串比较散落各处。序列化保持原字符串以兼容既有配置文件。

**若不做这一步，`weighted` 会静默退化为 `priority` 行为**：`current_id` 快路径命中后 dispatcher 根本不会被调用。

### 4.2 `src/admin/balance_cache.rs`（新增）

将 `AdminService.balance_cache`（`src/admin/service.rs:180`）抽出为独立的 `Arc<BalanceCache>`，`main.rs` 中先于二者创建并注入 `AdminService`（写方）与 `GroupDispatcher`（读方）。

模块位置说明：`src/kiro/` 已依赖 `src/admin/`（`token_manager.rs:19` 引 `trace_db`，`provider.rs:135` 持有 `admin::ops::SharedOpsRuntime`），放在 `src/admin/` 不产生新依赖方向。

必须同时修正的既有行为：

- **原子发布 + generation**。当前刷新是串行逐账号写入、账号间 sleep 400ms（`src/admin/service.rs:946`），dispatcher 会看到同一轮的新旧混合值。改为整轮收集完毕后一次性发布，并附带单调递增的 `generation: u64`。
- **持锁写盘**。当前序列化与磁盘写入在持锁状态下完成（`src/admin/service.rs:2759`）。改为锁内 clone 快照、锁外序列化写盘，否则调度会被磁盘延迟阻塞。
- **实际刷新间隔 > TTL**。当前是一轮结束后再 sleep 300s（`src/admin/service.rs:999`），实际间隔为 `300s + 本轮耗时`，必然超过 TTL。改为固定周期调度，或把 stale 判定阈值定义为独立于 TTL 的 `MAX_STALE`（见 §4.4）。
- **quota 耗尽账号不再被刷新**。后台刷新只遍历未禁用账号（`src/admin/service.rs:946`），而 402 会将账号 `disabled`。必须定义月度重置后的重新探测路径，否则被 402 禁用的账号永不回到调度池。

### 4.3 `src/kiro/dispatch.rs`（新增）

```rust
pub struct Candidate {
    pub id: u64,
    pub priority: i32,
    /// 本次为何可选/不可选的来源，用于区分临时排除与长期不可用
    pub excluded_kind: Option<ExclusionKind>,
}

pub enum ExclusionKind { Transient, Durable }   // queue/RPM 竞争 vs disabled/quota/throttled

pub struct GroupDispatcher {
    state: Mutex<DispatchState>,      // sticky 与 consumed 合为一把锁，见 §4.5
    balance: Arc<BalanceCache>,
}

struct DispatchState {
    sticky:   HashMap<String, StickyEntry>,        // 上限 10000
    consumed: HashMap<(String, u64), f64>,         // 本代次已消耗 credits
    generation: u64,                               // 余额快照代次，切换时清空 consumed
}

struct StickyEntry { cred_id: u64, last_seen: Instant }

impl GroupDispatcher {
    /// candidates 为调用方过完硬过滤后的快照，非空。now 由调用方传入以便测试推进时钟。
    pub fn pick(&self, group: Option<&str>, candidates: &[Candidate],
                sticky_key: Option<&str>, now: Instant) -> PickResult;

    /// 反向路径：请求结束后回写本次实际消耗。
    pub fn report_consumption(&self, group: Option<&str>, cred_id: u64, credits: f64);
}

pub struct PickResult { pub cred_id: u64, pub reason: PickReason }
pub enum PickReason { StickyHit, StickyMigrated, FreshSelect, TransientFallback }
```

**分组隔离**：`consumed` 按 `(group, id)` 分桶，粘滞 key 前缀带 group（`format!("{}|{}", group.unwrap_or(""), session_uuid)`）。修复现有 `success_count` 全局计数导致的跨组污染，以及 `current_id` 全局单例导致的跨组串扰。

**状态回收**：`consumed` 在 generation 切换时整体清空，天然有界。`sticky` 插入时若 `len >= 10000`，先清除全部超 30min idle 的条目，仍满则 O(n) 扫描淘汰 `last_seen` 最早的一条——不引入 `lru` 依赖（当前 `Cargo.toml` 无此 crate），正常负载下 O(n) 路径基本不触发。凭据删除或组改名时由调用方触发一次全量清理。

### 4.4 有效剩余的完整数值契约

```
effective_remaining(id) = balance_of(id) − consumed[(group, id)]
```

`balance_of(id)` 三态：

| 状态 | 判定 | 取值 |
|---|---|---|
| fresh | `now − cached_at < 300s` | `remaining` 原值 |
| stale | `300s <= now − cached_at < MAX_STALE` | `remaining` 原值 |
| unavailable | 超过 `MAX_STALE`、条目缺失、`now >= next_reset_at`（跨月重置后旧值必然失效）、或值非有限（NaN/inf） | 组内**已知**值的中位数 |

`MAX_STALE` 定为 3600s（12 个刷新周期）。跨 `next_reset_at` 无条件失效优先于 TTL 判定。

数值域规则：

- 全部为 f64 比较，不转整数——避免小于 1 的余额被截断为 0。
- **允许为负**：`remaining = usageLimit − currentUsage`，开启超额后可为负（`src/admin/service.rs:883` 注释明确说明）。负值参与排序不做 clamp——超额账号排在最后是正确行为，且组内全为负时仍会选出最不负的那个，不会返回空集。
- 组内**全部** unavailable 时中位数无定义，此时所有账号取同一常数 0。此时 `argmax(0 − consumed) = argmin(consumed)`，自动退化为「按已消耗量的最少使用轮转」，无需单独的等权分支，也不会除零或 panic。
- 排序为全序：先按 `effective_remaining` 降序（`partial_cmp`，非有限值已在上一步转为 unavailable 故不会出现 NaN），平局按 `priority` 升序，再平局按 `id` 升序。保证结果可复算，不依赖 HashMap 迭代顺序。
- `consumed` 累加使用饱和加法；单次 `credits` 非有限或为负时按 0 计入。

### 4.5 锁结构

codex 指出原设计「新锁放最内层即安全」的论证不成立——`token_manager.rs:1897` 的死锁警告实为「持有 `entries` 时再调用会取 `entries` 的 `available_count()` 会自锁」，不是一般性证明。

改为可证明的结构：

1. 在 `entries.lock()` + `credential_support.read()` 下构造完整的 `Vec<Candidate>` 快照，**释放这两把锁**；
2. 从 `BalanceCache` 取快照（含 generation），**释放该锁**；
3. 再调用 `dispatcher.pick()`，其内部只持有 `DispatchState` 一把锁。

`sticky` 与 `consumed` 合为单个 `Mutex<DispatchState>`，消除 dispatcher 自身形成锁环的可能，并保证同一 session 并发 miss 时的「选择 + 写入」原子。

`pick` 全程不做 IO、不 `await`、不回调 token_manager。

## 5. 数据流

### 正向：选号

```
post_messages (src/anthropic/handlers.rs:923)     此处有 payload 与 key_ctx
  └─ sticky_key = payload.metadata.user_id
                    → extract_session_id (src/anthropic/metadata.rs:9)
                    → Some("<uuid>") | None          ← 解析不出 UUID 即 None，不降级
  └─ ResponseProcessingConfig { group, sticky_key, .. }   新增字段
      └─ call_api_stream(body, tracer, group, sticky_key)  新增参数
          └─ call_api_with_retry(..., sticky_key)          照 pinned 参数既有先例
              └─ acquire_context_excluding(model, group, excluded, sticky_key)
                  └─ [硬过滤: disabled / throttled / RPM / group / model / opus]
                       ↑ RPM 过滤为工作区新增，原设计遗漏
                  └─ 构造 candidates 快照 → 释放锁 → dispatcher.pick(...)
```

`pick` 内部：

```
1. 查粘滞：sticky_key 命中且未超 30min idle
     a. 目标在 candidates 中           → StickyHit，返回
     b. 目标因 Transient 排除（queue/RPM）→ TransientFallback：
            本次选别的号，但【不覆写粘滞记录】
     c. 目标因 Durable 排除（disabled/quota/throttled）→ StickyMigrated：
            重选并覆写粘滞记录
2. 无粘滞或已过期 → FreshSelect：
     argmax(effective_remaining)，平局按 priority、id
     若 sticky_key 非 None 则写入粘滞表
```

### 反向：回写消耗

```
UsageRecordHook::record(credential_id, ..., credits, status)   (handlers.rs:70)
  └─ dispatcher.report_consumption(group, credential_id, credits)
```

`record` 已经带 `credential_id` 与 `credits`，回写点是现成的；只需给 `UsageRecordHook` 增加 `group` 字段与 dispatcher 句柄。粘滞命中与新分配两条路径都会经过这里，因此长会话的消耗照样计入——这是本设计能承诺「额度消耗趋同」而非仅「新会话数加权」的依据。

## 6. 错误处理与降级

| 情形 | 处理 | 理由 |
|---|---|---|
| `candidates` 为空 | `pick` 不会被调用（硬过滤后已 `return None`） | 保持前置不变量 |
| 粘滞目标被临时排除（queue full / RPM 竞争） | 本次换号，**不覆写粘滞** | 一次短暂拥塞不应永久毁掉会话的 prompt cache |
| 粘滞目标被长期排除（disabled / quota / throttled） | 重选并覆写粘滞 | 会话应整体迁移 |
| 同一请求内重试换号 | 按上两行的排除原因分别处理 | dispatcher 依 `ExclusionKind` 判断，不再压成同一种 |
| `sticky_key = None`（无 metadata、MCP 路径、admin 探针） | 不查表也不写表，纯 argmax 调度 | 避免同一 client key 下所有会话共享一条记录被钉死 |
| LRU 淘汰活跃会话 | 退化为重新分配，不报错 | 容量上限是防爆内存的兜底 |
| `loadBalancingMode` 值非法 | 落入 `_` 分支 = `priority` | 与现状一致 |
| 冷却结束后会话不迁回原号 | 接受，不迁回 | cache 已在新号建立，迁回等于再丢一次 |
| 组内全部余额 unavailable | 常数 0 + `argmin(consumed)` | 自动退化为最少消耗轮转，不除零、不返回空集 |
| 组内全部 `remaining <= 0`（月底耗尽） | 仍选出最不负者，请求照常发出 | 由上游返回 402 走既有 quota-exhausted 路径 |
| `credits` 为 0（上游未回报） | 见 §9，留待确认 | 影响长会话能否被正确计量 |

## 7. 测试

`dispatch.rs` 不依赖 tokio、不依赖凭据构造，核心行为为纯单测。`pick` 接受 `now: Instant` 以便推进时钟。

**dispatch 单测**

1. 有效剩余排序：余额 7000/3000、消耗均为 0 时选前者；给前者回写 5000 消耗后改选后者。
2. 缓存冻结期轮转：余额固定不变，连续请求下随 `consumed` 增长在账号间轮转，不出现单账号独占。
3. unavailable 取中位数：3 个已知 + 1 个缺失，缺失者有效值等于中位数，既非 0 亦非 max。
4. 全部 unavailable：退化为 `argmin(consumed)`，N 个号跑 N 次各命中一次。
5. 负余额：超额账号排最后；组内全负时仍选出最不负者，不 panic。
6. 跨 `next_reset_at`：旧快照直接判为 unavailable，不因 TTL 未到而沿用。
7. generation 切换清空 `consumed`。
8. 粘滞命中：同一 key 连续调用恒返回同一 id。
9. **临时排除不迁移**：粘滞目标被 `Transient` 排除时本次换号但粘滞记录不变，排除解除后回到原号。
10. **长期排除才迁移**：`Durable` 排除时粘滞记录被覆写。
11. idle 过期：`now` 推进 31 分钟后重新分配。
12. 容量淘汰：插入 10001 条，最早的被淘汰。
13. 分组隔离：同一 id 挂两组，两组 `consumed` 互不影响。
14. `sticky_key = None` 不写表。
15. **不同会话不共用粘滞**：相同 client key、相同 system/tools、两个不同 session UUID 必须得到独立的 sticky entry。

**集成测试（只测 `pick` 覆盖不到）**

16. `current_id` 快路径：预设 `current_id = A`，`weighted` 模式下等权、无粘滞的连续请求必须能选到 B。**这条是 §4.1 遗漏的唯一探针。**
17. 重试预算：前 4 个高有效剩余账号均返回 402，组内仍有健康账号时的预期行为需明确定义并断言（`MAX_TOTAL_RETRIES = 4`）。
18. 回归：`priority` / `balanced` 模式下选择结果与改动前逐位一致。

## 8. 配置与上线

- `loadBalancingMode` 增加取值 `"weighted"`。**默认仍为 `priority`**。
- admin UI 下拉增加一项（`admin-ui/src/components/topbar-tools.tsx:236`）。
- 生效方式：`PUT /api/admin/config/load-balancing`（真实路由见 `src/admin/router.rs:110`），热切换不重启。
- 余额 refresher 仅在 `weighted` 生效时启动（D10），切换模式时相应启停。
- 回退：切回 `priority`。内存中的粘滞表与消耗表留存，不影响任何行为。

### 可观测字段（必须，否则上线后无法归因）

调度结果需记录：`PickReason`（sticky hit / migrated / transient fallback / fresh）、候选集签名、所选账号的余额年龄与 generation、各候选的有效剩余。缺了这些，线上出现倾斜时无法区分是余额陈旧、长会话、候选过滤还是消耗计量造成的。

### 验证口径

**不能用 `success_count` 增量对比余额份额**——`success_count` 是请求次数，与本设计承诺的额度口径不同量纲。

判定标准：

1. 按 `credential_id` 聚合 `UsageRecord.credits` 增量，其组内分布应与各账号切换时刻的 `remaining` 反向对应（余额多的应分到更多消耗）；
2. `kiro_balance_cache.json` 中组内各号的 `usagePercentage` **极差**应随时间收敛而非发散。

两条都要看增量或极差，不看绝对值——`success_count` 与 `usagePercentage` 的历史包袱（id2 已 26.25%）在数万次请求内都会掩盖新行为。

## 9. 待确认（实现前需定）

以下由本人决定，属运营判断而非技术选型：

1. **`credits` 为 0 时如何计量。** `UsageRecordHook::record` 会把非有限或非正的 `credits` 归零（`handlers.rs:92`）。若上游在部分路径不回报 credits，这些请求的消耗将完全不计入，长会话可能借此绕过均衡。需要定：是按 token 数估算一个替代值，还是接受漏计。
2. **粘滞迁移阈值。** 除 §5 的排除原因外，是否在「粘滞账号的有效剩余低于组内最大值一定比例」时主动迁移。定了阈值就多一条迁移路径，代价是一次 cache 损失。
3. **`MAX_STALE = 3600s` 是否合适。**

实现时先搭好 `dispatch.rs` 骨架与签名并留 TODO。

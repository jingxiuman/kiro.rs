//! 组内选号：有效剩余额度调度。
//!
//! 有效剩余 = 缓存余额 − 本代次已消耗 credits。
//!
//! 为什么不是直接选 remaining 最大者：余额缓存 300s 才刷新一次，两次刷新之间
//! remaining 是一组冻结常量，argmax 会恒定指向同一账号，等于换皮的单账号粘滞。
//! 减去本地消耗后，冻结期内也会随消耗增长自然轮转。
//!
//! 为什么不是 SWRR：SWRR 只在粘滞 miss 时运行，粘滞命中不动权重，因此只能控制
//! 「新会话分配数」而控制不了额度消耗；且候选集变化时 current_weight 会冻结，
//! 恢复后携带旧信用或旧债务。

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;

use crate::admin::balance_cache::SharedBalanceCache;
use crate::admin::types::BalanceResponse;

/// 余额最长可被调度使用的陈旧期（秒）。超过即视为 unavailable。
/// 取 12 个刷新周期：外部消耗不进本地计数，陈旧期越长调度偏差越大。
pub const MAX_STALE_SECS: f64 = 3600.0;

/// 粘滞记录空闲过期时长。
pub const STICKY_IDLE_SECS: u64 = 30 * 60;

/// 粘滞表容量上限。防止刷 UUID 的客户端打爆内存，正常负载远用不满。
pub const STICKY_CAPACITY: usize = 10_000;

#[derive(Clone, Copy)]
pub struct Candidate {
    pub id: u64,
    pub priority: i32,
}

/// 本次请求为何不可选。决定粘滞是否应当迁移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionKind {
    /// 并发门禁队满/超时、RPM 窗口竞争——短暂拥塞，不应毁掉会话的 prompt cache
    Transient,
    /// disabled / quota 耗尽 / 429 冷却——长期不可用，会话应整体迁移
    Durable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickReason {
    StickyHit,
    StickyMigrated,
    TransientFallback,
    FreshSelect,
}

pub struct PickResult {
    pub cred_id: u64,
    pub reason: PickReason,
    /// 被选中账号的有效剩余额度（缓存余额 − 本代次已消耗）。用于事后复算
    /// 「为什么选了它」，区分是余额确实领先，还是粘滞/降级把请求钉在了别处。
    pub effective_remaining: f64,
    /// 被选中账号余额快照的陈旧秒数（`now - cached_at`）。缺失条目取
    /// `f64::INFINITY`——表示调度当时对该账号的余额一无所知。
    pub balance_age_secs: f64,
    /// 本次选号所处的余额代次（`BalanceCache::generation`）。
    pub generation: u64,
    /// 硬过滤（disabled/throttled/RPM/excluded）之后本轮参与竞争的候选数。
    /// 上线后若发现流量倾斜，用它区分是「候选被过滤剩很少」还是权重算法本身的问题。
    pub candidate_count: usize,
}

struct StickyEntry {
    cred_id: u64,
    last_seen: Instant,
}

struct DispatchState {
    sticky: HashMap<String, StickyEntry>,
    consumed: HashMap<(String, u64), f64>,
    generation: u64,
}

pub struct GroupDispatcher {
    state: Mutex<DispatchState>,
    balance: SharedBalanceCache,
}

impl GroupDispatcher {
    pub fn new(balance: SharedBalanceCache) -> Self {
        Self {
            state: Mutex::new(DispatchState {
                sticky: HashMap::new(),
                consumed: HashMap::new(),
                generation: 0,
            }),
            balance,
        }
    }

    pub fn report_consumption(&self, group: Option<&str>, cred_id: u64, credits: f64) {
        if !credits.is_finite() || credits <= 0.0 {
            return;
        }
        let key = (group.unwrap_or("").to_string(), cred_id);
        let generation = self.balance.generation();
        let mut st = self.state.lock();
        // 回写侧也要对齐代次，否则「余额发布 → 请求完成回写 → 下一次 pick」
        // 这个顺序下，本次消耗会被 pick 的清空动作吞掉。回写与选号谁先谁后
        // 由请求时序决定，不能只在 pick 里同步。
        sync_generation(&mut st, generation);
        let slot = st.consumed.entry(key).or_insert(0.0);
        *slot += credits;
    }

    /// candidates 非空。excluded 给出本次不可选者及其原因，sticky_key 为会话粘滞种子。
    ///
    /// 契约（I1）：若 `candidates` 中的所有 id 都在 `excluded` 里（例如同组
    /// 多账号同时 quota 耗尽），本函数**降级放行**而不是 panic 或返回错误——
    /// 返回值此时**可能仍属于 `excluded` 集合**（与仓库里并发门禁
    /// `gate_bypass` 的降级语义一致：宁可放行一个不理想的候选，也不要让
    /// 请求无路可走）。调用方若采用「pick → 失败 → 加入 excluded → 再
    /// pick」的重试模式，必须自行设置重试上限，不能假设重试后一定会拿到
    /// 一个不在 excluded 里的新 id。
    pub fn pick(
        &self,
        group: Option<&str>,
        candidates: &[Candidate],
        excluded: &HashMap<u64, ExclusionKind>,
        sticky_key: Option<&str>,
        now: Instant,
    ) -> PickResult {
        debug_assert!(!candidates.is_empty(), "调用方须保证候选非空");
        let snap = self.balance.snapshot();
        let g = group.unwrap_or("").to_string();
        let skey = sticky_key.map(|s| sticky_key_of(group, s));
        let now_ts = chrono::Utc::now().timestamp() as f64;

        let mut st = self.state.lock();
        // 必须走单调的 sync_generation（`>` 而非 `!=`）：`snap` 的读取
        // （上面的 `self.balance.snapshot()`）与这里拿到 `state` 锁之间没有
        // 原子性，其他线程（`pick` 或 `report_consumption`）可能已经把
        // `st.generation` 推进到比 `snap.generation` 更新的代次并写入了
        // consumed；若在这里用 `!=` 比较，会把 `st.generation` 错误地
        // **回退**到本线程手里这个过期的 `snap.generation`，连带清空别的
        // 线程刚写入的消耗。完整推导见 `sync_generation` 的文档注释。
        sync_generation(&mut st, snap.generation);

        // 本轮不可选者一律从候选池剔除，再参与有效剩余选号；否则平局规则
        // （相等有效剩余按 id 升序）可能把刚排除的那个又选回来。
        // 若剔除后候选池为空（全员本轮不可用），退化为不剔除——好过 panic
        // 或返回空结果，调用方仍能拿到一个可重试的候选。
        let available = filter_excluded(candidates, excluded);
        let candidate_count = available.len();
        let generation = st.generation;

        // 1. 查粘滞
        if let Some(k) = &skey {
            let hit = st.sticky.get(k).and_then(|e| {
                if now.duration_since(e.last_seen).as_secs() > STICKY_IDLE_SECS {
                    None
                } else {
                    Some(e.cred_id)
                }
            });
            if let Some(cred_id) = hit {
                match excluded.get(&cred_id) {
                    // 1a. 目标可用：命中，刷新 last_seen
                    None if candidates.iter().any(|c| c.id == cred_id) => {
                        if let Some(e) = st.sticky.get_mut(k) {
                            e.last_seen = now;
                        }
                        let balances = resolve_balances(&available, &snap.entries, now_ts);
                        let consumed = st.consumed.get(&(g.clone(), cred_id)).copied().unwrap_or(0.0);
                        let effective_remaining =
                            balances.get(&cred_id).copied().unwrap_or(0.0) - consumed;
                        return PickResult {
                            cred_id,
                            reason: PickReason::StickyHit,
                            effective_remaining,
                            balance_age_secs: balance_age_secs(&snap.entries, cred_id, now_ts),
                            generation,
                            candidate_count,
                        };
                    }
                    // 1b. 临时排除：本次换号，但保留粘滞记录
                    Some(ExclusionKind::Transient) => {
                        let (alt, effective_remaining) =
                            select_by_effective_remaining(&available, &st, &g, &snap, now_ts);
                        return PickResult {
                            cred_id: alt,
                            reason: PickReason::TransientFallback,
                            effective_remaining,
                            balance_age_secs: balance_age_secs(&snap.entries, alt, now_ts),
                            generation,
                            candidate_count,
                        };
                    }
                    // 1c. 长期排除或已不在候选池：迁移
                    _ => {
                        let (alt, effective_remaining) =
                            select_by_effective_remaining(&available, &st, &g, &snap, now_ts);
                        st.sticky.insert(k.clone(), StickyEntry { cred_id: alt, last_seen: now });
                        return PickResult {
                            cred_id: alt,
                            reason: PickReason::StickyMigrated,
                            effective_remaining,
                            balance_age_secs: balance_age_secs(&snap.entries, alt, now_ts),
                            generation,
                            candidate_count,
                        };
                    }
                }
            }
        }

        // 2. 无粘滞或已过期：重新选号
        let (winner, effective_remaining) =
            select_by_effective_remaining(&available, &st, &g, &snap, now_ts);
        if let Some(k) = skey {
            evict_if_full(&mut st.sticky, now);
            st.sticky.insert(k, StickyEntry { cred_id: winner, last_seen: now });
        }
        PickResult {
            cred_id: winner,
            reason: PickReason::FreshSelect,
            effective_remaining,
            balance_age_secs: balance_age_secs(&snap.entries, winner, now_ts),
            generation,
            candidate_count,
        }
    }

    #[cfg(test)]
    pub(crate) fn sticky_len(&self) -> usize {
        self.state.lock().sticky.len()
    }

    /// 读取 `(group, cred_id)` 桶内已累计的消耗。仅测试用，用于验证
    /// 生产路径（`UsageRecordHook::record`）是否真的调用了 `report_consumption`。
    #[cfg(test)]
    pub(crate) fn consumed_of(&self, group: Option<&str>, cred_id: u64) -> f64 {
        let key = (group.unwrap_or("").to_string(), cred_id);
        self.state.lock().consumed.get(&key).copied().unwrap_or(0.0)
    }
}

/// 对齐余额代次：换代即清空本地消耗累计。
///
/// 新一代 `remaining` 已经包含了上一代期间的全部消耗，若不清零会重复扣减。
/// 由 `pick` 与 `report_consumption` 两侧共同调用——哪一侧先遇到新代次都要
/// 完成同步，否则先到的那一侧会被后到的一侧清掉。
///
/// 必须用单调比较（`generation > st.generation`），不能用 `!=`：调用方
/// （`report_consumption`）读取 `balance.generation()` 与拿到 `state` 锁是
/// 两次独立加锁，中间无原子性，读到的代次可能在真正上锁前就已过期。用 `!=`
/// 会在如下交错下把代次**回退**、丢弃已经写入的消耗：
///   1. `state.generation = G`
///   2. 线程 A 进入 `report_consumption`，读到 `balance.generation() = G`
///   3. 缓存发布新代次，`balance.generation` 变为 `G+1`
///   4. 线程 B 进入 `pick`，`sync_generation(st, G+1)` → 清空 consumed，
///      `state.generation = G+1`
///   5. 线程 A 才拿到 state 锁，用第 2 步的过期值调 `sync_generation(st, G)`
///      → 若用 `!=`，`G != G+1` 成立 → 再次清空，并把 `state.generation`
///      **回退为 G**
///   6. 线程 A 写入 `consumed += credits`
///   7. 之后任意一次 `pick`（balance 仍是 G+1）→ `G != G+1` → 再清一次，
///      吞掉第 6 步写入的消耗
/// 用单调比较时，第 5 步 `G > G+1` 为假，不会回退、不会清空，线程 A 的写入
/// 在第 6 步正常落到当前代次上，不会被后续 `pick` 吞掉。
fn sync_generation(st: &mut DispatchState, generation: u64) {
    if generation > st.generation {
        st.generation = generation;
        st.consumed.clear();
    }
}

/// 粘滞 key 前缀带 group：同一 session 经不同 client key 打到不同组时，
/// 不应反复覆写同一条记录来回抖动。
///
/// M1：用裸 `|` 分隔，group 或 seed 含 `|` 时不同的 (group, seed) 组合可能
/// 拼出同一个 key（如 `("a|b", "c")` 与 `("a", "b|c")`）。当前 group 是
/// 运维配置的短名、seed 是 UUID，都不含 `|`，风险不可达，故不引入转义或
/// 长度前缀——没有当前收益不加复杂度。若日后 group/seed 的取值来源变得不
/// 可控，需要重新评估。
fn sticky_key_of(group: Option<&str>, seed: &str) -> String {
    format!("{}|{}", group.unwrap_or(""), seed)
}

/// 剔除本轮不可选者（无论 Transient 还是 Durable）。
///
/// 不剔除的话，平局规则（有效剩余相等按 id 升序）可能把刚被排除的候选又
/// 选回来——两账号余额相同时尤其容易触发。全员本轮不可用则退化为不剔除，
/// 好过返回空集或 panic：调用方至少能拿到一个可重试的候选。
fn filter_excluded(candidates: &[Candidate], excluded: &HashMap<u64, ExclusionKind>) -> Vec<Candidate> {
    if excluded.is_empty() {
        return candidates.to_vec();
    }
    let filtered: Vec<Candidate> = candidates.iter().copied().filter(|c| !excluded.contains_key(&c.id)).collect();
    if filtered.is_empty() { candidates.to_vec() } else { filtered }
}

/// 返回胜出候选的 id 与其有效剩余额度（供 [`PickResult`] 归因字段回填）。
fn select_by_effective_remaining(
    candidates: &[Candidate],
    st: &DispatchState,
    g: &str,
    snap: &crate::admin::balance_cache::BalanceSnapshotView,
    now_ts: f64,
) -> (u64, f64) {
    let balances = resolve_balances(candidates, &snap.entries, now_ts);
    candidates
        .iter()
        .map(|c| {
            let consumed = st.consumed.get(&(g.to_string(), c.id)).copied().unwrap_or(0.0);
            (c, balances[&c.id] - consumed)
        })
        .max_by(|(ca, ea), (cb, eb)| {
            // 有效剩余降序 → priority 升序 → id 升序，保证可复算。
            //
            // `Iterator::max_by` 在比较器返回 Equal 时保留**后遇到的**元素
            // （"取最后一个最大值"）；本闭包对 priority/id 特意反过来比较
            // （`cb` 减 `ca`）正是利用这个语义：当 ea == eb 时，只有
            // priority 更小（或 id 更小）的候选让整体比较结果为
            // Greater，才会被 max_by 判定为"更大"从而在平局中胜出。
            // 这个方向依赖 max_by 的"后者优先"这一不那么广为人知的规则——
            // 若日后有人改成看似等价的 `sort_by` + 取首元素，排序是稳定的
            // （相等元素保持原相对顺序，取最小/第一个），平局方向会静默
            // 反转，务必保持用 max_by 或显式反转比较逻辑。
            ea.partial_cmp(eb)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| cb.priority.cmp(&ca.priority))
                .then_with(|| cb.id.cmp(&ca.id))
        })
        .map(|(c, eff)| (c.id, eff))
        .expect("candidates 非空")
}

/// 某账号余额快照的陈旧秒数（`now_ts - cached_at`）。条目缺失（余额从未
/// 采集到过该账号）取 `f64::INFINITY`，与「已知但极旧」区分开。
fn balance_age_secs(
    entries: &HashMap<u64, crate::admin::balance_cache::CachedBalance>,
    id: u64,
    now_ts: f64,
) -> f64 {
    entries.get(&id).map(|e| now_ts - e.cached_at).unwrap_or(f64::INFINITY)
}

/// 到量时先清全部过期条目，仍满则 O(n) 淘汰 last_seen 最早的一条。
/// 不引入 lru 依赖：正常负载下 30min 过期会让表远小于上限，O(n) 基本不触发。
fn evict_if_full(sticky: &mut HashMap<String, StickyEntry>, now: Instant) {
    if sticky.len() < STICKY_CAPACITY {
        return;
    }
    sticky.retain(|_, e| now.duration_since(e.last_seen).as_secs() <= STICKY_IDLE_SECS);
    while sticky.len() >= STICKY_CAPACITY {
        let Some(oldest) = sticky
            .iter()
            .min_by_key(|(_, e)| e.last_seen)
            .map(|(k, _)| k.clone())
        else {
            break;
        };
        sticky.remove(&oldest);
    }
}

/// 解析每个候选的「可用于调度的余额」，返回 id → 余额 的完整映射（候选必全覆盖）。
///
/// 三态判定（规格 §4.4）：fresh（`< BALANCE_CACHE_TTL_SECS`）与 stale
/// （`< MAX_STALE_SECS`）取值相同——都是 `remaining` 原值，二者的区别只在
/// 可观测性，不影响选号——因此实现里合并为单一 `MAX_STALE_SECS` 判定，不
/// 单独引用 `BALANCE_CACHE_TTL_SECS`：
/// - 可用（fresh ∪ stale）：`now_ts - cached_at < MAX_STALE_SECS` → 用 `remaining` 原值
/// - unavailable：超过 MAX_STALE_SECS / 条目缺失 / `remaining` 非有限 /
///   `next_reset_at` 为 `Some(t)` 且 `now_ts >= t`（跨月重置后旧值必然失效，
///   此条**优先于** TTL 判定）
///   → 取组内**已知**值的中位数
///
/// `next_reset_at` 为 `None`（上游未回报重置时间）时**不**据此判失效，
/// 仅按 TTL / MAX_STALE 判定——没有信息不等于信息为「已过期」。
///
/// 边界：
/// - 组内全部 unavailable 时中位数无定义，一律取 0.0。此时
///   `argmax(0 - consumed) = argmin(consumed)`，自动退化为最少消耗轮转。
/// - `remaining` 允许为负（开启超额后 `usageLimit - currentUsage < 0`，
///   见 src/admin/service.rs 的注释）。负值不做 clamp——超额账号排最后是
///   正确行为，且组内全负时仍会选出最不负者。
/// - 偶数个已知值时中位数的取法由实现决定（取中间两者均值或偏低者均可，
///   但必须确定性）。
fn resolve_balances(
    candidates: &[Candidate],
    entries: &HashMap<u64, crate::admin::balance_cache::CachedBalance>,
    now_ts: f64,
) -> HashMap<u64, f64> {
    // 第一遍：只收集判定为可用的余额。fresh 与 stale 取值相同（都是 remaining
    // 原值），二者的区别只在可观测性，不影响选号，故此处只需区分「可用 / 不可用」。
    let known: HashMap<u64, f64> = candidates
        .iter()
        .filter_map(|c| entries.get(&c.id).and_then(|e| usable_remaining(e, now_ts).map(|v| (c.id, v))))
        .collect();

    // 组内全部不可用时中位数无定义，取 0.0：此时所有候选同值，
    // argmax(0 − consumed) 退化为 argmin(consumed)，即按已消耗量轮转。
    let fallback = median_or_zero(known.values().copied());

    candidates
        .iter()
        .map(|c| (c.id, known.get(&c.id).copied().unwrap_or(fallback)))
        .collect()
}

/// 单个缓存条目是否可用于调度。`None` 表示 unavailable。
fn usable_remaining(
    e: &crate::admin::balance_cache::CachedBalance,
    now_ts: f64,
) -> Option<f64> {
    // 跨重置判定优先于 TTL：重置时刻已过，旧快照必然失效——它既可能把刚重置的
    // 账号当成 0（永不被选），也可能把已耗尽的账号当成高余额（持续被选）。
    if let Some(reset_at) = e.data.next_reset_at
        && now_ts >= reset_at
    {
        return None;
    }
    // next_reset_at 为 None 时不据此判失效：没有信息不等于信息为「已过期」。

    // 陈旧期：注意 cached_at 可能因时钟回拨而落在未来，此时 age 为负，
    // 视作 fresh 而非 unavailable——宁可用一个偏新的值，也不要凭空丢弃数据。
    if now_ts - e.cached_at >= MAX_STALE_SECS {
        return None;
    }

    let remaining = e.data.remaining;
    if !remaining.is_finite() {
        return None;
    }
    // 不对负值做 clamp：开启超额后 remaining = usageLimit − currentUsage 可为负
    // （见 src/admin/service.rs 的注释）。超额账号排在最后是正确行为，且组内
    // 全负时仍会选出最不负者，不会返回空集。
    Some(remaining)
}

/// 中位数；空集取 0.0。
///
/// 偶数个样本取**偏低**的那个中间值，而非两者均值。理由：这个值会被赋给
/// 余额未知的账号，取偏低者使未知账号在与已知账号平局时不占优，是保守的
/// 一侧；同时避免引入一个没有任何账号真实持有的数值，便于事后复算「为什么
/// 选了它」。两种取法都确定性，此处选保守的一侧。
fn median_or_zero(values: impl Iterator<Item = f64>) -> f64 {
    let mut v: Vec<f64> = values.collect();
    if v.is_empty() {
        return 0.0;
    }
    // 已过滤非有限值，partial_cmp 不会返回 None；兜底为 Equal 保持全序。
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[(v.len() - 1) / 2]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::balance_cache::{BalanceCache, CachedBalance};
    use std::collections::HashMap;

    fn now_ts() -> f64 { chrono::Utc::now().timestamp() as f64 }

    fn disp(balances: &[(u64, f64)]) -> GroupDispatcher {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let mut m = HashMap::new();
        for (id, remaining) in balances {
            m.insert(*id, CachedBalance {
                cached_at: now_ts(),
                data: BalanceResponse { remaining: *remaining, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() },
            });
        }
        cache.publish(m);
        GroupDispatcher::new(cache)
    }

    fn cands(ids: &[u64]) -> Vec<Candidate> {
        ids.iter().enumerate()
            .map(|(i, id)| Candidate { id: *id, priority: i as i32 })
            .collect()
    }

    fn pick(d: &GroupDispatcher, c: &[Candidate]) -> u64 {
        d.pick(None, c, &HashMap::new(), None, Instant::now()).cred_id
    }

    #[test]
    fn picks_highest_effective_remaining() {
        let d = disp(&[(1, 7000.0), (2, 3000.0)]);
        let c = cands(&[1, 2]);
        assert_eq!(pick(&d, &c), 1);

        // 给 1 回写 5000 消耗后，有效剩余 2000 < 3000，应改选 2
        d.report_consumption(None, 1, 5000.0);
        assert_eq!(pick(&d, &c), 2);
    }

    #[test]
    fn rotates_while_balance_frozen() {
        // 余额固定不变（模拟 300s 缓存冻结），靠本地消耗驱动轮转
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let id = pick(&d, &c);
            seen.insert(id);
            d.report_consumption(None, id, 100.0);
        }
        assert_eq!(seen.len(), 2, "余额冻结期内必须仍然轮转，实际只用了 {:?}", seen);
    }

    #[test]
    fn unknown_balance_falls_back_to_median() {
        // 已知 1000/2000/9000，中位数 2000；id=4 无余额数据
        let d = disp(&[(1, 1000.0), (2, 2000.0), (3, 9000.0)]);
        let c = cands(&[1, 2, 3, 4]);

        // 直接断言取值，而不是透过 pick 的胜负间接推断：奇数个已知值时中位数
        // 必然等于其中某个候选的值（这里 2000 == id2），透过 pick 断言会撞上
        // 结构性平局，测不出中位数本身对不对。
        let snap = d.balance.snapshot();
        let resolved = resolve_balances(&c, &snap.entries, chrono::Utc::now().timestamp() as f64);
        assert_eq!(resolved[&1], 1000.0);
        assert_eq!(resolved[&2], 2000.0);
        assert_eq!(resolved[&3], 9000.0);
        assert_eq!(
            resolved[&4], 2000.0,
            "缺失者应取中位数：取 0 会形成「不被选中→无余额数据→权重恒 0」的死锁，取 max 会让新账号独吞流量"
        );

        // 行为侧：最大者先胜出
        assert_eq!(pick(&d, &c), 3, "最大者仍应胜出");
        // 把两个已知者都压到中位数之下，缺失者应当严格胜出（避免平局歧义）
        d.report_consumption(None, 3, 8000.0); // 9000 → 1000
        d.report_consumption(None, 2, 1500.0); // 2000 → 500
        assert_eq!(pick(&d, &c), 4, "缺失者以中位数 2000 参与竞争，此时严格最高");
    }

    #[test]
    fn all_unknown_degrades_to_least_consumed() {
        let d = disp(&[]);                       // 全组无余额数据
        let c = cands(&[1, 2, 3]);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let id = pick(&d, &c);
            seen.insert(id);
            d.report_consumption(None, id, 1.0);
        }
        assert_eq!(seen.len(), 3, "全部未知时应退化为最少消耗轮转，各命中一次");
    }

    #[test]
    fn negative_balance_sorts_last_but_not_starved_when_all_negative() {
        let d = disp(&[(1, -100.0), (2, -50.0)]);
        let c = cands(&[1, 2]);
        assert_eq!(pick(&d, &c), 2, "组内全负时仍应选出最不负者，不得 panic 或返回空");
    }

    #[test]
    fn stale_beyond_max_stale_is_unavailable() {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let old = now_ts() - 7200.0;             // 2 小时前 > MAX_STALE(3600)
        cache.publish(HashMap::from([
            (1, CachedBalance { cached_at: old, data: BalanceResponse { remaining: 9000.0, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() } }),
            (2, CachedBalance { cached_at: now_ts(), data: BalanceResponse { remaining: 100.0, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() } }),
        ]));
        let d = GroupDispatcher::new(cache);
        // id1 超期 → unavailable → 取已知中位数（只有 id2 已知，中位数 = 100）
        // 两者有效剩余相等，平局按 priority 升序 → id1 (priority 0)
        assert_eq!(pick(&d, &cands(&[1, 2])), 1);
    }

    #[test]
    fn past_next_reset_invalidates_snapshot() {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        cache.publish(HashMap::from([
            // cached_at 很新，但 next_reset_at 已过 → 必须判为 unavailable
            (1, CachedBalance { cached_at: now_ts(), data: BalanceResponse { remaining: 0.0, next_reset_at: Some(now_ts() - 10.0), ..Default::default() } }),
            (2, CachedBalance { cached_at: now_ts(), data: BalanceResponse { remaining: 500.0, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() } }),
        ]));
        let d = GroupDispatcher::new(cache);
        // id1 的 0 若被沿用会永不被选；正确行为是取中位数 500，与 id2 平局，按 priority 选 id1
        assert_eq!(pick(&d, &cands(&[1, 2])), 1);
    }

    #[test]
    fn generation_change_clears_consumed() {
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let mk = |r: f64| CachedBalance {
            cached_at: now_ts(),
            data: BalanceResponse { remaining: r, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() },
        };
        cache.publish(HashMap::from([(1, mk(5000.0)), (2, mk(5000.0))]));
        let d = GroupDispatcher::new(cache.clone());
        let c = cands(&[1, 2]);
        d.report_consumption(None, 1, 4000.0);
        assert_eq!(pick(&d, &c), 2);

        // 新一轮余额发布：本地消耗必须清零，否则会重复扣减
        cache.publish(HashMap::from([(1, mk(1000.0)), (2, mk(5000.0))]));
        d.report_consumption(None, 2, 4500.0);   // 2 降到 500
        assert_eq!(pick(&d, &c), 1, "generation 切换后旧消耗不得残留");
    }

    #[test]
    fn report_consumption_survives_stale_generation_race() {
        // 复现 C1 竞态：report_consumption 读 balance.generation() 与拿到
        // state 锁是两次独立加锁，中间可能被另一线程的 pick 抢先推进代次。
        // 若 sync_generation 用 `!=` 判断，携带过期代次姗姗来迟的一方会把
        // state.generation **回退**、连带清空刚写入的 consumed；下一次 pick
        // 见到 state 代次又落后于 balance，再清一次，静默丢失消耗。
        //
        // 不需要真实多线程也能确定性复现：手工按交错顺序调用 pick 与私有的
        // sync_generation（同模块内可见），模拟"线程 A 携带过期代次姗姗来迟
        // 才拿到 state 锁"这一步，避免真多线程测试的计时不确定性。
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let mk = |r: f64| CachedBalance {
            cached_at: now_ts(),
            data: BalanceResponse { remaining: r, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() },
        };
        cache.publish(HashMap::from([(1, mk(5000.0)), (2, mk(5000.0))])); // generation = 1
        let d = GroupDispatcher::new(cache.clone());
        let c = cands(&[1, 2]);

        // 先跑一次 pick，把 state.generation 同步到当前代次 1——对应线程 A
        // 在缓存推进之前读到的旧代次。
        let _ = pick(&d, &c);
        let stale_generation = cache.generation(); // == 1，线程 A 捕获的过期值

        // 缓存推进到新代次 2；线程 B 的 pick 先一步跑到，把 state 同步到 2
        // 并清空 consumed。
        cache.publish(HashMap::from([(1, mk(5000.0)), (2, mk(5000.0))])); // generation = 2
        let _ = pick(&d, &c);
        assert_eq!(d.state.lock().generation, 2, "前置条件：state 应已同步到最新代次");

        // 线程 A 此刻才拿到 state 锁：先用过期代次调用 sync_generation
        // （对应 report_consumption 内部先行调用），再写入消耗——完整重放
        // report_consumption 的函数体，只是把两次加锁之间人为插入的推进
        // 显式摆在中间。
        {
            let mut st = d.state.lock();
            sync_generation(&mut st, stale_generation);
            st.consumed.insert(("".to_string(), 1), 4000.0);
        }

        // 断言落到具体胜负而非"不 panic"：若代次被错误回退，下一次 pick 会
        // 先把 consumed 清空（因为 state.generation 又落后于当前 balance
        // 代次 2），1 的有效剩余仍是满额 5000，argmax 会错误选回 1。修复后
        // 代次不会回退，1 的有效剩余应为 5000 - 4000 = 1000 < 5000，胜者应为 2。
        assert_eq!(pick(&d, &c), 2, "过期代次的 report_consumption 写入不应被后续 pick 清空");
    }

    #[test]
    fn pick_does_not_regress_generation_when_state_already_advanced() {
        // 复现 C1：`pick` 自己的 `self.balance.snapshot()` 读到的代次，可能
        // 落后于此刻的 `state.generation`——现实中这来自
        // `report_consumption` 读 `balance.generation()` 与拿到 `state` 锁
        // 之间的非原子窗口，被另一个线程的 `pick` 抢先推进并写入了消耗。
        //
        // 与 `report_consumption_survives_stale_generation_race` 的区别：
        // 那条测试在最后一次断言前手工调用私有的 `sync_generation`，绕过了
        // `pick` 自己读取 snapshot 这一步；到它最后调 `pick()` 断言时，
        // `state.generation` 早已等于当前 `snap.generation`，`!=` 与 `>`
        // 结果一致，测不出 `pick` 内部若用 `!=` 会不会自己踩坑。这条测试
        // 直接摆好「state.generation 领先于 pick 即将读到的 balance 代次」
        // 这个前提，让 `pick()` 走一遍它自己真实的
        // `snapshot → lock → 判代次` 路径。
        let cache = std::sync::Arc::new(BalanceCache::new(None));
        let mk = |r: f64| CachedBalance {
            cached_at: now_ts(),
            data: BalanceResponse { remaining: r, next_reset_at: Some(now_ts() + 86400.0), ..Default::default() },
        };
        cache.publish(HashMap::from([(1, mk(5000.0)), (2, mk(5000.0))])); // 真实代次 = 1
        let d = GroupDispatcher::new(cache.clone());
        let c = cands(&[1, 2]);

        // 手工把 state.generation 摆到领先于当前 balance 真实代次（1）的
        // 位置，并写入一条已经在这个新代次下记的消耗——代表另一线程已经
        // 越过 pick 把 state 推到了这个代次。真实并发下这一步由另一线程的
        // pick/report_consumption 完成；这里直接摆结果，聚焦测 pick 自己
        // 遇到这个局面时的行为。
        {
            let mut st = d.state.lock();
            st.generation = 99;
            st.consumed.insert(("".to_string(), 1), 4000.0);
        }

        // pick() 内部调用 self.balance.snapshot()，读到真实代次 1——低于
        // state.generation(99)。若用 `!=`，会误判"代次变了"，把
        // state.generation 回退到 1 并清空刚写入的 consumed：1 的有效剩余
        // 被错误地重新算成满额 5000，胜者会错误地变回 1。用单调的 `>` 则
        // 不做任何清空：1 的有效剩余仍是 5000 - 4000 = 1000，胜者应为 2。
        assert_eq!(
            pick(&d, &c),
            2,
            "state.generation 领先于 pick 本次读到的 balance 代次时，不得回退清空 consumed"
        );
    }

    #[test]
    fn all_candidates_excluded_degrades_to_a_valid_id_not_panic() {
        // I1 契约：候选全部被排除时降级放行——不 panic，但返回值可能仍属于
        // excluded 集合。这里只固定"不 panic 且返回合法 id"这一行为，调用方
        // 若据此重试须自行设置重试上限（见 `pick` 文档注释）。
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let ex = HashMap::from([(1, ExclusionKind::Durable), (2, ExclusionKind::Transient)]);
        let r = d.pick(None, &c, &ex, None, Instant::now());
        assert!(c.iter().any(|cand| cand.id == r.cred_id), "全员排除时仍应返回候选池内的合法 id");
    }

    #[test]
    fn ties_break_by_priority_then_id() {
        let d = disp(&[(7, 100.0), (3, 100.0)]);
        // 两者有效剩余相同；priority 由 cands() 按顺序赋 0,1
        let c = vec![
            Candidate { id: 7, priority: 5 },
            Candidate { id: 3, priority: 1 },
        ];
        assert_eq!(pick(&d, &c), 3, "平局应按 priority 升序");

        let c2 = vec![
            Candidate { id: 7, priority: 1 },
            Candidate { id: 3, priority: 1 },
        ];
        assert_eq!(pick(&d, &c2), 3, "priority 也相同则按 id 升序");
    }

    fn pick_sticky(d: &GroupDispatcher, c: &[Candidate], key: &str, now: Instant) -> PickResult {
        d.pick(None, c, &HashMap::new(), Some(key), now)
    }

    #[test]
    fn sticky_key_returns_same_credential() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let first = pick_sticky(&d, &c, "sess-a", now).cred_id;
        for _ in 0..5 {
            let r = pick_sticky(&d, &c, "sess-a", now);
            assert_eq!(r.cred_id, first);
            assert_eq!(r.reason, PickReason::StickyHit);
        }
    }

    #[test]
    fn transient_exclusion_does_not_migrate_sticky() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let pinned = pick_sticky(&d, &c, "sess-a", now).cred_id;
        let other = if pinned == 1 { 2 } else { 1 };

        // 目标被临时排除（队满/RPM 竞争）：本次换号但不得改写粘滞
        let ex = HashMap::from([(pinned, ExclusionKind::Transient)]);
        let r = d.pick(None, &c, &ex, Some("sess-a"), now);
        assert_eq!(r.cred_id, other);
        assert_eq!(r.reason, PickReason::TransientFallback);

        // 排除解除后必须回到原号
        let back = pick_sticky(&d, &c, "sess-a", now);
        assert_eq!(back.cred_id, pinned, "临时拥塞不应永久迁移会话");
        assert_eq!(back.reason, PickReason::StickyHit);
    }

    #[test]
    fn durable_exclusion_migrates_sticky() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let pinned = pick_sticky(&d, &c, "sess-a", now).cred_id;
        let other = if pinned == 1 { 2 } else { 1 };

        let ex = HashMap::from([(pinned, ExclusionKind::Durable)]);
        let r = d.pick(None, &c, &ex, Some("sess-a"), now);
        assert_eq!(r.cred_id, other);
        assert_eq!(r.reason, PickReason::StickyMigrated);

        // 已迁移：即使原号恢复，也应留在新号上
        let after = pick_sticky(&d, &c, "sess-a", now);
        assert_eq!(after.cred_id, other);
    }

    #[test]
    fn sticky_expires_after_idle() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let t0 = Instant::now();
        let first = pick_sticky(&d, &c, "sess-a", t0).cred_id;
        // 把 first 的消耗打高，过期后重选必然换号
        d.report_consumption(None, first, 4000.0);

        let t1 = t0 + std::time::Duration::from_secs(STICKY_IDLE_SECS + 60);
        let r = pick_sticky(&d, &c, "sess-a", t1);
        assert_eq!(r.reason, PickReason::FreshSelect, "超过空闲期应重新分配");
        assert_ne!(r.cred_id, first);
    }

    #[test]
    fn sticky_table_is_capacity_bounded() {
        let d = disp(&[(1, 5000.0)]);
        let c = cands(&[1]);
        let now = Instant::now();
        for i in 0..(STICKY_CAPACITY + 100) {
            pick_sticky(&d, &c, &format!("sess-{i}"), now);
        }
        assert!(d.sticky_len() <= STICKY_CAPACITY, "粘滞表必须有界");
    }

    #[test]
    fn groups_have_isolated_consumption() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        d.report_consumption(Some("A"), 1, 4900.0);
        // B 组不受 A 组消耗影响：1 在 B 组仍是满额，与 2 平局按 priority 选 1
        assert_eq!(d.pick(Some("B"), &c, &HashMap::new(), None, Instant::now()).cred_id, 1);
        // A 组里 1 已被打低，应选 2
        assert_eq!(d.pick(Some("A"), &c, &HashMap::new(), None, Instant::now()).cred_id, 2);
    }

    #[test]
    fn distinct_sessions_do_not_share_entry() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let a = pick_sticky(&d, &c, "sess-a", now).cred_id;
        d.report_consumption(None, a, 4900.0);
        // 不同 session 必须独立分配，不得继承 sess-a 的粘滞
        let b = pick_sticky(&d, &c, "sess-b", now);
        assert_eq!(b.reason, PickReason::FreshSelect);
        assert_ne!(b.cred_id, a);
    }

    #[test]
    fn none_sticky_key_writes_nothing() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        d.pick(None, &c, &HashMap::new(), None, Instant::now());
        assert_eq!(d.sticky_len(), 0, "sticky_key=None 不得写表");
    }

    #[test]
    fn report_consumption_ignores_non_positive_and_non_finite() {
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        // 这些都不应改变选择结果（平局按 priority 选 1）
        d.report_consumption(None, 1, 0.0);
        d.report_consumption(None, 1, -100.0);
        d.report_consumption(None, 1, f64::NAN);
        d.report_consumption(None, 1, f64::INFINITY);
        assert_eq!(pick(&d, &c), 1);

        // 正常值才生效
        d.report_consumption(None, 1, 4900.0);
        assert_eq!(pick(&d, &c), 2);
    }

    #[test]
    fn sticky_hit_consumption_still_counted() {
        // 长会话粘在一个号上，其消耗必须照样计入，否则控制不了额度倾斜
        let d = disp(&[(1, 5000.0), (2, 5000.0)]);
        let c = cands(&[1, 2]);
        let now = Instant::now();
        let pinned = pick_sticky(&d, &c, "long", now).cred_id;
        for _ in 0..50 {
            let r = pick_sticky(&d, &c, "long", now);
            assert_eq!(r.cred_id, pinned);
            d.report_consumption(None, r.cred_id, 100.0);
        }
        // 新会话不应再流向被打爆的账号
        let fresh = pick_sticky(&d, &c, "new", now);
        assert_ne!(fresh.cred_id, pinned, "长会话的消耗必须影响新会话的分配");
    }
}

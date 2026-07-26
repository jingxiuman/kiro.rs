//! 上游模型同步。
//!
//! 核心安全规则：**一轮同步只有在「至少一个凭据成功返回非空模型列表」时
//! 才算可信；不可信轮次不写文件、不递增 missingSyncRounds。** 网络抖动、
//! 凭据集体过期、上游 5xx 都会让返回变空；若空列表被当成「上游啥都没了」，
//! 一次抖动就会把全表刷成 deprecated。
//!
//! 第二条规则：**只有探针凭据成功的「权威轮次」能判定消失。** 上游模型集
//! 随订阅等级不同（kiro/model/available_models.rs:6），采样到低等级凭据的
//! 轮次会把高等级独有模型误判为消失。

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use super::model_registry::{
    MatchKind, ModelOrigin, ModelRegistry, ModelRow, ModelStatus, SyncMeta,
};
use super::model_registry_store::{merge_synced_row, ModelRegistryStore};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 权威轮次连续多少轮未见才标 deprecated。
pub const MISSING_ROUNDS_THRESHOLD: u32 = 2;
/// 非权威轮次的采样凭据数。不遍历全部：凭据可达上百（支持批量导入），
/// 每轮遍历即上百次上游请求。
pub const SAMPLE_SIZE: usize = 3;
/// 无效 maxInputTokens 的回退值。
pub const FALLBACK_CONTEXT_WINDOW: i32 = 200_000;

/// 上游返回的单个模型。对应 kiro/model/available_models.rs 的 UpstreamModel
/// （经 token_manager.rs 里的 ModelListFetcher 实现转换而来）。
#[derive(Debug, Clone)]
pub struct UpstreamModel {
    pub model_id: String,
    pub model_name: Option<String>,
    pub max_input_tokens: Option<i64>,
}

/// 拉取上游模型列表。**必须是 trait** ——
/// 现有 get_available_models_for（token_manager.rs:2705）内部直接刷 token 并
/// 发网络请求，无法在单测中替换。
pub trait ModelListFetcher: Send + Sync {
    fn fetch(&self, credential_id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>>;
    /// 可用于同步的启用凭据 id，升序。
    fn candidate_credential_ids(&self) -> Vec<u64>;
    fn is_credential_usable(&self, credential_id: u64) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundKind {
    /// 探针凭据成功。可判定模型消失。
    Authoritative,
    /// 采样并集。只做新增与更新。
    Advisory,
}

#[derive(Debug, Clone)]
pub struct SyncSummary {
    pub round: RoundKind,
    pub added: usize,
    pub updated: usize,
    pub deprecated: usize,
    pub trusted: bool,
    pub source: String,
}

pub struct ModelSyncService {
    store: Arc<ModelRegistryStore>,
    fetcher: Arc<dyn ModelListFetcher>,
}

/// 按 provider 前缀派生对外名。
/// claude-* 点号转连字符；其他（含 gpt-5*）**原样保留** ——
/// handlers.rs 里刻意暴露带点号的 gpt-5.6-sol。
pub(crate) fn derive_exposed_id(upstream_id: &str) -> String {
    if upstream_id.starts_with("claude-") {
        upstream_id.replace('.', "-")
    } else {
        upstream_id.to_string()
    }
}

pub(crate) fn derive_thinking_variant(upstream_id: &str) -> bool {
    upstream_id.starts_with("claude-")
}

/// maxInputTokens: Option<i64> → i32。无效值回退 200000 并 warn。
fn sanitize_window(upstream_id: &str, raw: Option<i64>) -> i32 {
    match raw {
        Some(v) if v > 0 && v <= i64::from(i32::MAX) => v as i32,
        other => {
            tracing::warn!(
                "上游模型 {} 的 maxInputTokens 无效({:?})，回退 {}",
                upstream_id,
                other,
                FALLBACK_CONTEXT_WINDOW
            );
            FALLBACK_CONTEXT_WINDOW
        }
    }
}

/// 一次拉取的结果（可能是权威探针的单凭据拉取，也可能是采样并集）。
struct FetchOutcome {
    /// 按 upstream_id 去重后的并集。
    union: BTreeMap<String, UpstreamModel>,
    /// 凭据 id → 该凭据本轮返回的 upstream_id 列表。
    /// **注意（C1）**：只收录非空结果，见 `ModelSyncService::fetch_from` 内的注释。
    per_credential: HashMap<String, Vec<String>>,
    /// 是否至少一个凭据返回非空列表（可信度判定用）。
    any_nonempty: bool,
    /// 是否至少一个凭据拉取失败（用于探针失败后的降级判定，I3）。
    any_failed: bool,
}

impl ModelSyncService {
    pub fn new(store: Arc<ModelRegistryStore>, fetcher: Arc<dyn ModelListFetcher>) -> Self {
        Self { store, fetcher }
    }

    /// 按给定凭据 id 列表拉取并做并集。按凭据 id 升序调用（由调用方保证），
    /// 以保证并集冲突解决的确定性。
    async fn fetch_from(&self, credential_ids: &[u64]) -> FetchOutcome {
        let mut union: BTreeMap<String, UpstreamModel> = BTreeMap::new();
        let mut per_credential: HashMap<String, Vec<String>> = HashMap::new();
        let mut any_nonempty = false;
        let mut any_failed = false;

        for id in credential_ids {
            match self.fetcher.fetch(*id).await {
                Ok(models) => {
                    if !models.is_empty() {
                        any_nonempty = true;
                        // C1 修复：credential_support 的消费语义是「无记录 = 未知，放行；
                        // 有记录 = 强断言只支持记录内的模型」。空列表已经被下面的
                        // 可信度判定视为「不可信信号」（见 any_nonempty），若还把它当成
                        // 强断言写盘，就等于一边说「这次数据不可信」一边又把它当权威结论
                        // 持久化——自相矛盾。且一旦写入空记录，调度层会把该凭据对**所有**
                        // 模型都判为不支持，一次 token 抖动就能把凭据永久踢出轮换。
                        // 因此只在非空时才记录，空结果直接跳过、保留旧记录（未知语义）。
                        per_credential.insert(
                            id.to_string(),
                            models.iter().map(|m| m.model_id.clone()).collect(),
                        );
                    }
                    for m in models {
                        match union.get_mut(&m.model_id) {
                            Some(existing) => {
                                // 窗口取 max
                                let a = existing.max_input_tokens.unwrap_or(0);
                                let b = m.max_input_tokens.unwrap_or(0);
                                if b > a {
                                    existing.max_input_tokens = m.max_input_tokens;
                                }
                                // 名称按凭据 id 升序取首个非空 → 已有非空则不覆盖
                                if existing.model_name.as_deref().unwrap_or("").is_empty() {
                                    existing.model_name = m.model_name.clone();
                                }
                            }
                            None => {
                                union.insert(m.model_id.clone(), m);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("凭据 {} 拉取上游模型失败: {}，本轮跳过该凭据", id, e);
                    any_failed = true;
                }
            }
        }

        FetchOutcome { union, per_credential, any_nonempty, any_failed }
    }

    /// 跑一轮同步。`now` 由调用方注入，便于测试。
    pub async fn sync_once(
        &self,
        probe_credential_id: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<SyncSummary, String> {
        let fetch_started_at = now.to_rfc3339();

        // ---- 选凭据 ----
        let (mut round, mut credential_ids) = match probe_credential_id {
            Some(id) if self.fetcher.is_credential_usable(id) => {
                (RoundKind::Authoritative, vec![id])
            }
            _ => {
                let mut ids = self.fetcher.candidate_credential_ids();
                ids.sort_unstable();
                ids.truncate(SAMPLE_SIZE);
                (RoundKind::Advisory, ids)
            }
        };
        if credential_ids.is_empty() {
            return Err("没有可用于同步的凭据".to_string());
        }

        // ---- 拉取 + 并集（按凭据 id 升序，保证冲突解决确定性）----
        let mut outcome = self.fetch_from(&credential_ids).await;

        // I3 修复：探针（权威轮次唯一凭据）拉取失败时，此前整轮直接返回 Err —— 探针的
        // refresh token 一旦失效，每轮都会静默地进权威分支再失败，新模型永远进不来，
        // 且只有 warn 没有更醒目的信号。这里改为回退一次采样轮（advisory，只做新增/
        // 更新，绝不判消失），并在 source 里体现「probe_failed」，同时打一条更明确的
        // warn 提示探针不可用，便于运维介入。
        let mut probe_failed_fallback = false;
        if round == RoundKind::Authoritative && outcome.any_failed {
            tracing::warn!(
                "探针凭据 {:?} 拉取上游模型失败，探针可能不可用（token 刷新失败/被禁用/网络问题），\
                 回退为采样轮次（advisory，仅新增/更新，不判定消失），请检查探针凭据状态",
                credential_ids
            );
            // N4：把刚失败的探针从降级候选里剔除。它本轮已经失败过一次，重复选它
            // 既白占一个 SAMPLE_SIZE 名额（挤掉一个真能返回数据的凭据），又多发一次
            // 注定失败的上游请求。探针凭据本身仍在 candidate_credential_ids 里，
            // 所以必须显式排除。
            let failed_probe_ids = credential_ids.clone();
            let mut ids = self.fetcher.candidate_credential_ids();
            ids.retain(|id| !failed_probe_ids.contains(id));
            ids.sort_unstable();
            ids.truncate(SAMPLE_SIZE);
            round = RoundKind::Advisory;
            credential_ids = ids;
            probe_failed_fallback = true;
            if credential_ids.is_empty() {
                return Err("探针不可用，且回退采样后仍没有可用于同步的凭据".to_string());
            }
            outcome = self.fetch_from(&credential_ids).await;
        }

        let FetchOutcome { union, mut per_credential, any_nonempty, .. } = outcome;

        // ---- 可信度判定 ----
        if !any_nonempty {
            return Err(format!(
                "本轮同步不可信（{} 个凭据均失败或返回空列表），不改动 models.json",
                credential_ids.len()
            ));
        }

        let source = match round {
            RoundKind::Authoritative => format!("probe:{}", credential_ids[0]),
            RoundKind::Advisory if probe_failed_fallback => format!(
                "probe_failed_sample:{}",
                credential_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
            ),
            RoundKind::Advisory => format!(
                "sample:{}",
                credential_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
            ),
        };

        // ---- diff 并写入 ----
        let mut added = 0usize;
        let mut updated = 0usize;
        let mut deprecated = 0usize;
        let seen_at = now.to_rfc3339();

        let file = self
            .store
            .mutate(|file| {
                // MN1：不变量维护——`origin=builtin` 的行本来就不该出现在覆盖层里
                // （人工编辑产生 manual，同步产生 synced；builtin 只属于代码里的
                // builtin_rows()）。这不是一次性迁移逻辑，而是每次写盘都维持的不变量。
                // 必要性：跑过中间版本的开发环境，models.json 里躺着内置行的整行快照，
                // N1 修掉的「冻结」bug 在这些文件上会继续生效——覆盖层里的快照会把代码
                // 里的新定义挡掉。清掉它们即可让这些行回落到内置默认。
                file.models.retain(|r| r.origin != ModelOrigin::Builtin);

                // 乱序保护：已有更新的结果落盘则丢弃本轮。
                //
                // I2 修复：此前用 RFC3339 字符串字典序比较（`last > fetch_started_at.as_str()`），
                // 但字符串字典序不等价于时间先后——带时区偏移的 RFC3339 串比大小完全可能
                // 与真实时刻相反：
                //   - 负偏移会漏挡：`...T00:00:00-05:00`（真实 05:00Z）在字符串上小于
                //     `...T04:00:00Z`（更新的一轮），导致更旧的记录反而覆盖新观测。
                //   - 正偏移会误挡：`...T23:00:00+08:00`（真实 15:00Z）在字符串上大于
                //     `...T16:00:00Z`（合法的新一轮），导致同步在时区偏移的小时数内停摆。
                // 本服务自己写入的 lastSyncAt 恒为 UTC（不会触发上述问题），但 models.json
                // 是人可编辑的配置文件，手写 `...+08:00` 这类偏移就会踩中。
                // 修法：解析成 DateTime 后比较真实时刻；解析失败按“无记录”放行——
                // 一条手写坏格式的时间戳不该永久卡死同步。
                if let Some(last) = file.sync_state.last_sync_at.as_deref() {
                    if let Ok(last_dt) = DateTime::parse_from_rfc3339(last) {
                        if last_dt > now {
                            return Err(format!(
                                "已有更新的同步结果（{}）晚于本轮起始时间（{}），丢弃本轮",
                                last, fetch_started_at
                            ));
                        }
                    }
                }

                // I1+M1 修复：有效行表 = 内置默认 ∪ 覆盖层（ModelRegistry::from_file 的叠加
                // 逻辑），而非仅 file.models（覆盖层）。此前只拿 file.models 当基线，会导致：
                //   - sort_order 基线只看到覆盖层的最大值，新行与内置行（占用 [0,130] 步进 10）
                //     撞号，同值行间顺序不确定（M1）。
                //   - “两侧都有”的判定只看覆盖层，内置模型永远被当成“新增”（首轮同步会把
                //     全部内置模型算进 added，diff 摘要失真）。
                //   - 消失判定只遍历覆盖层的行，内置模型永远不在遍历范围内，于是永远不会
                //     累计 missing_sync_rounds、永远标不上 deprecated——恰恰是最该被标记的
                //     「上游已不再返回的老内置模型」被这个基线错误放过了（I1）。
                //
                // N1 修复：**绝不为内置行在 file.models 里补写覆盖行。** 同步元数据
                // （missing_sync_rounds / status / last_seen_at）一律写进
                // file.sync_state.model_meta，与 models 数组解耦。详见 SyncMeta 的文档：
                // from_file 的叠加是整行替换，补写=冻结快照，代码里改内置定义会对已有
                // 部署完全失效。
                let effective = ModelRegistry::from_file(file.clone())
                    .map_err(|e| format!("计算有效模型行集失败: {}", e))?;

                let mut max_sort = effective.rows().iter().map(|r| r.sort_order).max().unwrap_or(0);

                for (upstream_id, m) in &union {
                    let incoming = ModelRow {
                        upstream_id: upstream_id.clone(),
                        match_kind: MatchKind::Exact,
                        exposed_id: derive_exposed_id(upstream_id),
                        display_name: m
                            .model_name
                            .clone()
                            .filter(|s| !s.trim().is_empty())
                            .unwrap_or_else(|| upstream_id.clone()),
                        owned_by: if upstream_id.starts_with("claude-") {
                            "anthropic".to_string()
                        } else {
                            "openai".to_string()
                        },
                        model_type: "chat".to_string(),
                        created: now.timestamp(),
                        context_window: sanitize_window(upstream_id, m.max_input_tokens),
                        max_output_tokens: 64_000,
                        expose_thinking_variant: derive_thinking_variant(upstream_id),
                        enabled: true,
                        listed: true,
                        status: ModelStatus::Active,
                        origin: ModelOrigin::Synced,
                        sort_order: 0,
                        pinned: Vec::new(),
                        missing_sync_rounds: 0,
                        last_seen_at: Some(seen_at.clone()),
                        match_substrings: Vec::new(),
                    };

                    let existing_idx = file.models.iter().position(|r| &r.upstream_id == upstream_id);
                    match existing_idx {
                        Some(idx) => {
                            merge_synced_row(&mut file.models[idx], &incoming);
                            updated += 1;
                        }
                        // 覆盖层里没有这一行，但它已存在于有效行集 → 是个内置行。
                        // **不补写覆盖行**：内置行的字段本来就该由代码定义，同步只需要
                        // 记一条元数据（下面统一写 model_meta）。记账上算 updated 而非
                        // added——它本来就存在，不是新模型。
                        None if effective.rows().iter().any(|r| &r.upstream_id == upstream_id) => {
                            updated += 1;
                        }
                        None => {
                            let mut row = incoming;
                            max_sort += 10;
                            row.sort_order = max_sort;
                            file.models.push(row);
                            added += 1;
                        }
                    }

                    // 同步元数据统一落在 sync_state.model_meta：本轮见到 → 计数清零、
                    // 刷新 last_seen_at、若此前是 deprecated 则复活为 active。
                    file.sync_state.model_meta.insert(
                        upstream_id.clone(),
                        SyncMeta {
                            missing_sync_rounds: 0,
                            status: ModelStatus::Active,
                            last_seen_at: Some(seen_at.clone()),
                        },
                    );
                }

                // 消失判定：仅权威轮次。遍历有效行集（内置 ∪ 覆盖层），而非仅覆盖层。
                if round == RoundKind::Authoritative {
                    // N2：prefix 行是 handlers 侧的家族通配符伪行（如 gpt-5），
                    // 上游返回的 modelId 永远不可能等于它（它不代表一个真实上游模型，
                    // 只是「gpt-5* 一律放行」的解析规则载体）。留在判定里的话，
                    // 它必然在第 2 个权威轮次被标 Deprecated——确定性的、每次部署
                    // 都会发生的误报。
                    let judged: Vec<&ModelRow> = effective
                        .rows()
                        .iter()
                        .filter(|r| r.match_kind != MatchKind::Prefix)
                        .collect();
                    let missing: Vec<&ModelRow> = judged
                        .iter()
                        .copied()
                        .filter(|r| !union.contains_key(&r.upstream_id))
                        .collect();

                    // N3：单轮标记比例护栏。与「不可信轮次」（规则 1）同级，但防的是
                    // 另一条路径：规则 1 防网络抖动导致的空/失败返回，这里防**探针本身
                    // 不具代表性**——spec §6.2 假设探针是最高订阅等级的凭据，但代码里
                    // 没有任何东西保证或校验这一点。把一个低等级凭据设成探针，它能
                    // 「成功返回非空列表」从而通过规则 1，却只覆盖上游模型集的一小部分，
                    // 于是第 2 个权威轮次就能把整表刷成 deprecated。
                    // 处理方式：只跳过消失判定，本轮的新增/更新照常写入（数据本身没错，
                    // 错的是「把它当成完整快照」）。日志用 error 级别——这是需要人介入
                    // 修改探针配置的问题，不是可以忽略的抖动。
                    //
                    // N6：比例**只按 status == Active 的口径算**。行永不删除，若把已经
                    // Deprecated 的行也算进分子，护栏就成了只增不减的棘轮：每正常退役
                    // 一批模型，分子永久多一格、分母不变，比例单调上升，越过 50% 后消失
                    // 判定永久锁死，只剩每轮重复的 error 日志——部署跑得越久越容易踩中。
                    // 已经不在上游的行本来就不该参与「探针是否具代表性」的判断：护栏问的是
                    // 「本轮新缺失的在服役模型占比是否高到不像真实退役」，而不是
                    // 「表里有多少行不在上游」。
                    //
                    // 注意：只有**护栏判据**换口径，下面的计数循环仍遍历完整的 missing 集
                    // （对已 Deprecated 的行继续累加 missing_sync_rounds 无害，且保留了
                    // 「连续多少轮没见过」这个信息）。
                    let active_judged =
                        judged.iter().filter(|r| r.status == ModelStatus::Active).count();
                    let active_missing =
                        missing.iter().filter(|r| r.status == ModelStatus::Active).count();
                    let unrepresentative = active_missing * 2 > active_judged;
                    if unrepresentative {
                        tracing::error!(
                            "本轮权威同步的探针可能不具代表性：仍在服役（active）的 {} 行中有 {} 行\
                             未被上游返回（超过 50%）。已跳过本轮消失判定（新增/更新照常）。\
                             请确认 modelSyncProbeCredentialId 指向的是最高订阅等级的凭据。",
                            active_judged,
                            active_missing
                        );
                    } else {
                        for row in missing {
                            // 元数据的起点取有效行集上的当前值（from_file 已把
                            // model_meta 叠加上去；老格式的行内联字段也在这里体现），
                            // 因此老文件不需要迁移就能接着累计。
                            let meta = file
                                .sync_state
                                .model_meta
                                .entry(row.upstream_id.clone())
                                .or_insert_with(|| SyncMeta {
                                    missing_sync_rounds: row.missing_sync_rounds,
                                    status: row.status,
                                    last_seen_at: row.last_seen_at.clone(),
                                });
                            meta.missing_sync_rounds += 1;
                            if meta.missing_sync_rounds >= MISSING_ROUNDS_THRESHOLD
                                && meta.status == ModelStatus::Active
                            {
                                meta.status = ModelStatus::Deprecated;
                                deprecated += 1;
                                tracing::warn!(
                                    "模型 {} 连续 {} 轮权威同步未出现于上游，标记为 deprecated（保留可用）",
                                    row.upstream_id,
                                    meta.missing_sync_rounds
                                );
                            }
                        }
                    }
                }

                for (cred, models) in per_credential.drain() {
                    file.credential_support.insert(cred, models);
                }
                file.sync_state.last_sync_at = Some(now.to_rfc3339());
                file.sync_state.last_fetch_started_at = Some(fetch_started_at.clone());
                file.sync_state.source = Some(source.clone());
                Ok(())
            })
            .await?;

        // 落盘成功后才热替换
        match super::model_registry::ModelRegistry::from_file(file) {
            Ok(registry) => super::model_registry::install_registry(registry),
            Err(e) => {
                tracing::error!("同步后的 models.json 校验失败: {}，保持内存中旧表", e);
            }
        }

        Ok(SyncSummary { round, added, updated, deprecated, trusted: true, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    struct FakeFetcher {
        /// 凭据 id → 该凭据返回的模型（或错误）
        responses: StdMutex<HashMap<u64, Result<Vec<UpstreamModel>, String>>>,
        usable: Vec<u64>,
        /// 按调用先后记录的 fetch 凭据 id 序列（N4：断言降级采样不重复拉失败探针）
        calls: StdMutex<Vec<u64>>,
    }

    impl FakeFetcher {
        fn new(responses: Vec<(u64, Result<Vec<UpstreamModel>, String>)>) -> Self {
            let usable = responses.iter().map(|(id, _)| *id).collect();
            Self {
                responses: StdMutex::new(responses.into_iter().collect()),
                usable,
                calls: StdMutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<u64> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl ModelListFetcher for FakeFetcher {
        fn fetch(&self, id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>> {
            self.calls.lock().unwrap().push(id);
            let r = self
                .responses
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .unwrap_or_else(|| Err("no such credential".to_string()));
            Box::pin(async move { r })
        }
        fn candidate_credential_ids(&self) -> Vec<u64> {
            self.usable.clone()
        }
        fn is_credential_usable(&self, id: u64) -> bool {
            self.usable.contains(&id)
        }
    }

    fn upstream(id: &str, window: Option<i64>) -> UpstreamModel {
        UpstreamModel {
            model_id: id.to_string(),
            model_name: Some(id.to_uppercase()),
            max_input_tokens: window,
        }
    }

    /// 一个「有代表性」的探针会返回的模型集：全部内置的 exact 行。
    /// prefix 伪行（gpt-5）不是真实上游模型，上游永远不会返回它。
    ///
    /// N3 的比例护栏要求探针返回的模型覆盖有效行集的一半以上，所以凡是想真正
    /// 触发消失判定的测试，都必须从这个集合出发去掉待测的那一个，而不能只返回
    /// 一两个模型——只返回一两个恰恰是护栏要拦的「探针不具代表性」。
    fn builtin_upstream_models() -> Vec<UpstreamModel> {
        crate::anthropic::model_registry::builtin_rows()
            .into_iter()
            .filter(|r| r.match_kind != MatchKind::Prefix)
            .map(|r| UpstreamModel {
                model_id: r.upstream_id.clone(),
                model_name: Some(r.display_name.clone()),
                max_input_tokens: Some(i64::from(r.context_window)),
            })
            .collect()
    }

    /// 全部内置 exact 模型，去掉指定的若干个（模拟「上游不再返回它们」）。
    fn builtin_upstream_models_without(excluded: &[&str]) -> Vec<UpstreamModel> {
        builtin_upstream_models()
            .into_iter()
            .filter(|m| !excluded.contains(&m.model_id.as_str()))
            .collect()
    }

    fn tmp_store(name: &str) -> Arc<ModelRegistryStore> {
        let mut p = std::env::temp_dir();
        p.push(format!("kiro-sync-test-{}-{}.json", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        Arc::new(ModelRegistryStore::new(p))
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-07-25T04:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    /// 新模型经一轮同步应进入表中（本设计要解决的原始问题）
    #[tokio::test]
    async fn adds_new_upstream_model() {
        // I4 修复：sync_once 落盘成功后会调用 install_registry() 改写进程级全局状态，
        // 所有会走到这里的测试都必须先取 MODEL_GLOBALS_TEST_LOCK 串行化，否则并行测试互相
        // 覆盖全局注册表，其他模块（如 converter.rs）断言内置 upstream_id 的测试会随机挂。
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("add");
        let fetcher = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-opus-5", Some(1_000_000))]),
        )]));
        let svc = ModelSyncService::new(store.clone(), fetcher);

        let summary = svc.sync_once(Some(3), now()).await.unwrap();
        assert!(summary.trusted);
        assert!(matches!(summary.round, RoundKind::Authoritative));
        assert_eq!(summary.added, 1);

        let out = store.load();
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-5").unwrap();
        assert_eq!(row.exposed_id, "claude-opus-5");
        assert_eq!(row.context_window, 1_000_000);
        assert!(row.expose_thinking_variant, "claude-* 应派生 thinking 变体");
    }

    /// gpt 家族的 exposedId 必须原样保留点号
    #[tokio::test]
    async fn gpt_exposed_id_keeps_dots() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("gpt");
        let fetcher = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("gpt-5.9-nova", Some(272_000))]),
        )]));
        ModelSyncService::new(store.clone(), fetcher).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "gpt-5.9-nova").unwrap();
        assert_eq!(row.exposed_id, "gpt-5.9-nova", "gpt 家族不得把点号转成连字符");
        assert!(!row.expose_thinking_variant);
    }

    /// 全部凭据失败 → 本轮不可信 → 文件字节不变、无 deprecated
    #[tokio::test]
    async fn untrusted_round_does_not_touch_file() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("untrusted");
        // 先建一行
        let ok = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), ok).sync_once(Some(3), now()).await.unwrap();
        let before = std::fs::read(store.path()).unwrap();

        // 再来一轮全失败
        let bad = Arc::new(FakeFetcher::new(vec![(3, Err("network down".to_string()))]));
        let result = ModelSyncService::new(store.clone(), bad).sync_once(Some(3), now()).await;
        assert!(result.is_err() || !result.unwrap().trusted);
        assert_eq!(std::fs::read(store.path()).unwrap(), before, "不可信轮次不应改动文件");

        let out = store.load();
        assert!(out
            .registry
            .rows()
            .iter()
            .all(|r| r.status == crate::anthropic::model_registry::ModelStatus::Active));
    }

    /// 空列表同样视为不可信
    #[tokio::test]
    async fn empty_upstream_list_is_untrusted() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("empty");
        let ok = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), ok).sync_once(Some(3), now()).await.unwrap();
        let before = std::fs::read(store.path()).unwrap();

        let empty = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![]))]));
        let r = ModelSyncService::new(store.clone(), empty).sync_once(Some(3), now()).await;
        assert!(r.is_err() || !r.unwrap().trusted);
        assert_eq!(std::fs::read(store.path()).unwrap(), before);
    }

    /// 非权威（采样）轮次不得判定消失
    #[tokio::test]
    async fn advisory_round_never_deprecates() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("advisory");
        let full = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-a", Some(200_000)), upstream("claude-b", Some(200_000))]),
        )]));
        ModelSyncService::new(store.clone(), full).sync_once(Some(3), now()).await.unwrap();

        // 探针不可用 → 采样轮次；采样到的凭据只看得到 claude-a
        let partial = Arc::new(FakeFetcher::new(vec![(9, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        let svc = ModelSyncService::new(store.clone(), partial);
        // 探针 id=3 已不在 usable 列表中 → 回退采样
        let summary = svc.sync_once(Some(3), now()).await.unwrap();
        assert!(matches!(summary.round, RoundKind::Advisory));
        assert_eq!(summary.deprecated, 0, "非权威轮次不得判消失");

        let out = store.load();
        let b = out.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b.status, crate::anthropic::model_registry::ModelStatus::Active);
        assert_eq!(b.missing_sync_rounds, 0);
    }

    /// 权威轮次连续 2 轮未见 → deprecated，且行不被删除
    #[tokio::test]
    async fn authoritative_rounds_deprecate_after_threshold() {
        use crate::anthropic::model_registry::ModelStatus;
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("deprecate");
        // 注意（N3 护栏导致的必要调整）：探针必须「有代表性」，否则单轮缺失比例超过
        // 有效行集的 50%，消失判定会被整轮跳过（那是 N3 要拦的探针配置错误场景，
        // 由 unrepresentative_probe_skips_disappearance_check 专门覆盖）。
        // 因此这里让探针返回全部内置模型 + claude-a/claude-b，第二轮只去掉 claude-b ——
        // 本测试真正关心的「连续 2 轮未见才标记」语义完全不变，只是把 fixture 从
        // 「只返回 2 个模型的病态探针」换成了正常探针。
        let with_extras = |extras: Vec<UpstreamModel>| {
            let mut models = builtin_upstream_models();
            models.extend(extras);
            Arc::new(FakeFetcher::new(vec![(3, Ok(models))]))
        };
        let full = with_extras(vec![upstream("claude-a", Some(200_000)), upstream("claude-b", Some(200_000))]);
        ModelSyncService::new(store.clone(), full).sync_once(Some(3), now()).await.unwrap();

        let shrunk = || with_extras(vec![upstream("claude-a", Some(200_000))]);

        let s1 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s1.deprecated, 0, "第一轮只累计，不标记任何模型");
        let after_s1 = store.load();
        let b1 = after_s1.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b1.missing_sync_rounds, 1, "claude-b 第一轮只累计一次");
        assert_eq!(b1.status, ModelStatus::Active, "第一轮只累计，不标记");

        let s2 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s2.deprecated, 1, "第二轮达阈值，且只应标记 claude-b 一个");

        let out = store.load();
        let b = out.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b.status, ModelStatus::Deprecated);
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-b"), "永不删行");
    }

    /// 无效 maxInputTokens 回退 200000
    #[tokio::test]
    async fn invalid_max_input_tokens_falls_back() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("badwindow");
        let f = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![
                upstream("claude-none", None),
                upstream("claude-zero", Some(0)),
                upstream("claude-huge", Some(i64::from(i32::MAX) + 1)),
            ]),
        )]));
        ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        for id in ["claude-none", "claude-zero", "claude-huge"] {
            let row = out.registry.rows().iter().find(|r| r.upstream_id == id).unwrap();
            assert_eq!(row.context_window, 200_000, "{} 应回退到 200000", id);
        }
    }

    /// 并集冲突：窗口取 max，名称按凭据 id 升序取首个非空
    #[tokio::test]
    async fn union_conflict_resolution() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("union");
        let f = Arc::new(FakeFetcher::new(vec![
            (7, Ok(vec![UpstreamModel { model_id: "claude-x".into(), model_name: Some("从 7 来".into()), max_input_tokens: Some(200_000) }])),
            (2, Ok(vec![UpstreamModel { model_id: "claude-x".into(), model_name: Some("从 2 来".into()), max_input_tokens: Some(1_000_000) }])),
        ]));
        // 探针不可用 → 采样，会同时命中 2 与 7
        let summary = ModelSyncService::new(store.clone(), f).sync_once(None, now()).await.unwrap();
        assert!(matches!(summary.round, RoundKind::Advisory));

        let out = store.load();
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "claude-x").unwrap();
        assert_eq!(row.context_window, 1_000_000, "窗口应取 max");
        assert_eq!(row.display_name, "从 2 来", "名称应按凭据 id 升序取首个非空");
    }

    /// credentialSupport 记录本轮各凭据的可用模型集
    #[tokio::test]
    async fn records_credential_support() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("credsupport");
        let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        assert_eq!(out.file.credential_support.get("3").unwrap(), &vec!["claude-a".to_string()]);
    }

    /// C1：拉取成功但返回空列表的凭据，不得写入空的 credential_support 记录。
    ///
    /// 空记录会被调度层解读为「该凭据不支持任何模型」的强断言（见模块内
    /// fetch_from 的注释），而空列表本身已经被判定为「不可信信号」——用不可信信号
    /// 写出一条强断言、还让它对全部模型永久生效，自相矛盾。
    #[tokio::test]
    async fn empty_credential_is_not_recorded_in_credential_support() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("c1-empty-cred");
        // 采样轮次同时命中 2 个凭据：2 号返回空列表，3 号返回非空——整轮仍可信。
        let f = Arc::new(FakeFetcher::new(vec![
            (2, Ok(vec![])),
            (3, Ok(vec![upstream("claude-a", Some(200_000))])),
        ]));
        let summary = ModelSyncService::new(store.clone(), f).sync_once(None, now()).await.unwrap();
        assert!(summary.trusted);

        let out = store.load();
        assert!(
            !out.file.credential_support.contains_key("2"),
            "返回空列表的凭据不得出现在 credential_support 里（应保留旧记录/未知语义）"
        );
        assert_eq!(out.file.credential_support.get("3").unwrap(), &vec!["claude-a".to_string()]);
    }

    /// I1：诊断权威轮次里，内置模型（未同步过、只存在于 builtin_rows）在上游不再返回时
    /// 也必须能累计 missing_sync_rounds 并最终被标记 deprecated。此前消失判定只遍历
    /// file.models（覆盖层），内置模型永远不在遍历范围内，永远标不上 deprecated。
    #[tokio::test]
    async fn authoritative_round_deprecates_missing_builtin_model() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("i1-builtin-deprecate");
        // 上游返回除 claude-opus-4.8 以外的全部内置模型，外加一个真正的新模型。
        //
        // 注意（N3 护栏导致的必要调整）：本测试原先让探针每轮只返回 1 个模型，
        // 现在那属于「探针不具代表性」（缺失比例 > 50%），消失判定会被整轮跳过。
        // 换成「只少一个内置模型」后，本测试关心的两点语义丝毫未变：
        //   1) 内置模型（只存在于 builtin_rows、不在 file.models 里）也要参与消失判定；
        //   2) 首轮 added 不得把内置模型算成新增。
        let fetcher = || {
            let mut models = builtin_upstream_models_without(&["claude-opus-4.8"]);
            models.push(upstream("claude-only-one", Some(200_000)));
            Arc::new(FakeFetcher::new(vec![(3, Ok(models))]))
        };

        let s1 = ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        // 首轮：claude-only-one 是真正新增的一行；14 个内置模型不应被计入 added
        // （它们本来就存在于有效行集里，只是首次被同步判定「未见」，属于 updated 语义
        // 的记账对象，而非新增）。
        assert_eq!(s1.added, 1, "首轮 added 不应把内置模型算进去");

        ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        let opus = out.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(
            opus.status,
            crate::anthropic::model_registry::ModelStatus::Deprecated,
            "连续 2 轮权威同步未见的内置模型应被标记 deprecated"
        );
    }

    /// M1：新增行的 sort_order 基线必须取「内置 ∪ 覆盖层」的最大值，不能只看覆盖层
    /// （否则新行会拿到形如 10 的低位 sort_order，与内置行撞号）。
    #[tokio::test]
    async fn new_row_sort_order_exceeds_all_builtin_rows() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("m1-sort-order");
        let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-brand-new", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        let max_builtin_sort = crate::anthropic::model_registry::builtin_rows()
            .iter()
            .map(|r| r.sort_order)
            .max()
            .unwrap();
        let new_row = out.registry.rows().iter().find(|r| r.upstream_id == "claude-brand-new").unwrap();
        assert!(
            new_row.sort_order > max_builtin_sort,
            "新行 sort_order({}) 应大于所有内置行的最大值({})",
            new_row.sort_order,
            max_builtin_sort
        );
    }

    /// I2：乱序保护此前用 RFC3339 字符串字典序比较 lastSyncAt，时区偏移下会
    /// 双向失效（负偏移漏挡、正偏移误挡）。修复后按真实时刻比较；解析失败按
    /// 「无记录」放行，不永久卡死同步。
    #[tokio::test]
    async fn last_sync_at_ordering_uses_real_instant_not_string_order() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // 场景 1：负偏移曾经漏挡。lastSyncAt 写成 "...-05:00"（真实时刻是 05:00Z），
        // 本轮起始时间是 04:00Z（比真实的 lastSyncAt 更旧，应当被丢弃）。
        // 字符串比较下 "2026-07-25T00:00:00-05:00" < "2026-07-25T04:00:00Z"（首字符 '0'<'2'
        // 且后续位也偏小），会被误判为"更旧"从而放行本轮，让旧观测覆盖新观测。
        {
            let store = tmp_store("i2-negative-offset");
            store
                .mutate(|file| {
                    file.sync_state.last_sync_at = Some("2026-07-25T00:00:00-05:00".to_string());
                    Ok(())
                })
                .await
                .unwrap();
            let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-stale-round", Some(200_000))]))]));
            let older_round_start = chrono::DateTime::parse_from_rfc3339("2026-07-25T04:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc);
            let result = ModelSyncService::new(store.clone(), f).sync_once(Some(3), older_round_start).await;
            assert!(result.is_err(), "本轮（04:00Z）早于真实 lastSyncAt（05:00Z），应被丢弃而非漏挡");
        }

        // 场景 2：正偏移曾经误挡。lastSyncAt 写成 "...+08:00"（真实时刻是 15:00Z），
        // 本轮起始时间是 16:00Z（比真实的 lastSyncAt 更新，应当被放行）。
        // 字符串比较下 "2026-07-25T23:00:00+08:00" > "2026-07-25T16:00:00Z"，会被误判为
        // "更新"从而丢弃本轮，导致同步在时区偏移的小时数内停摆。
        {
            let store = tmp_store("i2-positive-offset");
            store
                .mutate(|file| {
                    file.sync_state.last_sync_at = Some("2026-07-25T23:00:00+08:00".to_string());
                    Ok(())
                })
                .await
                .unwrap();
            let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-newer-round", Some(200_000))]))]));
            let newer_round_start = chrono::DateTime::parse_from_rfc3339("2026-07-25T16:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc);
            let result = ModelSyncService::new(store.clone(), f).sync_once(Some(3), newer_round_start).await;
            assert!(result.is_ok(), "本轮（16:00Z）晚于真实 lastSyncAt（15:00Z），不应被误挡");
        }

        // 场景 3：无法解析的时间戳按"无记录"放行，不永久卡死同步。
        {
            let store = tmp_store("i2-unparseable");
            store
                .mutate(|file| {
                    file.sync_state.last_sync_at = Some("not-a-valid-timestamp".to_string());
                    Ok(())
                })
                .await
                .unwrap();
            let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-after-bad-ts", Some(200_000))]))]));
            let result = ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await;
            assert!(result.is_ok(), "解析失败的坏时间戳应按无记录放行，不应永久阻塞同步");
        }
    }

    /// I3：探针（权威轮次唯一凭据）拉取失败时，应回退为一次采样轮（advisory），
    /// 只做新增/更新、绝不判定消失，并在 source 里体现 probe_failed，而不是让
    /// 整轮直接返回 Err（那样新模型会静默地永远进不来）。
    #[tokio::test]
    async fn probe_fetch_failure_falls_back_to_advisory_sample() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("i3-probe-fallback");
        // 探针凭据 3 本身"可用"（未禁用），但 fetch 会失败（如 refreshToken 失效）；
        // 采样候选里还有 5 号凭据能正常返回结果。
        let fetcher = Arc::new(FakeFetcher::new(vec![
            (3, Err("token 刷新失败".to_string())),
            (5, Ok(vec![upstream("claude-via-sample", Some(200_000))])),
        ]));
        let summary = ModelSyncService::new(store.clone(), fetcher).sync_once(Some(3), now()).await.unwrap();

        assert!(matches!(summary.round, RoundKind::Advisory), "探针失败应回退为采样轮次");
        assert_eq!(summary.deprecated, 0, "回退后的采样轮次绝不能判定消失");
        assert!(
            summary.source.starts_with("probe_failed_sample:"),
            "source 应体现探针失败回退，实际: {}",
            summary.source
        );

        let out = store.load();
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-via-sample"));
    }

    /// N1：权威同步轮次**不得**把内置行逐字段快照进 models 数组。
    /// 同步元数据（missingSyncRounds / status / lastSeenAt）应落在
    /// syncState.modelMeta 下，与覆盖层解耦。
    #[tokio::test]
    async fn authoritative_sync_never_snapshots_builtin_rows_into_overlay() {
        use crate::anthropic::model_registry::{builtin_rows, ModelOrigin};
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n1-no-builtin-snapshot");

        // 探针有代表性（返回几乎全部内置模型），但少了 claude-opus-4.8 ——
        // 它会走消失判定，正是旧实现会为它补写快照行的那条路径。
        let fetcher = || {
            let mut models = builtin_upstream_models_without(&["claude-opus-4.8"]);
            models.push(upstream("claude-brand-new", Some(200_000)));
            Arc::new(FakeFetcher::new(vec![(3, Ok(models))]))
        };

        for _ in 0..3 {
            ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        }

        let out = store.load();

        // 1) models 数组里不得出现 origin=builtin 的补写行
        let snapshotted: Vec<&str> = out
            .file
            .models
            .iter()
            .filter(|r| r.origin == ModelOrigin::Builtin)
            .map(|r| r.upstream_id.as_str())
            .collect();
        assert!(snapshotted.is_empty(), "内置行被快照进了 models: {:?}", snapshotted);

        // 2) 更强的约束：任何内置 upstreamId 都不该出现在覆盖层里
        for b in builtin_rows() {
            assert!(
                !out.file.models.iter().any(|r| r.upstream_id == b.upstream_id),
                "内置行 {} 不该出现在 models 数组（会冻结成快照）",
                b.upstream_id
            );
        }

        // 3) 行数应保持在「人工覆盖 + 真正新增」的量级：这里只有 1 个真正的新模型
        assert_eq!(
            out.file.models.len(),
            1,
            "models 数组应只含真正新增的行，实际: {:?}",
            out.file.models.iter().map(|r| &r.upstream_id).collect::<Vec<_>>()
        );

        // 4) 内置模型的同步元数据仍然被正确记录、并能从解析结果里读出来
        let meta = out
            .file
            .sync_state
            .model_meta
            .get("claude-opus-4.8")
            .expect("消失的内置模型必须在 modelMeta 里留下记录");
        assert_eq!(meta.missing_sync_rounds, 3);
        assert_eq!(meta.status, ModelStatus::Deprecated);

        let opus = out.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(opus.missing_sync_rounds, 3, "解析结果要把 modelMeta 叠加到内置行上");
        assert_eq!(opus.status, ModelStatus::Deprecated);

        // 5) 仍被上游返回的内置行也要有 lastSeenAt 元数据
        let seen = out.file.sync_state.model_meta.get("claude-sonnet-5").unwrap();
        assert_eq!(seen.missing_sync_rounds, 0);
        assert_eq!(seen.status, ModelStatus::Active);
        assert!(seen.last_seen_at.is_some());
    }

    /// N1 关键回归：内置行定义在代码里改了以后，已有部署的 models.json 不得把
    /// 旧值冻结住。用同步真实写出的文件 + 一份「新版代码」的内置集来验证。
    #[tokio::test]
    async fn synced_file_does_not_freeze_changed_builtin_definition() {
        use crate::anthropic::model_registry::builtin_rows;
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n1-definition-not-frozen");

        let fetcher = || Arc::new(FakeFetcher::new(vec![(3, Ok(builtin_upstream_models()))]));
        for _ in 0..2 {
            ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        }
        let synced_file = store.load().file;

        // 模拟「下一个版本的代码把 claude-opus-4.8 的窗口和显示名改了」
        let mut next_version_builtin = builtin_rows();
        for r in next_version_builtin.iter_mut() {
            if r.upstream_id == "claude-opus-4.8" {
                r.context_window = 333_000;
                r.display_name = "Claude Opus 4.8（新版）".to_string();
            }
        }

        let registry = ModelRegistry::from_file_with_builtin(next_version_builtin, synced_file)
            .expect("同步写出的文件必须能被新版内置集解析");
        let row = registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(row.context_window, 333_000, "解析结果必须用新的代码定义，而不是同步时的快照");
        assert_eq!(row.display_name, "Claude Opus 4.8（新版）");
    }

    /// N2：prefix 伪行（gpt-5）是 handlers 侧的家族通配符，上游的 modelId 永远
    /// 不可能等于它，必须排除在消失判定之外，否则每次部署都确定性地被误标
    /// Deprecated。
    #[tokio::test]
    async fn prefix_pseudo_row_is_never_deprecated() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n2-prefix-not-deprecated");

        // 探针返回全部真实内置模型（不含也不可能含 "gpt-5" 这个伪行）
        let fetcher = || Arc::new(FakeFetcher::new(vec![(3, Ok(builtin_upstream_models()))]));
        for _ in 0..2 {
            ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        }

        let out = store.load();
        let gpt5 = out.registry.rows().iter().find(|r| r.upstream_id == "gpt-5").unwrap();
        assert_eq!(gpt5.match_kind, MatchKind::Prefix);
        assert_ne!(gpt5.status, ModelStatus::Deprecated, "prefix 伪行不得被标记为 Deprecated");
        assert_eq!(gpt5.missing_sync_rounds, 0, "prefix 伪行不该累计 missingSyncRounds");
        assert!(
            !out.file.sync_state.model_meta.contains_key("gpt-5"),
            "prefix 伪行不该进入消失判定，也就不该产生同步元数据"
        );
    }

    /// N3：单轮缺失比例超过有效行集的 50% → 判定探针不具代表性，跳过本轮消失判定
    /// （其余部分正常进行），并打 error 日志。
    #[tokio::test]
    async fn unrepresentative_probe_skips_disappearance_check() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n3-unrepresentative-probe");

        // 探针（例如订阅等级过低的凭据）只返回 1 个模型
        let fetcher = || Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-only-one", Some(200_000))]))]));

        let s1 = ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s1.deprecated, 0);
        assert_eq!(s1.added, 1, "新增仍应正常进行——只跳过消失判定，不中止整轮");

        let s2 = ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        assert!(matches!(s2.round, RoundKind::Authoritative));
        assert_eq!(s2.deprecated, 0, "缺失比例过半时不得标记任何模型");

        let out = store.load();
        for row in out.registry.rows() {
            assert_eq!(
                row.missing_sync_rounds, 0,
                "{} 的 missingSyncRounds 应保持不变（判定被跳过）",
                row.upstream_id
            );
            assert_eq!(row.status, ModelStatus::Active, "{} 不该被标记", row.upstream_id);
        }
        assert!(
            out.file.sync_state.model_meta.values().all(|m| m.missing_sync_rounds == 0),
            "跳过判定的轮次不得写出任何 missingSyncRounds 计数"
        );
    }

    /// N3 对照组：缺失比例低于阈值时，消失判定必须照常执行。
    #[tokio::test]
    async fn representative_probe_still_runs_disappearance_check() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n3-representative-probe");

        // 14 个内置 exact 行里只缺 1 个 → 远低于 50%
        let fetcher =
            || Arc::new(FakeFetcher::new(vec![(3, Ok(builtin_upstream_models_without(&["claude-opus-4.8"])))]));

        let s1 = ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s1.deprecated, 0, "第一轮只累计");
        let after1 = store.load();
        let opus1 = after1.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(opus1.missing_sync_rounds, 1, "低于阈值的轮次必须正常累计");

        let s2 = ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s2.deprecated, 1, "达阈值应标记，且只标记这一个");
        let out = store.load();
        let opus = out.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(opus.status, ModelStatus::Deprecated);
    }

    /// N6：护栏比例若把已 Deprecated 的行算进分子，就成了只增不减的棘轮 ——
    /// 行永不删除，每正常退役一批模型分子就永久多一格、分母不变，越过 50% 后
    /// 消失判定**再也回不去**，Task 9 存在的全部理由静默失效。
    ///
    /// 这里分两批退役，断言第二批仍能正常走到 Deprecated。
    /// 按旧口径第二批的比例是 8/13 = 62%（含第一批已 Deprecated 的 4 个）→ 永久卡死；
    /// 按 Active 口径是 4/9 = 44% → 正确放行。
    #[tokio::test]
    async fn guard_ratio_does_not_ratchet_after_normal_retirements() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n6-ratchet");

        const BATCH_1: [&str; 4] =
            ["claude-opus-4.5", "claude-sonnet-4.5", "claude-haiku-4.5", "gpt-5.6-luna"];
        const BATCH_2: [&str; 4] =
            ["claude-opus-4.6", "claude-sonnet-4.6", "claude-opus-4.7", "gpt-5.6-terra"];

        let probe = |missing: Vec<&str>| {
            Arc::new(FakeFetcher::new(vec![(3, Ok(builtin_upstream_models_without(&missing)))]))
        };

        // 第一批退役：4/13 = 31%，护栏放行，两轮后标记
        for _ in 0..2 {
            ModelSyncService::new(store.clone(), probe(BATCH_1.to_vec()))
                .sync_once(Some(3), now())
                .await
                .unwrap();
        }
        let after_batch1 = store.load();
        for id in BATCH_1 {
            let row = after_batch1.registry.rows().iter().find(|r| r.upstream_id == id).unwrap();
            assert_eq!(row.status, ModelStatus::Deprecated, "第一批 {} 应已退役", id);
        }

        // 第二批退役：原始缺失 8/13 = 62%（旧口径会永久卡死），
        // 但其中 4 个早已 Deprecated，Active 口径是 4/9 = 44% → 必须放行
        let both: Vec<&str> = BATCH_1.iter().chain(BATCH_2.iter()).copied().collect();
        let s1 = ModelSyncService::new(store.clone(), probe(both.clone()))
            .sync_once(Some(3), now())
            .await
            .unwrap();
        assert_eq!(s1.deprecated, 0, "第二批第一轮只累计");
        let mid = store.load();
        for id in BATCH_2 {
            let row = mid.registry.rows().iter().find(|r| r.upstream_id == id).unwrap();
            assert_eq!(row.missing_sync_rounds, 1, "{} 的计数必须递增，护栏不该触发", id);
        }

        let s2 = ModelSyncService::new(store.clone(), probe(both))
            .sync_once(Some(3), now())
            .await
            .unwrap();
        assert_eq!(s2.deprecated, BATCH_2.len(), "第二批达阈值应被标记");
        let out = store.load();
        for id in BATCH_2 {
            let row = out.registry.rows().iter().find(|r| r.upstream_id == id).unwrap();
            assert_eq!(row.status, ModelStatus::Deprecated, "第二批 {} 应能正常退役", id);
        }
    }

    /// N6 语义边界：护栏码路要区分**两种缺失比例相同、结论相反**的场景。
    ///
    /// 两个场景的「原始缺失比例」都是 8/13 = 62%，但：
    /// - 场景 A「探针不具代表性」：13 行全是 Active，探针只返回 5 个 → 必须拦。
    ///   探针看不到的那 8 个模型在上游是存在的，把它们标 deprecated 就是误报。
    /// - 场景 B「上游合法批量退役」：其中 4 个早已 Deprecated（上一批正常退役），
    ///   本轮真正新消失的只有 4 个 → 必须放行。已经不在上游的行不该再参与
    ///   「探针是否具代表性」的判断。
    ///
    /// 这条测试把护栏的语义钉死：判据是「**本轮新缺失的 Active 行**占
    /// **仍在服役的 Active 行**的比例」，不是「缺失行占全部行的比例」。
    #[tokio::test]
    async fn guard_distinguishes_unrepresentative_probe_from_bulk_retirement() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        const FIRST_WAVE: [&str; 4] =
            ["claude-opus-4.5", "claude-sonnet-4.5", "claude-haiku-4.5", "gpt-5.6-luna"];
        const SECOND_WAVE: [&str; 4] =
            ["claude-opus-4.6", "claude-sonnet-4.6", "claude-opus-4.7", "gpt-5.6-terra"];
        let all_missing: Vec<&str> =
            FIRST_WAVE.iter().chain(SECOND_WAVE.iter()).copied().collect();
        // 前置检查：两个场景的原始缺失比例确实相同，否则这条对照就不成立
        let exact_rows = crate::anthropic::model_registry::builtin_rows()
            .into_iter()
            .filter(|r| r.match_kind != MatchKind::Prefix)
            .count();
        assert_eq!(exact_rows, 13);
        assert!(all_missing.len() * 2 > exact_rows, "两个场景的原始缺失比例都应超过 50%");

        let probe = |missing: Vec<&str>| {
            Arc::new(FakeFetcher::new(vec![(3, Ok(builtin_upstream_models_without(&missing)))]))
        };

        // ---- 场景 A：探针不具代表性 → 拦 ----
        {
            let store = tmp_store("n6-boundary-unrepresentative");
            for _ in 0..2 {
                let s = ModelSyncService::new(store.clone(), probe(all_missing.clone()))
                    .sync_once(Some(3), now())
                    .await
                    .unwrap();
                assert_eq!(s.deprecated, 0);
            }
            let out = store.load();
            for id in all_missing.iter() {
                let row = out.registry.rows().iter().find(|r| &r.upstream_id == id).unwrap();
                assert_eq!(row.missing_sync_rounds, 0, "{} 不该被计数：探针不具代表性", id);
                assert_eq!(row.status, ModelStatus::Active);
            }
        }

        // ---- 场景 B：上游合法批量退役 → 放行 ----
        {
            let store = tmp_store("n6-boundary-bulk-retirement");
            // 先让 FIRST_WAVE 正常退役（4/13 = 31%，护栏不触发）
            for _ in 0..2 {
                ModelSyncService::new(store.clone(), probe(FIRST_WAVE.to_vec()))
                    .sync_once(Some(3), now())
                    .await
                    .unwrap();
            }
            // 此刻缺失集与场景 A 完全相同（8/13），但 4 个已 Deprecated
            for _ in 0..2 {
                ModelSyncService::new(store.clone(), probe(all_missing.clone()))
                    .sync_once(Some(3), now())
                    .await
                    .unwrap();
            }
            let out = store.load();
            for id in SECOND_WAVE {
                let row = out.registry.rows().iter().find(|r| r.upstream_id == id).unwrap();
                assert_eq!(
                    row.status,
                    ModelStatus::Deprecated,
                    "{}：缺失比例与场景 A 相同，但这是合法批量退役，必须放行",
                    id
                );
            }
        }
    }

    /// MN1：`origin=builtin` 的行本就不该出现在覆盖层（人工编辑产生 manual、
    /// 同步产生 synced）。跑过中间版本的开发环境里躺着 14 行 builtin 快照，
    /// N1 修掉的冻结 bug 在这些文件上继续生效，所以每次写盘顺手清理。
    #[tokio::test]
    async fn mutate_drops_stale_builtin_snapshot_rows_from_overlay() {
        use crate::anthropic::model_registry::{builtin_rows, ModelOrigin};
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("mn1-drop-builtin-snapshot");

        // 模拟老文件：覆盖层里躺着一份内置行的整行快照，窗口是写入那一刻的旧值
        store
            .mutate(|file| {
                let mut stale = builtin_rows()
                    .into_iter()
                    .find(|r| r.upstream_id == "claude-opus-4.8")
                    .unwrap();
                stale.context_window = 111_000;
                file.models.push(stale);
                Ok(())
            })
            .await
            .unwrap();

        let fetcher = || Arc::new(FakeFetcher::new(vec![(3, Ok(builtin_upstream_models()))]));
        ModelSyncService::new(store.clone(), fetcher()).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        assert!(
            !out.file.models.iter().any(|r| r.origin == ModelOrigin::Builtin),
            "覆盖层里不该残留 origin=builtin 的行"
        );
        // 冻结的旧值随之消失，解析结果回到代码里的内置定义
        let row = out.registry.rows().iter().find(|r| r.upstream_id == "claude-opus-4.8").unwrap();
        assert_eq!(row.context_window, 1_000_000, "清理后应回到代码里的内置定义");
    }

    /// N4：探针失败后的降级采样不得再把刚失败的探针 id 选进来 ——
    /// 它白占一个 SAMPLE_SIZE 名额，还多发一次注定失败的请求。
    #[tokio::test]
    async fn fallback_sampling_excludes_just_failed_probe() {
        let _registry_guard =
            crate::anthropic::model_registry::MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("n4-exclude-failed-probe");

        let fetcher = Arc::new(FakeFetcher::new(vec![
            (1, Err("token 刷新失败".to_string())),
            (2, Ok(vec![upstream("claude-from-2", Some(200_000))])),
            (4, Ok(vec![upstream("claude-from-4", Some(200_000))])),
            (6, Ok(vec![upstream("claude-from-6", Some(200_000))])),
        ]));
        let summary =
            ModelSyncService::new(store.clone(), fetcher.clone()).sync_once(Some(1), now()).await.unwrap();
        assert!(matches!(summary.round, RoundKind::Advisory));

        let calls = fetcher.calls();
        assert_eq!(calls[0], 1, "第一次仍应尝试探针本身");
        assert!(
            !calls[1..].contains(&1),
            "降级采样不得重复拉取刚失败的探针，实际调用序列: {:?}",
            calls
        );
        assert_eq!(calls, vec![1, 2, 4, 6], "降级应采样探针以外的 SAMPLE_SIZE 个凭据");
    }
}

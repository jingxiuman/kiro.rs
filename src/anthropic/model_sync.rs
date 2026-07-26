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

use super::model_registry::{MatchKind, ModelOrigin, ModelRegistry, ModelRow, ModelStatus};
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
fn derive_exposed_id(upstream_id: &str) -> String {
    if upstream_id.starts_with("claude-") {
        upstream_id.replace('.', "-")
    } else {
        upstream_id.to_string()
    }
}

fn derive_thinking_variant(upstream_id: &str) -> bool {
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
            let mut ids = self.fetcher.candidate_credential_ids();
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
                // 对首次被同步命中/判定消失的内置行，按需在 file.models 里补写一份覆盖，
                // 用来承载 missing_sync_rounds / status 这两个此前只属于覆盖层的字段。
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
                        None => {
                            // 覆盖层里没有这一行，但可能是内置行（还没被同步命中过）——
                            // 按有效行集判断，不能算“新增”，也要把内置行的其余字段带过来，
                            // 再叠加本轮同步结果，作为覆盖层的首份记录。
                            match effective.rows().iter().find(|r| &r.upstream_id == upstream_id) {
                                Some(builtin_row) => {
                                    let mut row = builtin_row.clone();
                                    merge_synced_row(&mut row, &incoming);
                                    file.models.push(row);
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
                        }
                    }
                }

                // 消失判定：仅权威轮次。遍历有效行集（内置 ∪ 覆盖层），而非仅覆盖层。
                if round == RoundKind::Authoritative {
                    for row in effective.rows() {
                        if union.contains_key(&row.upstream_id) {
                            continue;
                        }
                        let idx = match file.models.iter().position(|r| r.upstream_id == row.upstream_id) {
                            Some(idx) => idx,
                            None => {
                                // 内置行首次出现消失：补写覆盖层记录以承载 missing_sync_rounds。
                                file.models.push(row.clone());
                                file.models.len() - 1
                            }
                        };
                        let overlay = &mut file.models[idx];
                        overlay.missing_sync_rounds += 1;
                        if overlay.missing_sync_rounds >= MISSING_ROUNDS_THRESHOLD
                            && overlay.status == ModelStatus::Active
                        {
                            overlay.status = ModelStatus::Deprecated;
                            deprecated += 1;
                            tracing::warn!(
                                "模型 {} 连续 {} 轮权威同步未出现于上游，标记为 deprecated（保留可用）",
                                overlay.upstream_id,
                                overlay.missing_sync_rounds
                            );
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
    }

    impl FakeFetcher {
        fn new(responses: Vec<(u64, Result<Vec<UpstreamModel>, String>)>) -> Self {
            let usable = responses.iter().map(|(id, _)| *id).collect();
            Self { responses: StdMutex::new(responses.into_iter().collect()), usable }
        }
    }

    impl ModelListFetcher for FakeFetcher {
        fn fetch(&self, id: u64) -> BoxFuture<'_, Result<Vec<UpstreamModel>, String>> {
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
        // 所有会走到这里的测试都必须先取 REGISTRY_TEST_LOCK 串行化，否则并行测试互相
        // 覆盖全局注册表，其他模块（如 converter.rs）断言内置 upstream_id 的测试会随机挂。
        let _registry_guard =
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("deprecate");
        let full = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-a", Some(200_000)), upstream("claude-b", Some(200_000))]),
        )]));
        ModelSyncService::new(store.clone(), full).sync_once(Some(3), now()).await.unwrap();

        let shrunk = || Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));

        // 注意（I1 修复导致的必要调整）：消失判定基线从「仅覆盖层」改为「内置 ∪ 覆盖层」
        // 有效行集后，本测试用的探针每轮都只返回 claude-a/claude-b，14 个内置模型也会在
        // 每个权威轮里一起被判定「未见」，从而按自己的节奏累计/跨过阈值——这正是 I1 要修的
        // 行为（见下面 authoritative_round_deprecates_missing_builtin_model 的专门断言）。
        // 因此这里不再断言跨内置模型的 `summary.deprecated` 总数，只断言这条测试真正关心
        // 的 claude-b 自身的 missing_sync_rounds / status 转换（“第一轮只累计，不标记”）。
        let _s1 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        let after_s1 = store.load();
        let b1 = after_s1.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b1.missing_sync_rounds, 1, "claude-b 第一轮只累计一次");
        assert_eq!(b1.status, ModelStatus::Active, "第一轮只累计，不标记");

        let _s2 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        let b = out.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b.status, ModelStatus::Deprecated);
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-b"), "永不删行");
    }

    /// 无效 maxInputTokens 回退 200000
    #[tokio::test]
    async fn invalid_max_input_tokens_falls_back() {
        let _registry_guard =
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let store = tmp_store("i1-builtin-deprecate");
        // 每一轮上游都只返回 1 个模型（且不是任何内置 upstream_id）。
        let fetcher = || Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-only-one", Some(200_000))]))]));

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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

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
            crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
}

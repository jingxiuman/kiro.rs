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

use super::model_registry::{MatchKind, ModelOrigin, ModelRow, ModelStatus};
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

impl ModelSyncService {
    pub fn new(store: Arc<ModelRegistryStore>, fetcher: Arc<dyn ModelListFetcher>) -> Self {
        Self { store, fetcher }
    }

    /// 跑一轮同步。`now` 由调用方注入，便于测试。
    pub async fn sync_once(
        &self,
        probe_credential_id: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<SyncSummary, String> {
        let fetch_started_at = now.to_rfc3339();

        // ---- 选凭据 ----
        let (round, credential_ids) = match probe_credential_id {
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
        let mut union: BTreeMap<String, UpstreamModel> = BTreeMap::new();
        let mut per_credential: HashMap<String, Vec<String>> = HashMap::new();
        let mut any_nonempty = false;

        for id in &credential_ids {
            match self.fetcher.fetch(*id).await {
                Ok(models) => {
                    if !models.is_empty() {
                        any_nonempty = true;
                    }
                    per_credential.insert(
                        id.to_string(),
                        models.iter().map(|m| m.model_id.clone()).collect(),
                    );
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
                }
            }
        }

        // ---- 可信度判定 ----
        if !any_nonempty {
            return Err(format!(
                "本轮同步不可信（{} 个凭据均失败或返回空列表），不改动 models.json",
                credential_ids.len()
            ));
        }

        let source = match round {
            RoundKind::Authoritative => format!("probe:{}", credential_ids[0]),
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
                // 乱序保护：已有更新的结果落盘则丢弃本轮
                if let Some(last) = file.sync_state.last_sync_at.as_deref() {
                    if last > fetch_started_at.as_str() {
                        return Err(format!(
                            "已有更新的同步结果（{}）晚于本轮起始时间（{}），丢弃本轮",
                            last, fetch_started_at
                        ));
                    }
                }

                let mut max_sort = file.models.iter().map(|r| r.sort_order).max().unwrap_or(0);

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

                    match file.models.iter_mut().find(|r| &r.upstream_id == upstream_id) {
                        Some(existing) => {
                            merge_synced_row(existing, &incoming);
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

                // 消失判定：仅权威轮次
                if round == RoundKind::Authoritative {
                    for row in file.models.iter_mut() {
                        if union.contains_key(&row.upstream_id) {
                            continue;
                        }
                        row.missing_sync_rounds += 1;
                        if row.missing_sync_rounds >= MISSING_ROUNDS_THRESHOLD
                            && row.status == ModelStatus::Active
                        {
                            row.status = ModelStatus::Deprecated;
                            deprecated += 1;
                            tracing::warn!(
                                "模型 {} 连续 {} 轮权威同步未出现于上游，标记为 deprecated（保留可用）",
                                row.upstream_id,
                                row.missing_sync_rounds
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
        let store = tmp_store("deprecate");
        let full = Arc::new(FakeFetcher::new(vec![(
            3,
            Ok(vec![upstream("claude-a", Some(200_000)), upstream("claude-b", Some(200_000))]),
        )]));
        ModelSyncService::new(store.clone(), full).sync_once(Some(3), now()).await.unwrap();

        let shrunk = || Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));

        let s1 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s1.deprecated, 0, "第一轮只累计，不标记");
        let s2 = ModelSyncService::new(store.clone(), shrunk()).sync_once(Some(3), now()).await.unwrap();
        assert_eq!(s2.deprecated, 1);

        let out = store.load();
        let b = out.registry.rows().iter().find(|r| r.upstream_id == "claude-b").unwrap();
        assert_eq!(b.status, ModelStatus::Deprecated);
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-b"), "永不删行");
    }

    /// 无效 maxInputTokens 回退 200000
    #[tokio::test]
    async fn invalid_max_input_tokens_falls_back() {
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
        let store = tmp_store("credsupport");
        let f = Arc::new(FakeFetcher::new(vec![(3, Ok(vec![upstream("claude-a", Some(200_000))]))]));
        ModelSyncService::new(store.clone(), f).sync_once(Some(3), now()).await.unwrap();

        let out = store.load();
        assert_eq!(out.file.credential_support.get("3").unwrap(), &vec!["claude-a".to_string()]);
    }
}

//! 余额快照的共享只读视图。
//!
//! 从 AdminService 抽出的动机：选路需要读余额，而 AdminService 持有
//! Arc<MultiTokenManager>，所有权是单向的，token_manager 侧看不到它。
//! 抽成独立 Arc 后由 main.rs 双向注入，参照既有的 admin::ops 共享模式。
//!
//! generation 的动机：后台刷新是串行逐账号进行的（账号间 sleep 400ms），
//! 逐条写入会让调度器读到同一轮的新旧混合值。整轮收齐后一次性 publish，
//! 并推进 generation，调度侧据此判断「本代次」边界并清空本地消耗累计。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::admin::types::BalanceResponse;

/// 缓存条目 TTL（秒）。超过即视为 stale，但仍可在 MAX_STALE 内被调度使用。
pub const BALANCE_CACHE_TTL_SECS: i64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBalance {
    pub cached_at: f64,
    pub data: BalanceResponse,
}

pub struct BalanceSnapshotView {
    pub generation: u64,
    pub entries: HashMap<u64, CachedBalance>,
}

struct Inner {
    generation: u64,
    entries: HashMap<u64, CachedBalance>,
}

pub struct BalanceCache {
    inner: Mutex<Inner>,
    path: Option<PathBuf>,
}

pub type SharedBalanceCache = Arc<BalanceCache>;

impl BalanceCache {
    pub fn new(path: Option<PathBuf>) -> Self {
        let entries = Self::load_from(&path);
        Self {
            inner: Mutex::new(Inner { generation: 0, entries }),
            path,
        }
    }

    pub fn snapshot(&self) -> BalanceSnapshotView {
        let inner = self.inner.lock();
        BalanceSnapshotView {
            generation: inner.generation,
            entries: inner.entries.clone(),
        }
    }

    pub fn publish(&self, entries: HashMap<u64, CachedBalance>) {
        // 锁内只做替换与 clone，序列化和写盘在锁外——否则磁盘延迟会传播到选路。
        let to_persist = {
            let mut inner = self.inner.lock();
            inner.generation = inner.generation.saturating_add(1);
            inner.entries = entries;
            inner.entries.clone()
        };
        self.persist(&to_persist);
    }

    pub fn upsert_one(&self, id: u64, entry: CachedBalance) {
        let to_persist = {
            let mut inner = self.inner.lock();
            inner.entries.insert(id, entry);
            inner.entries.clone()
        };
        self.persist(&to_persist);
    }

    /// 失效单条缓存（如超额状态变更后）。不推进 generation。
    pub fn remove(&self, id: u64) {
        let to_persist = {
            let mut inner = self.inner.lock();
            inner.entries.remove(&id);
            inner.entries.clone()
        };
        self.persist(&to_persist);
    }

    fn persist(&self, entries: &HashMap<u64, CachedBalance>) {
        let Some(path) = &self.path else { return };
        let keyed: HashMap<String, &CachedBalance> =
            entries.iter().map(|(k, v)| (k.to_string(), v)).collect();
        match serde_json::to_string_pretty(&keyed) {
            Ok(s) => {
                if let Err(e) = std::fs::write(path, s) {
                    tracing::warn!("余额缓存写盘失败: {}", e);
                }
            }
            Err(e) => tracing::warn!("余额缓存序列化失败: {}", e),
        }
    }

    /// 启动加载：丢弃超过 TTL 的条目（沿用既有语义）。
    fn load_from(path: &Option<PathBuf>) -> HashMap<u64, CachedBalance> {
        let Some(path) = path else { return HashMap::new() };
        let Ok(text) = std::fs::read_to_string(path) else { return HashMap::new() };
        let Ok(raw) = serde_json::from_str::<HashMap<String, CachedBalance>>(&text) else {
            return HashMap::new();
        };
        let now = chrono::Utc::now().timestamp() as f64;
        raw.into_iter()
            .filter(|(_, v)| (now - v.cached_at) < BALANCE_CACHE_TTL_SECS as f64)
            .filter_map(|(k, v)| k.parse::<u64>().ok().map(|id| (id, v)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cb(cached_at: f64, remaining: f64) -> CachedBalance {
        CachedBalance { cached_at, data: BalanceResponse { remaining, ..Default::default() } }
    }

    #[test]
    fn publish_bumps_generation_and_replaces_all() {
        let c = BalanceCache::new(None);
        assert_eq!(c.snapshot().generation, 0);

        c.publish(HashMap::from([(1, cb(100.0, 9000.0))]));
        let s1 = c.snapshot();
        assert_eq!(s1.generation, 1);
        assert_eq!(s1.entries.len(), 1);

        // 整轮发布是替换而非合并：上一轮的 id=1 不应残留
        c.publish(HashMap::from([(2, cb(200.0, 8000.0))]));
        let s2 = c.snapshot();
        assert_eq!(s2.generation, 2);
        assert!(s2.entries.contains_key(&2));
        assert!(!s2.entries.contains_key(&1), "整轮发布必须替换而非合并");
    }

    #[test]
    fn upsert_one_does_not_bump_generation() {
        let c = BalanceCache::new(None);
        c.publish(HashMap::from([(1, cb(100.0, 9000.0))]));
        let before = c.snapshot().generation;
        c.upsert_one(1, cb(150.0, 8500.0));
        let after = c.snapshot();
        assert_eq!(after.generation, before, "单点更新不得推进 generation");
        assert_eq!(after.entries[&1].data.remaining, 8500.0);
    }
}

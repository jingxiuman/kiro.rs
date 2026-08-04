//! 请求体全量保留：把 /v1/messages 的原始入站字节 gzip 落盘，供事后分析。
//!
//! 动机：trace 只有元数据与形态摘要，「未知字段膨胀」类问题（如 2026-07-31 的
//! 208KB thinking 签名）只有原始字节能复盘。存的是**线上原始字节**而非 serde
//! 解析后的规范化视图——后者恰好会丢掉下一个「签名式盲点」所在的未知字段。
//!
//! 布局：`<root>/YYYY-MM-DD/<trace_id>.json.gz`，按天分目录，保留期到期整目录删除。
//! 隐私边界：内容含用户源码与对话，仅落本机盘，保留期跟随 trace（默认 7 天）。

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::Utc;

/// 单个请求体的体积上限（gzip 前）。超过则不存——防御性上限，
/// 正常 CC 请求远小于此；真有更大的先弄清楚是什么再说。
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

pub struct RequestBodyStore {
    root: PathBuf,
    enabled: AtomicBool,
    retention_days: AtomicU64,
}

impl RequestBodyStore {
    pub fn new(root: PathBuf, enabled: bool, retention_days: u64) -> Self {
        Self {
            root,
            enabled: AtomicBool::new(enabled),
            retention_days: AtomicU64::new(retention_days.max(1)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// gzip 压缩后落盘。失败只告警不影响主流程（观测组件不得反噬业务）。
    pub fn save(&self, trace_id: &str, body: &[u8]) {
        if !self.is_enabled() || body.is_empty() || body.len() > MAX_BODY_BYTES {
            return;
        }
        if let Err(e) = self.save_inner(trace_id, body) {
            tracing::warn!("请求体落盘失败 trace_id={}: {}", trace_id, e);
        }
    }

    fn day_dir_of(&self, day: &str) -> PathBuf {
        self.root.join(day)
    }

    fn save_inner(&self, trace_id: &str, body: &[u8]) -> std::io::Result<()> {
        let day = Utc::now().format("%Y-%m-%d").to_string();
        let dir = self.day_dir_of(&day);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.json.gz", trace_id));
        let file = std::fs::File::create(path)?;
        let mut enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        enc.write_all(body)?;
        enc.finish()?;
        Ok(())
    }

    /// 按 trace_id 读回解压后的原始字节。逆序扫保留期内的天目录（近期优先）。
    pub fn load(&self, trace_id: &str) -> Option<Vec<u8>> {
        // trace_id 是本服务生成的 uuid；防御路径穿越（admin 入参不可信）
        if trace_id.contains(['/', '\\', '.']) {
            return None;
        }
        let mut days: Vec<PathBuf> = std::fs::read_dir(&self.root)
            .ok()?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        days.sort();
        for dir in days.iter().rev() {
            let path = dir.join(format!("{}.json.gz", trace_id));
            let Ok(file) = std::fs::File::open(&path) else {
                continue;
            };
            let mut out = Vec::new();
            if flate2::read::GzDecoder::new(file).read_to_end(&mut out).is_ok() {
                return Some(out);
            }
        }
        None
    }

    /// 删除超过保留期的天目录。目录名非日期格式的一律不动（不猜不删）。
    pub fn cleanup(&self) {
        let cutoff = (Utc::now()
            - chrono::Duration::days(self.retention_days.load(Ordering::Relaxed) as i64))
        .format("%Y-%m-%d")
        .to_string();
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name().to_string_lossy().to_string();
            let is_day = name.len() == 10
                && chrono::NaiveDate::parse_from_str(&name, "%Y-%m-%d").is_ok();
            if is_day
                && name.as_str() < cutoff.as_str()
                && entry.path().is_dir()
                && let Err(e) = std::fs::remove_dir_all(entry.path())
            {
                tracing::warn!("请求体过期目录删除失败 {}: {}", name, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store(enabled: bool) -> (RequestBodyStore, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "kiro-rbs-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        (RequestBodyStore::new(root.clone(), enabled, 7), root)
    }

    #[test]
    fn save_and_load_roundtrip_gzip() {
        let (store, root) = temp_store(true);
        let body = br#"{"model":"claude-opus-5","messages":[{"role":"user","content":"hi"}]}"#;
        store.save("t-abc", body);
        assert_eq!(store.load("t-abc").as_deref(), Some(body.as_slice()));
        // 盘上是压缩文件而非明文
        let day = Utc::now().format("%Y-%m-%d").to_string();
        let raw = std::fs::read(root.join(day).join("t-abc.json.gz")).unwrap();
        assert!(raw.starts_with(&[0x1f, 0x8b]), "应为 gzip magic");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn disabled_store_writes_nothing() {
        let (store, root) = temp_store(false);
        store.save("t-off", b"{}");
        assert!(store.load("t-off").is_none());
        assert!(!root.exists(), "关闭时不应创建任何目录");
    }

    #[test]
    fn cleanup_removes_only_expired_day_dirs() {
        let (store, root) = temp_store(true);
        // 过期目录（8 天前）、未过期目录（今天）、非日期目录
        let old = (Utc::now() - chrono::Duration::days(8))
            .format("%Y-%m-%d")
            .to_string();
        let today = Utc::now().format("%Y-%m-%d").to_string();
        for d in [&old, &today, &"not-a-date".to_string()] {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        store.cleanup();
        assert!(!root.join(&old).exists(), "过期日目录应删除");
        assert!(root.join(&today).exists(), "未过期目录应保留");
        assert!(root.join("not-a-date").exists(), "非日期目录不猜不删");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn load_rejects_path_traversal() {
        let (store, root) = temp_store(true);
        store.save("t-safe", b"{}");
        assert!(store.load("../t-safe").is_none());
        assert!(store.load("a/b").is_none());
        std::fs::remove_dir_all(&root).ok();
    }
}

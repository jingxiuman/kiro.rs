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
        self.save_ext(trace_id, "json", body);
    }

    /// 同 [`Self::save`]，但自定义扩展名段：落到
    /// `<root>/YYYY-MM-DD/<trace_id>.<ext>.gz`。上游侧每跳一份请求/响应用它区分
    /// （`upstream-req-0.json` / `upstream-resp-0.bin`）。
    pub fn save_ext(&self, trace_id: &str, ext: &str, body: &[u8]) {
        if !self.is_enabled() || body.is_empty() || body.len() > MAX_BODY_BYTES {
            return;
        }
        if let Err(e) = self.save_inner(trace_id, ext, body) {
            tracing::warn!("请求体落盘失败 trace_id={}.{}: {}", trace_id, ext, e);
        }
    }

    fn day_dir_of(&self, day: &str) -> PathBuf {
        self.root.join(day)
    }

    fn save_inner(&self, trace_id: &str, ext: &str, body: &[u8]) -> std::io::Result<()> {
        let day = Utc::now().format("%Y-%m-%d").to_string();
        let dir = self.day_dir_of(&day);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.{}.gz", trace_id, ext));
        let file = std::fs::File::create(path)?;
        let mut enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        enc.write_all(body)?;
        enc.finish()?;
        Ok(())
    }

    /// 按 trace_id 读回解压后的原始字节。逆序扫保留期内的天目录（近期优先）。
    pub fn load(&self, trace_id: &str) -> Option<Vec<u8>> {
        self.load_ext(trace_id, "json")
    }

    /// 同 [`Self::load`]，但自定义扩展名段。`ext` 同样来自 admin 入参（attempt
    /// 拼串），一并做路径穿越防御。
    pub fn load_ext(&self, trace_id: &str, ext: &str) -> Option<Vec<u8>> {
        // trace_id 是本服务生成的 uuid；防御路径穿越（admin 入参不可信）
        if trace_id.contains(['/', '\\', '.']) {
            return None;
        }
        // ext 允许带 `.`（如 upstream-req-0.json），但不许有分隔符与上跳
        if ext.contains(['/', '\\']) || ext.contains("..") {
            return None;
        }
        let mut days: Vec<PathBuf> = std::fs::read_dir(&self.root)
            .ok()?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        days.sort();
        for dir in days.iter().rev() {
            let path = dir.join(format!("{}.{}.gz", trace_id, ext));
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

/// 把一次落盘甩到阻塞线程池；不在 tokio 运行时里（单测）则同步落盘。
///
/// 观测组件不得反噬业务：gzip + fs 不占请求路径，失败只在 store 内部告警。
pub fn save_ext_detached(
    store: std::sync::Arc<RequestBodyStore>,
    trace_id: String,
    ext: String,
    body: Vec<u8>,
) {
    match tokio::runtime::Handle::try_current() {
        Ok(h) => {
            h.spawn_blocking(move || store.save_ext(&trace_id, &ext, &body));
        }
        Err(_) => store.save_ext(&trace_id, &ext, &body),
    }
}

/// 上游原始响应字节的累积器：流式逐 chunk `push`，结束时 `finish` 落盘。
///
/// 为什么要 `Drop` 兜底：客户端提前断开 / 流中断时 unfold 的正常收尾分支根本
/// 不会执行（与 `StreamPhaseGuard` 同一个成因），而「只收到一半」的上游字节
/// 恰恰是事故复盘最需要的那份证据，不能因为没走到 finish 就丢掉。
pub struct UpstreamResponseRecorder {
    store: std::sync::Arc<RequestBodyStore>,
    trace_id: String,
    ext: String,
    buf: Vec<u8>,
    truncated: bool,
    saved: bool,
}

impl UpstreamResponseRecorder {
    pub fn new(store: std::sync::Arc<RequestBodyStore>, trace_id: String, ext: String) -> Self {
        Self {
            store,
            trace_id,
            ext,
            buf: Vec::new(),
            truncated: false,
            saved: false,
        }
    }

    /// 追加一段上游字节。累计超过 [`MAX_BODY_BYTES`] 后停止追加并标记截断——
    /// 保住已捕获的前缀，而不是整份丢弃（截断的证据也是证据）。
    pub fn push(&mut self, chunk: &[u8]) {
        if self.truncated {
            return;
        }
        if self.buf.len() + chunk.len() > MAX_BODY_BYTES {
            self.truncated = true;
            tracing::warn!(
                "上游响应超出保留上限，截断保存 trace_id={}.{} 已存 {} 字节",
                self.trace_id,
                self.ext,
                self.buf.len()
            );
            return;
        }
        self.buf.extend_from_slice(chunk);
    }

    /// 显式收尾落盘。
    pub fn finish(mut self) {
        self.flush();
    }

    fn flush(&mut self) {
        if self.saved || self.buf.is_empty() {
            return;
        }
        self.saved = true;
        save_ext_detached(
            self.store.clone(),
            self.trace_id.clone(),
            self.ext.clone(),
            std::mem::take(&mut self.buf),
        );
    }
}

impl Drop for UpstreamResponseRecorder {
    fn drop(&mut self) {
        self.flush();
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

    #[test]
    fn save_ext_and_load_ext_roundtrip() {
        let (store, root) = temp_store(true);
        store.save_ext("t-ext", "upstream-req-1.json", b"{\"a\":1}");
        assert_eq!(
            store.load_ext("t-ext", "upstream-req-1.json").as_deref(),
            Some(b"{\"a\":1}".as_slice())
        );
        let day = Utc::now().format("%Y-%m-%d").to_string();
        assert!(root.join(&day).join("t-ext.upstream-req-1.json.gz").exists());
        // 旧行为不变：save 写的仍是 <id>.json.gz，load 仍能找到
        store.save("t-plain", b"{}");
        assert!(root.join(&day).join("t-plain.json.gz").exists());
        assert_eq!(store.load("t-plain").as_deref(), Some(b"{}".as_slice()));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn load_ext_rejects_bad_ext() {
        let (store, root) = temp_store(true);
        store.save_ext("t-guard", "bin", b"x");
        assert!(store.load_ext("t-guard", "../bin").is_none());
        assert!(store.load_ext("t-guard", "a/b").is_none());
        assert!(store.load_ext("t-guard", "a\\b").is_none());
        assert_eq!(store.load_ext("t-guard", "bin").as_deref(), Some(b"x".as_slice()));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn recorder_truncates_at_cap_and_saves_prefix() {
        let (store, root) = temp_store(true);
        let store = std::sync::Arc::new(store);
        let mut rec = UpstreamResponseRecorder::new(
            store.clone(),
            "t-cap".to_string(),
            "upstream-resp-0.bin".to_string(),
        );
        let head = vec![b'a'; 1024];
        rec.push(&head);
        rec.push(&vec![b'b'; MAX_BODY_BYTES]); // 超限：整块丢弃并标记截断
        rec.push(b"c"); // 截断后不再追加
        rec.finish();
        let got = store.load_ext("t-cap", "upstream-resp-0.bin").unwrap();
        assert_eq!(got, head, "只应保留超限前的前缀");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn recorder_drop_without_finish_still_saves() {
        let (store, root) = temp_store(true);
        let store = std::sync::Arc::new(store);
        {
            let mut rec = UpstreamResponseRecorder::new(
                store.clone(),
                "t-drop".to_string(),
                "upstream-resp-0.bin".to_string(),
            );
            rec.push(b"half-stream");
        }
        assert_eq!(
            store.load_ext("t-drop", "upstream-resp-0.bin").as_deref(),
            Some(b"half-stream".as_slice())
        );
        std::fs::remove_dir_all(&root).ok();
    }
}

//! `models.json` 的持久化层：加载校验、pinned 逐字段合并、原子落盘。
//!
//! 所有写路径（定时同步、手动同步、UI 逐字段编辑）都经本模块的 Mutex 串行化，
//! 每次写都是 read-modify-write。这正是把模型表从 config.json 中独立出来的
//! 原因——后者只能依赖「读最新再写」的约定（src/admin/service.rs:1236），没有锁。

use std::path::PathBuf;

use tokio::sync::Mutex;

use super::model_registry::{
    ModelRegistry, ModelRegistryFile, ModelRow, ModelStatus, REGISTRY_SCHEMA_VERSION,
};

/// 加载结果。`degraded_reason` 非 None 表示覆盖层不可用、已退回内置默认。
pub struct LoadOutcome {
    pub registry: ModelRegistry,
    pub file: ModelRegistryFile,
    pub degraded_reason: Option<String>,
}

pub struct ModelRegistryStore {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl ModelRegistryStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path, write_lock: Mutex::new(()) }
    }

    #[cfg(test)]
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// 加载覆盖层。任何失败都退回内置默认，绝不 panic、绝不空表。
    pub fn load(&self) -> LoadOutcome {
        let builtin = || LoadOutcome {
            registry: ModelRegistry::builtin(),
            file: ModelRegistryFile::default(),
            degraded_reason: None,
        };

        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // 文件不存在是正常状态（零行为回归路径），不是降级
                return builtin();
            }
            Err(e) => {
                let reason = format!("读取 models.json 失败: {}", e);
                tracing::error!("{}，退回内置默认模型表", reason);
                let mut out = builtin();
                out.degraded_reason = Some(reason);
                return out;
            }
        };

        let file: ModelRegistryFile = match serde_json::from_str(&raw) {
            Ok(f) => f,
            Err(e) => {
                let reason = format!("解析 models.json 失败: {}", e);
                tracing::error!("{}，退回内置默认模型表", reason);
                let mut out = builtin();
                out.degraded_reason = Some(reason);
                return out;
            }
        };

        let file_for_return = file.clone();
        match ModelRegistry::from_file(file) {
            Ok(registry) => LoadOutcome { registry, file: file_for_return, degraded_reason: None },
            Err(reason) => {
                tracing::error!("models.json 校验失败: {}，退回内置默认模型表", reason);
                let mut out = builtin();
                out.degraded_reason = Some(reason);
                out
            }
        }
    }

    /// read-modify-write。闭包中修改文件内容，返回后立即校验并原子落盘。
    /// 校验失败则不写盘，返回 Err。
    pub async fn mutate<F>(&self, f: F) -> Result<ModelRegistryFile, String>
    where
        F: FnOnce(&mut ModelRegistryFile) -> Result<(), String>,
    {
        let _guard = self.write_lock.lock().await;

        let mut file = match std::fs::read_to_string(&self.path) {
            Ok(raw) => serde_json::from_str::<ModelRegistryFile>(&raw)
                .map_err(|e| format!("解析 models.json 失败: {}", e))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ModelRegistryFile::default(),
            Err(e) => return Err(format!("读取 models.json 失败: {}", e)),
        };
        file.version = REGISTRY_SCHEMA_VERSION;

        f(&mut file)?;

        // 落盘前必须先过校验，避免写出一个自己都加载不了的文件
        ModelRegistry::from_file(file.clone())?;

        let json = serde_json::to_string_pretty(&file)
            .map_err(|e| format!("序列化 models.json 失败: {}", e))?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("创建 models.json 所在目录失败: {}", e))?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes())
            .map_err(|e| format!("写入临时文件失败: {}", e))?;
        std::fs::rename(&tmp, &self.path)
            .map_err(|e| format!("原子替换 models.json 失败: {}", e))?;

        Ok(file)
    }
}

/// 把同步得到的 `incoming` 合并进已有行 `existing`，**逐字段跳过 pinned**。
///
/// 字段清单与拷贝逻辑都来自 `model_registry::SYNC_MANAGED_FIELDS` /
/// `copy_sync_managed_field`：写侧（这里）与读侧（`overlay_onto_builtin`）同源，
/// 不再各抄一份 —— 两份清单漂移会造成「同步跑了但值没变」这种极难排查的症状。
/// pinned 判定也因此收敛成循环里的一处（原先每个字段各判一次，语义完全相同：
/// 5 个字段互相独立，跳过与否只取决于自己的名字在不在 pinned 里）。
pub fn merge_synced_row(existing: &mut ModelRow, incoming: &ModelRow) {
    for field in super::model_registry::SYNC_MANAGED_FIELDS {
        if existing.pinned.iter().any(|p| p == field) {
            continue;
        }
        super::model_registry::copy_sync_managed_field(existing, incoming, field);
    }

    // 同步元数据不受 pinned 影响
    existing.missing_sync_rounds = 0;
    existing.last_seen_at = incoming.last_seen_at.clone();
    if existing.status == ModelStatus::Deprecated {
        // 上游重新出现 → 复活
        existing.status = ModelStatus::Active;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::model_registry::{builtin_rows, ModelOrigin};

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("kiro-models-test-{}-{}.json", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn row(upstream: &str) -> ModelRow {
        let mut r = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .unwrap();
        r.upstream_id = upstream.to_string();
        r.exposed_id = upstream.replace('.', "-");
        r.origin = ModelOrigin::Synced;
        r
    }

    #[test]
    fn load_missing_file_returns_builtin_without_degraded() {
        let store = ModelRegistryStore::new(tmp_path("missing"));
        let out = store.load();
        assert!(out.degraded_reason.is_none(), "文件不存在不是降级状态");
        assert_eq!(out.registry.rows().len(), builtin_rows().len());
    }

    #[test]
    fn load_corrupt_file_returns_builtin_with_degraded() {
        let path = tmp_path("corrupt");
        std::fs::write(&path, b"{ not json").unwrap();
        let out = ModelRegistryStore::new(path).load();
        assert!(out.degraded_reason.is_some(), "损坏文件必须置 degraded");
        assert_eq!(out.registry.rows().len(), builtin_rows().len());
    }

    #[test]
    fn load_invalid_schema_returns_builtin_with_degraded() {
        let path = tmp_path("badversion");
        std::fs::write(&path, br#"{"version":2,"models":[]}"#).unwrap();
        let out = ModelRegistryStore::new(path).load();
        assert!(out.degraded_reason.unwrap().contains("版本"));
    }

    #[tokio::test]
    async fn mutate_writes_atomically_and_reloads() {
        let path = tmp_path("write");
        let store = ModelRegistryStore::new(path.clone());
        store
            .mutate(|f| {
                f.models.push(row("claude-opus-5"));
                Ok(())
            })
            .await
            .unwrap();
        assert!(path.exists());
        let out = store.load();
        assert!(out.degraded_reason.is_none());
        assert!(out.registry.rows().iter().any(|r| r.upstream_id == "claude-opus-5"));
        // 不应留下临时文件
        let tmp = path.with_extension("json.tmp");
        assert!(!tmp.exists(), "临时文件未清理");
    }

    #[tokio::test]
    async fn mutate_rejects_invalid_result_and_keeps_old_file() {
        let path = tmp_path("reject");
        let store = ModelRegistryStore::new(path.clone());
        store.mutate(|f| { f.models.push(row("claude-a")); Ok(()) }).await.unwrap();
        let before = std::fs::read(&path).unwrap();

        // 写入重复 upstreamId → 校验失败 → 文件不变
        let err = store
            .mutate(|f| { f.models.push(row("claude-a")); Ok(()) })
            .await
            .unwrap_err();
        assert!(err.contains("重复"));
        assert_eq!(std::fs::read(&path).unwrap(), before, "校验失败时文件不应被改写");
    }

    /// pinned 字段逐字段跳过；未 pinned 字段正常更新
    #[test]
    fn merge_respects_pinned_fields() {
        let mut existing = row("claude-opus-4.8");
        existing.context_window = 800_000;
        existing.display_name = "旧名字".to_string();
        existing.pinned = vec!["contextWindow".to_string()];

        let mut incoming = row("claude-opus-4.8");
        incoming.context_window = 1_000_000;
        incoming.display_name = "Claude Opus 4.8".to_string();

        merge_synced_row(&mut existing, &incoming);

        assert_eq!(existing.context_window, 800_000, "pinned 字段被覆盖了");
        assert_eq!(existing.display_name, "Claude Opus 4.8", "未 pinned 字段应更新");
    }

    /// pinned 判定收敛成一处之后，**逐字段**语义必须原样保持：
    /// 对每一个同步管辖字段分别验证「未 pin → 被上游值覆盖」「pin 了 → 保持不变」。
    /// 原先每个字段各写一次 `if !pinned(...)`，这条测试替代那份重复代码的表达力。
    #[test]
    fn merge_is_pinned_aware_per_field() {
        use crate::anthropic::model_registry::SYNC_MANAGED_FIELDS;

        let base = row("claude-opus-4.8");
        let mut incoming = base.clone();
        incoming.display_name = "上游名字".to_string();
        incoming.context_window = 123_456;
        incoming.max_output_tokens = 7_890;
        incoming.exposed_id = "claude-opus-4-8-upstream".to_string();
        incoming.expose_thinking_variant = !base.expose_thinking_variant;

        let read = |row: &ModelRow, field: &str| -> String {
            let v = serde_json::to_value(row).unwrap();
            v.get(field).unwrap().to_string()
        };

        for field in SYNC_MANAGED_FIELDS {
            // 未 pin → 跟随上游
            let mut existing = base.clone();
            merge_synced_row(&mut existing, &incoming);
            assert_eq!(
                read(&existing, field),
                read(&incoming, field),
                "{} 未被 pin，应跟随上游值",
                field
            );

            // pin 了 → 保持原值
            let mut existing = base.clone();
            existing.pinned = vec![field.to_string()];
            merge_synced_row(&mut existing, &incoming);
            assert_eq!(
                read(&existing, field),
                read(&base, field),
                "{} 已被 pin，同步不得覆盖它",
                field
            );
        }
    }

    /// deprecated 行在上游重新出现时复活
    #[test]
    fn merge_revives_deprecated_row() {
        use crate::anthropic::model_registry::ModelStatus;
        let mut existing = row("claude-opus-4.8");
        existing.status = ModelStatus::Deprecated;
        existing.missing_sync_rounds = 2;
        let incoming = row("claude-opus-4.8");

        merge_synced_row(&mut existing, &incoming);

        assert_eq!(existing.status, ModelStatus::Active);
        assert_eq!(existing.missing_sync_rounds, 0);
    }
}

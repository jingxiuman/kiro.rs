//! Admin API 模块
//!
//! 提供凭据管理和监控功能的 HTTP API
//!
//! # 功能
//! - 查询所有凭据状态
//! - 启用/禁用凭据
//! - 修改凭据优先级
//! - 重置失败计数
//! - 查询凭据余额
//!
//! # 使用
//! ```ignore
//! let admin_service = AdminService::new(token_manager.clone(), endpoint_names);
//! let admin_state = AdminState::new(admin_api_key, admin_service);
//! let admin_router = create_admin_router(admin_state);
//! ```

mod error;
mod handlers;
mod middleware;
pub mod ops;
pub mod proxy_pool;
mod router;
mod service;
pub mod types;
mod binary_update;
pub mod client_keys;
pub mod groups;
pub mod usage_stats;
pub mod trace_db;

pub use client_keys::ClientKeyManager;
pub use groups::GroupManager;
pub use middleware::AdminState;
pub use router::create_admin_router;
pub use service::AdminService;
// 模型同步的运行时配置持有者由 main.rs 创建并与 AdminService 共享 ——
// 调度器必须活在 admin 分支之外，但改开关的入口在 admin 里，两边得看同一份。
pub use service::ModelSyncSettings;
pub(crate) use service::parse_auto_apply_time;
pub use ops::{OpsRuntime, OpsStore, SharedOpsRuntime, SharedOpsStore};
pub use usage_stats::{UsageAggregator, UsageRecorder};
pub use trace_db::{SharedTraceStore, TraceStore};

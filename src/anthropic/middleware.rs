//! Anthropic API 中间件

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};

use crate::admin::client_keys::SharedClientKeyManager;
use crate::admin::trace_db::{SharedTraceStore, TraceKeySource};
use crate::admin::usage_store::SharedUsageStore;
use crate::common::auth;
use crate::kiro::provider::KiroProvider;

use super::cache_metering::SharedCacheMeter;
use super::types::ErrorResponse;

/// 命中的鉴权上下文（注入到请求扩展，供 handler 记录用量）
#[derive(Clone, Debug)]
pub struct KeyContext {
    /// 命中的客户端 Key id
    pub key_id: u64,
    /// 该 Key 绑定的账号分组；None 表示未绑定，可使用全部账号
    pub group: Option<String>,
    /// 命中的入口 Key 类型。
    pub key_source: TraceKeySource,
}

/// 应用共享状态
#[derive(Clone)]
pub struct AppState {
    /// Kiro Provider（可选，用于实际 API 调用）
    /// 内部使用 MultiTokenManager，已支持线程安全的多凭据管理
    pub kiro_provider: Option<Arc<KiroProvider>>,
    /// 是否开启非流式响应的 thinking 块提取
    pub extract_thinking: bool,
    /// 工具兼容模式（ClaudeCode 内置工具名/入参双向适配 / Raw 透传）
    pub tool_compatibility_mode: crate::model::config::ToolCompatibilityMode,
    /// 客户端 Key 管理器（可选，未启用 Admin 时为 None）
    pub client_keys: Option<SharedClientKeyManager>,
    /// 用量存储（DuckDB：写入 + 统计查询）
    pub usage_store: Option<SharedUsageStore>,
    /// 中转层缓存计量（基于 cache_control 断点的内存缓存）
    pub cache_meter: Option<SharedCacheMeter>,
    /// 请求链路追踪存储（SQLite，可选）
    pub trace_store: Option<SharedTraceStore>,
    /// 请求体全量保留存储（可选，storeRequestBodies=true 时启用）
    pub request_body_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    /// omitted 思考正文存储（恢复键 kiro-thinking-v1 的后端，常开）
    pub thinking_text_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
}

/// 由 [`capture_raw_body`] 塞进 request extensions 的原始请求体字节。
/// Bytes 引用计数克隆，无拷贝开销。
#[derive(Clone)]
pub struct RawRequestBody(pub bytes::Bytes);

/// 缓冲请求体并把原始字节存入 extensions，供 handler 落盘请求体。
/// 仅在 request_body_store 启用时挂载本层；Json 解析语义不受影响。
pub async fn capture_raw_body(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let (parts, body) = req.into_parts();
    match axum::body::to_bytes(body, 50 * 1024 * 1024).await {
        Ok(bytes) => {
            let mut req = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes.clone()));
            req.extensions_mut().insert(RawRequestBody(bytes));
            next.run(req).await
        }
        Err(e) => {
            // 体积超限等：交回一个空体请求让下游 Json 提取器报它自己的错，
            // 观测层不改变错误语义。
            tracing::warn!("请求体缓冲失败: {}", e);
            let req = axum::extract::Request::from_parts(parts, axum::body::Body::empty());
            next.run(req).await
        }
    }
}

impl AppState {
    /// 创建新的应用状态（不含 client_keys 的基础构造，供嵌入 / 测试使用）
    #[allow(dead_code)]
    pub fn new(
        extract_thinking: bool,
        tool_compatibility_mode: crate::model::config::ToolCompatibilityMode,
    ) -> Self {
        Self {
            kiro_provider: None,
            extract_thinking,
            tool_compatibility_mode,
            client_keys: None,
            usage_store: None,
            cache_meter: None,
            trace_store: None,
            request_body_store: None,
            thinking_text_store: None,
        }
    }

    /// 设置 KiroProvider
    ///
    /// 收 `Arc` 而不是按值 move：Admin 的 `POST /models/test` 要与 `/v1/messages`
    /// 共用同一个 provider 实例（同一份账号池 / 代理 / client 缓存），否则测出来的
    /// 不是生产链路实际会发生的事。
    pub fn with_kiro_provider(mut self, provider: Arc<KiroProvider>) -> Self {
        self.kiro_provider = Some(provider);
        self
    }

    /// 注入用量记录组件
    pub fn with_usage(
        mut self,
        client_keys: Option<SharedClientKeyManager>,
        usage_store: Option<SharedUsageStore>,
    ) -> Self {
        self.client_keys = client_keys;
        self.usage_store = usage_store;
        self
    }

    /// 注入缓存计量器
    pub fn with_cache_meter(mut self, cache: Option<SharedCacheMeter>) -> Self {
        self.cache_meter = cache;
        self
    }

    /// 注入链路追踪存储
    pub fn with_trace_store(mut self, store: Option<SharedTraceStore>) -> Self {
        self.trace_store = store;
        self
    }
}

/// API Key 认证中间件
///
/// 所有入口 Key 统一按已存储的完整值精确匹配，不限制前缀。命中后向请求扩展注入
/// [`KeyContext`]，供 handler 记录用量时使用。
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let presented = match auth::extract_api_key(&request) {
        Some(k) => k,
        None => {
            let error = ErrorResponse::authentication_error();
            return (StatusCode::UNAUTHORIZED, Json(error)).into_response();
        }
    };

    if let Some(mgr) = &state.client_keys
        && let Some(id) = mgr.verify_and_touch(&presented) {
            let group = mgr.group_of(id);
            request.extensions_mut().insert(KeyContext {
                key_id: id,
                group,
                key_source: TraceKeySource::ClientKey,
            });
            return next.run(request).await;
        }

    let error = ErrorResponse::authentication_error();
    (StatusCode::UNAUTHORIZED, Json(error)).into_response()
}

/// CORS 中间件层
///
/// **安全说明**：当前配置允许所有来源（Any），这是为了支持公开 API 服务。
/// 如果需要更严格的安全控制，请根据实际需求配置具体的允许来源、方法和头信息。
///
/// # 配置说明
/// - `allow_origin(Any)`: 允许任何来源的请求
/// - `allow_methods(Any)`: 允许任何 HTTP 方法
/// - `allow_headers(Any)`: 允许任何请求头
pub fn cors_layer() -> tower_http::cors::CorsLayer {
    use tower_http::cors::{Any, CorsLayer};

    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
}
